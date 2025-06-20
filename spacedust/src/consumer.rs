use tokio_util::sync::CancellationToken;
use crate::LinkEvent;
use crate::error::ConsumerError;
use jetstream::{
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
    events::{CommitOp, Cursor, EventKind},
};
use links::collect_links;
use tokio::sync::broadcast;

const MAX_LINKS_PER_EVENT: usize = 100;

pub async fn consume(
    b: broadcast::Sender<LinkEvent>,
    jetstream_endpoint: String,
    cursor: Option<Cursor>,
    no_zstd: bool,
    shutdown: CancellationToken,
) -> Result<(), ConsumerError> {
    let endpoint = DefaultJetstreamEndpoints::endpoint_or_shortcut(&jetstream_endpoint);
    if endpoint == jetstream_endpoint {
        log::info!("connecting to jetstream at {endpoint}");
    } else {
        log::info!("connecting to jetstream at {jetstream_endpoint} => {endpoint}");
    }
    let config: JetstreamConfig = JetstreamConfig {
        endpoint,
        compression: if no_zstd {
            JetstreamCompression::None
        } else {
            JetstreamCompression::Zstd
        },
        replay_on_reconnect: true,
        channel_size: 1024, // buffer up to ~1s of jetstream events
        ..Default::default()
    };
    let mut receiver = JetstreamConnector::new(config)?
        .connect_cursor(cursor)
        .await?;

    log::info!("receiving jetstream messages..");
    loop {
        if shutdown.is_cancelled() {
            log::info!("exiting consumer for shutdown");
            return Ok(());
        }
        let Some(event) = receiver.recv().await else {
            log::error!("could not receive jetstream event, bailing");
            break;
        };

        if event.kind != EventKind::Commit {
            continue;
        }
        let Some(commit) = event.commit else {
            log::warn!("jetstream commit event missing commit data, ignoring");
            continue;
        };

        // TODO: keep a buffer and remove quick deletes to debounce notifs
        // for now we just drop all deletes eek
        if commit.operation == CommitOp::Delete {
            continue;
        }
        let Some(record) = commit.record else {
            log::warn!("jetstream commit update/delete missing record, ignoring");
            continue;
        };

        let jv = match record.get().parse() {
            Ok(v) => v,
            Err(e) => {
                log::warn!("jetstream record failed to parse, ignoring: {e}");
                continue;
            }
        };

        // todo: indicate if the link limit was reached (-> links omitted)
        for (i, link) in collect_links(&jv).into_iter().enumerate() {
            if i >= MAX_LINKS_PER_EVENT {
                log::warn!("jetstream event has too many links, ignoring the rest");
                break;
            }
            let link_ev = LinkEvent {
                collection: commit.collection.to_string(),
                path: link.path,
                origin: format!(
                    "at://{}/{}/{}",
                    &*event.did,
                    &*commit.collection,
                    &*commit.rkey,
                ),
                rev: commit.rev.to_string(),
                target: link.target.into_string(),
            };
            let _ = b.send(link_ev); // only errors if no subscribers are connected, which is just fine.
        }
    }

    Err(ConsumerError::JetstreamEnded)
}
