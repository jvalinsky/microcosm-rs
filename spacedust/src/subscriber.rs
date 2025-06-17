use crate::ClientEvent;
use crate::LinkEvent;
use crate::server::MultiSubscribeQuery;
use futures::SinkExt;
use std::error::Error;
use tokio::sync::broadcast;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use dropshot::WebsocketConnectionRaw;

pub async fn subscribe(
    mut sub: broadcast::Receiver<LinkEvent>,
    mut ws: WebSocketStream<WebsocketConnectionRaw>,
    query: MultiSubscribeQuery,
) -> Result<(), Box<dyn Error>> {
    // TODO: pingpong

    loop {
        match sub.recv().await {
            Ok(link) => {

                // subject + subject DIDs are logical OR
                let target_did = if link.target.starts_with("did:") {
                    link.target.clone()
                } else {
                    let Some(rest) = link.target.strip_prefix("at://") else {
                        continue;
                    };
                    if let Some((did, _)) = rest.split_once("/") {
                        did
                    } else {
                        rest
                    }.to_string()
                };
                if !(query.wanted_subjects.contains(&link.target) || query.wanted_subject_dids.contains(&target_did) || query.wanted_subjects.is_empty() && query.wanted_subject_dids.is_empty()) {
                    // wowwww ^^ fix that
                    continue;
                }

                // subjects together with sources are logical AND

                if !query.wanted_sources.is_empty() {
                    let undotted = link.path.strip_prefix('.').unwrap_or_else(|| {
                        eprintln!("link path did not have expected '.' prefix: {}", link.path);
                        ""
                    });
                    let source = format!("{}:{undotted}", link.collection);
                    if !query.wanted_sources.contains(&source) {
                        continue;
                    }
                }

                let ev = ClientEvent {
                    kind: "link".to_string(),
                    link: link.into(),
                };
                let json = serde_json::to_string(&ev)?;
                if let Err(e) = ws.send(Message::Text(json.into())).await {
                    eprintln!("client: failed to send event: {e:?}");
                    ws.close(None).await?; // TODO: do we need this one??
                    break;
                }
            }
            Err(broadcast::error::RecvError::Closed) => {
                ws.close(None).await?; // TODO: send reason
                break;
            }
            Err(broadcast::error::RecvError::Lagged(_n_missed)) => {
                eprintln!("client lagged, closing");
                ws.close(None).await?; // TODO: send reason
                break;
            }
        }
    }
    Ok(())
}
