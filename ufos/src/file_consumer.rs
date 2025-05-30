use crate::consumer::{Batcher, LimitedBatch, BATCH_QUEUE_SIZE};
use crate::store_types::SketchSecretPrefix;
use crate::Cursor;
use anyhow::Result;
use jetstream::{error::JetstreamEventError, events::JetstreamEvent};
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    sync::mpsc::{channel, Receiver, Sender},
};

async fn read_jsonl(f: File, sender: Sender<JetstreamEvent>, cursor: Option<Cursor>) -> Result<()> {
    let mut lines = BufReader::new(f).lines();
    if let Some(db_cursor) = cursor {
        log::info!("jsonl fixture: skipping events before cursor {db_cursor:?}");
        let mut bad_lines = 0;
        let mut skipped = 0;
        while let Some(line) = lines.next_line().await? {
            let Ok(event) = serde_json::from_str::<JetstreamEvent>(&line) else {
                bad_lines += 1;
                continue;
            };
            if event.cursor < db_cursor {
                skipped += 1;
                continue;
            }
            if event.cursor == db_cursor {
                log::info!("jsonl fixture: found existing db cursor! skipped {skipped} old events and failed parsing {bad_lines} lines");
                break;
            }
            anyhow::bail!("jsonl fixture: did not find existing db cursor, found event cursor {:?} which is newer. bailing.", event.cursor);
        }
    } else {
        log::info!("jsonl fixture: no cursor provided, sending every event");
    }

    log::info!("jsonl fixture: now sending events");
    while let Some(line) = lines.next_line().await? {
        match serde_json::from_str::<JetstreamEvent>(&line) {
            Ok(event) => match sender.send(event).await {
                Ok(_) => {}
                Err(e) => {
                    log::warn!("All receivers for the jsonl fixture have been dropped, bye: {e:?}");
                    return Err(JetstreamEventError::ReceiverClosedError.into());
                }
            },
            Err(parse_err) => {
                log::warn!("failed to parse event: {parse_err:?} from event:\n{line}");
                continue;
            }
        }
    }
    log::info!("reached end of jsonl file, looping on noop to keep server alive.");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs_f64(10.)).await;
    }
}

pub async fn consume(
    p: PathBuf,
    sketch_secret: SketchSecretPrefix,
    cursor: Option<Cursor>,
) -> Result<Receiver<LimitedBatch>> {
    let f = File::open(p).await?;
    let (jsonl_sender, jsonl_receiver) = channel::<JetstreamEvent>(16);
    let (batch_sender, batch_reciever) = channel::<LimitedBatch>(BATCH_QUEUE_SIZE);
    let mut batcher = Batcher::new(jsonl_receiver, batch_sender, sketch_secret);
    tokio::task::spawn(async move {
        let r = read_jsonl(f, jsonl_sender, cursor).await;
        log::warn!("read_jsonl finished: {r:?}");
    });
    tokio::task::spawn(async move {
        let r = batcher.run().await;
        log::warn!("batcher finished: {r:?}");
    });
    Ok(batch_reciever)
}
