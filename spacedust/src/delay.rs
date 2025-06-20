use crate::removable_delay_queue;
use crate::LinkEvent;
use tokio_util::sync::CancellationToken;
use tokio::sync::broadcast;
use crate::error::DelayError;

pub async fn to_broadcast(
    source: removable_delay_queue::Output<(String, usize), LinkEvent>,
    dest: broadcast::Sender<LinkEvent>,
    shutdown: CancellationToken,
) -> Result<(), DelayError> {
    loop {
        tokio::select! {
            ev = source.next() => match ev {
                Some(event) => {
                    let _ = dest.send(event); // only errors of there are no listeners, but that's normal
                },
                None => return Err(DelayError::DelayEnded),
            },
            _ = shutdown.cancelled() => return Ok(()),
        }
    }
}
