use std::sync::Arc;
use tokio::time::interval;
use std::time::Duration;
use futures::StreamExt;
use crate::{ClientMessage, FilterableProperties};
use crate::server::MultiSubscribeQuery;
use futures::SinkExt;
use std::error::Error;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use dropshot::WebsocketConnectionRaw;

const PING_PERIOD: Duration = Duration::from_secs(30);

pub struct Subscriber {
    query: MultiSubscribeQuery,
    shutdown: CancellationToken,
}

impl Subscriber {
    pub fn new(
        query: MultiSubscribeQuery,
        shutdown: CancellationToken,
    ) -> Self {
        Self { query, shutdown }
    }

    pub async fn start(
        self,
        ws: WebSocketStream<WebsocketConnectionRaw>,
        mut receiver: broadcast::Receiver<Arc<ClientMessage>>
    ) -> Result<(), Box<dyn Error>> {
        let mut ping_state = None;
        let (mut ws_sender, mut ws_receiver) = ws.split();
        let mut ping_interval = interval(PING_PERIOD);
        let _guard = self.shutdown.clone().drop_guard();

        // TODO: do we need to timeout ws sends??

        metrics::counter!("subscribers_connected_total").increment(1);
        metrics::gauge!("subscribers_connected").increment(1);

        loop {
            tokio::select! {
                l = receiver.recv() => match l {
                    Ok(link) => if self.filter(&link.properties) {
                        if let Err(e) = ws_sender.send(link.message.clone()).await {
                            log::warn!("failed to send link, dropping subscriber: {e:?}");
                            break;
                        }
                    },
                    Err(RecvError::Closed) => self.shutdown.cancel(),
                    Err(RecvError::Lagged(n)) => {
                        log::warn!("dropping lagging subscriber (missed {n} messages already)");
                        self.shutdown.cancel();
                    }
                },
                cm = ws_receiver.next() => match cm {
                    Some(Ok(Message::Ping(state))) => {
                        if let Err(e) = ws_sender.send(Message::Pong(state)).await {
                            log::error!("failed to reply pong to subscriber: {e:?}");
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(state))) => {
                        if let Some(expected_state) = ping_state {
                            if *state == expected_state {
                                ping_state = None; // good
                            } else {
                                log::error!("subscriber returned a pong with the wrong state, dropping");
                                self.shutdown.cancel();
                            }
                        } else {
                            log::error!("subscriber sent a pong when none was expected");
                            self.shutdown.cancel();
                        }
                    }
                    Some(Ok(m)) => log::trace!("subscriber sent an unexpected message: {m:?}"),
                    Some(Err(e)) => {
                        log::error!("failed to receive subscriber message: {e:?}");
                        break;
                    }
                    None => {
                        log::trace!("end of subscriber messages. bye!");
                        break;
                    }
                },
                _ = ping_interval.tick() => {
                    if ping_state.is_some() {
                        log::warn!("did not recieve pong within {PING_PERIOD:?}, dropping subscriber");
                        self.shutdown.cancel();
                    } else {
                        let new_state: [u8; 8] = rand::random();
                        let ping = new_state.to_vec().into();
                        ping_state = Some(new_state);
                        if let Err(e) = ws_sender.send(Message::Ping(ping)).await {
                            log::error!("failed to send ping to subscriber, dropping: {e:?}");
                            self.shutdown.cancel();
                        }
                    }
                }
                _ = self.shutdown.cancelled() => {
                    log::info!("subscriber shutdown requested, bye!");
                    if let Err(e) = ws_sender.close().await {
                        log::warn!("failed to close subscriber: {e:?}");
                    }
                    break;
                },
            }
        }
        log::trace!("end of subscriber. bye!");
        metrics::gauge!("subscribers_connected").decrement(1);
        Ok(())
    }

    fn filter(
        &self,
        properties: &FilterableProperties,
    ) -> bool {
        let query = &self.query;

        // subject + subject DIDs are logical OR
        if !(
            query.wanted_subjects.is_empty() && query.wanted_subject_dids.is_empty() ||
            query.wanted_subjects.contains(&properties.subject) ||
            properties.subject_did.as_ref().map(|did| query.wanted_subject_dids.contains(did)).unwrap_or(false)
        ) { // wowwww ^^ fix that
            return false
        }

        // subjects together with sources are logical AND
        if !(query.wanted_sources.is_empty() || query.wanted_sources.contains(&properties.source)) {
            return false
        }

        true
    }
}
