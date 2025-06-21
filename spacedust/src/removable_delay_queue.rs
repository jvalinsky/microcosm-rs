use std::ops::RangeBounds;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EnqueueError<T> {
    #[error("queue ouput dropped")]
    OutputDropped(T),
}

pub trait Key: Eq + Ord + Clone {}
impl<T: Eq + Ord + Clone> Key for T {}

#[derive(Debug)]
struct Queue<K: Key, T> {
    queue: VecDeque<(Instant, K)>,
    items: BTreeMap<K, T>
}

pub struct Input<K: Key, T> {
    q: Arc<Mutex<Queue<K, T>>>,
}

impl<K: Key, T> Input<K, T> {
    /// if a key is already present, its previous item will be overwritten and
    /// its delay time will be reset for the new item.
    ///
    /// errors if the remover has been dropped
    pub async fn enqueue(&self, key: K, item: T) -> Result<(), EnqueueError<T>> {
        if Arc::strong_count(&self.q) == 1 {
            return Err(EnqueueError::OutputDropped(item));
        }
        // TODO: try to push out an old element first
        // for now we just hope there's a listener
        let now = Instant::now();
        let mut q = self.q.lock().await;
        q.queue.push_back((now, key.clone()));
        q.items.insert(key, item);
        Ok(())
    }
    /// remove an item from the queue, by key
    ///
    /// the item itself is removed, but the key will remain in the queue -- it
    /// will simply be skipped over when a new output item is requested. this
    /// keeps the removal cheap (=btreemap remove), for a bit of space overhead
    pub async fn remove_range(&self, range: impl RangeBounds<K>) {
        let n = {
            let mut q = self.q.lock().await;
            let keys = q.items.range(range).map(|(k, _)| k).cloned().collect::<Vec<_>>();
            for k in &keys {
                q.items.remove(k);
            }
            keys.len()
        };
        if n == 0 {
            metrics::counter!("delay_queue_remove_not_found").increment(1);
        } else {
            metrics::counter!("delay_queue_remove_total_records").increment(1);
            metrics::counter!("delay_queue_remove_total_links").increment(n as u64);
        }
    }
}

pub struct Output<K: Key, T> {
    delay: Duration,
    q: Arc<Mutex<Queue<K, T>>>,
}

impl<K: Key, T> Output<K, T> {
    pub async fn next(&self) -> Option<T> {
        let get = || async {
            let mut q = self.q.lock().await;
            metrics::gauge!("delay_queue_queue_len").set(q.queue.len() as f64);
            metrics::gauge!("delay_queue_queue_capacity").set(q.queue.capacity() as f64);
            while let Some((t, k)) = q.queue.pop_front() {
                // skip over queued keys that were removed from items
                if let Some(item) = q.items.remove(&k) {
                    return Some((t, item));
                }
            }
            None
        };
        loop {
            if let Some((t, item)) = get().await {
                let now = Instant::now();
                let expected_release = t + self.delay;
                if expected_release.saturating_duration_since(now) > Duration::from_millis(1) {
                    tokio::time::sleep_until(expected_release.into()).await;
                    metrics::counter!("delay_queue_emit_total", "early" => "yes").increment(1);
                    metrics::histogram!("delay_queue_emit_overshoot").record(0);
                } else {
                    let overshoot = now.saturating_duration_since(expected_release);
                    metrics::counter!("delay_queue_emit_total", "early" => "no").increment(1);
                    metrics::histogram!("delay_queue_emit_overshoot").record(overshoot.as_secs_f64());
                }
                return Some(item)
            } else if Arc::strong_count(&self.q) == 1 {
                return None;
            }
            // the queue is *empty*, so we need to wait at least as long as the current delay
            tokio::time::sleep(self.delay).await;
            metrics::counter!("delay_queue_entirely_empty_total").increment(1);
        };
    }
}

pub fn removable_delay_queue<K: Key, T>(
    delay: Duration,
) -> (Input<K, T>, Output<K, T>) {
    let q: Arc<Mutex<Queue<K, T>>> = Arc::new(Mutex::new(Queue {
        queue: VecDeque::new(),
        items: BTreeMap::new(),
    }));

    let input = Input::<K, T> { q: q.clone() };
    let output = Output::<K, T> { q, delay };
    (input, output)
}
