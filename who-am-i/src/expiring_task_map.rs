use dashmap::DashMap;
use rand::{Rng, distr::Alphanumeric};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{JoinHandle, spawn};
use tokio::time::sleep; // 0.8

#[derive(Clone)]
pub struct ExpiringTaskMap<T>(Arc<TaskMap<T>>);

impl<T: Send + 'static> ExpiringTaskMap<T> {
    pub fn new(expiration: Duration) -> Self {
        let map = TaskMap {
            map: DashMap::new(),
            expiration,
        };
        Self(Arc::new(map))
    }

    pub fn dispatch(&self, task: impl Future<Output = T> + Send + 'static) -> String {
        let task_key: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();

        // spawn a tokio task and put the join handle in the map for later retrieval
        self.0.map.insert(task_key.clone(), spawn(task));

        // spawn a second task to clean up the map in case it doesn't get claimed
        spawn({
            let me = self.0.clone();
            let key = task_key.clone();
            async move {
                sleep(me.expiration).await;
                let _ = me.map.remove(&key);
                // TODO: also use a cancellation token so taking and expiring can mutually cancel
            }
        });

        task_key
    }

    pub fn take(&self, key: &str) -> Option<JoinHandle<T>> {
        eprintln!("trying to take...");
        self.0.map.remove(key).map(|(_, handle)| handle)
    }
}

struct TaskMap<T> {
    map: DashMap<String, JoinHandle<T>>,
    expiration: Duration,
}
