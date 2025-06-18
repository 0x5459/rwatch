use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use deadpool_redis::redis;
use deadpool_redis::Pool;
use futures::Stream;
use futures::StreamExt;
use pin_project_lite::pin_project;
use serde::Deserialize;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::time::timeout;
use uuid::Uuid;

use crate::codec::Decode;
use crate::WatchValue;
use crate::WATCH_DATA_PREFIX;
use crate::WATCH_PUBSUB_PREFIX;

/// Default timeout duration for polling Redis messages
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct BorrowGuard<'a, T> {
    inner: RwLockReadGuard<'a, T>,
    has_changed: bool,
}

impl<'a, T> BorrowGuard<'a, T> {
    pub fn has_changed(&self) -> bool {
        self.has_changed
    }
}

impl<'a, T> std::ops::Deref for BorrowGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Redis Watch Receiver
#[derive(Debug, Clone)]
pub struct Receiver<T> {
    notify: Arc<Notify>,
    observed_id: Uuid,
    current_value: Arc<RwLock<WatchValue<T>>>,
}

impl<T> Receiver<T> {
    pub fn new<'a, Dc>(
        client: redis::Client,
        conn_pool: Pool,
        channel: impl AsRef<str> + 'a,
        initial_value: T,
    ) -> (Self, impl Future<Output = Result<(), crate::RecvError<Dc>>>)
    where
        T: for<'de> Deserialize<'de>,
        Dc: Decode,
    {
        Self::new_with_timeout(
            client,
            conn_pool,
            channel,
            initial_value,
            DEFAULT_POLL_TIMEOUT,
        )
    }

    pub fn new_with_timeout<'a, Dc>(
        client: redis::Client,
        conn_pool: Pool,
        channel: impl AsRef<str> + 'a,
        initial_value: T,
        poll_timeout: Duration,
    ) -> (Self, impl Future<Output = Result<(), crate::RecvError<Dc>>>)
    where
        T: for<'de> Deserialize<'de>,
        Dc: Decode,
    {
        let notify = Arc::new(Notify::new());
        let current_value = Arc::new(RwLock::new(WatchValue {
            id: Uuid::default(),
            value: initial_value,
        }));

        let channel = channel.as_ref();
        let data_key = format!("{WATCH_DATA_PREFIX}{}", channel);
        let pubsub_key = format!("{WATCH_PUBSUB_PREFIX}{}", channel);
        (
            Self {
                notify: notify.clone(),
                observed_id: Uuid::default(),
                current_value: current_value.clone(),
            },
            run_receiver(
                client,
                conn_pool,
                data_key,
                pubsub_key,
                notify,
                current_value,
                poll_timeout,
            ),
        )
    }
    /// Wait for the value to change
    pub async fn changed(&mut self) {
        loop {
            // Start listening for notifications BEFORE checking the state
            let notified = self.notify.notified();

            // Check if there's already a change
            {
                let current_value = self.current_value.read().await;
                if current_value.id != self.observed_id {
                    // Mark the current value as observed
                    self.observed_id = current_value.id;
                    break;
                }
            }

            // Now wait for notification
            notified.await;
        }
    }

    /// Get the current value
    pub async fn borrow(&self) -> BorrowGuard<'_, T> {
        let current_value = self.current_value.read().await;
        let has_changed = current_value.id != self.observed_id;
        BorrowGuard {
            inner: RwLockReadGuard::map(current_value, |x| &x.value),
            has_changed,
        }
    }

    /// Get a reference to the current value and wait for changes
    pub async fn borrow_and_update(&mut self) -> BorrowGuard<'_, T> {
        let current_value = self.current_value.read().await;
        let has_changed = current_value.id != self.observed_id;
        self.observed_id = current_value.id;
        BorrowGuard {
            inner: RwLockReadGuard::map(current_value, |x| &x.value),
            has_changed,
        }
    }
}

pin_project! {
    #[project = WatchStreamProj]
    struct WatchStream<S: Stream> {
        #[pin]
        stream: S,
        latest: Option<S::Item>,
    }
}

impl<S> Stream for WatchStream<S>
where S: Stream
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            let latest = this.latest.take();

            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    *this.latest = Some(item);
                }
                Poll::Pending => {
                    return match latest {
                        Some(latest) => Poll::Ready(Some(latest)),
                        None => Poll::Pending,
                    };
                }
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

async fn run_receiver<T, Dc>(
    client: redis::Client,
    conn_pool: Pool,
    data_key: String,
    pubsub_key: String,
    notify: Arc<Notify>,
    current_value: Arc<RwLock<WatchValue<T>>>,
    poll_timeout: Duration,
) -> Result<(), crate::RecvError<Dc>>
where
    T: for<'de> Deserialize<'de>,
    Dc: Decode,
{
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .map_err(crate::RecvError::Redis)?;
    pubsub
        .subscribe(&pubsub_key)
        .await
        .map_err(crate::RecvError::Redis)?;
    let mut stream = WatchStream {
        stream: pubsub.into_on_message(),
        latest: None,
    }
    .fuse();

    loop {
        let _ = timeout(poll_timeout, stream.next()).await;
        let maybe_value: Option<String> = {
            let mut conn = conn_pool.get().await.map_err(crate::RecvError::Connect)?;
            redis::cmd("GET")
                .arg(&data_key)
                .query_async(&mut *conn)
                .await
                .map_err(crate::RecvError::Redis)?
        };
        if let Some(value) = maybe_value {
            let watch_value: WatchValue<T> =
                Dc::decode(&value.as_bytes()).map_err(crate::RecvError::Decode)?;

            let mut current_value = current_value.write().await;
            if current_value.id != watch_value.id {
                *current_value = watch_value;
                notify.notify_waiters();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;
    use crate::codec::JsonDecode;
    use crate::tests::setup_redis;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestData {
        id: u32,
        message: String,
    }

    #[tokio::test]
    async fn test_receiver_creation() {
        let (client, pool) = setup_redis().await;
        let initial_data = TestData {
            id: 1,
            message: "initial".to_string(),
        };

        let (receiver, _recv_task) =
            Receiver::new::<JsonDecode>(client, pool, "test_channel", initial_data.clone());

        // Test initial borrow
        let borrowed = receiver.borrow().await;
        assert_eq!(*borrowed, initial_data);
        assert!(!borrowed.has_changed()); // Should not have changed initially
    }

    #[tokio::test]
    async fn test_receiver_clone() {
        let (client, pool) = setup_redis().await;
        let initial_data = TestData {
            id: 2,
            message: "clone_test".to_string(),
        };

        let (receiver1, _recv_task) =
            Receiver::new::<JsonDecode>(client, pool, "test_clone", initial_data.clone());

        let receiver2 = receiver1.clone();

        // Both receivers should have the same initial value
        let borrowed1 = receiver1.borrow().await;
        let borrowed2 = receiver2.borrow().await;

        assert_eq!(*borrowed1, *borrowed2);
        assert_eq!(*borrowed1, initial_data);
    }
}
