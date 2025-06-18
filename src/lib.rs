//! A Redis-based, multi-producer, multi-consumer channel that only retains the *last* sent value.
//! Similar to `tokio::sync::watch` but backed by Redis for distributed systems.
pub mod codec;
mod receiver;
mod sender;

use std::future::Future;

use deadpool_redis::redis;
use deadpool_redis::Pool;
use deadpool_redis::PoolError;
pub use receiver::*;
pub use sender::*;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::codec::Decode;
use crate::codec::Encode;

#[derive(Error, Debug)]
pub enum RecvError<Dc: Decode> {
    #[error("Connect error: {0}")]
    Connect(#[from] PoolError),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Decode error: {0}")]
    Decode(Dc::Error),
}

const WATCH_DATA_PREFIX: &str = "rwatch:key:";
const WATCH_PUBSUB_PREFIX: &str = "rwatch:pubsub:";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct WatchValue<T> {
    id: Uuid,
    value: T,
}

pub fn channel<T, Ec, Dc>(
    // TODO: Remove this parameter once deadpool supports pubsub connections
    // Currently needed for pubsub due to deadpool issue #226
    // See: https://github.com/deadpool-rs/deadpool/issues/226
    redis_client: redis::Client,
    conn_pool: Pool,
    channel: impl AsRef<str>,
    initial_value: T,
) -> (
    Sender<T, Ec>,
    Receiver<T>,
    impl Future<Output = Result<(), crate::RecvError<Dc>>>,
)
where
    T: Serialize + for<'de> Deserialize<'de>,
    Ec: Encode,
    Dc: Decode,
{
    let sender = Sender::new(conn_pool.clone(), channel.as_ref());
    let (receiver, recv_task_fut) = Receiver::new(redis_client, conn_pool, channel, initial_value);
    (sender, receiver, recv_task_fut)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use deadpool_redis::redis;
    use deadpool_redis::Config;
    use deadpool_redis::Pool;
    use deadpool_redis::Runtime;
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;
    use crate::codec::JsonDecode;
    use crate::codec::JsonEncode;

    /// Setup Redis client and pool for testing
    pub(crate) async fn setup_redis() -> (redis::Client, Pool) {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

        let client = redis::Client::open(redis_url.clone()).expect("Failed to create Redis client");

        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .expect("Failed to create Redis pool");

        (client, pool)
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Message {
        id: u64,
        content: String,
        timestamp: u64,
    }

    #[tokio::test]
    async fn test_send_and_receive_message() {
        // Setup Redis connection
        let (client, pool) = setup_redis().await;

        // Create initial message
        let initial_message = Message {
            id: 0,
            content: "Initial".to_string(),
            timestamp: 0,
        };

        // Create channel
        let (mut sender, mut receiver, recv_task_fut) = channel::<Message, JsonEncode, JsonDecode>(
            client,
            pool,
            "test_channel",
            initial_message.clone(),
        );

        // Run receive task in background
        let recv_task = tokio::spawn(async move { recv_task_fut.await });

        // Wait a moment for receiver to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create message to send
        let test_message = Message {
            id: 42,
            content: "Hello from integration test!".to_string(),
            timestamp: 1234567890,
        };

        // Send message
        sender
            .send(test_message.clone())
            .await
            .expect("Failed to send message");

        // Wait for receiver to detect change
        receiver.changed().await;

        // Get current value and verify - changed() has already updated observed_id, so check value directly
        {
            let received_guard = receiver.borrow().await;
            assert_eq!(
                *received_guard, test_message,
                "Received message should match sent message"
            );
        }

        // Test sending second message
        let second_message = Message {
            id: 99,
            content: "Second message".to_string(),
            timestamp: 9999999999,
        };

        sender
            .send(second_message.clone())
            .await
            .expect("Failed to send second message");
        receiver.changed().await;

        {
            let received_guard = receiver.borrow().await;
            assert_eq!(
                *received_guard, second_message,
                "Second message should match"
            );
        }

        // Cleanup: abort receive task
        recv_task.abort();

        println!("Integration test completed successfully!");
    }

    #[tokio::test]
    async fn test_multiple_receivers() {
        // Test if multiple receivers can all receive the same message
        let (client, pool) = setup_redis().await;

        let initial_message = Message {
            id: 0,
            content: "Initial".to_string(),
            timestamp: 0,
        };

        // Create first channel
        let (mut sender1, mut receiver1, recv_task_fut1) = channel::<Message, JsonEncode, JsonDecode>(
            client.clone(),
            pool.clone(),
            "multi_test_channel",
            initial_message.clone(),
        );

        // Create second receiver (same channel)
        let (mut receiver2, recv_task_fut2) = Receiver::new::<JsonDecode>(
            client,
            pool,
            "multi_test_channel",
            initial_message.clone(),
        );

        // Start receive tasks
        let recv_task1 = tokio::spawn(async move { recv_task_fut1.await });
        let recv_task2 = tokio::spawn(async move { recv_task_fut2.await });

        // Wait for receivers to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        let test_message = Message {
            id: 999,
            content: "Broadcast message".to_string(),
            timestamp: 9999999999,
        };

        // Send message
        sender1
            .send(test_message.clone())
            .await
            .expect("Failed to send broadcast message");

        // Wait for both receivers to detect change
        receiver1.changed().await;
        receiver2.changed().await;

        // Verify both receivers received the message
        {
            let received1 = receiver1.borrow().await;
            assert_eq!(
                *received1, test_message,
                "Receiver 1 should receive the message"
            );
        }

        {
            let received2 = receiver2.borrow().await;
            assert_eq!(
                *received2, test_message,
                "Receiver 2 should receive the message"
            );
        }

        // Cleanup
        recv_task1.abort();
        recv_task2.abort();

        println!("Multiple receivers test completed successfully!");
    }
}
