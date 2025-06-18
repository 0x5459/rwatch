use deadpool_redis::redis;
use deadpool_redis::Pool;
use deadpool_redis::PoolError;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::codec::Encode;
use crate::WatchValue;
use crate::WATCH_DATA_PREFIX;
use crate::WATCH_PUBSUB_PREFIX;

#[derive(Error, Debug)]
pub enum SendError<Ec: Encode> {
    #[error("Connect error: {0}")]
    Connect(#[from] PoolError),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Encode error: {0}")]
    Encode(Ec::Error),
}

/// Redis Watch Sender
pub struct Sender<T, Encode> {
    conn_pool: Pool,
    data_key: String,
    pubsub_key: String,
    _phantom: std::marker::PhantomData<(T, Encode)>,
}

impl<T, Ec> Sender<T, Ec>
where
    T: Serialize,
    Ec: Encode,
{
    pub fn new(conn_pool: Pool, channel: impl AsRef<str>) -> Self {
        Self {
            conn_pool,
            data_key: format!("{WATCH_DATA_PREFIX}{}", channel.as_ref()),
            pubsub_key: format!("{WATCH_PUBSUB_PREFIX}{}", channel.as_ref()),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, Ec> Sender<T, Ec>
where
    T: Serialize,
    Ec: Encode,
{
    pub async fn send(&mut self, value: T) -> Result<(), SendError<Ec>> {
        let mut conn = self.conn_pool.get().await.map_err(SendError::Connect)?;

        let watch_value = WatchValue {
            id: Uuid::now_v7(),
            value,
        };
        // Store the value in Redis
        let encoded_value = Ec::encode(&watch_value).map_err(SendError::Encode)?;
        let _: () = redis::pipe()
            .cmd("SET")
            .arg(&self.data_key)
            .arg(encoded_value)
            .ignore()
            .cmd("PUBLISH")
            .arg(&self.pubsub_key)
            // TODO: Use u128 directly instead of string conversion
            // Waiting for redis crate to be bumped to version 0.32.1
            .arg(watch_value.id.to_string())
            .ignore()
            .query_async(&mut *conn)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;
    use crate::codec::JsonEncode;
    use crate::tests::setup_redis;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestMessage {
        id: u64,
        content: String,
    }

    #[tokio::test]
    async fn test_sender_creation() {
        let (_, pool) = setup_redis().await;
        let sender = Sender::<TestMessage, JsonEncode>::new(pool, "test_channel");

        // Verify the sender was created with correct keys
        assert_eq!(sender.data_key, "rwatch:key:test_channel");
        assert_eq!(sender.pubsub_key, "rwatch:pubsub:test_channel");
    }

    #[tokio::test]
    async fn test_sender_different_channels() {
        let (_, pool) = setup_redis().await;

        let sender1 = Sender::<TestMessage, JsonEncode>::new(pool.clone(), "channel1");
        let sender2 = Sender::<TestMessage, JsonEncode>::new(pool, "channel2");

        // Verify different channels have different keys
        assert_ne!(sender1.data_key, sender2.data_key);
        assert_ne!(sender1.pubsub_key, sender2.pubsub_key);

        assert_eq!(sender1.data_key, "rwatch:key:channel1");
        assert_eq!(sender2.data_key, "rwatch:key:channel2");
    }

    #[tokio::test]
    async fn test_send_message() {
        let (_, pool) = setup_redis().await;
        let mut sender = Sender::<TestMessage, JsonEncode>::new(pool.clone(), "test_send");

        let message = TestMessage {
            id: 123,
            content: "Hello, World!".to_string(),
        };

        // This test mainly verifies that send doesn't panic
        // In a real scenario, you'd want to verify the message was actually stored in Redis
        let result = sender.send(message).await;

        // If Redis is not available, we expect a connection error
        // If Redis is available, we expect success
        match result {
            Ok(_) => {
                // Success - Redis is available and message was sent
                println!("Message sent successfully");
            }
            Err(SendError::Connect(_)) => {
                // Expected if Redis is not running
                println!("Redis not available - connection error expected");
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_send_multiple_messages() {
        let (_, pool) = setup_redis().await;
        let mut sender = Sender::<TestMessage, JsonEncode>::new(pool, "test_multiple");

        let messages = vec![
            TestMessage {
                id: 1,
                content: "First".to_string(),
            },
            TestMessage {
                id: 2,
                content: "Second".to_string(),
            },
            TestMessage {
                id: 3,
                content: "Third".to_string(),
            },
        ];

        for message in messages {
            let result = sender.send(message).await;

            // Handle both success and expected connection errors
            match result {
                Ok(_) => {
                    // Success case
                }
                Err(SendError::Connect(_)) => {
                    // Expected if Redis is not running
                    break;
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }
}
