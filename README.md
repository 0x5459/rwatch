# rwatch

A Redis-based, multi-producer, multi-consumer channel that only retains the *last* sent value. Similar to `tokio::sync::watch` but backed by Redis for distributed systems.

## Examples
The following example prints
```
hello! 
world! 
```

```rust
use std::time::Duration;

use deadpool_redis::Config;
use deadpool_redis::Runtime;
use rwatch::channel;
use rwatch::codec::JsonDecode;
use rwatch::codec::JsonEncode;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Message {
    content: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup Redis connection
    let redis_url = "redis://127.0.0.1:6379";
    let client = deadpool_redis::redis::Client::open(redis_url)?;

    let cfg = Config::from_url(redis_url);
    let pool = cfg.create_pool(Some(Runtime::Tokio1))?;

    // Create initial message
    let initial_message = Message {
        content: "hello".to_string(),
    };

    // Create a channel
    let (mut tx, mut rx, recv_bg_task_fut) =
        channel::<Message, JsonEncode, JsonDecode>(client, pool, "my_channel", initial_message);
    // Start the receiver task in the background
    let recv_bg_task = tokio::spawn(recv_bg_task_fut);

    let recv_task = tokio::spawn(async move {
        // Use the equivalent of a "do-while" loop so the initial value is
        // processed before awaiting the `changed()` future.
        loop {
            {
                let content = &*rx.borrow_and_update().await.content;
                println!("{content}!");
            }
            rx.changed().await;
        }
    });

    // Send a message
    tx.send(Message {
        content: "world".to_string(),
    })
    .await?;

    let sleep = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(sleep);

    tokio::select! {
        result = recv_bg_task => {
            result??;
        }
        result = recv_task => {
            result?;
        }
        _ = &mut sleep => {}
    }

    Ok(())
}
```

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by [`tokio::sync::watch`](https://docs.rs/tokio/latest/tokio/sync/watch/index.html)
- Built on top of the excellent [`deadpool-redis`](https://github.com/deadpool-rs/deadpool) and [`redis`](https://github.com/redis-rs/redis-rs) crates 
