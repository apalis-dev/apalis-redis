# apalis-redis

Background task processing for rust using `apalis` and `redis`

## Features

- **Reliable task queue** using any `redis` compatible service as the backend.
- **Multiple storage types**: standard polling and `pubsub` based approaches.
- **Customizable codecs** for serializing/deserializing task arguments such as `json`, `msgpack` and `bincode`.
- **Heartbeat and orphaned tasks re-enqueueing** for consistent task processing.
- **Integration with `apalis` workers and middleware** such as `retry`, `long_running` and `parallelize`

## Usage

Add the latest versions from crates.io:

```toml
apalis = { version = "1", features = ["retry"] }
apalis-redis = { version = "1" }
```

### Example

```rust,no_run
use apalis::prelude::*;
use apalis_redis::{RedisStorage, RedisConfig as Config};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Email {
    to: String,
}

async fn send_email(task: Email) -> Result<(), BoxDynError> {
    Ok(())
}

#[tokio::main]
async fn main() {
    let conn = apalis_redis::connect(env!("REDIS_URL")).await.expect("Could not connect");
    let mut storage = RedisStorage::new(conn);

    let task = Email {
        to: "test@example.com".to_owned()
    };

    storage.push(task).await.unwrap();

    let worker = WorkerBuilder::new("tasty-pear")
        .backend(storage)
        .build(send_email);

    worker.run().await;
}
```

### Shared Example

This shows an example of multiple backends using the same connection.
This can improve performance if you have many task types.

```rust,no_run
use apalis::prelude::*;
use apalis_redis::{RedisStorage, Client, shared::SharedRedisStorage};
use tokio::time::Duration;
use std::collections::HashMap;
use futures::stream;
use std::env;

#[tokio::main]
async fn main() {
    let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
    let mut store = SharedRedisStorage::new(client).await.unwrap();

    let mut map_store = store.make_shared().unwrap();

    let mut int_store = store.make_shared().unwrap();

    map_store
        .push_stream(&mut stream::iter(vec![HashMap::<String, String>::new()]))
        .await
        .unwrap();
    int_store.push(99).await.unwrap();

    async fn send_reminder<T, I>(
        _: T,
        task_id: TaskId<I>,
        wrk: WorkerContext,
    ) -> Result<(), BoxDynError> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        wrk.stop().unwrap();
        Ok(())
    }

    let int_worker = WorkerBuilder::new("rango-tango-2")
        .backend(int_store)
        .build(send_reminder);
    let map_worker = WorkerBuilder::new("rango-tango-1")
        .backend(map_store)
        .build(send_reminder);
    tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
}
```

### Workflow example

```rust,no_run
use apalis::prelude::*;
use apalis_redis::{RedisStorage, RedisConfig as Config};
use apalis_workflow::Workflow;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize, Serialize)]
struct Data {
   value: u32,
}

async fn task1(task: u32) -> Result<Data, BoxDynError> {
   Ok(Data { value: task + 1 })
}

async fn task2(task: Data) -> Result<Data, BoxDynError> {
  Ok(Data { value: task.value * 2 })
}

async fn task3(task: Data) -> Result<(), BoxDynError> {
  println!("Final value: {}", task.value);
  Ok(())
}

#[tokio::main]
async fn main() {
  let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
  let conn = apalis_redis::connect(redis_url).await.expect("Could not connect");
  let storage = RedisStorage::new(conn);

  let work_flow = Workflow::new("sample-workflow")
      .and_then(task1)
      .and_then(task2)
      .and_then(task3);

  let worker = WorkerBuilder::new("tasty-carrot")
      .backend(storage)
      .build(work_flow);

  worker.run().await;
}
```

## Observability

You can track your tasks using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)

## License

Licensed under the MIT license.
