use std::{env, time::Duration};

use apalis::prelude::*;
use apalis_redis::RedisStorage;
use redis::Client;

#[tokio::main]
async fn main() {
    let dedupe_key = "7902f170-3d80-47ff-a83c-ebb523247d3c";
    let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
    let conn = client.get_connection_manager().await.unwrap();
    let mut backend = RedisStorage::new(conn);

    let task_1 = TaskBuilder::new(42).idempotency_key(dedupe_key).build();

    let task_2 = TaskBuilder::new(43).idempotency_key(dedupe_key).build();

    backend.push_task(task_1).await.unwrap();
    backend.push_task(task_2).await.unwrap();

    async fn task(task: u32, ctx: TaskContext) -> Result<(), BoxDynError> {
        let handle = std::thread::current();
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
        assert_eq!(task, 42, "This should be the only task");

        Ok(())
    }

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend)
        .on_event(|ctx, ev| {
            println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
        })
        .build(task);
    worker.run().await.unwrap();
}
