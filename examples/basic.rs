use std::{env, time::Duration};

use apalis::prelude::*;
use apalis_redis::{Config, RedisStorage};
use redis::Client;

#[tokio::main]
async fn main() {
    let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
    let conn = client.get_connection_manager().await.unwrap();
    let config = Config::default()
        .queue("redis_basic_worker")
        .heartbeat_interval(Duration::from_secs(1))
        .lock_tasks(false)
        .batch_size(100);
    let mut backend = RedisStorage::new(conn)
        .with_config(config)
        .poll_with_interval(Duration::from_secs(1));
    backend.push(42).await.unwrap();
    async fn task(task: u32, ctx: TaskContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
        let handle = std::thread::current();
        println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
        wrk.stop().unwrap();
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
