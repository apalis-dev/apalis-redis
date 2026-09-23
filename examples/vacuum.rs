use std::env;

use apalis::prelude::*;
use apalis_core::backend::Vacuum;
use apalis_redis::{Config, RedisStorage};
use redis::Client;

#[tokio::main]
async fn main() {
    let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
    let conn = client.get_connection_manager().await.unwrap();
    let config = Config::default()
        .queue("redis_vacuum_worker")
        .batch_size(100);
    let mut backend = RedisStorage::new(conn).with_config(config);
    backend.push(42).await.unwrap();
    async fn task(task: u32, ctx: TaskContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
        let handle = std::thread::current();
        println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
        wrk.stop().unwrap();
        Ok(())
    }

    let worker = WorkerBuilder::new("rango-tango")
        .backend(backend.clone())
        .on_event(|ctx, ev| {
            println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
        })
        .build(task);
    worker.run().await.unwrap();

    // You can combine this with `apalis-cron` to vacuum on interval
    backend.vacuum().await.unwrap();
}
