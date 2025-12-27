use std::env;

use apalis::prelude::*;
use apalis_redis::{RedisConfig, RedisContext, RedisStorage};
use redis::Client;

#[tokio::main]
async fn main() {
    let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
    let conn = client.get_connection_manager().await.unwrap();
    let mut backend = RedisStorage::new_with_config(
        conn,
        RedisConfig::default()
            .set_namespace("redis_basic_worker")
            .set_buffer_size(100),
    );
    backend.push(42).await.unwrap();
    async fn task(task: u32, ctx: RedisContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
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
