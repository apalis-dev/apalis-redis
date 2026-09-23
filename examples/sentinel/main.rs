use std::env;

use apalis::prelude::*;
use apalis_redis::{Config, RedisStorage};
use redis::sentinel::Sentinel;

#[tokio::main]
async fn main() {
    let nodes_url = env::var("SENTINEL_NODES").unwrap();

    // Sentinel nodes
    let nodes = nodes_url.split(",").collect();

    // Master name defined in sentinel.conf
    let master_name = "mymaster";

    // Build a sentinel client
    let mut sentinel = Sentinel::build(nodes).unwrap();

    let client = sentinel.master_for(master_name, None).unwrap();

    let conn = client.get_connection_manager().await.unwrap();

    let config = Config::default()
        .queue("redis_sentinel_worker")
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
        .backend(backend)
        .on_event(|ctx, ev| {
            println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
        })
        .build(task);

    worker.run().await.unwrap();
}
