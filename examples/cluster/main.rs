use std::env;

use apalis::prelude::*;
use apalis_redis::{Config, RedisStorage};
use redis::Client;

const SLOT: &'static str = "{clustered_queue}";

#[tokio::main]
async fn main() {
    unsafe {
        std::env::set_var("RUST_LOG", "trace");
    };
    tracing_subscriber::fmt::init();

    let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
    let conn = client.get_connection_manager().await.unwrap();
    let config = Config::default()
        .queue(SLOT) // Remember to wrap in "{}" for cross slot
        .batch_size(100);
    let mut backend = RedisStorage::new(conn).with_config(config);
    backend.push(42).await.unwrap();
    async fn task(task: u32, ctx: TaskContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
        let handle = std::thread::current();
        println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
        wrk.stop().unwrap();
        Ok(())
    }

    let worker = WorkerBuilder::new(format!("{SLOT}:worker-1"))
        .backend(backend)
        .on_event(|ctx, ev| {
            println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
        })
        .build(task);
    worker.run().await.unwrap();
}
