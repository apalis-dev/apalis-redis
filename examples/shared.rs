use std::{env, time::Duration};

use apalis::prelude::*;
use apalis_redis::{Config, factory::RedisStorageFactory};
use redis::Client;

#[tokio::main]
async fn main() {
    let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
    let mut store = RedisStorageFactory::new(client).await.unwrap();

    let config = Config::default().queue("str-task-queue").batch_size(5);

    let mut string_store = store.create_with_config(config).unwrap();
    let mut int_store = store.create().unwrap();

    string_store.push("ITEM".to_owned()).await.unwrap();
    int_store.push(42).await.unwrap();

    async fn task(job: u32, ctx: WorkerContext) -> Result<usize, BoxDynError> {
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(job, 42);
        ctx.stop().unwrap();
        Ok(job as usize)
    }

    let int_worker = WorkerBuilder::new("rango-tango-int")
        .backend(int_store)
        .on_event(|worker, ev| {
            println!("CTX {:?}, On Event = {:?}", worker.name(), ev);
        })
        .build(task)
        .run();

    let string_worker = WorkerBuilder::new("rango-tango-string")
        .backend(string_store)
        .on_event(|worker, ev| {
            println!("CTX {:?}, On Event = {:?}", worker.name(), ev);
        })
        .build(|req: String, worker: WorkerContext| async move {
            tokio::time::sleep(Duration::from_millis(3)).await;
            assert_eq!(req, "ITEM".to_owned());
            worker.stop().unwrap();
        })
        .run();
    let _ = futures::future::try_join(int_worker, string_worker)
        .await
        .unwrap();
}
