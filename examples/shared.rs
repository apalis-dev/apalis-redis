use std::{env, time::Duration};

use apalis::prelude::*;
use apalis_redis::{RedisConfig, shared::SharedRedisStorage};
use redis::Client;

#[tokio::main]
async fn main() {
    let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
    let mut store = SharedRedisStorage::new(client).await.unwrap();

    let mut string_store = store
        .make_shared_with_config(
            RedisConfig::default()
                .set_namespace("str-task-queue")
                .set_poll_interval(Duration::from_secs(1))
                .set_buffer_size(5),
        )
        .unwrap();
    let mut int_store = store
        .make_shared_with_config(
            RedisConfig::default()
                .set_namespace("int-task-queue")
                .set_poll_interval(Duration::from_secs(2))
                .set_buffer_size(5),
        )
        .unwrap();

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
        .on_event(|ctx, ev| {
            println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
        })
        .build(task)
        .run();

    let string_worker = WorkerBuilder::new("rango-tango-string")
        .backend(string_store)
        .on_event(|ctx, ev| {
            println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
        })
        .build(|req: String, ctx: WorkerContext| async move {
            tokio::time::sleep(Duration::from_millis(3)).await;
            assert_eq!(req, "ITEM".to_owned());
            ctx.stop().unwrap();
        })
        .run();
    let _ = futures::future::try_join(int_worker, string_worker)
        .await
        .unwrap();
}
