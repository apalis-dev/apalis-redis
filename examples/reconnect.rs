//! Keep a worker running across Redis restarts.
//!
//! When the Redis connection drops, the worker stops with an error. A `Monitor`
//! with `should_restart` brings it back, and `before_start` runs before every
//! (re)start, so that is the place to back off. Without a backoff the monitor
//! retries in a tight loop: the restarted worker re-registers under the same
//! name and is turned away until its previous registration ages out.
//!
//! Run it, then restart Redis (e.g. `docker restart <redis>`) while it works:
//!   REDIS_URL=redis://127.0.0.1:6379 cargo run --example reconnect
use std::{env, time::Duration};

use apalis::prelude::*;
use apalis_redis::RedisStorage;

async fn task(job: u32) -> Result<(), BoxDynError> {
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("done {job}");
    Ok(())
}

#[tokio::main]
async fn main() {
    let url = env::var("REDIS_URL").unwrap();
    let conn = apalis_redis::connect(url.as_str()).await.unwrap();

    // Producer: one job every 200 ms, so you can watch processing resume after a restart.
    let mut storage = RedisStorage::new(conn.clone());
    tokio::spawn(async move {
        let mut i = 0u32;
        loop {
            if let Err(e) = storage.push(i).await {
                eprintln!("push {i} failed: {e}");
            }
            i = i.wrapping_add(1);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    Monitor::new()
        .register(move |restarts| {
            let backend = RedisStorage::new(conn.clone()).before_start(move |_| async move {
                // Exponential backoff on restarts: 100 ms, 200 ms, ... capped at 5 s.
                if restarts > 0 {
                    let ms = (100u64 << (restarts - 1).min(16)).min(5_000);
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                Ok(())
            });
            WorkerBuilder::new("reconnect-worker")
                .backend(backend)
                .build(task)
        })
        .should_restart(|_, err| {
            eprintln!("worker stopped ({err}), restarting");
            true
        })
        .run()
        .await
        .unwrap();
}
