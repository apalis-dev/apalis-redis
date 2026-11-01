//! Integration coverage for the Redis read queries used by `apalis-board`.
//!
//! These run against a real Redis (`REDIS_URL`), like the existing worker
//! tests. Every test uses a unique queue name and cleans its keys up again.

use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use apalis::prelude::WorkerBuilder;
use apalis_core::backend::TaskSink;
use apalis_core::backend::{
    Filter, ListQueues, ListTasks, ListWorkers, Metrics, QueueInfo, Statistic,
};
use apalis_core::task::status::Status;
use apalis_redis::{Config, RedisStorage};
use redis::{Client, aio::ConnectionManager};
use tokio::sync::oneshot;

const QUEUES_REGISTRY: &str = "core:apalis:queues:list";

fn redis_url() -> String {
    env::var("REDIS_URL").expect("REDIS_URL must be set for the Redis query tests")
}

async fn connection() -> ConnectionManager {
    Client::open(redis_url())
        .expect("valid REDIS_URL")
        .get_connection_manager()
        .await
        .expect("redis connection")
}

fn queue_name(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("test:queries:{label}:{nanos}")
}

fn storage(conn: ConnectionManager, queue: &str) -> RedisStorage<u32> {
    RedisStorage::new(conn).with_config(Config::default().queue(queue))
}

async fn query_storage(queue: &str) -> RedisStorage<u32> {
    storage(connection().await, queue)
}

async fn cleanup(conn: &mut ConnectionManager, queue: &str) {
    let pattern = format!("{queue}:*");
    let mut cursor = 0u64;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(500)
            .query_async(conn)
            .await
            .expect("scan test keys");
        if !keys.is_empty() {
            let _: i64 = redis::cmd("DEL")
                .arg(keys)
                .query_async(conn)
                .await
                .expect("delete test keys");
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }

    let _: i64 = redis::cmd("ZREM")
        .arg(QUEUES_REGISTRY)
        .arg(format!("{queue}:workers"))
        .query_async(conn)
        .await
        .expect("remove queue from registry");
}

fn stat_value(stats: &[Statistic], title: &str) -> u64 {
    stats
        .iter()
        .find(|stat| stat.title == title)
        .unwrap_or_else(|| panic!("missing statistic {title}"))
        .value
        .parse()
        .unwrap_or_else(|_| panic!("statistic {title} is not numeric"))
}

async fn noop_task(_job: u32) {}

struct TestWorker {
    stop: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<Result<(), apalis_core::error::WorkerError>>,
}

impl TestWorker {
    async fn spawn(queue: &str) -> Self {
        let backend = query_storage(queue).await;
        let (stop, stop_rx) = oneshot::channel();
        let worker = WorkerBuilder::new(format!("{queue}-worker"))
            .backend(backend)
            .build(noop_task)
            .run_until(async move {
                let _ = stop_rx.await;
                Ok::<(), std::io::Error>(())
            });
        let handle = tokio::spawn(worker);
        TestWorker {
            stop: Some(stop),
            handle,
        }
    }

    async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = tokio::time::timeout(Duration::from_secs(10), self.handle).await;
    }
}

async fn wait_for_worker(storage: &RedisStorage<u32>) -> apalis_core::backend::RunningWorker {
    for _ in 0..100 {
        if let Some(worker) = storage
            .list_workers()
            .await
            .expect("list workers")
            .into_iter()
            .next()
        {
            return worker;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("worker did not register in time");
}

async fn wait_for_done(storage: &RedisStorage<u32>, expected: u64) {
    for _ in 0..100 {
        let stats = storage.fetch_by_queue().await.expect("queue stats");
        if stat_value(&stats, "DONE_JOBS") >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("jobs did not complete in time");
}

#[tokio::test]
async fn empty_redis_queries_return_without_errors() {
    let queue = queue_name("empty");
    let mut conn = connection().await;
    let storage = storage(conn.clone(), &queue);

    let stats = storage.fetch_by_queue().await.expect("queue stats");
    assert_eq!(stat_value(&stats, "PENDING_JOBS"), 0);
    assert_eq!(stat_value(&stats, "DONE_JOBS"), 0);

    assert!(
        storage
            .list_workers()
            .await
            .expect("list workers")
            .is_empty()
    );
    storage.list_all_workers().await.expect("list all workers");

    let queues = storage.list_queues().await.expect("list queues");
    assert!(queues.iter().all(|info| info.name != queue));

    storage.global().await.expect("global stats");
    cleanup(&mut conn, &queue).await;
}

#[tokio::test]
async fn completed_jobs_are_counted_with_rolling_windows() {
    let queue = queue_name("done");
    let mut conn = connection().await;
    let mut storage = storage(conn.clone(), &queue);

    for job in 0..3 {
        storage.push(job).await.expect("push job");
    }

    let worker = TestWorker::spawn(&queue).await;
    wait_for_done(&storage, 3).await;

    let stats = storage.fetch_by_queue().await.expect("queue stats");
    assert_eq!(stat_value(&stats, "DONE_JOBS"), 3);
    assert_eq!(stat_value(&stats, "PENDING_JOBS"), 0);
    assert_eq!(stat_value(&stats, "FAILED_JOBS"), 0);
    assert_eq!(stat_value(&stats, "TOTAL_JOBS"), 3);
    assert_eq!(stat_value(&stats, "JOBS_PAST_HOUR"), 3);

    let global = storage.global().await.expect("global stats");
    assert!(stat_value(&global, "DONE_JOBS") >= 3);

    let queues = storage.list_queues().await.expect("list queues");
    let info = queues
        .iter()
        .find(|info| info.name == queue)
        .expect("registered queue");
    assert_eq!(info.activity.len(), 7);
    assert_eq!(info.activity.last().copied(), Some(3));
    assert!(!info.workers.is_empty());
    assert_eq!(stat_value(&info.stats, "DONE_JOBS"), 3);

    let done_key_type: String = redis::cmd("TYPE")
        .arg(format!("{queue}:done"))
        .query_async(&mut conn)
        .await
        .expect("done key type");
    assert_eq!(done_key_type, "zset");

    worker.stop().await;
    cleanup(&mut conn, &queue).await;
}

#[tokio::test]
async fn legacy_set_state_is_counted_without_windows() {
    let queue = queue_name("legacy");
    let mut conn = connection().await;
    let storage = storage(conn.clone(), &queue);

    let _: i64 = redis::cmd("SADD")
        .arg(format!("{queue}:done"))
        .arg("018f0000-0000-0000-0000-000000000001")
        .query_async(&mut conn)
        .await
        .expect("seed legacy done set");
    let _: i64 = redis::cmd("SADD")
        .arg(format!("{queue}:dead"))
        .arg("018f0000-0000-0000-0000-000000000002")
        .query_async(&mut conn)
        .await
        .expect("seed legacy dead set");

    let stats = storage.fetch_by_queue().await.expect("queue stats");
    assert_eq!(stat_value(&stats, "DONE_JOBS"), 1);
    assert_eq!(stat_value(&stats, "FAILED_JOBS"), 1);
    assert_eq!(stat_value(&stats, "JOBS_PAST_HOUR"), 0);

    cleanup(&mut conn, &queue).await;
}

#[tokio::test]
async fn mixed_sorted_and_legacy_set_states_are_counted() {
    let queue = queue_name("mixed");
    let mut conn = connection().await;
    let storage = storage(conn.clone(), &queue);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let _: i64 = redis::cmd("ZADD")
        .arg(format!("{queue}:done"))
        .arg(now)
        .arg("018f0000-0000-0000-0000-000000000021")
        .query_async(&mut conn)
        .await
        .expect("seed sorted set done");
    let _: i64 = redis::cmd("SADD")
        .arg(format!("{queue}:dead"))
        .arg("018f0000-0000-0000-0000-000000000022")
        .query_async(&mut conn)
        .await
        .expect("seed legacy dead set");

    let stats = storage.fetch_by_queue().await.expect("queue stats");
    assert_eq!(stat_value(&stats, "DONE_JOBS"), 1);
    assert_eq!(stat_value(&stats, "FAILED_JOBS"), 1);
    assert_eq!(stat_value(&stats, "JOBS_PAST_HOUR"), 1);

    cleanup(&mut conn, &queue).await;
}

#[tokio::test]
async fn workers_are_listed_with_metadata_and_their_queue() {
    let queue = queue_name("workers");
    let mut conn = connection().await;
    let storage = storage(conn.clone(), &queue);

    let worker = TestWorker::spawn(&queue).await;
    let seen = wait_for_worker(&storage).await;
    assert_eq!(seen.queue, queue);
    assert_eq!(seen.backend, "RedisStorage");
    assert!(seen.id.contains(&queue));

    let all = storage.list_all_workers().await.expect("list all workers");
    assert!(all.iter().any(|worker| worker.id == seen.id));

    let queues = storage.list_queues().await.expect("list queues");
    let info: &QueueInfo = queues
        .iter()
        .find(|info| info.name == queue)
        .expect("queue registered by its worker");
    assert!(info.workers.contains(&seen.id));

    worker.stop().await;
    assert!(
        storage
            .list_workers()
            .await
            .expect("list workers after shutdown")
            .is_empty(),
        "a stopped worker is removed from its queue"
    );
    cleanup(&mut conn, &queue).await;
}

#[tokio::test]
async fn tasks_are_listed_from_the_active_queue() {
    let queue = queue_name("tasks");
    let mut conn = connection().await;
    let storage = storage(conn.clone(), &queue);

    // `push` writes to the scheduled set, so seed the active queue directly to
    // exercise the query key.
    for (id, payload) in [
        ("018f0000-0000-0000-0000-000000000011", "1"),
        ("018f0000-0000-0000-0000-000000000012", "2"),
    ] {
        let _: i64 = redis::cmd("HSET")
            .arg(format!("{queue}:data"))
            .arg(id)
            .arg(payload)
            .query_async(&mut conn)
            .await
            .expect("seed task data");
        let _: i64 = redis::cmd("HSET")
            .arg(format!("{queue}:meta:{id}"))
            .arg("attempts")
            .arg(0)
            .arg("max_attempts")
            .arg(25)
            .arg("status")
            .arg("Pending")
            .query_async(&mut conn)
            .await
            .expect("seed task meta");
        let _: i64 = redis::cmd("RPUSH")
            .arg(format!("{queue}:active"))
            .arg(id)
            .query_async(&mut conn)
            .await
            .expect("seed active queue");
    }

    let filter = Filter {
        status: None,
        page: 1,
        page_size: Some(10),
    };
    let tasks = storage.list_tasks(&filter).await.expect("list tasks");
    assert_eq!(tasks.len(), 2);

    let pending = Filter {
        status: Some(Status::Pending),
        page: 1,
        page_size: Some(10),
    };
    assert_eq!(
        storage
            .list_tasks(&pending)
            .await
            .expect("list pending tasks")
            .len(),
        2
    );

    cleanup(&mut conn, &queue).await;
}
