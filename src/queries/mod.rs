use std::time::{SystemTime, UNIX_EPOCH};

mod fetch_by_id;
mod fetch_next;
mod handle_results;
mod keep_alive;
mod list_queues;
mod list_tasks;
mod list_workers;
mod metrics;
mod push_tasks;
mod reenqueue;
mod register_worker;
mod task_lease;
mod vacuum;
mod wait_for;

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Sorted set written by `register_worker.lua`: members are the queues'
/// `<queue>:workers` keys, scored by last registration time.
pub(crate) const QUEUES_REGISTRY: &str = "core:apalis:queues:list";

/// Normalizes the global registry into unique queue names.
pub(crate) fn queue_names(entries: &[String]) -> Vec<String> {
    let mut names: Vec<String> = entries
        .iter()
        .filter_map(|entry| entry.strip_suffix(":workers").map(str::to_string))
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Returns the Redis key type, or `none` when the key does not exist.
pub(crate) async fn key_type<C>(conn: &mut C, key: &str) -> Result<String, redis::RedisError>
where
    C: redis::aio::ConnectionLike + Send,
{
    redis::cmd("TYPE").arg(key).query_async(conn).await
}

/// Counts members of a sorted set within a score range. Plain sets have no
/// scores, so they count as zero.
pub(crate) async fn window_count<C>(
    conn: &mut C,
    key: &str,
    min: i64,
    max: i64,
) -> Result<u64, redis::RedisError>
where
    C: redis::aio::ConnectionLike + Send,
{
    if key_type(conn, key).await?.as_str() == "zset" {
        redis::cmd("ZCOUNT")
            .arg(key)
            .arg(min)
            .arg(max)
            .query_async(conn)
            .await
    } else {
        Ok(0)
    }
}

pub use fetch_next::CompactTask;
pub use fetch_next::deserialize_with_meta;
pub use fetch_next::fetch_next;
pub(crate) use handle_results::handle_results;
pub(crate) use keep_alive::keep_alive;
pub use push_tasks::push_tasks;
pub(crate) use reenqueue::reenqueue_orphaned;
pub(crate) use register_worker::register_worker;
pub(crate) use task_lease::{acquire_leases, release_leases, renew_leases};
