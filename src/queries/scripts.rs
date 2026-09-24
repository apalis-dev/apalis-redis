//! Redis Lua script sources and loading helpers.

use redis::{RedisResult, Script, aio::ConnectionLike};

pub(crate) const ACQUIRE_LEASES: &str = include_str!("../../lua/acquire_leases.lua");
pub(crate) const RENEW_LEASES: &str = include_str!("../../lua/renew_leases.lua");
pub(crate) const RELEASE_LEASES: &str = include_str!("../../lua/release_leases.lua");
pub(crate) const FETCH_NEXT: &str = include_str!("../../lua/fetch_next.lua");
pub(crate) const FETCH_BY_ID: &str = include_str!("../../lua/fetch_by_id.lua");
pub(crate) const KEEP_ALIVE: &str = include_str!("../../lua/keep_alive.lua");
pub(crate) const REGISTER_WORKER: &str = include_str!("../../lua/register_worker.lua");
pub(crate) const HANDLE_RESULTS: &str = include_str!("../../lua/handle_results.lua");
pub(crate) const REENQUEUE_ORPHANED: &str = include_str!("../../lua/reenqueue_orphaned.lua");
pub(crate) const VACUUM: &str = include_str!("../../lua/vacuum.lua");
pub(crate) const LIST_TASKS: &str = include_str!("../../lua/list_tasks.lua");
pub(crate) const LIST_ALL_TASKS: &str = include_str!("../../lua/list_all_tasks.lua");
pub(crate) const OVERVIEW: &str = include_str!("../../lua/overview.lua");
pub(crate) const OVERVIEW_BY_QUEUE: &str = include_str!("../../lua/overview_by_queue.lua");
pub(crate) const BATCH_PUSH: &str = include_str!("../../lua/batch_push.lua");

/// Loads `source` with Redis `SCRIPT LOAD` and returns a script handle.
///
/// `Script::invoke_async` executes the returned handle with `EVALSHA`; keeping
/// the load and invocation on the same connection ensures the script is
/// available to that Redis server connection.
pub(crate) async fn load<C>(conn: &mut C, source: &'static str) -> RedisResult<Script>
where
    C: ConnectionLike,
{
    let loaded_sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(source)
        .query_async(conn)
        .await?;

    // Script computes the SHA1 Redis uses for EVALSHA. Verify the server's
    // response so a changed script cannot silently be invoked with a stale id.
    let script = Script::new(source);
    debug_assert_eq!(loaded_sha, script.hash_digest());
    Ok(script)
}
