use apalis_core::backend::WorkerFilter;
use redis::{Script, aio::ConnectionLike};

use crate::{Config, error::Error, queries::current_timestamp};

pub(crate) async fn reenqueue_orphaned<C>(
    conn: &mut C,
    config: &Config,
    worker_filter: &WorkerFilter,
) -> Result<u32, Error>
where
    C: ConnectionLike,
{
    let worker_set = config.workers_set();
    let active_jobs_list = config.active_jobs_list();
    let signal_list = config.signal_list();

    let script = Script::new(include_str!("../../lua/reenqueue_orphaned.lua"));

    let now = current_timestamp();
    let processed = match worker_filter {
        WorkerFilter::Only(worker_name) => {
            let dead_for = config.heartbeat_interval.as_secs();

            let expired_before = now - dead_for;
            script
                .key(worker_set)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(expired_before)
                .arg(worker_name)
                .invoke_async::<u32>(conn)
                .await?
        }
        _ => {
            let dead_for = config.orphaned_duration().as_secs();

            let expired_before = now - dead_for;
            script
                .key(worker_set)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(expired_before)
                .invoke_async::<u32>(conn)
                .await?
        }
    };

    Ok(processed)
}
