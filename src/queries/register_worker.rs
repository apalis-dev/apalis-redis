use apalis_core::worker::context::WorkerContext;
use redis::aio::ConnectionLike;

use crate::{Config, error::Error, queries::current_timestamp};

pub(crate) async fn register_worker<Conn>(
    conn: &mut Conn,
    worker: &WorkerContext,
    config: &Config,
) -> Result<(), Error>
where
    Conn: ConnectionLike,
{
    let register_worker = redis::Script::new(include_str!("../../lua/register_worker.lua"));
    let workers_set = config.workers_set();

    let metadata_key = config.worker_metadata_key(worker);

    let now = current_timestamp();

    register_worker
        .key(workers_set)
        .key(metadata_key)
        .arg(now)
        .arg(worker.name())
        .arg(config.orphaned_duration().as_secs())
        .arg("RedisStorage")
        .arg(worker.get_service())
        .invoke_async::<()>(conn)
        .await?;
    Ok(())
}
