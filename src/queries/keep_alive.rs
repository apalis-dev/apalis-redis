use apalis_core::worker::context::WorkerContext;
use redis::aio::ConnectionLike;

use crate::{Config, error::Error, queries::current_timestamp};

pub(crate) async fn keep_alive<Conn>(
    conn: &mut Conn,
    worker: &WorkerContext,
    config: &Config,
) -> Result<(), Error>
where
    Conn: ConnectionLike,
{
    let keep_alive = redis::Script::new(include_str!("../../lua/keep_alive.lua"));

    let workers_set = config.workers_set();

    let now = current_timestamp();

    keep_alive
        .key(workers_set)
        .arg(now)
        .arg(worker.name())
        .invoke_async::<bool>(conn)
        .await?;
    Ok(())
}
