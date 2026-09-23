use std::sync::LazyLock;

use redis::{Script, aio::ConnectionLike};
use ulid::Ulid;

use crate::{Config, RedisTask, error::Error, queries::current_timestamp};

static BATCH_PUSH_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../../lua/batch_push.lua")));

/// Pushes tasks to Redis using a batch Lua script.
pub async fn push_tasks<Conn>(
    conn: &mut Conn,
    config: &Config,
    tasks: &[RedisTask],
) -> Result<(u32, u32), Error>
where
    Conn: ConnectionLike,
{
    let mut batch = BATCH_PUSH_SCRIPT.key(config.job_data_hash());
    let mut script = batch
        .key(config.active_jobs_list())
        .key(config.signal_list())
        .key(config.job_meta_hash())
        .key(config.scheduled_jobs_set())
        .key(config.idempotency_key_set());
    for request in tasks {
        let task_id = request
            .task_id()
            .map(|s| s.to_string())
            .unwrap_or(Ulid::generate().to_string());
        let attempts = request.attempt() as u32;
        let max_attempts = request.max_attempts().unwrap_or(25);
        let job = &request.args;
        let meta = serde_json::to_string(request.metadata()).map_err(Error::Json)?;
        let run_at = request.run_at().unwrap_or_default();
        let current = current_timestamp();
        // Ensure run_at is not in the past
        let run_at = if run_at > current { run_at } else { current };

        let idempotency_key = request.idempotency_key().unwrap_or("");

        script = script
            .arg(task_id)
            .arg(job)
            .arg(attempts)
            .arg(max_attempts)
            .arg(meta)
            .arg(run_at)
            .arg(idempotency_key);
    }

    let res = script.invoke_async::<(u32, u32)>(conn).await?;
    Ok(res)
}
