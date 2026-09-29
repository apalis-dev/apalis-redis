use apalis_core::{backend::TaskResult, worker::context::WorkerContext};
use redis::{Script, aio::ConnectionLike};
use serde_json::Value;

use crate::{Config, error::Error, queries::current_timestamp};

pub(crate) async fn handle_results<C: ConnectionLike>(
    conn: &mut C,
    ack_payloads: &[&TaskResult<Value>],
    worker: &WorkerContext,
    config: &Config,
) -> Result<(), Error> {
    if ack_payloads.is_empty() {
        return Ok(());
    }

    let done_jobs_set = config.done_jobs_set();
    let dead_jobs_set = config.dead_jobs_set();
    let job_meta_hash = config.job_meta_hash();
    let scheduled_jobs_set = config.scheduled_jobs_set();
    let worker_inflight_set = config.inflight_set_for(worker);

    let script = Script::new(include_str!("../../lua/handle_results.lua"));
    let mut invocation = script.prepare_invoke();

    invocation
        .key(worker_inflight_set)
        .key(done_jobs_set)
        .key(dead_jobs_set)
        .key(scheduled_jobs_set)
        .key(job_meta_hash)
        .arg(config.emit_events);

    let timestamp = current_timestamp();

    for payload in ack_payloads {
        let status = payload.status().to_string();
        let result_data = serde_json::to_value(&payload.result).map_err(Error::Json)?;

        invocation
            .arg(payload.task_id.to_string())
            .arg(timestamp)
            .arg(result_data.to_string())
            .arg(status)
            .arg(payload.attempt);
    }

    invocation.invoke_async::<u32>(conn).await?;

    Ok(())
}
