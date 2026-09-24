use apalis_core::worker::context::WorkerContext;
use redis::{Script, aio::ConnectionLike};

use crate::{Config, RedisTask, error::Error, queries::current_timestamp};

pub(crate) async fn acquire_leases<C>(
    conn: &mut C,
    worker: &WorkerContext,
    config: &Config,
    task_ids: &[String],
) -> Result<u32, Error>
where
    C: ConnectionLike,
{
    if task_ids.is_empty() {
        return Ok(0);
    }

    let script = redis::Script::new(include_str!("../../lua/acquire_leases.lua"));
    let worker_inflight_set = config.inflight_set_for(worker);
    let workers_set = config.workers_set();
    let job_meta_hash = config.job_meta_hash();
    let active_jobs_list = config.active_jobs_list();
    let now: u64 = current_timestamp();

    let mut invocation = script.prepare_invoke();
    invocation
        .key(worker_inflight_set)
        .key(&workers_set)
        .key(&job_meta_hash)
        .key(&active_jobs_list)
        .arg(worker.name())
        .arg(now)
        .arg(config.emit_events);

    for task_id in task_ids {
        invocation.arg(task_id);
    }

    let acquired = invocation.invoke_async::<u32>(conn).await?;

    Ok(acquired)
}

pub(crate) async fn renew_leases<C>(
    conn: &mut C,
    worker: &WorkerContext,
    config: &Config,
) -> Result<u32, Error>
where
    C: ConnectionLike,
{
    if worker.task_count() == 0 {
        return Ok(0);
    }

    let script = redis::Script::new(include_str!("../../lua/renew_leases.lua"));
    let worker_inflight_set = config.inflight_set_for(worker);
    let workers_set = config.workers_set();
    let job_meta_hash = config.job_meta_hash();
    let now: u64 = current_timestamp();

    let mut invocation = script.prepare_invoke();
    invocation
        .key(worker_inflight_set)
        .key(&workers_set)
        .key(&job_meta_hash)
        .arg(worker.name())
        .arg(now)
        .arg(config.emit_events);

    for task in worker.tasks() {
        invocation.arg(task.task_id());
    }

    let renewed = invocation.invoke_async::<u32>(conn).await?;

    Ok(renewed)
}

pub(crate) async fn release_leases<C: ConnectionLike>(
    conn: &mut C,
    config: &Config,
    worker: &WorkerContext,
    tasks: &Vec<RedisTask>,
) -> Result<u64, Error> {
    let worker_inflight_set = config.inflight_set_for(worker);
    let active_jobs_list = config.active_jobs_list();
    let signal_list = config.signal_list();
    let workers_set = config.workers_set();
    let job_meta_hash = config.job_meta_hash();

    let script = Script::new(include_str!("../../lua/release_leases.lua"));
    let mut invocation = script.prepare_invoke();

    invocation
        .key(worker_inflight_set)
        .key(active_jobs_list)
        .key(signal_list)
        .key(workers_set)
        .key(job_meta_hash)
        .arg(config.emit_events);

    for task in tasks {
        invocation.arg(task.task_id().unwrap().to_string());
    }

    Ok(invocation.invoke_async::<u64>(conn).await?)
}
