use std::{collections::HashMap, str::FromStr};

use apalis_core::{
    backend::codec::Codec,
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataStore, status::Status, task_id::TaskId},
    worker::context::WorkerContext,
};
use redis::{RedisError, aio::ConnectionLike};

use crate::{Config, RedisTask, build_error, error::Error, queries::current_timestamp};

/// A task structure that includes metadata.
#[derive(Debug, Clone)]
pub struct CompactTask {
    /// The task data in its compact form.
    pub data: Vec<u8>,
    /// The number of attempts made for this task.
    pub attempts: u32,
    /// The maximum number of attempts allowed for this task.
    pub max_attempts: u32,
    /// The current status of the task.
    pub status: Status,
    /// The unique identifier for the task.
    pub task_id: TaskId,
    /// A unique key to prevent duplicates
    pub idempotency_key: Option<String>,
    /// Metadata associated with the task.
    pub meta: HashMap<String, String>,
    /// The current worker.
    pub lock_by: Option<String>,

    /// The time the task was locked
    pub lock_at: Option<u64>,
}

impl CompactTask {
    /// Converts the task data into a full Task with compact arguments.
    pub fn into_full_compact(self) -> RedisTask {
        let mut task = TaskBuilder::new(self.data.to_vec())
            .task_id(self.task_id)
            .status(self.status)
            .with_metadata(MetadataStore::from_map(self.meta))
            .lock_by(self.lock_by)
            .max_attempts(self.max_attempts as usize)
            .lock_at(self.lock_at)
            .attempt(self.attempts as usize);

        if let Some(key) = self.idempotency_key {
            task = task.idempotency_key(key);
        }
        task.build()
    }

    /// Converts the task data into a full Task with decoded arguments.
    pub fn into_full_task<Args: 'static, C>(self, codec: &C) -> Result<Task<Args>, RedisError>
    where
        C: Codec<Args, Compact = Vec<u8>>,
        C::Error: Into<BoxDynError>,
    {
        let args: Args =
            C::decode(codec, &self.data).map_err(|e| build_error(&e.into().to_string()))?;
        let mut task = TaskBuilder::new(args)
            .task_id(self.task_id)
            .status(self.status)
            .with_metadata(MetadataStore::from_map(self.meta))
            .lock_by(self.lock_by)
            .max_attempts(self.max_attempts as usize)
            .lock_at(self.lock_at)
            .attempt(self.attempts as usize);

        if let Some(key) = self.idempotency_key {
            task = task.idempotency_key(key);
        }
        Ok(task.build())
    }
}

/// Extracts a &str view from a redis::Value without allocating.
#[inline]
pub(super) fn str_from_val<'a>(val: &'a redis::Value, field: &str) -> Result<&'a str, RedisError> {
    match val {
        redis::Value::BulkString(bytes) => {
            str::from_utf8(bytes).map_err(|_| build_error(&format!("{field} not UTF-8")))
        }
        _ => Err(build_error(&format!("{field} not bulk string"))),
    }
}

/// Deserialize a stream of redis values into [`CompactTask`]
pub fn deserialize_with_meta(data: Vec<redis::Value>) -> Result<Vec<CompactTask>, RedisError> {
    let mut iter = data.into_iter();
    let job_data_val = iter
        .next()
        .ok_or_else(|| build_error("Expected two elements: job_data and metadata"))?;
    let meta_val = iter
        .next()
        .ok_or_else(|| build_error("Expected two elements: job_data and metadata"))?;
    if iter.next().is_some() {
        return Err(build_error("Expected exactly two elements"));
    }

    let job_data_list = match job_data_val {
        redis::Value::Array(vals) => vals,
        _ => return Err(build_error("Expected job_data to be array")),
    };
    let meta_list = match meta_val {
        redis::Value::Array(vals) => vals,
        _ => return Err(build_error("Expected metadata to be array")),
    };

    if job_data_list.len() != meta_list.len() {
        return Err(build_error("Job data and metadata length mismatch"));
    }

    let mut result = Vec::with_capacity(job_data_list.len());

    for (data_val, meta_val) in job_data_list.into_iter().zip(meta_list) {
        let data = match data_val {
            redis::Value::BulkString(bytes) => bytes,
            _ => return Err(build_error("Invalid job data format")),
        };

        let meta_fields = match meta_val {
            redis::Value::Array(fields) => fields,
            _ => return Err(build_error("Invalid metadata format")),
        };
        if meta_fields.is_empty() {
            return Err(build_error("Metadata array too short"));
        }

        // fields[0] is always task_id (inserted positionally by the Lua script,
        // not part of the hash itself), the rest is field/value pairs in
        // unspecified hash iteration order.
        let task_id = TaskId::from_str(str_from_val(&meta_fields[0], "task_id")?)
            .map_err(|e| build_error(&e.to_string()))?;

        let rest = &meta_fields[1..];
        if rest.len() % 2 != 0 {
            return Err(build_error("Metadata field/value count is odd"));
        }

        let mut meta: HashMap<String, String> = HashMap::with_capacity(rest.len() / 2);
        for chunk in rest.as_chunks::<2>().0 {
            let k = str_from_val(&chunk[0], "meta key")?.to_owned();
            let v = str_from_val(&chunk[1], "meta value")?.to_owned();
            meta.insert(k, v);
        }

        let attempts = meta
            .remove("attempts")
            .ok_or_else(|| build_error("Missing attempts in metadata"))
            .and_then(|v| v.parse::<u32>().map_err(|e| build_error(&e.to_string())))?;

        let max_attempts = meta
            .remove("max_attempts")
            .ok_or_else(|| build_error("Missing max_attempts in metadata"))
            .and_then(|v| v.parse::<u32>().map_err(|e| build_error(&e.to_string())))?;

        let status = meta
            .remove("status")
            .ok_or_else(|| build_error("Missing status in metadata"))
            .and_then(|v| Status::from_str(&v).map_err(|e| build_error(&e.to_string())))?;

        let idempotency_key = meta.remove("idempotency_key").filter(|v| !v.is_empty());

        let lock_by = meta.remove("locked_by");
        let lock_at = meta
            .remove("locked_at")
            .map(|a| a.parse().unwrap_or_default());

        result.push(CompactTask {
            task_id,
            data,
            attempts,
            max_attempts,
            status,
            idempotency_key,
            meta,
            lock_by,
            lock_at,
        });
    }

    Ok(result)
}

/// Manually fetch the next set of jobs
pub async fn fetch_next<C>(
    conn: &mut C,
    worker: &WorkerContext,
    config: &Config,
) -> Result<Vec<RedisTask>, Error>
where
    C: ConnectionLike,
{
    let fetch_jobs = redis::Script::new(include_str!("../../lua/fetch_next.lua"));
    let workers_set = config.workers_set();
    let active_jobs_list = config.active_jobs_list();
    let job_data_hash = config.job_data_hash();
    let inflight_worker_id = config.inflight_worker_id(worker);
    let signal_list = config.signal_list();

    let result = fetch_jobs
        .key(&workers_set)
        .key(&active_jobs_list)
        .key(config.inflight_jobs_set())
        .key(&job_data_hash)
        .key(&signal_list)
        .key(config.job_meta_hash())
        .key(config.scheduled_jobs_set())
        .arg(current_timestamp())
        .arg(config.batch_size)
        .arg(inflight_worker_id)
        .invoke_async::<Vec<redis::Value>>(conn)
        .await?;

    let tasks = deserialize_with_meta(result)?;

    Ok(tasks
        .into_iter()
        .map(CompactTask::into_full_compact)
        .collect::<Vec<RedisTask>>())
}
