use std::str::FromStr;

use apalis_core::{
    backend::codec::Codec,
    error::BoxDynError,
    task::{Task, attempt::Attempt, status::Status, task_id::TaskId},
    worker::context::WorkerContext,
};
use redis::{RedisError, Value, aio::ConnectionLike};
use ulid::Ulid;

use crate::{RedisStorage, build_error, config::RedisConfig, context::RedisContext};

impl<Args, Conn, C> RedisStorage<Args, Conn, C>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: ConnectionLike + Send + Sync + 'static,
    C: Codec<Args, Compact = Vec<u8>>,
    C::Error: Into<BoxDynError>,
{
    /// Fetches the next batch of tasks for the given worker.
    pub async fn fetch_next(
        worker: &WorkerContext,
        config: &RedisConfig,
        conn: &mut Conn,
    ) -> Result<Vec<Task<Vec<u8>, RedisContext, Ulid>>, RedisError> {
        let fetch_jobs = redis::Script::new(include_str!("../lua/get_jobs.lua"));
        let workers_set = config.workers_set();
        let active_jobs_list = config.active_jobs_list();
        let job_data_hash = config.job_data_hash();
        let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker.name());
        let signal_list = config.signal_list();

        let result = fetch_jobs
            .key(&workers_set)
            .key(&active_jobs_list)
            .key(&inflight_set)
            .key(&job_data_hash)
            .key(&signal_list)
            .key(config.job_meta_hash())
            .arg(config.get_buffer_size()) // No of jobs to fetch
            .arg(&inflight_set)
            .invoke_async::<Vec<Value>>(&mut *conn)
            .await;
        match result {
            Ok(jobs) => {
                let mut processed = vec![];
                let tasks = deserialize_with_meta(&jobs)?;
                for unprocessed in tasks {
                    let mut task = unprocessed.into_full_compact()?;
                    task.parts.ctx.lock_by = Some(worker.name().to_string());
                    processed.push(task)
                }
                Ok(processed)
            }
            Err(e) => Err(e),
        }
    }
}

/// A task structure that includes metadata.
#[derive(Debug, Clone)]
pub struct CompactTask<'a> {
    /// The task data in its compact form.
    pub data: &'a Vec<u8>,
    /// The number of attempts made for this task.
    pub attempts: u32,
    /// The maximum number of attempts allowed for this task.
    pub max_attempts: u32,
    /// The current status of the task.
    pub status: Status,
    /// The unique identifier for the task.
    pub task_id: TaskId<Ulid>,
    /// Metadata associated with the task.
    pub meta: serde_json::Map<String, serde_json::Value>,
}

impl CompactTask<'_> {
    /// Converts the task data into a full Task with compact arguments.
    pub fn into_full_compact(self) -> Result<Task<Vec<u8>, RedisContext, Ulid>, RedisError> {
        let context = RedisContext {
            max_attempts: self.max_attempts,
            lock_by: None,
            meta: self.meta,
        };
        let task = Task::builder(self.data.clone())
            .with_task_id(self.task_id)
            .with_status(self.status)
            .with_attempt(Attempt::new_with_value(self.attempts as usize))
            .with_ctx(context)
            .build();
        Ok(task)
    }

    /// Converts the task data into a full Task with decoded arguments.
    pub fn into_full_task<Args: 'static, C>(
        self,
    ) -> Result<Task<Args, RedisContext, Ulid>, RedisError>
    where
        C: Codec<Args, Compact = Vec<u8>>,
        C::Error: Into<BoxDynError>,
    {
        let args: Args = C::decode(self.data).map_err(|e| build_error(&e.into().to_string()))?;
        let context = RedisContext {
            max_attempts: self.max_attempts,
            lock_by: None,
            meta: self.meta,
        };
        let task = Task::builder(args)
            .with_task_id(self.task_id)
            .with_status(self.status)
            .with_attempt(Attempt::new_with_value(self.attempts as usize))
            .with_ctx(context)
            .build();
        Ok(task)
    }
}

/// Extracts a &str from a redis::Value, returning an error if the value is not a bulk string.
pub fn str_from_val<'a>(val: &'a redis::Value, field: &'a str) -> Result<&'a str, RedisError> {
    match val {
        redis::Value::BulkString(bytes) => {
            str::from_utf8(bytes).map_err(|_| build_error(&format!("{field} not UTF-8")))
        }
        _ => Err(build_error(&format!("{field} not bulk string"))),
    }
}

/// Parses a u32 from a redis::Value
pub fn parse_u32(value: &Value, field: &str) -> Result<u32, RedisError> {
    match value {
        Value::BulkString(bytes) => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| build_error(&format!("{field} not UTF-8")))?;
            s.parse::<u32>()
                .map_err(|_| build_error(&format!("{field} not u32")))
        }
        _ => Err(build_error(&format!("{field} not bulk string"))),
    }
}

/// Deserializes task data and metadata from Redis values.
pub fn deserialize_with_meta<'a>(
    data: &'a [redis::Value],
) -> Result<Vec<CompactTask<'a>>, RedisError> {
    if data.len() != 2 {
        return Err(build_error("Expected two elements: job_data and metadata"));
    }
    let job_data_list = match &data[0] {
        redis::Value::Array(vals) => vals,
        _ => return Err(build_error("Expected job_data to be array")),
    };

    let meta_list = match &data[1] {
        redis::Value::Array(vals) => vals,
        _ => return Err(build_error("Expected metadata to be array")),
    };

    if job_data_list.len() != meta_list.len() {
        return Err(build_error("Job data and metadata length mismatch"));
    }

    let mut result = Vec::with_capacity(job_data_list.len());

    for (data_val, meta_val) in job_data_list.iter().zip(meta_list.iter()) {
        let data = match data_val {
            redis::Value::BulkString(bytes) => bytes,
            _ => return Err(build_error("Invalid job data format")),
        };

        let meta_fields = match meta_val {
            redis::Value::Array(fields) => fields,
            _ => return Err(build_error("Invalid metadata format")),
        };

        let task_id = TaskId::from_str(str_from_val(&meta_fields[0], "task_id")?)
            .map_err(|e| build_error(&e.to_string()))?;
        let attempts = parse_u32(&meta_fields[2], "attempts")?;
        let max_attempts = parse_u32(&meta_fields[4], "max_attempts")?;
        let status = Status::from_str(str_from_val(&meta_fields[6], "status")?)
            .map_err(|e| build_error(&e.to_string()))?;

        let meta = meta_fields[7..]
            .chunks(2)
            .filter_map(|chunk| {
                if chunk.len() == 2 {
                    Some((
                        str_from_val(&chunk[0], "meta key").ok()?,
                        str_from_val(&chunk[1], "meta value").ok()?,
                    ))
                } else {
                    None
                }
            })
            .try_fold(serde_json::Map::new(), |mut acc, (key, val)| {
                acc.insert(
                    key.to_owned(),
                    serde_json::from_str(val).unwrap_or_default(),
                );
                Ok::<_, RedisError>(acc)
            })?;

        result.push(CompactTask {
            task_id,
            data,
            attempts,
            max_attempts,
            status,
            meta,
        });
    }

    Ok(result)
}
