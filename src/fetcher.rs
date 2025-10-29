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
    pub(super) async fn fetch_next(
        worker: &WorkerContext,
        config: &RedisConfig,
        conn: &mut Conn,
    ) -> Result<Vec<Task<Args, RedisContext, Ulid>>, RedisError> {
        let fetch_jobs = redis::Script::new(include_str!("../lua/get_jobs.lua"));
        let consumers_set = config.consumers_set();
        let active_jobs_list = config.active_jobs_list();
        let job_data_hash = config.job_data_hash();
        let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker.name());
        let signal_list = config.signal_list();

        let result = fetch_jobs
            .key(&consumers_set)
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
                let tasks = deserialize_with_meta(jobs.try_into().map_err(|c: Vec<Value>| {
                    build_error(&format!("Expected 2 items, found {}", c.len()))
                })?)?;
                for task in tasks {
                    let args =
                        if std::any::TypeId::of::<Args>() == std::any::TypeId::of::<Vec<u8>>() {
                            // SAFETY: We've verified that Args and CompactType are the same type.
                            // We use ptr::read to move the value out without calling drop on self.job.
                            // Then we use mem::forget to prevent self from being dropped (which would
                            // try to drop self.job again, causing a double free).
                            unsafe {
                                let job_ptr = &task.data as *const Vec<u8> as *const Args;
                                let args = std::ptr::read(job_ptr);
                                std::mem::forget(task.data);
                                args
                            }
                        } else {
                            let args: Args = C::decode(&task.data)
                                .map_err(|e| build_error(&e.into().to_string()))?;
                            args
                        };
                    let context = RedisContext {
                        max_attempts: task.max_attempts,
                        lock_by: Some(worker.name().to_owned()),
                        meta: task.meta,
                    };
                    let task = Task::builder(args)
                        .with_task_id(task.task_id)
                        .with_status(task.status)
                        .with_attempt(Attempt::new_with_value(task.attempts as usize))
                        .with_ctx(context)
                        .build();
                    processed.push(task)
                }
                Ok(processed)
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug)]
struct TaskWithMeta {
    pub data: Vec<u8>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub status: Status,
    pub task_id: TaskId<Ulid>,
    pub meta: serde_json::Map<String, serde_json::Value>,
}

fn parse_u32(value: &Value, field: &str) -> Result<u32, RedisError> {
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

fn deserialize_with_meta(data: [redis::Value; 2]) -> Result<Vec<TaskWithMeta>, RedisError> {
    let [job_data_list, meta_list] = data;
    let job_data_list = match job_data_list {
        redis::Value::Array(vals) => vals,
        _ => return Err(build_error("Expected job_data to be array")),
    };

    let meta_list = match meta_list {
        redis::Value::Array(vals) => vals,
        _ => return Err(build_error("Expected metadata to be array")),
    };

    if job_data_list.len() != meta_list.len() {
        return Err(build_error("Job data and metadata length mismatch"));
    }

    let mut result = Vec::with_capacity(job_data_list.len());

    for (data_val, meta_val) in job_data_list.into_iter().zip(meta_list.into_iter()) {
        let data = match data_val {
            redis::Value::BulkString(bytes) => bytes,
            _ => return Err(build_error("Invalid job data format")),
        };

        let meta_fields = match meta_val {
            redis::Value::Array(fields) => fields,
            _ => return Err(build_error("Invalid metadata format")),
        };

        fn str_from_val<'a>(val: &'a redis::Value, field: &'a str) -> Result<&'a str, RedisError> {
            match val {
                redis::Value::BulkString(bytes) => {
                    str::from_utf8(bytes).map_err(|_| build_error(&format!("{field} not UTF-8")))
                }
                _ => Err(build_error(&format!("{field} not bulk string"))),
            }
        }

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

        result.push(TaskWithMeta {
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
