use std::marker::PhantomData;

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::codec::Codec, error::BoxDynError, task::Parts, worker::ext::ack::Acknowledge,
};
use chrono::Utc;
use futures::{FutureExt, future::BoxFuture};
use redis::{
    RedisError, Script,
    aio::{ConnectionLike, ConnectionManager},
};
use ulid::Ulid;

use crate::{build_error, config::RedisConfig, context::RedisContext};

/// A Redis acknowledgment Layer
#[derive(Debug)]
pub struct RedisAck<Conn = ConnectionManager, Encode = JsonCodec<Vec<u8>>> {
    conn: Conn,
    config: RedisConfig,
    _codec: PhantomData<Encode>,
}
impl<Conn: Clone, Encode> RedisAck<Conn, Encode> {
    /// Creates a new Redis acknowledgment layer.
    pub fn new(conn: &Conn, config: &RedisConfig) -> Self {
        Self {
            conn: conn.clone(),
            config: config.clone(),
            _codec: PhantomData,
        }
    }
}

impl<Conn, Encode> Clone for RedisAck<Conn, Encode>
where
    Conn: Clone,
    RedisConfig: Clone,
{
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            config: self.config.clone(),
            _codec: PhantomData,
        }
    }
}

impl<Conn: ConnectionLike + Send + Clone + 'static, Res, Encode>
    Acknowledge<Res, RedisContext, Ulid> for RedisAck<Conn, Encode>
where
    Encode: Codec<Res, Compact = Vec<u8>>,
{
    type Future = BoxFuture<'static, Result<(), RedisError>>;

    type Error = RedisError;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<RedisContext, Ulid>,
    ) -> Self::Future {
        let task_id = parts.task_id.unwrap().to_string();
        let attempt = parts.attempt.current();
        let worker_id = &parts.ctx.lock_by.as_ref().unwrap();
        let inflight_set = format!("{}:{}", self.config.inflight_jobs_set(), worker_id);
        let done_jobs_set = self.config.done_jobs_set();
        let dead_jobs_set = self.config.dead_jobs_set();
        let job_meta_hash = self.config.job_meta_hash();
        let status = if res.is_ok() { "ok" } else { "err" };
        let res = res.as_ref().map_err(|e| e.to_string().bytes().collect());

        let result_data = match res {
            Ok(res) => Encode::encode(res)
                .map_err(|_| build_error("could not encode result"))
                .unwrap(),
            Err(e) => e,
        };
        let timestamp = Utc::now().timestamp();
        let script = Script::new(include_str!("../lua/ack_job.lua"));
        let mut conn = self.conn.clone();

        async move {
            let mut script = script.key(inflight_set);
            let _ = script
                .key(done_jobs_set)
                .key(dead_jobs_set)
                .key(job_meta_hash)
                .arg(task_id)
                .arg(timestamp)
                .arg(result_data)
                .arg(status)
                .arg(attempt)
                .invoke_async::<u32>(&mut conn)
                .boxed()
                .await?;
            Ok(())
        }
        .boxed()
    }
}
