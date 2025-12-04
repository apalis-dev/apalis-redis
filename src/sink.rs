use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

use apalis_core::task::Task;
use chrono::Utc;
use futures::{
    FutureExt, Sink,
    future::{BoxFuture, Shared},
};
use redis::{
    RedisError, Script,
    aio::{ConnectionLike, ConnectionManager},
};
use ulid::Ulid;

use crate::{RedisStorage, build_error, config::RedisConfig, context::RedisContext};

type SinkFuture = Shared<BoxFuture<'static, Result<(u32, u32), Arc<RedisError>>>>;

/// A Redis sink that batches task pushes to Redis.
#[derive(Debug)]
pub struct RedisSink<Args, Encode, Conn = ConnectionManager> {
    _args: PhantomData<(Args, Encode)>,
    config: RedisConfig,
    pending: Vec<Task<Vec<u8>, RedisContext, Ulid>>,
    conn: Conn,
    invoke_future: Option<SinkFuture>,
}
impl<Args, Conn: Clone, Encode> RedisSink<Args, Encode, Conn> {
    /// Creates a new Redis sink.
    pub fn new(conn: &Conn, config: &RedisConfig) -> Self {
        Self {
            conn: conn.clone(),
            config: config.clone(),
            _args: PhantomData,
            invoke_future: None,
            pending: Vec::new(),
        }
    }
}

impl<Args, Conn: Clone, Cdc: Clone> Clone for RedisSink<Args, Cdc, Conn> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            config: self.config.clone(),
            _args: PhantomData,
            invoke_future: None,
            pending: Vec::new(),
        }
    }
}

static BATCH_PUSH_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/batch_push.lua")));

/// Pushes tasks to Redis using a batch Lua script.
pub async fn push_tasks<Conn: ConnectionLike>(
    tasks: Vec<Task<Vec<u8>, RedisContext, Ulid>>,
    config: RedisConfig,
    mut conn: Conn,
) -> Result<(u32, u32), Arc<RedisError>> {
    let mut batch = BATCH_PUSH_SCRIPT.key(config.job_data_hash());
    let mut script = batch
        .key(config.active_jobs_list())
        .key(config.signal_list())
        .key(config.job_meta_hash())
        .key(config.scheduled_jobs_set());
    for request in tasks {
        let task_id = request
            .parts
            .task_id
            .map(|s| s.to_string())
            .unwrap_or(Ulid::new().to_string());
        let attempts = request.parts.attempt.current() as u32;
        let max_attempts = request.parts.ctx.max_attempts;
        let job = request.args;
        let meta = serde_json::to_string(&request.parts.ctx.meta)
            .map_err(|e| Arc::new(build_error(&e.to_string())))?;
        let run_at = request.parts.run_at;
        let current = Utc::now().timestamp() as u64;
        // Ensure run_at is not in the past
        let run_at = if run_at > current { run_at } else { current };

        script = script
            .arg(task_id)
            .arg(job)
            .arg(attempts)
            .arg(max_attempts)
            .arg(meta)
            .arg(run_at);
    }

    script
        .invoke_async::<(u32, u32)>(&mut conn)
        .await
        .map_err(Arc::new)
}

impl<Args, Cdc, Conn> Sink<Task<Vec<u8>, RedisContext, Ulid>> for RedisStorage<Args, Conn, Cdc>
where
    Args: Unpin,
    Conn: ConnectionLike + Unpin + Send + Clone + 'static,
    Cdc: Unpin,
{
    type Error = RedisError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Task<Vec<u8>, RedisContext, Ulid>,
    ) -> Result<(), Self::Error> {
        let this = Pin::get_mut(self);
        this.sink.pending.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::get_mut(self);

        // If there's no in-flight Redis future and we have pending items, build the future
        if this.sink.invoke_future.is_none() && !this.sink.pending.is_empty() {
            let tasks: Vec<_> = this.sink.pending.drain(..).collect();
            let fut = push_tasks(tasks, this.config.clone(), this.conn.clone());

            this.sink.invoke_future = Some(fut.boxed().shared());
        }

        // If we have a future in flight, poll it
        if let Some(fut) = &mut this.sink.invoke_future {
            match fut.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    // Clear the future after it completes
                    this.sink.invoke_future = None;

                    // Propagate the Redis result
                    Poll::Ready(result.map(|_| ()).map_err(|e| Arc::into_inner(e).unwrap()))
                }
            }
        } else {
            // No pending work, flush is complete
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Task<Vec<u8>, RedisContext, Ulid>>::poll_flush(self, cx)
    }
}
