use std::{
    marker::PhantomData,
    pin::Pin,
    sync::LazyLock,
    task::{Context, Poll},
};

use apalis_core::{backend::codec::Codec, task::Task};
use futures::{FutureExt, Sink, future::BoxFuture};
use redis::{
    ErrorKind, RedisError, Script,
    aio::{ConnectionLike, ConnectionManager},
};
use ulid::Ulid;

use crate::{RedisStorage, config::RedisConfig, context::RedisContext};

pub struct RedisSink<Args, Encode, Conn = ConnectionManager> {
    _args: PhantomData<(Args, Encode)>,
    config: RedisConfig,
    pending: Vec<Task<Vec<u8>, RedisContext, Ulid>>,
    conn: Conn,
    invoke_future: Option<BoxFuture<'static, Result<u32, RedisError>>>,
}
impl<Args, Conn: Clone, Encode> RedisSink<Args, Encode, Conn> {
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

static BATCH_PUSH_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/batch_push.lua")));
async fn push_tasks<Conn: ConnectionLike>(
    tasks: Vec<Task<Vec<u8>, RedisContext, Ulid>>,
    config: RedisConfig,
    mut conn: Conn,
) -> Result<u32, RedisError> {
    let mut batch = BATCH_PUSH_SCRIPT.key(config.job_data_hash());
    let mut script = batch
        .key(config.active_jobs_list())
        .key(config.signal_list())
        .key(config.job_meta_hash());
    for request in tasks {
        let task_id = request
            .parts
            .task_id
            .map(|s| s.to_string())
            .unwrap_or(Ulid::new().to_string());
        let attempts = request.parts.attempt.current() as u32;
        let max_attempts = request.parts.ctx.max_attempts;
        let job = request.args;
        script = script.arg(task_id).arg(job).arg(attempts).arg(max_attempts);
    }

    script.invoke_async::<u32>(&mut conn).await
}

impl<Args, Cdc, Conn> Sink<Task<Args, RedisContext, Ulid>> for RedisStorage<Args, Conn, Cdc>
where
    Args: Unpin,
    Cdc: Unpin + Codec<Args, Compact = Vec<u8>>,
    Conn: ConnectionLike + Unpin + Send + Clone + 'static,
{
    type Error = RedisError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Task<Args, RedisContext, Ulid>,
    ) -> Result<(), Self::Error> {
        let this = Pin::get_mut(self);
        let req = item
            .try_map(|req| Cdc::encode(&req))
            .map_err(|_| RedisError::from((ErrorKind::IoError, "Encoding error")))?;
        this.sink.pending.push(req);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::get_mut(self);

        // If there's no in-flight Redis future and we have pending items, build the future
        if this.sink.invoke_future.is_none() && !this.sink.pending.is_empty() {
            let tasks: Vec<_> = this.sink.pending.drain(..).collect();
            let fut = push_tasks(tasks, this.config.clone(), this.conn.clone());

            this.sink.invoke_future = Some(fut.boxed());
        }

        // If we have a future in flight, poll it
        if let Some(fut) = &mut this.sink.invoke_future {
            match fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    // ✅ Clear the future after it completes
                    this.sink.invoke_future = None;

                    // Propagate the Redis result
                    Poll::Ready(result.map(|_| ()))
                }
            }
        } else {
            // No pending work, flush is complete
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Task<Args, RedisContext, Ulid>>::poll_flush(self, cx)
    }
}
