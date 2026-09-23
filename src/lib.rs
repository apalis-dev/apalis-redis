#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
use std::{
    io,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        finalize::Durable,
        persistence::{Persisted, TaskPersistLayer},
    },
    features_table,
    task::Task,
    worker::context::WorkerContext,
};

use futures::Sink;

pub use redis::{
    Client, IntoConnectionInfo, RedisError, aio::ConnectionLike, aio::ConnectionManager,
};

mod config;

/// Raw queries used to make low level redis calls
pub mod queries;

/// Factory utilities for building [`RedisStorage`] instances.
pub mod factory;

use serde_json::Value;
use ulid::Ulid;

pub use crate::config::Config;

pub use crate::{error::Error, persist::RedisPersistence};

mod persist;

mod error;

/// A Redis task type alias
pub type RedisTask<Args = Vec<u8>> = Task<Args>;

/// Represents a [Backend] that uses Redis for storage.
///
#[doc = "# Feature Support\n"]
#[doc = features_table! {
    setup = r#"
    # {
    #    use apalis_redis::RedisStorage;
    #    use std::env;
    #    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
    #    let conn = apalis_redis::connect(redis_url).await.expect("Could not connect");
    #    RedisStorage::<u32>::new(conn)
    # };
    "#,
    TaskSink => supported("Ability to push new tasks", true),
    MakeShared => supported("Share the same connection across multiple workers", false),
    Workflow => supported("Supports workflows and orchestration", true),
    WebUI => supported("Supports `apalis-board` for monitoring and managing tasks", true),
    WaitForCompletion => supported("Wait for tasks to complete without blocking", true),
    Serialization => supported("Supports multiple serialization formats such as JSON and MessagePack", false),
    RegisterWorker => supported("Allow registering a worker with the backend", false),
    ResumeAbandoned => supported("Resume abandoned tasks", false),
}]
#[derive(Debug)]
pub struct RedisStorage<Args, Conn = ConnectionManager>
where
    Conn: ConnectionLike + Send + Sync + 'static + Clone,
{
    job_type: PhantomData<Args>,
    persist: Persisted<RedisPersistence<Conn>>,
    codec: JsonCodec,
}

impl<Args, Conn> Clone for RedisStorage<Args, Conn>
where
    Conn: ConnectionLike + Send + Sync + Clone + 'static,
{
    fn clone(&self) -> Self {
        Self {
            job_type: PhantomData,
            persist: self.persist.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<T, Conn> RedisStorage<T, Conn>
where
    Conn: ConnectionLike + Send + Sync + 'static + Clone,
{
    /// Start a new connection
    pub fn new(conn: Conn) -> RedisStorage<T, Conn> {
        let config = Config::default().queue(std::any::type_name::<T>());
        RedisStorage {
            job_type: PhantomData,
            persist: Persisted::new(RedisPersistence { config, conn }),
            codec: JsonCodec::default(),
        }
    }

    /// Customize the backend
    pub fn with_config(mut self, config: Config) -> RedisStorage<T, Conn> {
        self.persist.config = config;
        RedisStorage {
            job_type: PhantomData,
            persist: self.persist,
            codec: self.codec,
        }
    }

    /// Get current connection
    pub fn get_connection(&mut self) -> &mut Conn {
        &mut self.persist.conn
    }

    /// Get the config used by the storage
    pub fn get_config(&self) -> &Config {
        &self.persist.config
    }
}

impl<Args, Conn> Backend for RedisStorage<Args, Conn>
where
    Conn: ConnectionLike + Send + Sync + 'static + Clone,
{
    type Task = RedisTask;

    type Error = Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        let heartbeat_interval = self.persist.config.heartbeat_interval;
        self.persist.poll_ready(cx, worker, heartbeat_interval)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.persist.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.persist.poll_close(cx, worker)
    }
}

impl<Args, Conn> BackendConfig for RedisStorage<Args, Conn>
where
    Conn: ConnectionLike + Send + Sync + 'static + Clone,
{
    type Args = Args;

    type Id = Ulid;

    type Kind = Durable;

    type Config = Config;

    type Layer = TaskPersistLayer<JsonCodec<Value>, Value>;

    fn config(&self) -> &Self::Config {
        &self.persist.config
    }

    fn middleware(&mut self, _worker: &mut WorkerContext) -> Self::Layer {
        self.persist
            .layer(JsonCodec::<Value>::default(), self.config().batch_size)
            .persist_results(self.config().persist_results)
            .lock_tasks(self.config().lock_tasks)
    }
}

impl<Args, Conn> WireFormatBackend for RedisStorage<Args, Conn>
where
    Conn: ConnectionLike + Send + Sync + 'static + Clone,
{
    type Codec = JsonCodec;

    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<Args, Conn> Sink<RedisTask> for RedisStorage<Args, Conn>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: ConnectionLike + Send + Sync + 'static + Clone + Unpin,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_ready(Pin::new(&mut self.get_mut().persist), cx)
    }

    fn start_send(self: Pin<&mut Self>, item: RedisTask) -> Result<(), Self::Error> {
        Sink::start_send(Pin::new(&mut self.get_mut().persist), item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_flush(Pin::new(&mut self.get_mut().persist), cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_close(Pin::new(&mut self.get_mut().persist), cx)
    }
}

/// Shorthand to create a client and connect
pub async fn connect<S: IntoConnectionInfo>(redis: S) -> Result<ConnectionManager, RedisError> {
    let client = Client::open(redis.into_connection_info()?)?;
    let conn = client.get_connection_manager().await?;
    Ok(conn)
}

fn build_error(message: &str) -> RedisError {
    RedisError::from(io::Error::new(io::ErrorKind::InvalidData, message))
}

#[cfg(test)]
mod tests {
    use apalis_codec::msgpack::MsgPackCodec;
    use apalis_core::{
        backend::{ext::BackendExt, factory::BackendFactory},
        error::BoxDynError,
        task::{builder::TaskBuilder, context::TaskContext},
        worker::ext::parallelize::ParallelizeExt,
    };
    use apalis_workflow::{SteppedFlow, WorkflowSink};

    use redis::Client;
    use std::{env, time::Duration};

    use apalis_core::{
        backend::TaskSink,
        worker::{builder::WorkerBuilder, ext::event_listener::EventListenerExt},
    };

    use crate::factory::RedisStorageFactory;

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn basic_worker() {
        let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let config = Config::default()
            .queue("redis_basic_worker")
            .batch_size(100);
        let mut backend = RedisStorage::new(conn).with_config(config);
        for i in 0..ITEMS {
            backend.push(i).await.unwrap();
        }

        async fn task(task: u32, ctx: TaskContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
            let handle = std::thread::current();
            println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
            if task == ITEMS - 1 {
                wrk.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn basic_worker_msgpack() {
        let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let config = Config::default().queue("msgpack-queue").batch_size(100);
        let mut backend = RedisStorage::new(conn)
            .with_config(config)
            .with_codec(MsgPackCodec::default());

        for i in 0..ITEMS {
            let req = TaskBuilder::new(i).build();
            backend.push_task(req).await.unwrap();
        }

        async fn task(
            task: u32,
            meta: TaskContext,
            wrk: WorkerContext,
        ) -> Result<String, BoxDynError> {
            let handle = std::thread::current();
            println!("{task:?}, {meta:?}, Thread: {:?}", handle.id());
            if task == ITEMS - 1 {
                wrk.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok("Worker".to_owned())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .parallelize(tokio::spawn)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn shared_workers() {
        let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
        let mut store = RedisStorageFactory::new(client).await.unwrap();

        let mut string_store = store.create().unwrap();
        let mut int_store = store.create().unwrap();

        for i in 0..ITEMS {
            string_store.push(format!("ITEM: {i}")).await.unwrap();
            int_store.push(i).await.unwrap();
        }

        async fn task(job: u32, ctx: WorkerContext) -> Result<usize, BoxDynError> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if job == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(job as usize)
        }

        let int_worker = WorkerBuilder::new("rango-tango-int")
            .backend(int_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task)
            .run();

        let string_worker = WorkerBuilder::new("rango-tango-string")
            .backend(string_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(|req: String, ctx: WorkerContext| async move {
                tokio::time::sleep(Duration::from_millis(3)).await;
                println!("{req}");
                if req.ends_with(&(ITEMS - 1).to_string()) {
                    ctx.stop().unwrap();
                }
            })
            .run();
        let _ = futures::future::try_join(int_worker, string_worker)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn workflow() {
        async fn task1(job: u32) -> Result<Vec<u32>, BoxDynError> {
            Ok((job..2).collect())
        }

        async fn task2(_: Vec<u32>) -> Result<usize, BoxDynError> {
            Ok(42)
        }

        async fn task3(_: usize, wrk: WorkerContext, _: TaskContext) -> Result<(), io::Error> {
            wrk.stop().unwrap();
            Ok(())
        }

        let work_flow = SteppedFlow::new("sample-workflow")
            .and_then(task1)
            .delay_for(Duration::from_millis(1000))
            .and_then(task2)
            .and_then(task3);

        let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let config = Config::default().queue("workflow:sample");
        let mut backend = RedisStorage::new(conn).with_config(config);

        backend.push_start(0u32).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("Worker {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(work_flow);
        worker.run().await.unwrap();
    }
}
