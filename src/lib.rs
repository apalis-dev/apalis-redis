#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
use std::{any::type_name, io, marker::PhantomData, sync::Arc};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue},
    error::BoxDynError,
    features_table,
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use chrono::Utc;
use event_listener::Event;
use futures::{
    FutureExt, StreamExt,
    future::select,
    stream::{self, BoxStream},
};
use redis::aio::ConnectionLike;
pub use redis::{Client, IntoConnectionInfo, aio};

mod ack;
mod config;
mod context;
mod fetcher;
mod queries;
/// Shared utilities for Redis storage.
pub mod shared;
/// Redis sink module.
pub mod sink;

pub use redis::{RedisError, aio::ConnectionManager};

use ulid::Ulid;

pub use crate::{
    ack::RedisAck, config::RedisConfig, context::RedisContext, fetcher::*, sink::RedisSink,
};

/// A Redis task type alias
pub type RedisTask<Args> = Task<Args, RedisContext, Ulid>;

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
    #    RedisStorage::new(conn)
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
pub struct RedisStorage<Args, Conn = ConnectionManager, C = JsonCodec<Vec<u8>>> {
    conn: Conn,
    job_type: PhantomData<Args>,
    config: RedisConfig,
    codec: PhantomData<C>,
    poller: Arc<Event>,
    sink: RedisSink<Args, C, Conn>,
}

impl<Args, Conn: Clone, Cdc: Clone> Clone for RedisStorage<Args, Conn, Cdc> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            job_type: PhantomData,
            config: self.config.clone(),
            codec: PhantomData,
            poller: self.poller.clone(),
            sink: self.sink.clone(),
        }
    }
}

impl<T, Conn: Clone> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
    /// Start a new connection
    pub fn new(conn: Conn) -> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
        Self::new_with_codec::<JsonCodec<Vec<u8>>>(
            conn,
            RedisConfig::default().set_namespace(type_name::<T>()),
        )
    }

    /// Start a connection with a custom config
    pub fn new_with_config(
        conn: Conn,
        config: RedisConfig,
    ) -> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
        Self::new_with_codec::<JsonCodec<Vec<u8>>>(conn, config)
    }

    /// Start a new connection providing custom config and a codec
    pub fn new_with_codec<K>(conn: Conn, config: RedisConfig) -> RedisStorage<T, Conn, K>
    where
        K: Sync + Send + 'static,
    {
        let sink = RedisSink::new(&conn, &config);
        RedisStorage {
            conn,
            job_type: PhantomData,
            config,
            codec: PhantomData::<K>,
            poller: Arc::new(Event::new()),
            sink,
        }
    }

    /// Get current connection
    pub fn get_connection(&self) -> &Conn {
        &self.conn
    }

    /// Get the config used by the storage
    pub fn get_config(&self) -> &RedisConfig {
        &self.config
    }
}

impl<Args, Conn, C> Backend for RedisStorage<Args, Conn, C>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: Clone + ConnectionLike + Send + Sync + 'static,
    C: Codec<Args, Compact = Vec<u8>> + Unpin + Send + 'static,
    C::Error: Into<BoxDynError>,
{
    type Args = Args;
    type Stream = TaskStream<Task<Args, RedisContext, Ulid>, RedisError>;

    type IdType = Ulid;

    type Error = RedisError;
    type Layer = AcknowledgeLayer<RedisAck<Conn, C>>;

    type Context = RedisContext;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let keep_alive = *self.config.get_keep_alive();

        let config = self.config.clone();
        let worker_id = worker.name().to_owned();
        let conn = self.conn.clone();
        let service = worker.get_service().to_owned();

        let keep_alive = stream::unfold(
            (
                keep_alive,
                worker_id.clone(),
                conn.clone(),
                config.clone(),
                service,
            ),
            |(keep_alive, worker_id, mut conn, config, service)| async move {
                apalis_core::timer::sleep(keep_alive).await;
                let register_worker =
                    redis::Script::new(include_str!("../lua/register_worker.lua"));
                let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker_id);
                let workers_set = config.workers_set();

                let now: i64 = Utc::now().timestamp();

                let res = register_worker
                    .key(workers_set)
                    .key("core::apalis::workers:metadata::")
                    .arg(now)
                    .arg(inflight_set)
                    .arg(config.get_keep_alive().as_secs())
                    .arg("RedisStorage")
                    .arg(&service)
                    .invoke_async::<()>(&mut conn)
                    .await;
                Some((res, (keep_alive, worker_id, conn, config, service)))
            },
        );

        let enqueue_scheduled = stream::unfold(
            (worker_id, conn, config),
            |(worker_id, mut conn, config)| async move {
                apalis_core::timer::sleep(*config.get_enqueue_scheduled()).await;
                let scheduled_jobs_set = config.scheduled_jobs_set();
                let active_jobs_list = config.active_jobs_list();
                let signal_list = config.signal_list();
                let now: i64 = Utc::now().timestamp();
                let enqueue_jobs =
                    redis::Script::new(include_str!("../lua/enqueue_scheduled_jobs.lua"));
                let res: Result<usize, _> = enqueue_jobs
                    .key(scheduled_jobs_set)
                    .key(active_jobs_list)
                    .key(signal_list)
                    .arg(now)
                    .arg(100)
                    .invoke_async(&mut conn)
                    .await;
                match res {
                    Ok(_) => Some((Ok(()), (worker_id, conn, config))),
                    Err(e) => Some((Err(e), (worker_id, conn, config))),
                }
            },
        );
        stream::select(keep_alive, enqueue_scheduled).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(RedisAck::new(&self.conn, &self.config))
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_compact(worker)
            .map(|a| match a {
                Ok(Some(task)) => Ok(Some(
                    task.try_map(|t| C::decode(&t))
                        .map_err(|e| build_error(&e.into().to_string()))?,
                )),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Conn, C> BackendExt for RedisStorage<Args, Conn, C>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: Clone + ConnectionLike + Send + Sync + 'static,
    C: Codec<Args, Compact = Vec<u8>> + Unpin + Send + 'static,
    C::Error: Into<BoxDynError>,
{
    type Compact = Vec<u8>;

    type Codec = C;

    type CompactStream = TaskStream<Task<Self::Compact, RedisContext, Ulid>, RedisError>;

    fn get_queue(&self) -> Queue {
        self.config.get_namespace().clone()
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        let worker = worker.clone();
        let worker_id = worker.name().to_owned();
        let config = self.config.clone();
        let mut conn = self.conn.clone();
        let event_listener = self.poller.clone();
        let service = worker.get_service().to_owned();
        let register = futures::stream::once(async move {
            let register_worker = redis::Script::new(include_str!("../lua/register_worker.lua"));
            let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker_id);
            let workers_set = config.workers_set();

            let now: i64 = Utc::now().timestamp();

            register_worker
                .key(workers_set)
                .key("core::apalis::workers:metadata::")
                .arg(now)
                .arg(inflight_set)
                .arg(config.get_keep_alive().as_secs())
                .arg("RedisStorage")
                .arg(service)
                .invoke_async::<()>(&mut conn)
                .await?;
            Ok(None)
        })
        .filter_map(
            |res: Result<Option<Task<Args, RedisContext, Ulid>>, RedisError>| async move {
                match res {
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                }
            },
        );
        let stream = stream::unfold(
            (
                worker,
                self.config.clone(),
                self.conn.clone(),
                event_listener,
            ),
            |(worker, config, mut conn, event_listener)| async {
                let interval = apalis_core::timer::sleep(*config.get_poll_interval()).boxed();
                let pub_sub = event_listener.listen().boxed();
                select(pub_sub, interval).await; // Pubsub or else interval
                let data = Self::fetch_next(&worker, &config, &mut conn).await;
                Some((data, (worker, config, conn, event_listener)))
            },
        )
        .flat_map(|res| match res {
            Ok(s) => {
                let stm: Vec<_> = s
                    .into_iter()
                    .map(|s| Ok::<_, RedisError>(Some(s)))
                    .collect();
                stream::iter(stm)
            }
            Err(e) => stream::iter(vec![Err(e)]),
        });
        register.chain(stream).boxed()
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
    use apalis_workflow::Workflow;
    use apalis_workflow::WorkflowSink;

    use redis::Client;
    use std::{env, time::Duration};

    use apalis_core::{
        backend::{TaskSink, shared::MakeShared},
        task::builder::TaskBuilder,
        worker::{
            builder::WorkerBuilder,
            ext::{event_listener::EventListenerExt, parallelize::ParallelizeExt},
        },
    };

    use crate::shared::SharedRedisStorage;

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn basic_worker() {
        let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let mut backend = RedisStorage::new_with_config(
            conn,
            RedisConfig::default()
                .set_namespace("redis_basic_worker")
                .set_buffer_size(100),
        );
        for i in 0..ITEMS {
            backend.push(i).await.unwrap();
        }

        async fn task(task: u32, ctx: RedisContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
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
    async fn basic_worker_bincode() {
        struct Bincode;

        impl<T: bincode::Decode<()> + bincode::Encode> Codec<T> for Bincode {
            type Compact = Vec<u8>;
            type Error = bincode::error::DecodeError;
            fn decode(val: &Self::Compact) -> Result<T, Self::Error> {
                bincode::decode_from_slice(val, bincode::config::standard()).map(|s| s.0)
            }

            fn encode(val: &T) -> Result<Self::Compact, Self::Error> {
                Ok(bincode::encode_to_vec(val, bincode::config::standard()).unwrap())
            }
        }

        let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let mut backend = RedisStorage::new_with_codec::<Bincode>(
            conn,
            RedisConfig::new("bincode-queue").set_buffer_size(100),
        );

        for i in 0..ITEMS {
            let req = TaskBuilder::new(i).build();
            backend.push_task(req).await.unwrap();
        }

        async fn task(
            task: u32,
            meta: RedisContext,
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
        let mut store = SharedRedisStorage::new(client).await.unwrap();

        let mut string_store = store
            .make_shared_with_config(
                RedisConfig::default()
                    .set_namespace("strrrrrr")
                    .set_poll_interval(Duration::from_secs(1))
                    .set_buffer_size(5),
            )
            .unwrap();
        let mut int_store = store
            .make_shared_with_config(
                RedisConfig::default()
                    .set_namespace("Intttttt")
                    .set_poll_interval(Duration::from_secs(2))
                    .set_buffer_size(5),
            )
            .unwrap();

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

        async fn task3(job: usize, wrk: WorkerContext, ctx: RedisContext) -> Result<(), io::Error> {
            wrk.stop().unwrap();
            println!("{job}");
            dbg!(&ctx);
            Ok(())
        }

        let work_flow = Workflow::new("sample-workflow")
            .and_then(task1)
            .delay_for(Duration::from_millis(1000))
            .and_then(task2)
            .and_then(task3);

        let client = Client::open(env::var("REDIS_URL").unwrap()).unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let mut backend = RedisStorage::new_with_config(
            conn,
            RedisConfig::default().set_namespace("redis_workflow"),
        );

        backend.push_start(0u32).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                use apalis_core::worker::event::Event;
                println!("Worker {:?}, On Event = {:?}", ctx.name(), ev);
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(work_flow);
        worker.run().await.unwrap();
    }
}
