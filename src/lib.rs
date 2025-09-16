#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! apalis storage using Redis as a backend
//! ```rust,no_run
//! use apalis::prelude::*;
//! use apalis_redis::{RedisStorage, Config};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! struct Email {
//!     to: String,
//! }
//!
//! async fn send_email(job: Email) -> Result<(), Error> {
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let redis_url = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
//!     let conn = apalis_redis::connect(redis_url).await.expect("Could not connect");
//!     let storage = RedisStorage::new(conn);
//!     let worker = WorkerBuilder::new("tasty-pear")
//!         .backend(storage.clone())
//!         .build(send_email);
//!
//!     worker.run().await;
//! }
//! ```

use std::{
    any::type_name,
    collections::HashMap,
    convert::Infallible,
    future::Future,
    io,
    marker::PhantomData,
    pin::Pin,
    str::FromStr,
    sync::{Arc, LazyLock, Mutex, OnceLock},
    task::{Context, Poll},
    time::{Duration, SystemTime},
    usize,
};

use apalis_core::{
    backend::{
        Backend, TaskSink, TaskStream,
        codec::{Codec, json::JsonCodec},
        shared::MakeShared,
    },
    error::BoxDynError,
    task::{Parts, Task, attempt::Attempt, status::Status, task_id::TaskId},
    task_fn::from_request::FromRequest,
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
};
use chrono::Utc;
use event_listener::Event;
use futures::{
    FutureExt, Sink, StreamExt, TryFuture,
    future::{BoxFuture, select},
    stream::{self, BoxStream},
};
use redis::{
    AsyncConnectionConfig, Client, ErrorKind, PushInfo, Script, Value,
    aio::{ConnectionLike, MultiplexedConnection},
};
// mod expose;
// mod storage;
mod ack;
mod config;
mod context;
mod fetcher;
mod shared;
mod sink;

pub use redis::{RedisError, aio::ConnectionManager};

use ulid::Ulid;

use crate::{ack::RedisAck, config::RedisConfig, context::RedisContext, sink::RedisSink};

/// Represents a [Backend] that uses Redis for storage.
#[doc = "# Feature Support\n"]
pub struct RedisStorage<Args, Conn = ConnectionManager, C = JsonCodec<Vec<u8>>> {
    conn: Conn,
    job_type: PhantomData<Args>,
    config: RedisConfig,
    codec: PhantomData<C>,
    poller: Arc<Event>,
    sink: RedisSink<Args, C, Conn>,
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

impl<Args, Conn, C> Backend<Args> for RedisStorage<Args, Conn, C>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: Clone + ConnectionLike + Send + Sync + 'static,
    C: Codec<Args, Compact = Vec<u8>> + Unpin + Send + 'static,
    C::Error: Into<BoxDynError>,
{
    type Stream = TaskStream<Task<Args, RedisContext, Ulid>, RedisError>;

    type IdType = Ulid;

    type Error = RedisError;
    type Layer = AcknowledgeLayer<RedisAck<Conn, C>>;

    type Codec = C;

    type Context = RedisContext;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let keep_alive = *self.config.get_keep_alive();

        let config = self.config.clone();
        let worker_id = worker.name().to_owned();
        let conn = self.conn.clone();

        let stream = stream::unfold(
            (keep_alive, worker_id, conn, config),
            |(keep_alive, worker_id, mut conn, config)| async move {
                apalis_core::timer::sleep(keep_alive).await;
                let register_consumer =
                    redis::Script::new(include_str!("../lua/register_consumer.lua"));
                let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker_id);
                let consumers_set = config.consumers_set();

                let now: i64 = Utc::now().timestamp();

                let res = register_consumer
                    .key(consumers_set)
                    .arg(now)
                    .arg(inflight_set)
                    .invoke_async::<()>(&mut conn)
                    .await;
                Some((res, (keep_alive, worker_id, conn, config)))
            },
        );
        stream.boxed()
    }
    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(RedisAck::new(&self.conn, &self.config))
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let worker = worker.clone();
        let worker_id = worker.name().to_owned();
        let config = self.config.clone();
        let mut conn = self.conn.clone();
        let event_listener = self.poller.clone();
        let register = futures::stream::once(async move {
            let register_consumer =
                redis::Script::new(include_str!("../lua/register_consumer.lua"));
            let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker_id);
            let consumers_set = config.consumers_set();

            let now: i64 = Utc::now().timestamp();

            register_consumer
                .key(consumers_set)
                .arg(now)
                .arg(inflight_set)
                .invoke_async::<()>(&mut conn)
                .await?;
            Ok(None)
        })
        .filter_map(
            |res: Result<Option<Task<Args, RedisContext>>, RedisError>| async move {
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

fn build_error(message: &str) -> RedisError {
    RedisError::from(io::Error::new(io::ErrorKind::InvalidData, message))
}

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use futures::{SinkExt, TryFutureExt, future::ready};
    use redis::{Client, ConnectionInfo, IntoConnectionInfo, parse_redis_url};

    use apalis_core::{
        backend::{TaskSink, memory::MemoryStorage},
        task::{builder::TaskBuilder, data::Data},
        task_fn::{self, TaskFn},
        worker::{
            builder::WorkerBuilder,
            ext::{
                ack::AcknowledgementExt, circuit_breaker::CircuitBreaker,
                event_listener::EventListenerExt, long_running::LongRunningExt,
            },
        },
    };
    use tokio::task::JoinError;

    use crate::shared::SharedRedisStorage;

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn basic_worker() {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let mut backend = RedisStorage::new_with_config(
            conn,
            RedisConfig::default()
                .set_namespace("redis_basic_worker")
                .set_buffer_size(100),
        );
        for i in 0..ITEMS {
            let req = TaskBuilder::new(i).build();
            backend.send(req).await.unwrap();
        }

        async fn task(
            task: u32,
            meta: RedisContext,
            wrk: WorkerContext,
        ) -> Result<(), BoxDynError> {
            let handle = std::thread::current();
            // println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
            if task == ITEMS - 1 {
                wrk.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                // println!("CTX {:?}, On Event = {:?}", ctx.get_service(), ev);
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

        let client = Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let mut backend = RedisStorage::new_with_codec::<Bincode>(
            conn,
            RedisConfig::default()
                .set_namespace("redis_bincode_worker")
                .set_buffer_size(100),
        );

        for i in 0..ITEMS {
            let req = TaskBuilder::new(i).build();
            backend.send(req).await.unwrap();
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
            Ok("Worrker".to_owned())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                // println!("CTX {:?}, On Event = {:?}", ctx.get_service(), ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn shared_workers() {
        let client = Client::open("redis://127.0.0.1/?protocol=resp3").unwrap();
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

    // #[tokio::test]
    // async fn stepped_workflow() {
    //     async fn task1(job: u32) -> Result<GoTo<()>, BoxDynError> {
    //         println!("{job}");
    //         Ok(GoTo::Next(()))
    //     }

    //     async fn task2(_: ()) -> Result<GoTo<usize>, BoxDynError> {
    //         Ok(GoTo::Next(1))
    //     }

    //     async fn task3(
    //         job: usize,
    //         wrk: WorkerContext,
    //         ctx: Data<Parts<RedisContext>>,
    //     ) -> Result<GoTo<()>, io::Error> {
    //         wrk.stop().unwrap();
    //         println!("{job}");
    //         dbg!(&ctx);
    //         Ok(GoTo::Done(()))
    //     }

    //     async fn recover<Req: Debug>(req: Req) -> Result<(), BoxDynError> {
    //         println!("Recovering request: {req:?}");
    //         Err("Unable to recover".into())
    //     }

    //     let steps = StepBuilder::new()
    //         .step_fn(task1)
    //         .step_fn(task2)
    //         .step_fn(task3)
    //         .fallback(recover);

    //     // assert_stepped::<RedisStorage<StepRequest<Vec<u8>>>, _, _, _, _, _, _, _>(&steps);

    //     let client = Client::open("redis://127.0.0.1/").unwrap();
    //     let conn = client.get_connection_manager().await.unwrap();
    //     let backend = RedisStorage::new_with_config(
    //         conn,
    //         RedisConfig::default().set_namespace("redis_workflow"),
    //     );
    //     let mut sink = backend.sink();
    //     let _res = sink.push_start(0u32).await.unwrap();

    //     let worker = WorkerBuilder::new("rango-tango")
    //         .backend(backend)
    //         .on_event(|ctx, ev| {
    //             use apalis_core::worker::event::Event;
    //             println!("Worker {:?}, On Event = {:?}", ctx.name(), ev);
    //             if matches!(ev, Event::Error(_)) {
    //                 ctx.stop().unwrap();
    //             }
    //         })
    //         .build(steps);
    //     let mut event_stream = worker.stream();
    //     while let Some(Ok(ev)) = event_stream.next().await {
    //         println!("On Event = {:?}", ev);
    //     }
    // }
}
