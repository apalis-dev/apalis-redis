use std::{
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{
            AtomicBool,
            Ordering::{self},
        },
    },
    task::{Context, Poll},
};

use apalis_codec::json::JsonCodec;
use apalis_core::backend::{
    ext::poll_strategy::{PollWith, StreamStrategy},
    factory::BackendFactory,
    persistence::Persisted,
};
use futures::{Stream, task::AtomicWaker};
use redis::{
    AsyncConnectionConfig, Client, PushInfo, RedisError, Value, aio::MultiplexedConnection,
};

use crate::{Config, RedisStorage, persist::RedisPersistence};

/// A shared Redis storage that can create multiple RedisStorage instances.
#[derive(Debug, Clone)]
pub struct RedisStorageFactory {
    conn: MultiplexedConnection,
    registry: Arc<Mutex<HashMap<String, Pubsub>>>,
}

fn parse_channel_info(push: &PushInfo) -> Option<(&str, &str, &str)> {
    if let Some(Value::BulkString(channel_bytes)) = push.data.get(1)
        && let Ok(channel_str) = std::str::from_utf8(channel_bytes)
    {
        let parts: Vec<&str> = channel_str.split(':').collect();
        if parts.len() >= 4 {
            let namespace = parts[1];
            let action = parts[2];
            let signal = parts[3];
            return Some((namespace, action, signal));
        }
    }
    None
}

impl RedisStorageFactory {
    /// Creates a new SharedRedisStorage with the given Redis client.
    pub async fn new(client: Client) -> Result<Self, RedisError> {
        let registry: Arc<Mutex<HashMap<String, Pubsub>>> = Arc::new(Mutex::new(HashMap::new()));
        let r2 = registry.clone();
        let config = AsyncConnectionConfig::new().set_push_sender(move |msg| {
            let Ok(registry) = r2.lock() else {
                return Err(redis::aio::SendError);
            };
            if let Some((namespace, _, "available")) = parse_channel_info(&msg)
                && let Some(f) = registry.get(namespace)
            {
                f.signal()
            }
            Ok(())
        });
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        conn.psubscribe("tasks:*:available").await?;
        Ok(RedisStorageFactory { conn, registry })
    }
}

impl<Args> BackendFactory<Args> for RedisStorageFactory {
    type Backend = PollWith<RedisStorage<Args, MultiplexedConnection>, StreamStrategy<Pubsub>>;

    type Error = RedisError;

    fn create(&mut self) -> Result<Self::Backend, Self::Error> {
        let config = Config::default().queue(std::any::type_name::<Args>());
        Self::create_with_config(self, config)
    }

    fn create_with_config(&mut self, config: Config) -> Result<Self::Backend, Self::Error> {
        let poller = Pubsub::new();
        self.registry
            .lock()
            .unwrap()
            .insert(config.queue.to_string(), poller.clone());
        let conn = self.conn.clone();
        let redis_storage = RedisStorage {
            job_type: PhantomData,
            persist: Persisted::new(RedisPersistence { config, conn }),
            codec: JsonCodec::default(),
            cleanup: None,
        };
        Ok(PollWith::new(redis_storage, StreamStrategy::new(poller)))
    }
}

#[derive(Debug)]
struct Inner {
    waker: AtomicWaker,
    set: AtomicBool,
}

/// A basic stream that wakes a worker when pubsub fires
#[derive(Debug, Clone)]
pub struct Pubsub(Arc<Inner>);

impl Pubsub {
    fn new() -> Self {
        Self(Arc::new(Inner {
            waker: AtomicWaker::new(),
            set: AtomicBool::new(false),
        }))
    }

    fn signal(&self) {
        self.0.set.store(true, Ordering::Release);
        self.0.waker.wake();
    }
}

impl Default for Pubsub {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream for Pubsub {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = &self.get_mut().0;

        // Consume an already-pending notification.
        if inner.set.swap(false, Ordering::AcqRel) {
            return Poll::Ready(Some(()));
        }

        // Register before checking again to avoid a lost wakeup.
        inner.waker.register(cx.waker());

        if inner.set.swap(false, Ordering::AcqRel) {
            Poll::Ready(Some(()))
        } else {
            Poll::Pending
        }
    }
}
