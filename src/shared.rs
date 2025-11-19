use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use apalis_core::backend::shared::MakeShared;
use event_listener::Event;
use redis::{
    AsyncConnectionConfig, Client, PushInfo, RedisError, Value, aio::MultiplexedConnection,
};

use crate::{RedisStorage, config::RedisConfig, sink::RedisSink};

/// A shared Redis storage that can create multiple RedisStorage instances.
#[derive(Debug, Clone)]
pub struct SharedRedisStorage {
    conn: MultiplexedConnection,
    registry: Arc<Mutex<HashMap<String, Arc<Event>>>>,
}

fn parse_channel_info(push: &PushInfo) -> Option<(String, String, String)> {
    if let Some(Value::BulkString(channel_bytes)) = push.data.get(1)
        && let Ok(channel_str) = std::str::from_utf8(channel_bytes)
    {
        let parts: Vec<&str> = channel_str.split(':').collect();
        if parts.len() >= 4 {
            let namespace = parts[1].to_owned();
            let action = parts[2].to_owned();
            let signal = parts[3].to_string();
            return Some((namespace, action, signal));
        }
    }
    None
}

impl SharedRedisStorage {
    /// Creates a new SharedRedisStorage with the given Redis client.
    pub async fn new(client: Client) -> Result<Self, RedisError> {
        let registry: Arc<Mutex<HashMap<String, Arc<Event>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let r2 = registry.clone();
        let config = AsyncConnectionConfig::new().set_push_sender(move |msg| {
            let Ok(registry) = r2.lock() else {
                return Err(redis::aio::SendError);
            };
            if let Some((namespace, _, signal_kind)) = parse_channel_info(&msg)
                && signal_kind == "available"
            {
                registry.get(&namespace).map(|f| f.notify(usize::MAX));
            }
            Ok(())
        });
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        conn.psubscribe("tasks:*:available").await?;
        Ok(SharedRedisStorage { conn, registry })
    }
}

impl<Args> MakeShared<Args> for SharedRedisStorage {
    type Backend = RedisStorage<Args, MultiplexedConnection>;
    type Config = RedisConfig;

    type MakeError = RedisError;

    fn make_shared(&mut self) -> Result<RedisStorage<Args, MultiplexedConnection>, Self::MakeError>
    where
        Self::Config: Default,
    {
        let config = RedisConfig::default().set_namespace(std::any::type_name::<Args>());
        Self::make_shared_with_config(self, config)
    }

    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<RedisStorage<Args, MultiplexedConnection>, Self::MakeError> {
        let poller = Arc::new(Event::new());
        self.registry
            .lock()
            .unwrap()
            .insert(config.get_namespace().to_string(), poller.clone());
        let conn = self.conn.clone();
        let sink = RedisSink::new(&conn, &config);
        Ok(RedisStorage {
            conn,
            job_type: PhantomData,
            config,
            codec: PhantomData,
            poller,
            sink,
        })
    }
}
