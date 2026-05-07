use apalis_core::backend::{BackendExt, ListWorkers, RunningWorker, codec::Codec};
use redis::{RedisError, Script};
use ulid::Ulid;

use crate::{RedisContext, RedisStorage};

impl<Args: Sync, Conn, C> ListWorkers for RedisStorage<Args, Conn, C>
where
    RedisStorage<Args, Conn, C>: BackendExt<
            Context = RedisContext,
            Compact = Vec<u8>,
            IdType = Ulid,
            Error = redis::RedisError,
        >,
    C: Codec<Args, Compact = Vec<u8>> + Send,
    C::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static + Send,
    Conn: redis::aio::ConnectionLike + Send + Clone,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = self.config.get_namespace().to_string();
        let mut conn = self.conn.clone();
        async move {
            let worker_metadata_key = format!("{}:workers:metadata", queue);
            let json: String = Script::new(include_str!("../../lua/list_workers.lua"))
                .key(format!("{}:workers", queue))
                .key(worker_metadata_key)
                .invoke_async(&mut conn)
                .await?;
            let workers: Vec<RunningWorker> = serde_json::from_str(&json).map_err(|e| {
                redis::RedisError::from((redis::ErrorKind::Parse, "invalid JSON", e.to_string()))
            })?;

            Ok(workers)
        }
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let mut conn = self.conn.clone();
        async move {
            let json: String = Script::new(include_str!("../../lua/list_all_workers.lua"))
                .invoke_async(&mut conn)
                .await?;

            let workers: Vec<RunningWorker> = serde_json::from_str(&json).map_err(|e| {
                RedisError::from((redis::ErrorKind::Parse, "invalid JSON", e.to_string()))
            })?;

            Ok(workers)
        }
    }
}
