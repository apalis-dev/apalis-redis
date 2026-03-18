use apalis_core::backend::{BackendExt, ListWorkers, RunningWorker, codec::Codec};
use redis::Script;
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
    fn list_workers(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = queue.to_string();
        let mut conn = self.conn.clone();
        async move {
            let json: String = Script::new(include_str!("../../lua/list_workers.lua"))
                .key(format!("{}:workers", queue))
                .key("core::apalis::workers:metadata::")
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
            let queues = redis::cmd("ZRANGE")
                .arg("core::apalis::queues::list")
                .arg(0)
                .arg(-1)
                .query_async::<Vec<String>>(&mut conn)
                .await?;

            let script = Script::new(include_str!("../../lua/list_all_workers.lua"));
            let mut script = script.key(queues);
            let script = script.arg("core::apalis::workers:metadata::");

            let json: String = script.invoke_async(&mut conn).await?;

            let workers: Vec<RunningWorker> = serde_json::from_str(&json).map_err(|e| {
                redis::RedisError::from((redis::ErrorKind::Parse, "invalid JSON", e.to_string()))
            })?;

            Ok(workers)
        }
    }
}
