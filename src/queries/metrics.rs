use apalis_core::backend::{BackendExt, Metrics, Statistic, codec::Codec};
use ulid::Ulid;

use crate::{RedisContext, RedisStorage, build_error, scripts};

impl<Args, Conn, C> Metrics for RedisStorage<Args, Conn, C>
where
    RedisStorage<Args, Conn, C>: BackendExt<
            Context = RedisContext,
            Compact = Vec<u8>,
            IdType = Ulid,
            Error = redis::RedisError,
        >,
    C: Codec<Args, Compact = Vec<u8>> + Send + Sync,
    C::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static + Send + Sync,
    Conn: redis::aio::ConnectionLike + Send + Clone + Sync,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let mut conn = self.conn.clone();

        async move {
            let queues = redis::cmd("ZRANGE")
                .arg("core::apalis::queues")
                .arg(0)
                .arg(-1)
                .query_async::<Vec<String>>(&mut conn)
                .await?;
            let now = chrono::Utc::now().timestamp();
            let mut script = &mut scripts::OVERVIEW.arg(now);
            for queue in queues {
                script = script.key(queue);
            }
            let res = script
                .invoke_async::<String>(&mut conn)
                .await
                .and_then(|json| {
                    let stats: Vec<Statistic> =
                        serde_json::from_str(&json).map_err(|e| build_error(&e.to_string()))?;
                    Ok(stats)
                })?;

            Ok(res)
        }
    }
    fn fetch_by_queue(
        &self,
        queue_id: &str,
    ) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let mut conn = self.conn.clone();

        let queue_name = queue_id.to_string();
        async move {
            let active = format!("{}:active", queue_name);
            let done = format!("{}:done", queue_name);
            let dead = format!("{}:dead", queue_name);
            let inflight = format!("{}:inflight", queue_name);

            // Execute the Lua script with 4 keys
            let json: String = scripts::OVERVIEW_BY_QUEUE
                .key(active)
                .key(done)
                .key(dead)
                .key(inflight)
                .invoke_async(&mut conn)
                .await?;

            let stats: Vec<Statistic> =
                serde_json::from_str(&json).map_err(|e| build_error(&e.to_string()))?;

            Ok(stats)
        }
    }
}
