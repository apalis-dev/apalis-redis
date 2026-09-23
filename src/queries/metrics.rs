use apalis_core::backend::{Backend, Metrics, Statistic};
use redis::Script;

use crate::{RedisStorage, build_error, error::Error, queries::current_timestamp};

impl<Args, Conn> Metrics for RedisStorage<Args, Conn>
where
    RedisStorage<Args, Conn>: Backend<Error = Error>,
    Args: 'static + Send + Sync,
    Conn: redis::aio::ConnectionLike + Send + Clone + Sync + 'static,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Error>> + Send {
        let mut conn = self.persist.conn.clone();

        async move {
            let queues = redis::cmd("ZRANGE")
                .arg("core:apalis:queues")
                .arg(0)
                .arg(-1)
                .query_async::<Vec<String>>(&mut conn)
                .await?;
            let lua = include_str!("../../lua/overview.lua");
            let script = Script::new(lua);
            let now = current_timestamp();
            let mut script = &mut script.arg(now);
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
    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let mut conn = self.persist.conn.clone();

        let queue_name = self.persist.config.queue.to_string();
        async move {
            let lua = include_str!("../../lua/overview_by_queue.lua");
            let script = Script::new(lua);

            let active = format!("{}:active", queue_name);
            let done = format!("{}:done", queue_name);
            let dead = format!("{}:dead", queue_name);
            let inflight = format!("{}:inflight", queue_name);

            // Execute the Lua script with 4 keys
            let json: String = script
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
