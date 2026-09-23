use apalis_core::backend::{ListQueues, QueueInfo};
use redis::Script;

use crate::{RedisStorage, build_error, error::Error, queries::current_timestamp};

impl<Args, Conn> ListQueues for RedisStorage<Args, Conn>
where
    Args: 'static + Send,
    Conn: redis::aio::ConnectionLike + Send + Clone + Sync + 'static,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Error>> + Send {
        let mut conn = self.persist.conn.clone();

        async move {
            let queues = redis::cmd("ZRANGE")
                .arg("core:apalis:queues:list")
                .arg(0)
                .arg(-1)
                .query_async::<Vec<String>>(&mut conn)
                .await?
                .into_iter()
                .map(|name| name.replace(":workers", ""))
                .collect::<Vec<_>>();
            let lua = include_str!("../../lua/overview_by_queue.lua");
            let script = Script::new(lua);
            let now = current_timestamp();
            let res = script
                .arg(now)
                .key(queues)
                .invoke_async::<String>(&mut conn)
                .await
                .and_then(|json| {
                    let stats =
                        serde_json::from_str(&json).map_err(|e| build_error(&e.to_string()))?;
                    Ok(stats)
                })?;
            Ok(res)
        }
    }
}
