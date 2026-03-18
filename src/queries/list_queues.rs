use apalis_core::backend::{BackendExt, ListQueues, QueueInfo, codec::Codec};
use ulid::Ulid;

use crate::{RedisContext, RedisStorage};

impl<Args, Conn, C> ListQueues for RedisStorage<Args, Conn, C>
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
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let mut conn = self.conn.clone();

        async move {
            let queues = redis::cmd("ZRANGE")
                .arg("core::apalis::queues::list")
                .arg(0)
                .arg(-1)
                .query_async::<Vec<String>>(&mut conn)
                .await?
                .into_iter()
                .map(|name| QueueInfo {
                    name: name.replace(":workers", ""),
                    activity: Vec::new(),
                    stats: Vec::new(),
                    workers: Vec::new(),
                })
                .collect::<Vec<_>>();
            // let lua = include_str!("../../lua/overview_by_queue.lua");
            // let script = Script::new(lua);
            // let now = chrono::Utc::now().timestamp();
            // let res = script
            //     .arg(now)
            //     .key(queues.)
            //     .invoke_async::<String>(&mut conn)
            //     .await
            //     .and_then(|json| {
            //         dbg!(&json);
            //         let stats =
            //             serde_json::from_str(&json).map_err(|e| build_error(&e.to_string()))?;
            //         Ok(stats)
            //     })?;
            Ok(queues)
        }
    }
}
