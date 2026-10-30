use apalis_core::backend::{Backend, Metrics, Statistic};
use redis::Script;

use crate::{
    RedisStorage,
    error::Error,
    queries::{QUEUES_REGISTRY, current_timestamp, queue_names},
};

impl<Args, Conn> Metrics for RedisStorage<Args, Conn>
where
    RedisStorage<Args, Conn>: Backend<Error = Error>,
    Args: 'static + Send + Sync,
    Conn: redis::aio::ConnectionLike + Send + Clone + Sync + 'static,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Error>> + Send {
        let mut conn = self.persist.conn.clone();

        async move {
            let entries: Vec<String> = redis::cmd("ZRANGE")
                .arg(QUEUES_REGISTRY)
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await?;
            let script = Script::new(include_str!("../../lua/overview.lua"));
            let mut invocation = script.prepare_invoke();
            invocation.arg(current_timestamp());
            for queue in queue_names(&entries) {
                invocation.arg(queue);
            }

            let json: String = invocation.invoke_async(&mut conn).await?;
            let stats: Vec<Statistic> = serde_json::from_str(&json).map_err(Error::Json)?;
            Ok(stats)
        }
    }

    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let config = self.persist.config.clone();
        let mut conn = self.persist.conn.clone();

        async move {
            let script = Script::new(include_str!("../../lua/overview_by_queue.lua"));
            let mut invocation = script.prepare_invoke();
            invocation
                .key(config.active_jobs_list())
                .key(config.done_jobs_set())
                .key(config.dead_jobs_set())
                .key(config.inflight_jobs_set())
                .arg(current_timestamp());

            let json: String = invocation.invoke_async(&mut conn).await?;
            let stats: Vec<Statistic> = serde_json::from_str(&json).map_err(Error::Json)?;
            Ok(stats)
        }
    }
}
