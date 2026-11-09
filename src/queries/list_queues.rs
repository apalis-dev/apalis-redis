use apalis_core::backend::{ListQueues, QueueInfo, Statistic};
use redis::Script;

use crate::{
    RedisStorage,
    error::Error,
    queries::{QUEUES_REGISTRY, current_timestamp, queue_names},
};

impl<Args, Conn> ListQueues for RedisStorage<Args, Conn>
where
    Args: 'static + Send,
    Conn: redis::aio::ConnectionLike + Send + Clone + Sync + 'static,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Error>> + Send {
        let mut conn = self.persist.conn.clone();

        async move {
            let entries: Vec<String> = redis::cmd("ZRANGE")
                .arg(QUEUES_REGISTRY)
                .arg(0)
                .arg(-1)
                .query_async(&mut conn)
                .await?;
            let now = current_timestamp();
            let script = Script::new(include_str!("../../lua/overview_by_queue.lua"));
            let mut queues = Vec::new();

            for name in queue_names(&entries) {
                let mut invocation = script.prepare_invoke();
                invocation
                    .key(format!("{name}:active"))
                    .key(format!("{name}:done"))
                    .key(format!("{name}:dead"))
                    .key(format!("{name}:inflight"))
                    .arg(now);
                let json: String = invocation.invoke_async(&mut conn).await?;
                let stats: Vec<Statistic> = serde_json::from_str(&json).map_err(Error::Json)?;

                let workers: Vec<String> = redis::cmd("ZRANGE")
                    .arg(format!("{name}:workers"))
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut conn)
                    .await?;

                let activity = queue_activity(&mut conn, &name, now).await?;
                queues.push(QueueInfo {
                    name,
                    stats,
                    workers,
                    activity,
                });
            }

            Ok(queues)
        }
    }
}

/// Completed jobs per day for the last 7 days, oldest first.
async fn queue_activity<C>(conn: &mut C, queue: &str, now: u64) -> Result<Vec<usize>, Error>
where
    C: redis::aio::ConnectionLike + Send,
{
    const DAY_SECS: u64 = 86_400;
    let today_start = (now / DAY_SECS) * DAY_SECS;
    let script = Script::new(include_str!("../../lua/queue_activity.lua"));
    let activity: Vec<usize> = script
        .key(format!("{queue}:done"))
        .key(format!("{queue}:dead"))
        .arg(today_start)
        .arg(DAY_SECS)
        .invoke_async(conn)
        .await?;

    Ok(activity)
}
