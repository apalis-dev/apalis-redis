use apalis_core::backend::{Backend, ListWorkers, RunningWorker};
use redis::Script;

use crate::{RedisStorage, error::Error};

impl<Args: Sync, Conn> ListWorkers for RedisStorage<Args, Conn>
where
    RedisStorage<Args, Conn>: Backend<Error = Error>,
    Args: 'static + Send,
    Conn: redis::aio::ConnectionLike + Sync + Send + Clone,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let config = self.persist.config.clone();
        let mut conn = self.persist.conn.clone();

        async move {
            let worker_metadata_key = format!("{}:workers:", config.queue);
            let json: String = Script::new(include_str!("../../lua/list_workers.lua"))
                .key(config.workers_set())
                .key(worker_metadata_key)
                .invoke_async(&mut conn)
                .await?;
            let workers: Vec<RunningWorker> = serde_json::from_str(&json).map_err(Error::Json)?;

            Ok(workers)
        }
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let mut conn = self.persist.conn.clone();

        async move {
            let json: String = Script::new(include_str!("../../lua/list_all_workers.lua"))
                .invoke_async(&mut conn)
                .await?;

            let workers: Vec<RunningWorker> = serde_json::from_str(&json).map_err(Error::Json)?;

            Ok(workers)
        }
    }
}
