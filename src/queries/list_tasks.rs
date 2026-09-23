use apalis_core::backend::{Backend, Filter, ListAllTasks, ListTasks};
use redis::{Script, Value};

use crate::{RedisStorage, RedisTask, error::Error, queries::fetch_next::deserialize_with_meta};

impl<Args, Conn> ListTasks for RedisStorage<Args, Conn>
where
    RedisStorage<Args, Conn>: Backend<Error = crate::error::Error>,
    Args: 'static + Send + Sync,
    Conn: redis::aio::ConnectionLike + Send + Sync + 'static + Clone,
{
    async fn list_tasks(&self, filter: &Filter) -> Result<Vec<RedisTask>, Self::Error> {
        let config = &self.persist.config;
        let queue = config.queue.as_ref();
        let script = Script::new(include_str!("../../lua/list_tasks.lua"));
        let mut conn = self.persist.conn.clone();
        let status_str = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let page = filter.page;
        let page_size = filter.page_size.unwrap_or(10);

        let result: Value = script
            .key(config.job_data_hash())
            .key(config.job_meta_hash())
            .key(queue)
            .arg(status_str)
            .arg(page.to_string())
            .arg(page_size.to_string())
            .invoke_async(&mut conn)
            .await?;

        if let Value::Array(arr) = result {
            Ok(deserialize_with_meta(arr).map(|tasks| {
                tasks
                    .into_iter()
                    .map(|t| t.into_full_compact())
                    .collect::<Vec<RedisTask>>()
            })?)
        } else {
            Ok(vec![])
        }
    }
}

impl<Args, Conn> ListAllTasks for RedisStorage<Args, Conn>
where
    RedisStorage<Args, Conn>: Backend<Error = Error>,
    Args: 'static + Send + Sync,
    Conn: redis::aio::ConnectionLike + Send + Sync + Clone + 'static,
{
    async fn list_all_tasks(&self, filter: &Filter) -> Result<Vec<RedisTask>, Self::Error> {
        let config = &self.persist.config;
        let mut conn = self.persist.conn.clone();
        let script = Script::new(include_str!("../../lua/list_all_tasks.lua"));
        let status_str = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let page = filter.page;
        let page_size = filter.page_size.unwrap_or(10);

        let result: Value = script
            .key(config.job_data_hash())
            .key(config.job_meta_hash())
            .arg(status_str)
            .arg(page.to_string())
            .arg(page_size.to_string())
            .invoke_async(&mut conn)
            .await?;

        if let Value::Array(arr) = result {
            let val = deserialize_with_meta(arr)
                .map(|tasks| tasks.into_iter().map(|t| t.into_full_compact()).collect())?;
            Ok(val)
        } else {
            Ok(vec![])
        }
    }
}
