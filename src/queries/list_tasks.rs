use apalis_core::backend::{BackendExt, Filter, ListAllTasks, ListTasks, codec::Codec};
use redis::{Script, Value};
use ulid::Ulid;

use crate::{RedisContext, RedisStorage, RedisTask, fetcher::deserialize_with_meta};

impl<Args, Conn, C> ListTasks<Args> for RedisStorage<Args, Conn, C>
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
    async fn list_tasks(
        &self,
        queue: &str,
        filter: &Filter,
    ) -> Result<Vec<RedisTask<Args>>, Self::Error> {
        let script = Script::new(include_str!("../../lua/list_tasks.lua"));
        let mut conn = self.conn.clone();
        let status_str = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let page = filter.page;
        let page_size = filter.page_size.unwrap_or(10);

        let result: Value = script
            .key(self.config.job_data_hash())
            .key(self.config.job_meta_hash())
            .key(queue)
            .arg(status_str)
            .arg(page.to_string())
            .arg(page_size.to_string())
            .invoke_async(&mut conn)
            .await?;

        if let Value::Array(arr) = &result {
            deserialize_with_meta(arr)
                .map(|tasks| {
                    tasks
                        .into_iter()
                        .map(|t| t.into_full_task::<Args, C>())
                        .collect::<Result<Vec<RedisTask<Args>>, _>>()
                })
                .and_then(|s| s)
        } else {
            Ok(vec![])
        }
    }
}

impl<Args, Conn, C> ListAllTasks for RedisStorage<Args, Conn, C>
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
    Conn: redis::aio::ConnectionLike + Send + Sync + Clone,
{
    async fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> Result<Vec<RedisTask<Vec<u8>>>, Self::Error> {
        let mut conn = self.conn.clone();
        let script = Script::new(include_str!("../../lua/list_all_tasks.lua"));
        let status_str = filter
            .status
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let page = filter.page;
        let page_size = filter.page_size.unwrap_or(10);

        let result: Value = script
            .key(self.config.job_data_hash())
            .key(self.config.job_meta_hash())
            .arg(status_str)
            .arg(page.to_string())
            .arg(page_size.to_string())
            .invoke_async(&mut conn)
            .await?;

        if let Value::Array(arr) = result {
            deserialize_with_meta(&arr)
                .map(|tasks| {
                    tasks
                        .into_iter()
                        .map(|t| t.into_full_compact())
                        .collect::<Result<Vec<RedisTask<Vec<u8>>>, _>>()
                })
                .and_then(|s| s)
        } else {
            Ok(vec![])
        }
    }
}
