use apalis_core::{
    backend::{BackendExt, FetchById, codec::Codec},
    task::task_id::TaskId,
};
use redis::{Script, Value};
use ulid::Ulid;

use crate::{RedisContext, RedisStorage, RedisTask, fetcher::deserialize_with_meta};

impl<Args, Conn, C> FetchById<Args> for RedisStorage<Args, Conn, C>
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
    Conn: redis::aio::ConnectionLike + Send,
{
    async fn fetch_by_id(
        &mut self,
        task_id: &TaskId<Self::IdType>,
    ) -> Result<Option<RedisTask<Args>>, Self::Error> {
        let fetch_by_id_script = Script::new(include_str!("../../lua/fetch_by_id.lua"));
        let result: Value = fetch_by_id_script
            .key(self.config.job_data_hash())
            .key(self.config.job_meta_hash())
            .arg(task_id.to_string())
            .invoke_async(&mut self.conn)
            .await?;

        match result {
            Value::ServerError(s) => Err(s.into()),
            Value::Array(ref data) => {
                let tasks = deserialize_with_meta(data).expect("Failed to deserialize");

                if let Some(task) = tasks.into_iter().take(1).next() {
                    let task = task.into_full_task::<Args, C>()?;
                    Ok(Some(task))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}
