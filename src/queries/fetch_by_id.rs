use apalis_core::{backend::FetchById, task::task_id::TaskId};
use redis::{Script, Value};

use crate::{RedisStorage, RedisTask, error::Error, queries::fetch_next::deserialize_with_meta};

impl<Args, Conn> FetchById for RedisStorage<Args, Conn>
where
    Args: 'static + Send,
    Conn: redis::aio::ConnectionLike + Send + Sync + 'static + Clone,
{
    async fn fetch_by_id(&mut self, task_id: &TaskId) -> Result<Option<RedisTask>, Self::Error> {
        let fetch_by_id_script = Script::new(include_str!("../../lua/fetch_by_id.lua"));
        let result: Value = fetch_by_id_script
            .key(self.persist.config.job_data_hash())
            .key(self.persist.config.job_meta_hash())
            .arg(task_id.to_string())
            .invoke_async(&mut self.persist.conn)
            .await?;

        match result {
            Value::ServerError(s) => Err(Error::Database(s.into())),
            Value::Array(data) => {
                let tasks = deserialize_with_meta(data)?;

                if let Some(task) = tasks.into_iter().take(1).next() {
                    let task = task.into_full_compact();
                    Ok(Some(task))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}
