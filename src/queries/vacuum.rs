use apalis_core::backend::{Backend, Vacuum};
use redis::aio::ConnectionLike;

use crate::{RedisStorage, error::Error};

impl<Args, Conn> Vacuum for RedisStorage<Args, Conn>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: ConnectionLike + Clone + Send + Sync + 'static,
    Self: Backend<Error = Error>,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        let vacuum_script = redis::Script::new(include_str!("../../lua/vacuum.lua"));
        let items = vacuum_script
            .key(self.persist.config.job_data_hash())
            .key(self.persist.config.job_meta_hash())
            .invoke_async(self.get_connection())
            .await?;
        Ok(items)
    }
}
