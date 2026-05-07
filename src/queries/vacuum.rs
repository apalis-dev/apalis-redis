use apalis_core::backend::Vacuum;
use apalis_core::backend::codec::Codec;
use redis::aio::ConnectionLike;

use crate::RedisStorage;

impl<Args, Conn, C> Vacuum for RedisStorage<Args, Conn, C>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: ConnectionLike + Clone + Send + Sync + 'static,
    C: Codec<Args, Compact = Vec<u8>> + Unpin + Send + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        let vacuum_script = redis::Script::new(include_str!("../../lua/vacuum.lua"));
        vacuum_script
            .key(self.config.job_data_hash())
            .key(self.config.job_meta_hash())
            .invoke_async(&mut self.conn)
            .await
    }
}
