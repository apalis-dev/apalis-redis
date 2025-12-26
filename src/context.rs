use apalis_core::{
    task::{Task, metadata::MetadataExt},
    task_fn::FromRequest,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::convert::Infallible;
use ulid::Ulid;

/// The context for a redis storage job
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisContext {
    /// The maximum number of attempts for the task
    pub max_attempts: u32,
    /// The worker that has locked the task, if any
    pub lock_by: Option<String>,
    /// Additional metadata associated with the task
    pub meta: serde_json::Map<String, serde_json::Value>,
}

impl Default for RedisContext {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            lock_by: None,
            meta: serde_json::Map::new(),
        }
    }
}

impl<T: Serialize + DeserializeOwned> MetadataExt<T> for RedisContext {
    type Error = serde_json::Error;
    fn extract(&self) -> Result<T, serde_json::Error> {
        use serde::de::Error as _;
        let key = std::any::type_name::<T>();
        match self.meta.get(key) {
            Some(value) => T::deserialize(value),
            None => Err(serde_json::Error::custom(format!(
                "No entry for type `{key}` in metadata"
            ))),
        }
    }

    fn inject(&mut self, value: T) -> Result<(), serde_json::Error> {
        let key = std::any::type_name::<T>();
        let json_value = serde_json::to_value(value)?;
        self.meta.insert(key.to_owned(), json_value);
        Ok(())
    }
}

impl<Args: Sync> FromRequest<Task<Args, RedisContext, Ulid>> for RedisContext {
    type Error = Infallible;
    async fn from_request(req: &Task<Args, RedisContext, Ulid>) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}
