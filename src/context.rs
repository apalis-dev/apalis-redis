use std::{convert::Infallible, time::SystemTime};

use apalis_core::{task::Task, task_fn::FromRequest};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// The context for a redis storage job
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisContext {
    pub(super) max_attempts: u32,
    pub(super) lock_by: Option<String>,
    pub(super) run_at: Option<SystemTime>,
}

impl Default for RedisContext {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            lock_by: None,
            run_at: None,
        }
    }
}

impl<Args: Sync> FromRequest<Task<Args, RedisContext, Ulid>> for RedisContext {
    type Error = Infallible;
    async fn from_request(req: &Task<Args, RedisContext, Ulid>) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}
