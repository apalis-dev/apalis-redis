use apalis_core::{
    backend::{
        WorkerFilter,
        persistence::{Persistence, TaskEvent},
    },
    task::Task,
    worker::context::WorkerContext,
};
use redis::aio::{ConnectionLike, ConnectionManager};
use serde_json::Value;

use crate::{
    Config, RedisTask,
    error::Error,
    queries::{
        self, keep_alive, push_tasks, reenqueue_orphaned, register_worker, release_leases,
        renew_leases,
    },
};

/// The default persistence implementation
#[derive(Debug)]
pub struct RedisPersistence<Conn = ConnectionManager> {
    pub(crate) conn: Conn,
    pub(crate) config: Config,
}

impl<C: Clone> Clone for RedisPersistence<C> {
    fn clone(&self) -> Self {
        RedisPersistence {
            conn: self.conn.clone(),
            config: self.config.clone(),
        }
    }
}

impl<C> Persistence for RedisPersistence<C>
where
    C: ConnectionLike + Send + Sync + 'static + Clone,
{
    type Compact = Vec<u8>;
    type Error = Error;
    type Response = Value;
    async fn register(&mut self, worker: &WorkerContext) -> Result<(), Error> {
        let count = reenqueue_orphaned(
            &mut self.conn,
            &self.config,
            &WorkerFilter::Only(worker.name().to_owned()),
        )
        .await?;
        if count > 0 {
            tracing::debug!("{count} re-enqueued orphaned tasks",);
        }
        register_worker(&mut self.conn, worker, &self.config).await?;
        tracing::debug!("registered worker successfully");
        Ok(())
    }
    async fn heartbeat(&mut self, worker: &WorkerContext) -> Result<(), Error> {
        let config = &self.config;
        keep_alive(&mut self.conn, worker, config).await?;
        if self.config.lock_tasks {
            let count = renew_leases(&mut self.conn, worker, config).await?;
            tracing::debug!("renewed leases for {count} tasks");
        }
        let count = reenqueue_orphaned(
            &mut self.conn,
            &self.config,
            &WorkerFilter::AllExcept(worker.name().to_owned()),
        )
        .await?;

        if count > 0 {
            tracing::debug!("re-enqueued {count} orphaned task(s)");
        }
        Ok(())
    }
    async fn fetch_next(&mut self, worker: &WorkerContext) -> Result<Vec<RedisTask>, Error> {
        queries::fetch_next(&mut self.conn, worker, &self.config).await
    }

    async fn handle_events(
        &mut self,
        messages: Vec<TaskEvent<Self::Response>>,
        worker: &WorkerContext,
    ) -> Result<(), Error> {
        let lock_ids = messages
            .iter()
            .filter_map(|msg| {
                if let TaskEvent::Lock { task_id, .. } = msg {
                    Some(task_id.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let ack_payloads = messages
            .iter()
            .filter_map(|msg| {
                if let TaskEvent::Complete(payload) = msg {
                    Some(payload)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if lock_ids.is_empty() && ack_payloads.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            "Processing {} messages ({} locks, {} acks)",
            messages.len(),
            lock_ids.len(),
            ack_payloads.len()
        );

        if !lock_ids.is_empty() {
            // TODO: Better error handling rather than stopping everything
            queries::acquire_leases(&mut self.conn, worker, &self.config, &lock_ids).await?;
        }
        if !ack_payloads.is_empty() {
            queries::handle_results(&mut self.conn, &ack_payloads, worker, &self.config).await?;
        }

        Ok(())
    }

    async fn reenqueue_abandoned(
        &mut self,
        tasks: Vec<RedisTask>,
        worker: &WorkerContext,
    ) -> Result<u64, Error> {
        let config = &self.config;
        let count = release_leases(&mut self.conn, config, worker, &tasks).await?;
        if self.config.lock_tasks && count as usize != tasks.len() {
            return Err(Error::ReenqueueMismatch {
                queued: tasks.len(),
                abandoned: count as usize,
            });
        }
        Ok(count)
    }
    async fn push_tasks(&mut self, tasks: Vec<Task<Self::Compact>>) -> Result<(), Self::Error> {
        let config = &self.config;
        push_tasks(&mut self.conn, config, &tasks).await?;
        Ok(())
    }
}
