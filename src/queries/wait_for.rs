use apalis_core::backend::{TaskResult, WaitForCompletion};
use apalis_core::task::status::Status;
use apalis_core::task::task_id::TaskId;
use apalis_core::timer::sleep;
use futures::stream::{self, BoxStream, StreamExt};
use redis::aio::ConnectionLike;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use crate::error::Error;
use crate::{RedisStorage, build_error};

impl<Res, Args, Conn> WaitForCompletion<Res> for RedisStorage<Args, Conn>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: ConnectionLike + Send + Sync + 'static + Clone,
    Res: Send + DeserializeOwned + 'static,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<Res>, Error>>;

    fn wait_for(&self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        let storage = self.clone();
        let pending_ids: HashSet<_> = task_ids.into_iter().map(|id| id.to_string()).collect();

        stream::unfold(
            (storage, pending_ids),
            |(storage, mut pending_ids)| async move {
                if pending_ids.is_empty() {
                    return None;
                }

                // Poll for completed tasks
                let ids_to_check: Vec<_> = pending_ids
                    .iter()
                    .map(|t| TaskId::from_str(t).unwrap())
                    .collect();

                match storage.check_status(ids_to_check).await {
                    Ok(results) => {
                        if results.is_empty() {
                            // No tasks completed yet, wait before next poll
                            sleep(Duration::from_millis(100)).await;
                            Some((vec![], (storage, pending_ids)))
                        } else {
                            // Remove completed task IDs from pending set
                            for result in &results {
                                pending_ids.remove(&result.task_id().to_string());
                            }

                            Some((
                                results.into_iter().map(Ok).collect(),
                                (storage, pending_ids),
                            ))
                        }
                    }
                    Err(e) => {
                        // Emit error and terminate stream
                        Some((vec![Err(e)], (storage, pending_ids)))
                    }
                }
            },
        )
        .flat_map(stream::iter)
        .boxed()
    }

    async fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<Res>>, Self::Error> {
        use redis::AsyncCommands;
        let task_ids: Vec<_> = task_ids.into_iter().collect();
        if task_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.persist.conn.clone();
        let mut results = Vec::new();

        for task_id in task_ids {
            let task_id_str = task_id.to_string();
            let task_meta_key = format!("{}:{}", self.persist.config.job_meta_hash(), task_id_str);

            // Check if task has a status (Done or Failed)
            let status: Option<String> = conn.hget(&task_meta_key, "status").await?;

            if let Some(status_str) = status {
                let status = Status::from_str(&status_str)
                    .map_err(|e| build_error(e.to_string().as_str()))?;

                // Fetch the serialized result
                let serialized_result: Option<Vec<u8>> =
                    conn.hget(&task_meta_key, "result").await?;
                let attempt: Option<usize> = conn.hget(&task_meta_key, "attempts").await?;

                if let Some(data) = serialized_result {
                    // Deserialize the Result<Res, String>
                    let result: Result<Res, String> =
                        serde_json::from_slice(&data).map_err(Error::Json)?;

                    results.push(TaskResult {
                        task_id: TaskId::from_str(&task_id.to_string()).unwrap(),
                        attempt: attempt.unwrap_or(1),
                        status,
                        result,
                    });
                }
            }
        }

        Ok(results)
    }
}
