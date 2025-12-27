use apalis_core::backend::codec::Codec;
use apalis_core::backend::{TaskResult, WaitForCompletion};
use apalis_core::error::BoxDynError;
use apalis_core::task::status::Status;
use apalis_core::task::task_id::TaskId;
use apalis_core::timer::sleep;
use futures::stream::{self, BoxStream, StreamExt};
use redis::aio::ConnectionLike;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;
use ulid::Ulid;

use crate::{RedisStorage, build_error};

impl<Res, Args, Conn, Decode, Err> WaitForCompletion<Res> for RedisStorage<Args, Conn, Decode>
where
    Args: Unpin + Send + Sync + 'static,
    Conn: Clone + ConnectionLike + Send + Sync + 'static,
    Decode: Codec<Args, Compact = Vec<u8>, Error = Err>
        + Codec<Result<Res, String>, Compact = Vec<u8>, Error = Err>
        + Send
        + Sync
        + Unpin
        + 'static
        + Clone,
    Err: Into<BoxDynError> + Send + 'static,
    Res: Send + 'static,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<Res, Ulid>, Self::Error>>;

    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
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
                    .cloned()
                    .map(|t| TaskId::from_str(&t).unwrap())
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
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> Result<Vec<TaskResult<Res, Ulid>>, Self::Error> {
        use redis::AsyncCommands;
        let task_ids: Vec<_> = task_ids.into_iter().collect();
        if task_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.conn.clone();
        let mut results = Vec::new();

        for task_id in task_ids {
            let task_id_str = task_id.to_string();
            let task_meta_key = format!("{}:{}", self.config.job_meta_hash(), task_id_str);

            // Check if task has a status (Done or Failed)
            let status: Option<String> = conn.hget(&task_meta_key, "status").await?;

            if let Some(status_str) = status {
                let status = Status::from_str(&status_str)
                    .map_err(|e| build_error(e.to_string().as_str()))?;

                // Fetch the serialized result
                let result_ns = format!("{}:result", self.config.job_meta_hash());
                let serialized_result: Option<Vec<u8>> =
                    conn.hget(&result_ns, &task_id_str).await?;

                if let Some(data) = serialized_result {
                    // Deserialize the Result<Res, String>
                    let result: Result<Res, String> = Decode::decode(&data)
                        .map_err(|e: Err| build_error(e.into().to_string().as_str()))?;

                    results.push(TaskResult::new(
                        TaskId::from_str(&task_id.to_string()).unwrap(),
                        status,
                        result,
                    ));
                }
            }
        }

        Ok(results)
    }
}
