use std::time::Duration;

use apalis_core::backend::queue::Queue;

const ACTIVE_TASKS_LIST: &str = "{queue}:active";
const WORKERS_SET: &str = "{queue}:workers";
const DEAD_TASKS_SET: &str = "{queue}:dead";
const DONE_TASKS_SET: &str = "{queue}:done";
const FAILED_TASKS_SET: &str = "{queue}:failed";
const INFLIGHT_TASKS_SET: &str = "{queue}:inflight";
const TASK_DATA_HASH: &str = "{queue}:data";
const JOB_META_HASH: &str = "{queue}:meta";
const SCHEDULED_TASKS_SET: &str = "{queue}:scheduled";
const SIGNAL_LIST: &str = "{queue}:signal";

/// Config for a [`RedisStorage`]
///
/// RedisConfig allows you to customize various settings for the Redis storage backend,
/// including polling intervals, buffer sizes, namespaces, and job re-enqueueing behavior.
///
/// [`RedisStorage`]: crate::RedisStorage
#[derive(Clone, Debug)]
pub struct RedisConfig {
    poll_interval: Duration,
    buffer_size: usize,
    keep_alive: Duration,
    enqueue_scheduled: Duration,
    reenqueue_orphaned_after: Duration,
    queue: Queue,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            buffer_size: 10,
            keep_alive: Duration::from_secs(30),
            enqueue_scheduled: Duration::from_secs(1),
            reenqueue_orphaned_after: Duration::from_secs(300),
            queue: Queue::from("default"),
        }
    }
}

impl RedisConfig {
    /// Creates a new RedisConfig with the specified queue namespace.
    pub fn new(queue: &str) -> Self {
        Self {
            queue: Queue::from(queue),
            ..Default::default()
        }
    }
    /// Get the interval of polling
    pub fn get_poll_interval(&self) -> &Duration {
        &self.poll_interval
    }

    /// Get the number of jobs to fetch
    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// get the keep live rate
    pub fn get_keep_alive(&self) -> &Duration {
        &self.keep_alive
    }

    /// get the enqueued setting
    pub fn get_enqueue_scheduled(&self) -> &Duration {
        &self.enqueue_scheduled
    }

    /// get the namespace
    pub fn get_namespace(&self) -> &Queue {
        &self.queue
    }

    /// get the poll interval
    pub fn set_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// set the buffer setting
    pub fn set_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// set the keep-alive setting
    pub fn set_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// get the enqueued setting
    pub fn set_enqueue_scheduled(mut self, enqueue_scheduled: Duration) -> Self {
        self.enqueue_scheduled = enqueue_scheduled;
        self
    }

    /// set the namespace for the Storage
    pub fn set_namespace(mut self, namespace: &str) -> Self {
        self.queue = Queue::from(namespace);
        self
    }

    /// Returns the Redis key for the list of pending jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the pending jobs list.
    pub fn active_jobs_list(&self) -> String {
        ACTIVE_TASKS_LIST.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the set of workers associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the workers set.
    pub fn workers_set(&self) -> String {
        WORKERS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the set of dead jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the dead jobs set.
    pub fn dead_jobs_set(&self) -> String {
        DEAD_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the set of done jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the done jobs set.
    pub fn done_jobs_set(&self) -> String {
        DONE_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the set of failed jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the failed jobs set.
    pub fn failed_jobs_set(&self) -> String {
        FAILED_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the set of inflight jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the inflight jobs set.
    pub fn inflight_jobs_set(&self) -> String {
        INFLIGHT_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the hash storing job data associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the job data hash.
    pub fn job_data_hash(&self) -> String {
        TASK_DATA_HASH.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the hash storing job metadata associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the job meta hash.
    pub fn job_meta_hash(&self) -> String {
        JOB_META_HASH.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the set of scheduled jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the scheduled jobs set.
    pub fn scheduled_jobs_set(&self) -> String {
        SCHEDULED_TASKS_SET.replace("{queue}", self.queue.as_ref())
    }

    /// Returns the Redis key for the list of signals associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the signal list.
    pub fn signal_list(&self) -> String {
        SIGNAL_LIST.replace("{queue}", self.queue.as_ref())
    }

    /// Gets the reenqueue_orphaned_after duration.
    pub fn reenqueue_orphaned_after(&self) -> Duration {
        self.reenqueue_orphaned_after
    }

    /// Gets a mutable reference to the reenqueue_orphaned_after.
    pub fn reenqueue_orphaned_after_mut(&mut self) -> &mut Duration {
        &mut self.reenqueue_orphaned_after
    }

    /// Occasionally some workers die, or abandon jobs because of panics.
    /// This is the time a task takes before its back to the queue
    ///
    /// Defaults to 5 minutes
    pub fn set_reenqueue_orphaned_after(mut self, after: Duration) -> Self {
        self.reenqueue_orphaned_after = after;
        self
    }
}
