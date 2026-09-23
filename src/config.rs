use std::time::Duration;

use apalis_core::worker::context::WorkerContext;
use serde::{Deserialize, Serialize};

pub(crate) use apalis_core::backend::queue::Queue;

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
const IDEMPOTENCY_KEY_SET: &str = "{queue}:idempotency";

/// Configuration for a worker's queue, batching, and liveness detection.
///
/// `Config` controls how jobs are fetched from a queue and how worker
/// liveness is monitored.
///
/// # Defaults
///
/// - `batch_size`: `10`
/// - `heartbeat_interval`: `30` seconds
/// - `missed_heartbeats`: `2`
/// - `queue`: `"default"`
/// - `database_url`: `None`
/// - `lock_tasks`: `true`
/// - `persist_results`: `true`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// The maximum number of jobs fetched in a single batch.
    ///
    /// Must be greater than zero.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// The interval between worker heartbeats.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,

    /// The number of missed heartbeats allowed before a worker is
    /// considered dead.
    #[serde(default = "default_missed_heartbeats")]
    pub missed_heartbeats: usize,

    /// The queue from which jobs are consumed.
    pub queue: Queue,

    /// An optional database URL used by the worker.
    pub database_url: Option<String>,

    /// Whether tasks should be locked while being processed.
    #[serde(default = "default_events")]
    pub lock_tasks: bool,

    /// Whether job results should be persisted.
    #[serde(default = "default_events")]
    pub persist_results: bool,

    /// Whether to emit task events via pubsub
    #[serde(default = "default_events")]
    pub emit_events: bool,

    /// How long an idempotency key should be retained.
    ///
    /// When `None`, idempotency keys do not expire.
    /// When `Some(duration)`, the key expires after the configured duration.
    #[serde(default)]
    pub idempotency_ttl: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            batch_size: 10,
            heartbeat_interval: Duration::from_secs(30),
            missed_heartbeats: 10,
            queue: Queue::from("default"),
            database_url: None,
            lock_tasks: true,
            persist_results: true,
            emit_events: true,
            idempotency_ttl: None,
        }
    }
}

fn default_batch_size() -> usize {
    10
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_missed_heartbeats() -> usize {
    2
}

fn default_events() -> bool {
    true
}

impl Config {
    /// Sets the maximum number of jobs to fetch in a single batch.
    ///
    /// Larger batches can improve throughput by reducing the number of
    /// queue operations, while smaller batches can reduce memory usage
    /// and improve job distribution between workers.
    ///
    /// # Panics
    ///
    /// Panics if `size` is `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redis::Config;
    ///
    /// let config = Config::default().batch_size(50);
    ///
    /// assert_eq!(config.batch_size, 50);
    /// ```
    #[must_use]
    pub fn batch_size(mut self, size: usize) -> Self {
        assert!(size > 0, "batch size cannot be 0");
        self.batch_size = size;
        self
    }

    /// Sets the interval between worker heartbeats.
    ///
    /// A shorter interval detects failed workers sooner but produces
    /// heartbeat activity more frequently.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redis::Config;
    /// use std::time::Duration;
    ///
    /// let config = Config::default()
    ///     .heartbeat_interval(Duration::from_secs(15));
    ///
    /// assert_eq!(config.heartbeat_interval, Duration::from_secs(15));
    /// ```
    #[must_use]
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Sets the queue from which jobs are consumed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use apalis_redis::Config;
    /// let config = Config::default()
    ///     .queue("high-priority");
    ///
    /// assert_eq!(config.queue.as_ref(), "high-priority");
    /// ```
    #[must_use]
    pub fn queue(mut self, queue: impl AsRef<str>) -> Self {
        self.queue = Queue::from(queue.as_ref());
        self
    }

    /// Sets the number of missed heartbeats allowed before a worker is
    /// considered dead.
    ///
    /// This value works together with [`Self::heartbeat_interval`].
    /// For example, a 30-second heartbeat interval with `2` missed
    /// heartbeats results in an orphan timeout of 60 seconds.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redis::Config;
    /// use std::time::Duration;
    ///
    /// let config = Config::default().missed_heartbeats(3);
    ///
    /// assert_eq!(config.missed_heartbeats, 3);
    /// assert_eq!(
    ///     config.orphaned_duration(),
    ///     Duration::from_secs(90)
    /// );
    /// ```
    #[must_use]
    pub fn missed_heartbeats(mut self, missed_heartbeats: usize) -> Self {
        self.missed_heartbeats = missed_heartbeats;
        self
    }

    /// Sets the database URL used by the worker.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redis::Config;
    ///
    /// let config = Config::default()
    ///     .database_url("redis://localhost/1");
    ///
    /// assert_eq!(
    ///     config.database_url.as_deref(),
    ///     Some("redis://localhost/1")
    /// );
    /// ```
    #[must_use]
    pub fn database_url(mut self, database_url: impl Into<String>) -> Self {
        self.database_url = Some(database_url.into());
        self
    }

    /// Enables or disables task locking and worker lease management.
    ///
    /// When enabled, tasks are locked while being processed to prevent
    /// multiple workers from processing the same task concurrently.
    ///
    /// This can be turned off for tasks that last shorter than a worker heartbeat as lease renewal will not be helpful
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redis::Config;
    ///
    /// let config = Config::default().lock_tasks(false);
    ///
    /// assert!(!config.lock_tasks);
    /// ```
    #[must_use]
    pub fn lock_tasks(mut self, lock_tasks: bool) -> Self {
        self.lock_tasks = lock_tasks;
        self
    }

    /// Enables or disables emitting pubsub events such as task events.
    ///
    /// When enabled, task and worker events will be emitted via pubsub
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redis::Config;
    ///
    /// let config = Config::default().emit_events(false);
    ///
    /// assert!(!config.emit_events);
    /// ```
    #[must_use]
    pub fn emit_events(mut self, emit_events: bool) -> Self {
        self.emit_events = emit_events;
        self
    }

    /// Enables or disables result persistence.
    ///
    /// When enabled, results produced by completed tasks are persisted.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redis::Config;
    ///
    /// let config = Config::default().persist_results(false);
    ///
    /// assert!(!config.persist_results);
    /// ```
    #[must_use]
    pub fn persist_results(mut self, persist_results: bool) -> Self {
        self.persist_results = persist_results;
        self
    }

    /// Sets the time-to-live for task idempotency keys.
    ///
    /// When configured, idempotency keys expire after the specified
    /// duration, allowing a task with the same key to be enqueued again.
    ///
    /// Use `None` to disable expiration.
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_redis::Config;
    /// use std::time::Duration;
    ///
    /// let config = Config::default()
    ///     .idempotency_ttl(Some(Duration::from_secs(3600)));
    ///
    /// assert_eq!(
    ///     config.idempotency_ttl,
    ///     Some(Duration::from_secs(3600))
    /// );
    /// ```
    #[must_use]
    pub fn idempotency_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.idempotency_ttl = ttl;
        self
    }

    /// Returns the amount of time after which a worker may be considered
    /// orphaned.
    ///
    /// The duration is calculated as:
    ///
    /// ```text
    /// heartbeat_interval × missed_heartbeats
    /// ```
    #[must_use]
    pub fn orphaned_duration(&self) -> Duration {
        self.heartbeat_interval * self.missed_heartbeats as u32
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

    /// Returns the Redis key for the workers metadata key.
    pub fn worker_metadata_key(&self) -> String {
        format!("{}:workers:", self.queue.as_ref())
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

    /// Returns the unique inflight set.
    pub fn inflight_worker_id(&self, worker: &WorkerContext) -> String {
        format!("{}:{}", self.inflight_jobs_set(), worker.name())
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

    /// Gets the set used to store idempotency keys preventing duplicates
    pub fn idempotency_key_set(&self) -> String {
        IDEMPOTENCY_KEY_SET.replace("{queue}", self.queue.as_ref())
    }
}
