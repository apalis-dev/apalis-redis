//! Cached Redis Lua scripts for improved performance.
//!
//! Scripts are lazily initialized on first use and reuse their SHA1 hashes
//! for subsequent EVALSHA calls, avoiding repeated hash computation overhead.
//!
//! See: <https://github.com/apalis-dev/apalis-redis/issues/17>

use std::sync::LazyLock;

pub use redis::Script;

/// Script for fetching jobs from the queue.
pub static GET_JOBS: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/get_jobs.lua")));

/// Script for registering a worker with the backend.
pub static REGISTER_WORKER: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/register_worker.lua")));

/// Script for enqueueing scheduled jobs.
pub static ENQUEUE_SCHEDULED: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/enqueue_scheduled_jobs.lua")));

/// Script for acknowledging job completion.
pub static ACK_JOB: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/ack_job.lua")));

/// Script for batch pushing jobs.
pub static BATCH_PUSH: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/batch_push.lua")));

/// Script for listing tasks by queue.
pub static LIST_TASKS: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/list_tasks.lua")));

/// Script for listing all tasks.
pub static LIST_ALL_TASKS: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/list_all_tasks.lua")));

/// Script for fetching global metrics overview.
pub static OVERVIEW: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/overview.lua")));

/// Script for fetching metrics by queue.
pub static OVERVIEW_BY_QUEUE: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/overview_by_queue.lua")));

/// Script for fetching a task by ID.
pub static FETCH_BY_ID: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/fetch_by_id.lua")));

/// Script for listing workers by queue.
pub static LIST_WORKERS: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/list_workers.lua")));

/// Script for listing all workers.
pub static LIST_ALL_WORKERS: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/list_all_workers.lua")));