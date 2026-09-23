use std::time::{SystemTime, UNIX_EPOCH};

mod fetch_by_id;
mod fetch_next;
mod handle_results;
mod keep_alive;
mod list_queues;
mod list_tasks;
mod list_workers;
mod metrics;
mod push_tasks;
mod reenqueue;
mod register_worker;
mod task_lease;
mod vacuum;
mod wait_for;

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub use fetch_next::CompactTask;
pub use fetch_next::deserialize_with_meta;
pub use fetch_next::fetch_next;
pub(crate) use handle_results::handle_results;
pub(crate) use keep_alive::keep_alive;
pub use push_tasks::push_tasks;
pub(crate) use reenqueue::reenqueue_orphaned;
pub(crate) use register_worker::register_worker;
pub(crate) use task_lease::{acquire_leases, release_leases, renew_leases};
