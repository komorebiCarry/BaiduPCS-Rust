//! 调度器模块

pub mod backup_scheduler;
pub mod change_aggregator;
pub mod poll_scheduler;
pub mod task_controller;

pub use backup_scheduler::{BackupScheduler, FileTaskContext, SchedulerEvent, SchedulerStatus};
pub use change_aggregator::{
    bounded_event_channel, bounded_event_channel_with_strategy, BackpressureStrategy,
    ChangeAggregator, ChangeEvent, EventSender, GlobalPollType, DEFAULT_EVENT_CHANNEL_CAPACITY,
};
pub use poll_scheduler::{
    is_global_poll_id, PollScheduleConfig, PollScheduler, ScheduledTime,
    GLOBAL_POLL_DOWNLOAD_INTERVAL, GLOBAL_POLL_DOWNLOAD_SCHEDULED, GLOBAL_POLL_SYNC_INTERVAL,
    GLOBAL_POLL_SYNC_SCHEDULED, GLOBAL_POLL_UPLOAD_INTERVAL, GLOBAL_POLL_UPLOAD_SCHEDULED,
};
pub use task_controller::{task_loop, ControllerStatus, TaskController, TriggerSource};
