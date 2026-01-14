//! 统一优先级策略

use std::sync::Arc;

/// 优先级枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// 普通任务（优先级 10，最高）
    Normal = 10,
    /// 文件夹子任务（优先级 20）
    SubTask = 20,
    /// 自动备份任务（优先级 30，最低）
    Backup = 30,
}

impl Priority {
    /// 获取优先级数值（数值越小优先级越高）
    pub fn value(&self) -> u8 {
        *self as u8
    }

    /// 是否可以抢占目标优先级
    pub fn can_preempt(&self, target: Priority) -> bool {
        self.value() < target.value()
    }
}

/// 优先级上下文
#[derive(Debug, Clone)]
pub struct PriorityContext {
    /// 当前活跃任务数
    pub active_count: usize,
    /// 等待队列长度
    pub waiting_count: usize,
    /// 最大并发数
    pub max_concurrent: usize,
    /// 当前活跃的普通任务数
    pub active_normal_count: usize,
    /// 当前活跃的子任务数
    pub active_subtask_count: usize,
    /// 当前活跃的备份任务数
    pub active_backup_count: usize,
}

/// 任务优先级策略 trait
pub trait TaskPriorityPolicy: Send + Sync {
    /// 是否可以分配资源
    fn can_allocate(&self, context: &PriorityContext) -> bool;

    /// 当被抢占时调用
    fn on_preempt(&self, context: &PriorityContext);

    /// 获取优先级
    fn priority(&self) -> Priority;

    /// 获取策略名称（用于日志）
    fn name(&self) -> &'static str;
}

/// 普通任务优先级策略
pub struct NormalPriorityPolicy;

impl TaskPriorityPolicy for NormalPriorityPolicy {
    fn can_allocate(&self, context: &PriorityContext) -> bool {
        // 普通任务总是可以分配（如果有槽位或可以抢占）
        context.active_count < context.max_concurrent || context.active_backup_count > 0
    }

    fn on_preempt(&self, _context: &PriorityContext) {
        // 普通任务不会被抢占
    }

    fn priority(&self) -> Priority {
        Priority::Normal
    }

    fn name(&self) -> &'static str {
        "Normal"
    }
}

/// 子任务优先级策略
pub struct SubTaskPriorityPolicy;

impl TaskPriorityPolicy for SubTaskPriorityPolicy {
    fn can_allocate(&self, context: &PriorityContext) -> bool {
        // 子任务可以在有槽位时分配，或抢占备份任务
        context.active_count < context.max_concurrent || context.active_backup_count > 0
    }

    fn on_preempt(&self, _context: &PriorityContext) {
        // 子任务不会被抢占
    }

    fn priority(&self) -> Priority {
        Priority::SubTask
    }

    fn name(&self) -> &'static str {
        "SubTask"
    }
}

/// 备份任务优先级策略
pub struct BackupPriorityPolicy;

impl TaskPriorityPolicy for BackupPriorityPolicy {
    fn can_allocate(&self, context: &PriorityContext) -> bool {
        // 只有在没有普通任务和子任务等待时，且有空闲槽位才允许分配
        context.waiting_count == 0 && context.active_count < context.max_concurrent
    }

    fn on_preempt(&self, _context: &PriorityContext) {
        // 备份任务被抢占时，暂停并释放槽位
        tracing::info!("Backup task preempted, releasing slot");
    }

    fn priority(&self) -> Priority {
        Priority::Backup
    }

    fn name(&self) -> &'static str {
        "Backup"
    }
}

/// 优先级管理器
pub struct PriorityManager {
    max_concurrent: usize,
}

impl PriorityManager {
    pub fn new(max_concurrent: usize) -> Self {
        Self { max_concurrent }
    }

    /// 检查任务是否可以获取槽位
    pub fn can_acquire_slot(&self, priority: Priority, context: &PriorityContext) -> bool {
        match priority {
            Priority::Normal => {
                // 普通任务：有槽位就可以，或者可以抢占备份任务
                context.active_count < self.max_concurrent || context.active_backup_count > 0
            }
            Priority::SubTask => {
                // 子任务：有槽位就可以，或者可以抢占备份任务
                context.active_count < self.max_concurrent || context.active_backup_count > 0
            }
            Priority::Backup => {
                // 备份任务：只有在没有等待的高优先级任务且有空闲槽位时才可以
                context.waiting_count == 0 && context.active_count < self.max_concurrent
            }
        }
    }

    /// 获取应该被抢占的任务优先级
    pub fn get_preempt_target(&self, priority: Priority, context: &PriorityContext) -> Option<Priority> {
        if context.active_count >= self.max_concurrent {
            // 需要抢占
            match priority {
                Priority::Normal | Priority::SubTask => {
                    if context.active_backup_count > 0 {
                        Some(Priority::Backup)
                    } else {
                        None
                    }
                }
                Priority::Backup => None, // 备份任务不能抢占其他任务
            }
        } else {
            None
        }
    }
}

/// 槽位借用结果
#[derive(Debug, Clone)]
pub enum SlotAcquireResult {
    /// 成功获取槽位
    Acquired,
    /// 需要等待
    Wait,
    /// 需要抢占指定优先级的任务
    Preempt(Priority),
}

/// 让位请求
#[derive(Debug, Clone)]
pub struct PreemptRequest {
    /// 请求者任务 ID
    pub requester_task_id: String,
    /// 请求者优先级
    pub requester_priority: Priority,
    /// 目标优先级（被抢占的）
    pub target_priority: Priority,
}

/// 槽位管理器（增强版）
pub struct SlotManager {
    /// 最大并发数
    max_concurrent: usize,
    /// 当前活跃任务（按优先级分组）
    active_tasks: parking_lot::RwLock<ActiveTasks>,
    /// 等待队列（按优先级排序）
    waiting_queue: parking_lot::RwLock<Vec<WaitingTask>>,
    /// 抢占通知通道
    preempt_tx: tokio::sync::broadcast::Sender<PreemptRequest>,
}

/// 活跃任务统计
#[derive(Debug, Default)]
struct ActiveTasks {
    normal: Vec<String>,
    subtask: Vec<String>,
    backup: Vec<String>,
}

impl ActiveTasks {
    fn total(&self) -> usize {
        self.normal.len() + self.subtask.len() + self.backup.len()
    }

    fn add(&mut self, task_id: String, priority: Priority) {
        match priority {
            Priority::Normal => self.normal.push(task_id),
            Priority::SubTask => self.subtask.push(task_id),
            Priority::Backup => self.backup.push(task_id),
        }
    }

    fn remove(&mut self, task_id: &str) -> Option<Priority> {
        if let Some(pos) = self.normal.iter().position(|id| id == task_id) {
            self.normal.remove(pos);
            return Some(Priority::Normal);
        }
        if let Some(pos) = self.subtask.iter().position(|id| id == task_id) {
            self.subtask.remove(pos);
            return Some(Priority::SubTask);
        }
        if let Some(pos) = self.backup.iter().position(|id| id == task_id) {
            self.backup.remove(pos);
            return Some(Priority::Backup);
        }
        None
    }

    fn get_backup_task_to_preempt(&self) -> Option<String> {
        self.backup.first().cloned()
    }
}

/// 等待中的任务
#[derive(Debug, Clone)]
struct WaitingTask {
    task_id: String,
    priority: Priority,
    /// 入队时间（预留用于等待时间统计）
    #[allow(dead_code)]
    queued_at: std::time::Instant,
}

impl SlotManager {
    /// 创建新的槽位管理器
    pub fn new(max_concurrent: usize) -> Self {
        let (preempt_tx, _) = tokio::sync::broadcast::channel(16);
        Self {
            max_concurrent,
            active_tasks: parking_lot::RwLock::new(ActiveTasks::default()),
            waiting_queue: parking_lot::RwLock::new(Vec::new()),
            preempt_tx,
        }
    }

    /// 尝试获取槽位
    pub fn try_acquire(&self, task_id: &str, priority: Priority) -> SlotAcquireResult {
        let mut active = self.active_tasks.write();
        let waiting = self.waiting_queue.read();

        // 检查是否有空闲槽位
        if active.total() < self.max_concurrent {
            // 对于备份任务，还需要检查是否有高优先级任务在等待
            if priority == Priority::Backup {
                let has_higher_waiting = waiting.iter().any(|w| w.priority.value() < priority.value());
                if has_higher_waiting {
                    return SlotAcquireResult::Wait;
                }
            }
            active.add(task_id.to_string(), priority);
            return SlotAcquireResult::Acquired;
        }

        // 槽位已满，检查是否可以抢占
        if priority != Priority::Backup && !active.backup.is_empty() {
            return SlotAcquireResult::Preempt(Priority::Backup);
        }

        SlotAcquireResult::Wait
    }

    /// 释放槽位
    pub fn release(&self, task_id: &str) {
        let mut active = self.active_tasks.write();
        active.remove(task_id);

        // 尝试唤醒等待队列中的任务
        self.try_wake_waiting();
    }

    /// 加入等待队列
    pub fn enqueue(&self, task_id: &str, priority: Priority) {
        let mut queue = self.waiting_queue.write();

        // 按优先级插入（优先级高的在前）
        let task = WaitingTask {
            task_id: task_id.to_string(),
            priority,
            queued_at: std::time::Instant::now(),
        };

        let pos = queue.iter().position(|w| w.priority.value() > priority.value())
            .unwrap_or(queue.len());
        queue.insert(pos, task);
    }

    /// 从等待队列移除
    pub fn dequeue(&self, task_id: &str) -> bool {
        let mut queue = self.waiting_queue.write();
        if let Some(pos) = queue.iter().position(|w| w.task_id == task_id) {
            queue.remove(pos);
            true
        } else {
            false
        }
    }

    /// 执行抢占
    pub fn preempt(&self, requester_task_id: &str, requester_priority: Priority) -> Option<String> {
        let active = self.active_tasks.read();

        if let Some(victim_id) = active.get_backup_task_to_preempt() {
            // 发送抢占通知
            let request = PreemptRequest {
                requester_task_id: requester_task_id.to_string(),
                requester_priority,
                target_priority: Priority::Backup,
            };
            let _ = self.preempt_tx.send(request);
            Some(victim_id)
        } else {
            None
        }
    }

    /// 订阅抢占通知
    pub fn subscribe_preempt(&self) -> tokio::sync::broadcast::Receiver<PreemptRequest> {
        self.preempt_tx.subscribe()
    }

    /// 获取当前状态
    pub fn get_context(&self) -> PriorityContext {
        let active = self.active_tasks.read();
        let waiting = self.waiting_queue.read();

        PriorityContext {
            active_count: active.total(),
            waiting_count: waiting.len(),
            max_concurrent: self.max_concurrent,
            active_normal_count: active.normal.len(),
            active_subtask_count: active.subtask.len(),
            active_backup_count: active.backup.len(),
        }
    }

    /// 尝试唤醒等待队列中的任务
    fn try_wake_waiting(&self) {
        // 这里只是标记，实际唤醒逻辑由调用方处理
        // 可以通过 channel 或 condvar 实现
    }
}

/// 准备阶段资源池
pub struct PrepareResourcePool {
    /// 扫描信号量
    scan_semaphore: Arc<tokio::sync::Semaphore>,
    /// 加密信号量
    encrypt_semaphore: Arc<tokio::sync::Semaphore>,
    /// 配置
    max_concurrent_scans: usize,
    max_concurrent_encrypts: usize,
}

impl PrepareResourcePool {
    pub fn new(max_concurrent_scans: usize, max_concurrent_encrypts: usize) -> Self {
        Self {
            scan_semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent_scans)),
            encrypt_semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent_encrypts)),
            max_concurrent_scans,
            max_concurrent_encrypts,
        }
    }

    /// 获取扫描许可
    pub async fn acquire_scan_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.scan_semaphore.clone().acquire_owned().await
    }

    /// 尝试获取扫描许可（非阻塞）
    pub fn try_acquire_scan_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.scan_semaphore.clone().try_acquire_owned().ok()
    }

    /// 获取加密许可
    pub async fn acquire_encrypt_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.encrypt_semaphore.clone().acquire_owned().await
    }

    /// 尝试获取加密许可（非阻塞）
    pub fn try_acquire_encrypt_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.encrypt_semaphore.clone().try_acquire_owned().ok()
    }

    /// 获取当前扫描槽位使用情况
    pub fn scan_slots_info(&self) -> (usize, usize) {
        let available = self.scan_semaphore.available_permits();
        (self.max_concurrent_scans - available, self.max_concurrent_scans)
    }

    /// 获取当前加密槽位使用情况
    pub fn encrypt_slots_info(&self) -> (usize, usize) {
        let available = self.encrypt_semaphore.available_permits();
        (self.max_concurrent_encrypts - available, self.max_concurrent_encrypts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_values() {
        assert_eq!(Priority::Normal.value(), 10);
        assert_eq!(Priority::SubTask.value(), 20);
        assert_eq!(Priority::Backup.value(), 30);
    }

    #[test]
    fn test_priority_can_preempt() {
        // Normal can preempt SubTask and Backup
        assert!(Priority::Normal.can_preempt(Priority::SubTask));
        assert!(Priority::Normal.can_preempt(Priority::Backup));
        assert!(!Priority::Normal.can_preempt(Priority::Normal));

        // SubTask can preempt Backup only
        assert!(!Priority::SubTask.can_preempt(Priority::Normal));
        assert!(Priority::SubTask.can_preempt(Priority::Backup));
        assert!(!Priority::SubTask.can_preempt(Priority::SubTask));

        // Backup cannot preempt anyone
        assert!(!Priority::Backup.can_preempt(Priority::Normal));
        assert!(!Priority::Backup.can_preempt(Priority::SubTask));
        assert!(!Priority::Backup.can_preempt(Priority::Backup));
    }

    #[test]
    fn test_normal_priority_policy() {
        let policy = NormalPriorityPolicy;

        // Can allocate when slots available
        let context = PriorityContext {
            active_count: 2,
            waiting_count: 0,
            max_concurrent: 3,
            active_normal_count: 2,
            active_subtask_count: 0,
            active_backup_count: 0,
        };
        assert!(policy.can_allocate(&context));
        assert_eq!(policy.priority(), Priority::Normal);
        assert_eq!(policy.name(), "Normal");

        // Can allocate by preempting backup
        let context_full = PriorityContext {
            active_count: 3,
            waiting_count: 0,
            max_concurrent: 3,
            active_normal_count: 2,
            active_subtask_count: 0,
            active_backup_count: 1,
        };
        assert!(policy.can_allocate(&context_full));
    }

    #[test]
    fn test_subtask_priority_policy() {
        let policy = SubTaskPriorityPolicy;

        let context = PriorityContext {
            active_count: 2,
            waiting_count: 0,
            max_concurrent: 3,
            active_normal_count: 1,
            active_subtask_count: 1,
            active_backup_count: 0,
        };
        assert!(policy.can_allocate(&context));
        assert_eq!(policy.priority(), Priority::SubTask);
        assert_eq!(policy.name(), "SubTask");
    }

    #[test]
    fn test_backup_priority_policy() {
        let policy = BackupPriorityPolicy;

        // Can allocate when no waiting tasks and slots available
        let context = PriorityContext {
            active_count: 2,
            waiting_count: 0,
            max_concurrent: 3,
            active_normal_count: 2,
            active_subtask_count: 0,
            active_backup_count: 0,
        };
        assert!(policy.can_allocate(&context));
        assert_eq!(policy.priority(), Priority::Backup);
        assert_eq!(policy.name(), "Backup");

        // Cannot allocate when tasks waiting
        let context_waiting = PriorityContext {
            active_count: 2,
            waiting_count: 1,
            max_concurrent: 3,
            active_normal_count: 2,
            active_subtask_count: 0,
            active_backup_count: 0,
        };
        assert!(!policy.can_allocate(&context_waiting));

        // Cannot allocate when slots full
        let context_full = PriorityContext {
            active_count: 3,
            waiting_count: 0,
            max_concurrent: 3,
            active_normal_count: 3,
            active_subtask_count: 0,
            active_backup_count: 0,
        };
        assert!(!policy.can_allocate(&context_full));
    }

    #[test]
    fn test_priority_manager_can_acquire_slot() {
        let manager = PriorityManager::new(3);

        // Normal task can acquire when slots available
        let context = PriorityContext {
            active_count: 2,
            waiting_count: 0,
            max_concurrent: 3,
            active_normal_count: 2,
            active_subtask_count: 0,
            active_backup_count: 0,
        };
        assert!(manager.can_acquire_slot(Priority::Normal, &context));
        assert!(manager.can_acquire_slot(Priority::SubTask, &context));
        assert!(manager.can_acquire_slot(Priority::Backup, &context));

        // Backup cannot acquire when tasks waiting
        let context_waiting = PriorityContext {
            active_count: 2,
            waiting_count: 1,
            max_concurrent: 3,
            active_normal_count: 2,
            active_subtask_count: 0,
            active_backup_count: 0,
        };
        assert!(!manager.can_acquire_slot(Priority::Backup, &context_waiting));
    }

    #[test]
    fn test_priority_manager_get_preempt_target() {
        let manager = PriorityManager::new(3);

        // No preemption needed when slots available
        let context = PriorityContext {
            active_count: 2,
            waiting_count: 0,
            max_concurrent: 3,
            active_normal_count: 2,
            active_subtask_count: 0,
            active_backup_count: 0,
        };
        assert_eq!(manager.get_preempt_target(Priority::Normal, &context), None);

        // Preempt backup when slots full and backup running
        let context_full = PriorityContext {
            active_count: 3,
            waiting_count: 0,
            max_concurrent: 3,
            active_normal_count: 2,
            active_subtask_count: 0,
            active_backup_count: 1,
        };
        assert_eq!(manager.get_preempt_target(Priority::Normal, &context_full), Some(Priority::Backup));
        assert_eq!(manager.get_preempt_target(Priority::SubTask, &context_full), Some(Priority::Backup));
        assert_eq!(manager.get_preempt_target(Priority::Backup, &context_full), None);
    }

    #[test]
    fn test_slot_manager_try_acquire() {
        let manager = SlotManager::new(3);

        // Acquire slots
        let result1 = manager.try_acquire("task1", Priority::Normal);
        assert!(matches!(result1, SlotAcquireResult::Acquired));

        let result2 = manager.try_acquire("task2", Priority::SubTask);
        assert!(matches!(result2, SlotAcquireResult::Acquired));

        let result3 = manager.try_acquire("task3", Priority::Backup);
        assert!(matches!(result3, SlotAcquireResult::Acquired));

        // Slots full, normal task should preempt backup
        let result4 = manager.try_acquire("task4", Priority::Normal);
        assert!(matches!(result4, SlotAcquireResult::Preempt(Priority::Backup)));

        // Backup task should wait
        let result5 = manager.try_acquire("task5", Priority::Backup);
        assert!(matches!(result5, SlotAcquireResult::Wait));
    }

    #[test]
    fn test_slot_manager_release() {
        let manager = SlotManager::new(2);

        manager.try_acquire("task1", Priority::Normal);
        manager.try_acquire("task2", Priority::Normal);

        let context = manager.get_context();
        assert_eq!(context.active_count, 2);

        manager.release("task1");

        let context = manager.get_context();
        assert_eq!(context.active_count, 1);
    }

    #[test]
    fn test_slot_manager_enqueue_dequeue() {
        let manager = SlotManager::new(2);

        manager.enqueue("task1", Priority::Backup);
        manager.enqueue("task2", Priority::Normal);
        manager.enqueue("task3", Priority::SubTask);

        let context = manager.get_context();
        assert_eq!(context.waiting_count, 3);

        // Dequeue
        assert!(manager.dequeue("task2"));
        let context = manager.get_context();
        assert_eq!(context.waiting_count, 2);

        // Dequeue non-existent
        assert!(!manager.dequeue("task_not_exist"));
    }

    #[test]
    fn test_slot_manager_get_context() {
        let manager = SlotManager::new(3);

        manager.try_acquire("task1", Priority::Normal);
        manager.try_acquire("task2", Priority::SubTask);
        manager.try_acquire("task3", Priority::Backup);

        let context = manager.get_context();
        assert_eq!(context.active_count, 3);
        assert_eq!(context.active_normal_count, 1);
        assert_eq!(context.active_subtask_count, 1);
        assert_eq!(context.active_backup_count, 1);
        assert_eq!(context.max_concurrent, 3);
    }

    #[tokio::test]
    async fn test_prepare_resource_pool_scan_permit() {
        let pool = PrepareResourcePool::new(2, 2);

        // Acquire permits
        let permit1 = pool.acquire_scan_permit().await.unwrap();
        let permit2 = pool.acquire_scan_permit().await.unwrap();

        let (used, total) = pool.scan_slots_info();
        assert_eq!(used, 2);
        assert_eq!(total, 2);

        // Try acquire should fail
        assert!(pool.try_acquire_scan_permit().is_none());

        // Release and try again
        drop(permit1);
        assert!(pool.try_acquire_scan_permit().is_some());

        drop(permit2);
    }

    #[tokio::test]
    async fn test_prepare_resource_pool_encrypt_permit() {
        let pool = PrepareResourcePool::new(2, 2);

        let permit1 = pool.acquire_encrypt_permit().await.unwrap();
        let permit2 = pool.acquire_encrypt_permit().await.unwrap();

        let (used, total) = pool.encrypt_slots_info();
        assert_eq!(used, 2);
        assert_eq!(total, 2);

        assert!(pool.try_acquire_encrypt_permit().is_none());

        drop(permit1);
        drop(permit2);

        let (used, _) = pool.encrypt_slots_info();
        assert_eq!(used, 0);
    }
}
