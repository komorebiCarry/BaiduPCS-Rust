//! 任务位池管理模块
//!
//! 管理任务级槽位（固定位 + 借调位），决定哪些任务能获得运行资格。
//! - 固定位：单文件或文件夹的主任务位
//! - 借调位：文件夹借用的额外位，用于子任务并行
//!
//! 优先级支持（参考 autobackup/priority/policy.rs）：
//! - 普通任务（Normal）：优先级最高，可抢占备份任务的槽位
//! - 子任务（SubTask）：中等优先级，可抢占备份任务的槽位
//! - 备份任务（Backup）：优先级最低，只能使用空闲槽位，可被抢占

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// 槽位过期警告阈值（2分钟未更新）
pub const STALE_WARNING_THRESHOLD: Duration = Duration::from_secs(120);
/// 槽位过期释放阈值（5分钟未更新）
pub const STALE_RELEASE_THRESHOLD: Duration = Duration::from_secs(300);
/// 清理任务执行间隔（30秒）
pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// 任务位类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSlotType {
    /// 固定任务位（单文件或文件夹的主任务位）
    Fixed,
    /// 借调任务位（文件夹借用的额外位）
    Borrowed,
}

/// 任务优先级（与 autobackup/priority/policy.rs 中的 Priority 对应）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    /// 普通任务（优先级最高，值=10）
    Normal = 10,
    /// 子任务（中等优先级，值=20）
    SubTask = 20,
    /// 备份任务（优先级最低，值=30）
    Backup = 30,
}

impl TaskPriority {
    /// 是否可以抢占目标优先级的任务
    pub fn can_preempt(&self, target: TaskPriority) -> bool {
        (*self as u8) < (target as u8)
    }
}

impl Default for TaskPriority {
    fn default() -> Self {
        TaskPriority::Normal
    }
}

/// 任务位
#[derive(Debug, Clone)]
pub struct TaskSlot {
    /// 槽位ID
    pub id: usize,
    /// 槽位类型
    pub slot_type: TaskSlotType,
    /// 占用此位的任务ID
    pub task_id: Option<String>,
    /// 是否为文件夹主任务位
    pub is_folder_main: bool,
    /// 任务优先级
    pub priority: TaskPriority,
    /// 槽位分配时间戳
    pub allocated_at: Option<Instant>,
    /// 最后更新时间戳
    pub last_updated_at: Option<Instant>,
}

impl TaskSlot {
    /// 创建新的任务位
    fn new(id: usize) -> Self {
        Self {
            id,
            slot_type: TaskSlotType::Fixed,
            task_id: None,
            is_folder_main: false,
            priority: TaskPriority::Normal,
            allocated_at: None,
            last_updated_at: None,
        }
    }

    /// 检查槽位是否空闲
    pub fn is_free(&self) -> bool {
        self.task_id.is_none()
    }

    /// 检查槽位是否被备份任务占用（可被抢占）
    pub fn is_preemptable(&self) -> bool {
        self.task_id.is_some() && self.priority == TaskPriority::Backup
    }

    /// 分配给任务
    fn allocate(&mut self, task_id: &str, slot_type: TaskSlotType, is_folder_main: bool) {
        let now = Instant::now();
        self.task_id = Some(task_id.to_string());
        self.slot_type = slot_type;
        self.is_folder_main = is_folder_main;
        self.priority = TaskPriority::Normal;
        self.allocated_at = Some(now);
        self.last_updated_at = Some(now);
    }

    /// 分配给任务（带优先级）
    fn allocate_with_priority(&mut self, task_id: &str, slot_type: TaskSlotType, is_folder_main: bool, priority: TaskPriority) {
        let now = Instant::now();
        self.task_id = Some(task_id.to_string());
        self.slot_type = slot_type;
        self.is_folder_main = is_folder_main;
        self.priority = priority;
        self.allocated_at = Some(now);
        self.last_updated_at = Some(now);
    }

    /// 释放槽位
    fn release(&mut self) {
        self.task_id = None;
        self.slot_type = TaskSlotType::Fixed;
        self.is_folder_main = false;
        self.priority = TaskPriority::Normal;
        self.allocated_at = None;
        self.last_updated_at = None;
    }
}

/// 任务位池管理器
#[derive(Debug)]
pub struct TaskSlotPool {
    /// 最大槽位数（支持动态调整）
    max_slots: Arc<AtomicUsize>,
    /// 槽位列表
    slots: Arc<RwLock<Vec<TaskSlot>>>,
    /// 文件夹的借调位记录 folder_id -> [borrowed_slot_ids]
    borrowed_map: Arc<RwLock<HashMap<String, Vec<usize>>>>,
    /// 清理任务句柄（用于 shutdown 时取消）
    cleanup_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl TaskSlotPool {
    /// 创建新的任务位池
    pub fn new(max_slots: usize) -> Self {
        let slots = (0..max_slots).map(TaskSlot::new).collect();

        info!("创建任务位池，最大槽位数: {}", max_slots);

        Self {
            max_slots: Arc::new(AtomicUsize::new(max_slots)),
            slots: Arc::new(RwLock::new(slots)),
            borrowed_map: Arc::new(RwLock::new(HashMap::new())),
            cleanup_task_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// 获取最大槽位数
    pub fn max_slots(&self) -> usize {
        self.max_slots.load(Ordering::SeqCst)
    }

    /// 动态调整槽位池容量
    ///
    /// # Arguments
    /// * `new_max` - 新的最大槽位数
    ///
    /// # 扩容策略
    /// - 直接追加新的空闲槽位到池中
    ///
    /// # 缩容策略
    /// - 不会中断已占用的槽位，超出新上限的任务继续运行到完成
    /// - 只移除空闲槽位
    /// - 新的分配只会在新上限范围内进行
    /// - 如果有超出新上限的占用槽位，会记录警告
    pub async fn resize(&self, new_max: usize) {
        let old_max = self.max_slots.load(Ordering::SeqCst);

        if new_max == old_max {
            debug!("任务位池容量无需调整: {}", old_max);
            return;
        }

        let mut slots = self.slots.write().await;

        if new_max > old_max {
            let additional = new_max - old_max;
            for i in old_max..new_max {
                slots.push(TaskSlot::new(i));
            }
            info!("✅ 任务位池扩容: {} -> {} (+{}个槽位)", old_max, new_max, additional);
        } else {
            let occupied_beyond_limit = slots
                .iter()
                .filter(|s| s.id >= new_max && !s.is_free())
                .count();

            if occupied_beyond_limit > 0 {
                warn!(
                    "⚠️ 任务位池缩容: {} -> {} (有 {} 个超出新上限的槽位仍被占用，将继续运行)",
                    old_max, new_max, occupied_beyond_limit
                );
            } else {
                slots.retain(|s| s.id < new_max);
                info!("✅ 任务位池缩容: {} -> {} (已清理空闲槽位)", old_max, new_max);
            }
        }

        self.max_slots.store(new_max, Ordering::SeqCst);
    }

    /// 尝试分配固定任务位
    ///
    /// # Arguments
    /// * `task_id` - 任务ID
    /// * `is_folder` - 是否为文件夹任务
    ///
    /// # Returns
    /// 分配成功返回 Some(slot_id)，否则返回 None
    pub async fn allocate_fixed_slot(&self, task_id: &str, is_folder: bool) -> Option<usize> {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let mut slots = self.slots.write().await;

        for slot in slots.iter_mut() {
            if slot.id < max_slots && slot.is_free() {
                slot.allocate(task_id, TaskSlotType::Fixed, is_folder);
                info!(
                    "分配固定任务位: slot_id={}, task_id={}, is_folder={}",
                    slot.id, task_id, is_folder
                );
                return Some(slot.id);
            }
        }
        debug!("无可用固定任务位: task_id={}", task_id);
        None
    }

    /// 尝试分配固定任务位（带优先级）
    ///
    /// 支持优先级抢占：
    /// - 普通任务和子任务可以抢占备份任务的槽位
    /// - 备份任务只能使用空闲槽位
    ///
    /// # Arguments
    /// * `task_id` - 任务ID
    /// * `is_folder` - 是否为文件夹任务
    /// * `priority` - 任务优先级
    ///
    /// # Returns
    /// 分配成功返回 Some((slot_id, preempted_task_id))
    /// - slot_id: 分配的槽位ID
    /// - preempted_task_id: 被抢占的任务ID（如果有）
    pub async fn allocate_fixed_slot_with_priority(
        &self,
        task_id: &str,
        is_folder: bool,
        priority: TaskPriority,
    ) -> Option<(usize, Option<String>)> {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let mut slots = self.slots.write().await;

        for slot in slots.iter_mut() {
            if slot.id < max_slots && slot.is_free() {
                slot.allocate_with_priority(task_id, TaskSlotType::Fixed, is_folder, priority);
                info!(
                    "分配固定任务位: slot_id={}, task_id={}, is_folder={}, priority={:?}",
                    slot.id, task_id, is_folder, priority
                );
                return Some((slot.id, None));
            }
        }

        if priority.can_preempt(TaskPriority::Backup) {
            for slot in slots.iter_mut() {
                if slot.id < max_slots && slot.is_preemptable() {
                    let preempted_task_id = slot.task_id.clone();
                    info!(
                        "抢占备份任务槽位: slot_id={}, new_task={}, preempted_task={:?}, priority={:?}",
                        slot.id, task_id, preempted_task_id, priority
                    );
                    slot.allocate_with_priority(task_id, TaskSlotType::Fixed, is_folder, priority);
                    return Some((slot.id, preempted_task_id));
                }
            }
        }

        debug!("无可用固定任务位（优先级={:?}）: task_id={}", priority, task_id);
        None
    }

    /// 为备份任务分配槽位（仅使用空闲槽位，不抢占）
    ///
    /// # Arguments
    /// * `task_id` - 任务ID
    ///
    /// # Returns
    /// 分配成功返回 Some(slot_id)，否则返回 None
    pub async fn allocate_backup_slot(&self, task_id: &str) -> Option<usize> {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let mut slots = self.slots.write().await;

        for slot in slots.iter_mut() {
            if slot.id < max_slots && slot.is_free() {
                slot.allocate_with_priority(task_id, TaskSlotType::Fixed, false, TaskPriority::Backup);
                info!(
                    "分配备份任务位: slot_id={}, task_id={}",
                    slot.id, task_id
                );
                return Some(slot.id);
            }
        }
        debug!("无可用备份任务位: task_id={}", task_id);
        None
    }

    /// 获取当前被备份任务占用的槽位数
    pub async fn backup_slots_count(&self) -> usize {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let slots = self.slots.read().await;
        slots.iter().filter(|s| s.id < max_slots && s.priority == TaskPriority::Backup && !s.is_free()).count()
    }

    /// 查找可被抢占的备份任务槽位
    ///
    /// # Returns
    /// 返回第一个可被抢占的备份任务的 (slot_id, task_id)
    pub async fn find_preemptable_backup_slot(&self) -> Option<(usize, String)> {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let slots = self.slots.read().await;

        for slot in slots.iter() {
            if slot.id < max_slots && slot.is_preemptable() {
                if let Some(ref task_id) = slot.task_id {
                    return Some((slot.id, task_id.clone()));
                }
            }
        }
        None
    }

    /// 计算可借调位数量（空闲槽位 + 可被抢占的备份任务槽位）
    ///
    /// 文件夹任务借调槽位时，不仅可以使用空闲槽位，还可以抢占备份任务的槽位
    /// 因为备份任务优先级最低（TaskPriority::Backup），应被文件夹任务抢占
    pub async fn available_borrow_slots(&self) -> usize {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let slots = self.slots.read().await;
        slots.iter().filter(|s| s.id < max_slots && (s.is_free() || s.is_preemptable())).count()
    }

    /// 获取可用槽位数（包括固定位和可借调位）
    /// 
    /// 返回当前空闲的槽位总数，用于替代预注册机制中的余量查询
    pub async fn available_slots(&self) -> usize {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let slots = self.slots.read().await;
        slots.iter().filter(|s| s.id < max_slots && s.is_free()).count()
    }

    /// 获取当前已使用槽位数
    pub async fn used_slots(&self) -> usize {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let slots = self.slots.read().await;
        slots.iter().filter(|s| s.id < max_slots && !s.is_free()).count()
    }

    /// 为文件夹分配借调位（支持抢占备份任务）
    ///
    /// 优先分配空闲槽位，如果空闲槽位不足，则抢占备份任务的槽位。
    /// 备份任务优先级最低（TaskPriority::Backup），应被文件夹任务抢占。
    ///
    /// # Arguments
    /// * `folder_id` - 文件夹ID
    /// * `count` - 请求的借调位数量
    ///
    /// # Returns
    /// (实际分配的借调位ID列表, 被抢占的备份任务ID列表)
    pub async fn allocate_borrowed_slots(&self, folder_id: &str, count: usize) -> (Vec<usize>, Vec<String>) {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let mut allocated = Vec::new();
        let mut preempted_tasks = Vec::new();
        let mut slots = self.slots.write().await;

        for slot in slots.iter_mut() {
            if allocated.len() >= count {
                break;
            }
            if slot.id < max_slots && slot.is_free() {
                slot.allocate(folder_id, TaskSlotType::Borrowed, false);
                allocated.push(slot.id);
            }
        }

        if allocated.len() < count {
            for slot in slots.iter_mut() {
                if allocated.len() >= count {
                    break;
                }
                if slot.id < max_slots && slot.is_preemptable() {
                    if let Some(ref task_id) = slot.task_id {
                        preempted_tasks.push(task_id.clone());
                        info!(
                            "借调位抢占备份任务: slot_id={}, preempted_task={}, folder={}",
                            slot.id, task_id, folder_id
                        );
                    }
                    slot.allocate(folder_id, TaskSlotType::Borrowed, false);
                    allocated.push(slot.id);
                }
            }
        }

        if !allocated.is_empty() {
            drop(slots);
            let mut borrowed_map = self.borrowed_map.write().await;
            borrowed_map
                .entry(folder_id.to_string())
                .or_insert_with(Vec::new)
                .extend(&allocated);

            info!(
                "文件夹 {} 借调 {} 个任务位: {:?} (抢占 {} 个备份任务)",
                folder_id,
                allocated.len(),
                allocated,
                preempted_tasks.len()
            );
        }

        (allocated, preempted_tasks)
    }

    /// 为文件夹分配借调位（仅空闲槽位，不抢占）
    ///
    /// 用于不需要抢占备份任务的场景
    ///
    /// # Arguments
    /// * `folder_id` - 文件夹ID
    /// * `count` - 请求的借调位数量
    ///
    /// # Returns
    /// 实际分配的借调位ID列表
    pub async fn allocate_borrowed_slots_no_preempt(&self, folder_id: &str, count: usize) -> Vec<usize> {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let mut allocated = Vec::new();
        let mut slots = self.slots.write().await;

        for slot in slots.iter_mut() {
            if allocated.len() >= count {
                break;
            }
            if slot.id < max_slots && slot.is_free() {
                slot.allocate(folder_id, TaskSlotType::Borrowed, false);
                allocated.push(slot.id);
            }
        }

        if !allocated.is_empty() {
            drop(slots);
            let mut borrowed_map = self.borrowed_map.write().await;
            borrowed_map
                .entry(folder_id.to_string())
                .or_insert_with(Vec::new)
                .extend(&allocated);

            info!(
                "文件夹 {} 借调 {} 个任务位（不抢占）: {:?}",
                folder_id,
                allocated.len(),
                allocated
            );
        }

        allocated
    }

    /// 释放借调位
    ///
    /// # Arguments
    /// * `folder_id` - 文件夹ID
    /// * `slot_id` - 槽位ID
    pub async fn release_borrowed_slot(&self, folder_id: &str, slot_id: usize) {
        let mut slots = self.slots.write().await;
        if let Some(slot) = slots.iter_mut().find(|s| s.id == slot_id) {
            if slot.task_id.as_deref() == Some(folder_id) {
                slot.release();
                info!("释放借调位: slot_id={}, folder_id={}", slot_id, folder_id);
            } else {
                warn!(
                    "借调位释放失败：slot {} 不属于 folder {}",
                    slot_id, folder_id
                );
            }
        }

        drop(slots);

        let mut borrowed_map = self.borrowed_map.write().await;
        if let Some(borrowed_list) = borrowed_map.get_mut(folder_id) {
            borrowed_list.retain(|&id| id != slot_id);
            if borrowed_list.is_empty() {
                borrowed_map.remove(folder_id);
            }
        }
    }

    /// 释放固定位
    ///
    /// # Arguments
    /// * `task_id` - 任务ID
    pub async fn release_fixed_slot(&self, task_id: &str) {
        let mut slots = self.slots.write().await;
        for slot in slots.iter_mut() {
            if slot.task_id.as_deref() == Some(task_id) && slot.slot_type == TaskSlotType::Fixed {
                info!("释放固定任务位: slot_id={}, task_id={}", slot.id, task_id);
                slot.release();
                break;
            }
        }
    }

    /// 释放任务的所有槽位（固定位 + 借调位）
    ///
    /// # Arguments
    /// * `task_id` - 任务ID
    pub async fn release_all_slots(&self, task_id: &str) {
        let mut slots = self.slots.write().await;
        let mut released_count = 0;

        for slot in slots.iter_mut() {
            if slot.task_id.as_deref() == Some(task_id) {
                slot.release();
                released_count += 1;
            }
        }

        if released_count > 0 {
            info!(
                "释放任务 {} 的所有槽位: 共 {} 个",
                task_id, released_count
            );
        }

        drop(slots);

        let mut borrowed_map = self.borrowed_map.write().await;
        borrowed_map.remove(task_id);
    }

    /// 查找有借调位的文件夹（用于回收）
    ///
    /// # Returns
    /// 返回第一个有借调位的文件夹ID
    pub async fn find_folder_with_borrowed_slots(&self) -> Option<String> {
        let borrowed_map = self.borrowed_map.read().await;
        borrowed_map
            .iter()
            .find(|(_, slots)| !slots.is_empty())
            .map(|(folder_id, _)| folder_id.clone())
    }

    /// 获取文件夹的借调位列表
    ///
    /// # Arguments
    /// * `folder_id` - 文件夹ID
    ///
    /// # Returns
    /// 借调位ID列表
    pub async fn get_borrowed_slots(&self, folder_id: &str) -> Vec<usize> {
        let borrowed_map = self.borrowed_map.read().await;
        borrowed_map.get(folder_id).cloned().unwrap_or_default()
    }

    /// 检查任务是否占用槽位
    ///
    /// # Arguments
    /// * `task_id` - 任务ID
    ///
    /// # Returns
    /// 如果任务占用槽位，返回 (slot_id, slot_type)
    pub async fn get_task_slot(&self, task_id: &str) -> Option<(usize, TaskSlotType)> {
        let slots = self.slots.read().await;
        for slot in slots.iter() {
            if slot.task_id.as_deref() == Some(task_id) {
                return Some((slot.id, slot.slot_type));
            }
        }
        None
    }

    /// 获取所有槽位状态（用于调试）
    pub async fn get_all_slots_status(&self) -> Vec<(usize, Option<String>, TaskSlotType)> {
        let slots = self.slots.read().await;
        slots
            .iter()
            .map(|s| (s.id, s.task_id.clone(), s.slot_type))
            .collect()
    }

    /// 获取指定槽位的详细信息（用于调试和测试）
    ///
    /// # Arguments
    /// * `slot_id` - 槽位ID
    ///
    /// # Returns
    /// 槽位的详细信息，包括时间戳
    pub async fn get_slot_details(&self, slot_id: usize) -> Option<TaskSlot> {
        let slots = self.slots.read().await;
        slots.iter().find(|s| s.id == slot_id).cloned()
    }

    /// 设置槽位的最后更新时间戳（仅用于测试）
    ///
    /// # Arguments
    /// * `task_id` - 任务ID
    /// * `last_updated_at` - 要设置的时间戳
    ///
    /// # Returns
    /// 如果找到并更新了槽位返回 true，否则返回 false
    pub async fn set_slot_last_updated(&self, task_id: &str, last_updated_at: Instant) -> bool {
        let mut slots = self.slots.write().await;
        for slot in slots.iter_mut() {
            if slot.task_id.as_deref() == Some(task_id) {
                slot.last_updated_at = Some(last_updated_at);
                return true;
            }
        }
        false
    }

    /// 获取所有借调记录（用于调试）
    pub async fn get_all_borrowed_records(&self) -> HashMap<String, Vec<usize>> {
        let borrowed_map = self.borrowed_map.read().await;
        borrowed_map.clone()
    }

    /// 刷新槽位的最后更新时间戳
    ///
    /// 当任务有进度更新时调用此方法，防止槽位被误判为过期
    ///
    /// # Arguments
    /// * `task_id` - 任务ID
    ///
    /// # Returns
    /// 如果找到并更新了槽位返回 true，否则返回 false
    pub async fn touch_slot(&self, task_id: &str) -> bool {
        let mut slots = self.slots.write().await;
        for slot in slots.iter_mut() {
            if slot.task_id.as_deref() == Some(task_id) {
                let now = Instant::now();
                slot.last_updated_at = Some(now);
                debug!(
                    "刷新槽位时间戳: slot_id={}, task_id={}, last_updated_at={:?}",
                    slot.id, task_id, now
                );
                return true;
            }
        }
        debug!("未找到任务的槽位: task_id={}", task_id);
        false
    }

    /// 清理过期槽位
    ///
    /// 检测超过 5 分钟未更新的槽位，自动释放并记录日志。
    /// 超过 2 分钟但未达到 5 分钟的槽位会记录警告。
    ///
    /// # Returns
    /// 被释放的任务ID列表
    pub async fn cleanup_stale_slots(&self) -> Vec<String> {
        let now = Instant::now();
        let mut released_tasks = Vec::new();
        let mut warned_tasks = Vec::new();
        let max_slots = self.max_slots.load(Ordering::SeqCst);

        let mut slots = self.slots.write().await;
        
        for slot in slots.iter_mut() {
            if slot.id >= max_slots || slot.is_free() {
                continue;
            }

            if let Some(last_updated) = slot.last_updated_at {
                let elapsed = now.duration_since(last_updated);
                
                if elapsed >= STALE_RELEASE_THRESHOLD {
                    // 超过5分钟，自动释放
                    let task_id = slot.task_id.clone().unwrap_or_default();
                    let allocated_at = slot.allocated_at;
                    
                    error!(
                        "槽位过期自动释放: slot_id={}, task_id={}, 已占用时间={:?}, 最后更新={:?}",
                        slot.id, task_id, 
                        allocated_at.map(|t| now.duration_since(t)),
                        elapsed
                    );
                    
                    released_tasks.push(task_id);
                    slot.release();
                } else if elapsed >= STALE_WARNING_THRESHOLD {
                    // 超过2分钟，记录警告
                    let task_id = slot.task_id.as_deref().unwrap_or("unknown");
                    warned_tasks.push((slot.id, task_id.to_string(), elapsed));
                }
            }
        }

        // 释放写锁后记录警告日志
        drop(slots);

        for (slot_id, task_id, elapsed) in warned_tasks {
            warn!(
                "槽位可能过期: slot_id={}, task_id={}, 未更新时间={:?}",
                slot_id, task_id, elapsed
            );
        }

        // 清理 borrowed_map 中已释放的槽位
        if !released_tasks.is_empty() {
            let mut borrowed_map = self.borrowed_map.write().await;
            for task_id in &released_tasks {
                borrowed_map.remove(task_id);
            }
            
            info!(
                "清理过期槽位完成: 释放了 {} 个槽位",
                released_tasks.len()
            );
        }

        released_tasks
    }

    /// 启动定期清理任务
    ///
    /// 使用 tokio::spawn 启动后台任务，每 30 秒执行一次槽位清理检查。
    /// 返回一个 JoinHandle，可用于取消清理任务。
    /// 
    /// 注意：此方法返回的 JoinHandle 不会被自动保存。
    /// 如果需要在 shutdown 时自动取消任务，请使用 start_cleanup_task_managed 方法。
    ///
    /// # Arguments
    /// * `self` - Arc 包装的 TaskSlotPool 实例
    ///
    /// # Returns
    /// tokio::task::JoinHandle，可用于等待或取消任务
    pub fn start_cleanup_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        info!("启动槽位清理后台任务，间隔: {:?}", CLEANUP_INTERVAL);
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
            
            loop {
                interval.tick().await;
                
                let released = self.cleanup_stale_slots().await;
                
                if !released.is_empty() {
                    warn!(
                        "定期清理发现 {} 个过期槽位: {:?}",
                        released.len(),
                        released
                    );
                }
            }
        })
    }

    /// 启动定期清理任务并保存句柄
    ///
    /// 与 start_cleanup_task 类似，但会将句柄保存到内部字段中，
    /// 以便后续通过 shutdown 方法取消任务。
    ///
    /// # Arguments
    /// * `self` - Arc 包装的 TaskSlotPool 实例
    pub async fn start_cleanup_task_managed(self: Arc<Self>) {
        info!("启动槽位清理后台任务（托管模式），间隔: {:?}", CLEANUP_INTERVAL);
        
        let pool = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
            
            loop {
                interval.tick().await;
                
                let released = pool.cleanup_stale_slots().await;
                
                if !released.is_empty() {
                    warn!(
                        "定期清理发现 {} 个过期槽位: {:?}",
                        released.len(),
                        released
                    );
                }
            }
        });
        
        // 保存句柄
        let mut guard = self.cleanup_task_handle.lock().await;
        *guard = Some(handle);
    }

    /// 关闭任务位池，取消清理任务
    ///
    /// 取消正在运行的清理后台任务，并等待其完成。
    /// 如果没有运行中的清理任务，此方法会立即返回。
    pub async fn shutdown(&self) {
        info!("正在关闭任务位池...");
        
        let mut guard = self.cleanup_task_handle.lock().await;
        if let Some(handle) = guard.take() {
            info!("取消槽位清理后台任务");
            handle.abort();
            // 等待任务完成（会因为 abort 而返回 Err）
            match handle.await {
                Ok(_) => info!("槽位清理任务正常结束"),
                Err(e) if e.is_cancelled() => info!("槽位清理任务已取消"),
                Err(e) => warn!("槽位清理任务异常结束: {}", e),
            }
        } else {
            debug!("没有运行中的清理任务需要取消");
        }
        
        info!("任务位池已关闭");
    }

    /// 检查清理任务是否正在运行
    pub async fn is_cleanup_task_running(&self) -> bool {
        let guard = self.cleanup_task_handle.lock().await;
        if let Some(ref handle) = *guard {
            !handle.is_finished()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_task_slot_pool_creation() {
        let pool = TaskSlotPool::new(5);
        assert_eq!(pool.max_slots(), 5);
        assert_eq!(pool.available_borrow_slots().await, 5);
        assert_eq!(pool.used_slots().await, 0);
    }

    #[tokio::test]
    async fn test_allocate_fixed_slot() {
        let pool = TaskSlotPool::new(3);

        let slot1 = pool.allocate_fixed_slot("task1", false).await;
        assert!(slot1.is_some());
        assert_eq!(slot1.unwrap(), 0);

        let slot2 = pool.allocate_fixed_slot("task2", true).await;
        assert!(slot2.is_some());
        assert_eq!(slot2.unwrap(), 1);

        let slot3 = pool.allocate_fixed_slot("task3", false).await;
        assert!(slot3.is_some());
        assert_eq!(slot3.unwrap(), 2);

        let slot4 = pool.allocate_fixed_slot("task4", false).await;
        assert!(slot4.is_none());

        assert_eq!(pool.used_slots().await, 3);
        assert_eq!(pool.available_borrow_slots().await, 0);
    }

    #[tokio::test]
    async fn test_allocate_borrowed_slots() {
        let pool = TaskSlotPool::new(5);

        let fixed = pool.allocate_fixed_slot("folder1", true).await;
        assert!(fixed.is_some());

        let (borrowed, preempted) = pool.allocate_borrowed_slots("folder1", 3).await;
        assert_eq!(borrowed.len(), 3);
        assert_eq!(borrowed, vec![1, 2, 3]);
        assert!(preempted.is_empty());

        let borrowed_slots = pool.get_borrowed_slots("folder1").await;
        assert_eq!(borrowed_slots.len(), 3);

        assert_eq!(pool.used_slots().await, 4);
        assert_eq!(pool.available_borrow_slots().await, 1);
    }

    #[tokio::test]
    async fn test_release_borrowed_slot() {
        let pool = TaskSlotPool::new(5);

        pool.allocate_fixed_slot("folder1", true).await;

        let (borrowed, _) = pool.allocate_borrowed_slots("folder1", 2).await;
        assert_eq!(borrowed.len(), 2);

        pool.release_borrowed_slot("folder1", borrowed[0]).await;

        let remaining = pool.get_borrowed_slots("folder1").await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0], borrowed[1]);

        assert_eq!(pool.available_borrow_slots().await, 3);
    }

    #[tokio::test]
    async fn test_release_fixed_slot() {
        let pool = TaskSlotPool::new(3);

        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;

        assert_eq!(pool.used_slots().await, 2);

        pool.release_fixed_slot("task1").await;
        assert_eq!(pool.used_slots().await, 1);
        assert_eq!(pool.available_borrow_slots().await, 2);
    }

    #[tokio::test]
    async fn test_release_all_slots() {
        let pool = TaskSlotPool::new(5);

        pool.allocate_fixed_slot("folder1", true).await;

        pool.allocate_borrowed_slots("folder1", 3).await;

        assert_eq!(pool.used_slots().await, 4);

        pool.release_all_slots("folder1").await;

        assert_eq!(pool.used_slots().await, 0);
        assert_eq!(pool.get_borrowed_slots("folder1").await.len(), 0);
    }

    #[tokio::test]
    async fn test_find_folder_with_borrowed_slots() {
        let pool = TaskSlotPool::new(5);

        assert!(pool.find_folder_with_borrowed_slots().await.is_none());

        pool.allocate_fixed_slot("folder1", true).await;
        pool.allocate_borrowed_slots("folder1", 2).await;

        let folder = pool.find_folder_with_borrowed_slots().await;
        assert!(folder.is_some());
        assert_eq!(folder.unwrap(), "folder1");
    }

    #[tokio::test]
    async fn test_get_task_slot() {
        let pool = TaskSlotPool::new(3);

        pool.allocate_fixed_slot("task1", false).await;

        let slot_info = pool.get_task_slot("task1").await;
        assert!(slot_info.is_some());
        let (slot_id, slot_type) = slot_info.unwrap();
        assert_eq!(slot_id, 0);
        assert_eq!(slot_type, TaskSlotType::Fixed);

        assert!(pool.get_task_slot("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_borrowed_slots_limit() {
        let pool = TaskSlotPool::new(3);

        pool.allocate_fixed_slot("folder1", true).await;

        let (borrowed, _) = pool.allocate_borrowed_slots("folder1", 5).await;
        assert_eq!(borrowed.len(), 2);
    }

    #[tokio::test]
    async fn test_borrowed_slots_preempt_backup() {
        let pool = TaskSlotPool::new(5);

        pool.allocate_backup_slot("backup1").await;
        pool.allocate_backup_slot("backup2").await;
        pool.allocate_backup_slot("backup3").await;
        assert_eq!(pool.used_slots().await, 3);
        assert_eq!(pool.backup_slots_count().await, 3);

        pool.allocate_fixed_slot("folder1", true).await;
        assert_eq!(pool.used_slots().await, 4);

        assert_eq!(pool.available_borrow_slots().await, 4);

        let (borrowed, preempted) = pool.allocate_borrowed_slots("folder1", 4).await;
        assert_eq!(borrowed.len(), 4);
        assert_eq!(preempted.len(), 3);
        assert!(preempted.contains(&"backup1".to_string()));
        assert!(preempted.contains(&"backup2".to_string()));
        assert!(preempted.contains(&"backup3".to_string()));

        assert_eq!(pool.used_slots().await, 5);
        assert_eq!(pool.backup_slots_count().await, 0);
    }

    #[tokio::test]
    async fn test_borrowed_slots_partial_preempt() {
        let pool = TaskSlotPool::new(5);

        pool.allocate_backup_slot("backup1").await;
        pool.allocate_backup_slot("backup2").await;
        assert_eq!(pool.backup_slots_count().await, 2);

        pool.allocate_fixed_slot("folder1", true).await;

        assert_eq!(pool.available_borrow_slots().await, 4);

        let (borrowed, preempted) = pool.allocate_borrowed_slots("folder1", 3).await;
        assert_eq!(borrowed.len(), 3);
        assert_eq!(preempted.len(), 1);

        assert_eq!(pool.used_slots().await, 5);
        assert_eq!(pool.backup_slots_count().await, 1);
    }

    #[tokio::test]
    async fn test_borrowed_slots_no_preempt_needed() {
        let pool = TaskSlotPool::new(5);

        pool.allocate_backup_slot("backup1").await;

        pool.allocate_fixed_slot("folder1", true).await;

        assert_eq!(pool.available_borrow_slots().await, 4);

        let (borrowed, preempted) = pool.allocate_borrowed_slots("folder1", 2).await;
        assert_eq!(borrowed.len(), 2);
        assert!(preempted.is_empty());

        assert_eq!(pool.used_slots().await, 4);
        assert_eq!(pool.backup_slots_count().await, 1);
    }

    #[tokio::test]
    async fn test_concurrent_allocation() {
        let pool = Arc::new(TaskSlotPool::new(10));

        let mut handles = Vec::new();

        for i in 0..15 {
            let pool_clone = pool.clone();
            let handle = tokio::spawn(async move {
                pool_clone
                    .allocate_fixed_slot(&format!("task{}", i), false)
                    .await
            });
            handles.push(handle);
        }

        let mut success_count = 0;
        for handle in handles {
            if handle.await.unwrap().is_some() {
                success_count += 1;
            }
        }

        assert_eq!(success_count, 10);
        assert_eq!(pool.used_slots().await, 10);
    }

    #[tokio::test]
    async fn test_resize_expand() {
        let pool = TaskSlotPool::new(3);

        assert_eq!(pool.max_slots(), 3);
        assert_eq!(pool.available_borrow_slots().await, 3);

        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        assert_eq!(pool.used_slots().await, 2);
        assert_eq!(pool.available_borrow_slots().await, 1);

        pool.resize(5).await;
        assert_eq!(pool.max_slots(), 5);

        assert_eq!(pool.used_slots().await, 2);
        assert_eq!(pool.available_borrow_slots().await, 3);

        let slot3 = pool.allocate_fixed_slot("task3", false).await;
        assert!(slot3.is_some());
        let slot4 = pool.allocate_fixed_slot("task4", false).await;
        assert!(slot4.is_some());
        assert_eq!(pool.used_slots().await, 4);
    }

    #[tokio::test]
    async fn test_resize_shrink_with_free_slots() {
        let pool = TaskSlotPool::new(5);

        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        assert_eq!(pool.used_slots().await, 2);

        pool.resize(3).await;
        assert_eq!(pool.max_slots(), 3);

        assert_eq!(pool.used_slots().await, 2);
        assert_eq!(pool.available_borrow_slots().await, 1);

        let slot3 = pool.allocate_fixed_slot("task3", false).await;
        assert!(slot3.is_some());
        
        let slot4 = pool.allocate_fixed_slot("task4", false).await;
        assert!(slot4.is_none());
    }

    #[tokio::test]
    async fn test_resize_shrink_with_occupied_slots() {
        let pool = TaskSlotPool::new(5);

        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        pool.allocate_fixed_slot("task3", false).await;
        pool.allocate_fixed_slot("task4", false).await;
        pool.allocate_fixed_slot("task5", false).await;
        assert_eq!(pool.used_slots().await, 5);

        pool.resize(3).await;
        assert_eq!(pool.max_slots(), 3);

        assert_eq!(pool.used_slots().await, 3);

        let slot_new = pool.allocate_fixed_slot("task_new", false).await;
        assert!(slot_new.is_none());

        pool.release_fixed_slot("task1").await;
        assert_eq!(pool.used_slots().await, 2);

        let slot_new2 = pool.allocate_fixed_slot("task_new2", false).await;
        assert!(slot_new2.is_some());
        assert_eq!(slot_new2.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_resize_no_change() {
        let pool = TaskSlotPool::new(5);

        pool.allocate_fixed_slot("task1", false).await;
        assert_eq!(pool.used_slots().await, 1);

        pool.resize(5).await;
        assert_eq!(pool.max_slots(), 5);

        assert_eq!(pool.used_slots().await, 1);
        assert_eq!(pool.available_borrow_slots().await, 4);
    }

    #[tokio::test]
    async fn test_resize_expand_then_shrink() {
        let pool = TaskSlotPool::new(3);

        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;

        pool.resize(7).await;
        assert_eq!(pool.max_slots(), 7);
        assert_eq!(pool.available_borrow_slots().await, 5);

        pool.allocate_fixed_slot("task3", false).await;
        pool.allocate_fixed_slot("task4", false).await;
        pool.allocate_fixed_slot("task5", false).await;
        assert_eq!(pool.used_slots().await, 5);

        pool.resize(4).await;
        assert_eq!(pool.max_slots(), 4);
        assert_eq!(pool.used_slots().await, 4);
        assert_eq!(pool.available_borrow_slots().await, 0);

        pool.release_fixed_slot("task1").await;
        assert_eq!(pool.used_slots().await, 3);

        let slot_new = pool.allocate_fixed_slot("task_new", false).await;
        assert!(slot_new.is_some());
    }

    #[tokio::test]
    async fn test_available_slots_basic() {
        let pool = TaskSlotPool::new(5);
        
        assert_eq!(pool.available_slots().await, 5);
        
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        assert_eq!(pool.available_slots().await, 3);
        
        pool.allocate_borrowed_slots("folder1", 1).await;
        assert_eq!(pool.available_slots().await, 2);
        
        pool.release_fixed_slot("task1").await;
        assert_eq!(pool.available_slots().await, 3);
    }

    #[tokio::test]
    async fn test_available_slots_edge_cases() {
        let pool = TaskSlotPool::new(3);
        
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        pool.allocate_fixed_slot("task3", false).await;
        assert_eq!(pool.available_slots().await, 0);
        
        pool.release_fixed_slot("task1").await;
        pool.release_fixed_slot("task2").await;
        pool.release_fixed_slot("task3").await;
        assert_eq!(pool.available_slots().await, 3);
    }

    #[tokio::test]
    async fn test_available_slots_concurrent() {
        let pool = Arc::new(TaskSlotPool::new(10));
        let mut handles = vec![];
        
        for i in 0..20 {
            let pool_clone = pool.clone();
            let handle = tokio::spawn(async move {
                let available = pool_clone.available_slots().await;
                if available > 0 {
                    pool_clone.allocate_fixed_slot(&format!("task{}", i), false).await
                } else {
                    None
                }
            });
            handles.push(handle);
        }
        
        let mut success = 0;
        for handle in handles {
            if handle.await.unwrap().is_some() {
                success += 1;
            }
        }
        
        assert_eq!(success, 10);
        assert_eq!(pool.available_slots().await, 0);
    }
}
