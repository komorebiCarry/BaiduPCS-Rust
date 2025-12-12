//! 任务位池管理模块
//!
//! 管理任务级槽位（固定位 + 借调位），决定哪些任务能获得运行资格。
//! - 固定位：单文件或文件夹的主任务位
//! - 借调位：文件夹借用的额外位，用于子任务并行

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// 任务位类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSlotType {
    /// 固定任务位（单文件或文件夹的主任务位）
    Fixed,
    /// 借调任务位（文件夹借用的额外位）
    Borrowed,
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
}

impl TaskSlot {
    /// 创建新的任务位
    fn new(id: usize) -> Self {
        Self {
            id,
            slot_type: TaskSlotType::Fixed,
            task_id: None,
            is_folder_main: false,
        }
    }

    /// 检查槽位是否空闲
    pub fn is_free(&self) -> bool {
        self.task_id.is_none()
    }

    /// 分配给任务
    fn allocate(&mut self, task_id: &str, slot_type: TaskSlotType, is_folder_main: bool) {
        self.task_id = Some(task_id.to_string());
        self.slot_type = slot_type;
        self.is_folder_main = is_folder_main;
    }

    /// 释放槽位
    fn release(&mut self) {
        self.task_id = None;
        self.slot_type = TaskSlotType::Fixed;
        self.is_folder_main = false;
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

        // 无需调整
        if new_max == old_max {
            debug!("任务位池容量无需调整: {}", old_max);
            return;
        }

        let mut slots = self.slots.write().await;

        if new_max > old_max {
            // 扩容：追加新槽位
            let additional = new_max - old_max;
            for i in old_max..new_max {
                slots.push(TaskSlot::new(i));
            }
            info!("✅ 任务位池扩容: {} -> {} (+{}个槽位)", old_max, new_max, additional);
        } else {
            // 缩容：检查超出范围的槽位占用情况
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
                // 移除超出范围的空闲槽位
                slots.retain(|s| s.id < new_max);
                info!("✅ 任务位池缩容: {} -> {} (已清理空闲槽位)", old_max, new_max);
            }
        }

        // 更新 max_slots（无论是否有占用槽位，都更新上限）
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
        
        // 只在有效范围内分配（id < max_slots）
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

    /// 计算可借调位数量（空闲槽位数）
    pub async fn available_borrow_slots(&self) -> usize {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let slots = self.slots.read().await;
        slots.iter().filter(|s| s.id < max_slots && s.is_free()).count()
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

    /// 为文件夹分配借调位
    ///
    /// # Arguments
    /// * `folder_id` - 文件夹ID
    /// * `count` - 请求的借调位数量
    ///
    /// # Returns
    /// 实际分配的借调位ID列表
    pub async fn allocate_borrowed_slots(&self, folder_id: &str, count: usize) -> Vec<usize> {
        let max_slots = self.max_slots.load(Ordering::SeqCst);
        let mut allocated = Vec::new();
        let mut slots = self.slots.write().await;

        // 只在有效范围内分配（id < max_slots）
        for slot in slots.iter_mut() {
            if allocated.len() >= count {
                break;
            }
            if slot.id < max_slots && slot.is_free() {
                slot.allocate(folder_id, TaskSlotType::Borrowed, false);
                allocated.push(slot.id);
            }
        }

        // 记录借调关系
        if !allocated.is_empty() {
            drop(slots); // 释放 slots 锁
            let mut borrowed_map = self.borrowed_map.write().await;
            borrowed_map
                .entry(folder_id.to_string())
                .or_insert_with(Vec::new)
                .extend(&allocated);

            info!(
                "文件夹 {} 借调 {} 个任务位: {:?}",
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

        drop(slots); // 释放 slots 锁

        // 更新借调记录
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

        // 同时清理借调记录
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

    /// 获取所有借调记录（用于调试）
    pub async fn get_all_borrowed_records(&self) -> HashMap<String, Vec<usize>> {
        let borrowed_map = self.borrowed_map.read().await;
        borrowed_map.clone()
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

        // 分配第一个固定槽位
        let slot1 = pool.allocate_fixed_slot("task1", false).await;
        assert!(slot1.is_some());
        assert_eq!(slot1.unwrap(), 0);

        // 分配第二个固定槽位
        let slot2 = pool.allocate_fixed_slot("task2", true).await;
        assert!(slot2.is_some());
        assert_eq!(slot2.unwrap(), 1);

        // 分配第三个固定槽位
        let slot3 = pool.allocate_fixed_slot("task3", false).await;
        assert!(slot3.is_some());
        assert_eq!(slot3.unwrap(), 2);

        // 没有更多槽位
        let slot4 = pool.allocate_fixed_slot("task4", false).await;
        assert!(slot4.is_none());

        assert_eq!(pool.used_slots().await, 3);
        assert_eq!(pool.available_borrow_slots().await, 0);
    }

    #[tokio::test]
    async fn test_allocate_borrowed_slots() {
        let pool = TaskSlotPool::new(5);

        // 先分配一个固定槽位给文件夹
        let fixed = pool.allocate_fixed_slot("folder1", true).await;
        assert!(fixed.is_some());

        // 借调3个槽位
        let borrowed = pool.allocate_borrowed_slots("folder1", 3).await;
        assert_eq!(borrowed.len(), 3);
        assert_eq!(borrowed, vec![1, 2, 3]);

        // 验证借调记录
        let borrowed_slots = pool.get_borrowed_slots("folder1").await;
        assert_eq!(borrowed_slots.len(), 3);

        assert_eq!(pool.used_slots().await, 4);
        assert_eq!(pool.available_borrow_slots().await, 1);
    }

    #[tokio::test]
    async fn test_release_borrowed_slot() {
        let pool = TaskSlotPool::new(5);

        // 分配固定槽位
        pool.allocate_fixed_slot("folder1", true).await;

        // 借调槽位
        let borrowed = pool.allocate_borrowed_slots("folder1", 2).await;
        assert_eq!(borrowed.len(), 2);

        // 释放一个借调位
        pool.release_borrowed_slot("folder1", borrowed[0]).await;

        let remaining = pool.get_borrowed_slots("folder1").await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0], borrowed[1]);

        assert_eq!(pool.available_borrow_slots().await, 3);
    }

    #[tokio::test]
    async fn test_release_fixed_slot() {
        let pool = TaskSlotPool::new(3);

        // 分配固定槽位
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;

        assert_eq!(pool.used_slots().await, 2);

        // 释放固定槽位
        pool.release_fixed_slot("task1").await;
        assert_eq!(pool.used_slots().await, 1);
        assert_eq!(pool.available_borrow_slots().await, 2);
    }

    #[tokio::test]
    async fn test_release_all_slots() {
        let pool = TaskSlotPool::new(5);

        // 分配固定槽位
        pool.allocate_fixed_slot("folder1", true).await;

        // 借调槽位
        pool.allocate_borrowed_slots("folder1", 3).await;

        assert_eq!(pool.used_slots().await, 4);

        // 释放所有槽位
        pool.release_all_slots("folder1").await;

        assert_eq!(pool.used_slots().await, 0);
        assert_eq!(pool.get_borrowed_slots("folder1").await.len(), 0);
    }

    #[tokio::test]
    async fn test_find_folder_with_borrowed_slots() {
        let pool = TaskSlotPool::new(5);

        // 没有借调位时
        assert!(pool.find_folder_with_borrowed_slots().await.is_none());

        // 分配固定槽位
        pool.allocate_fixed_slot("folder1", true).await;
        pool.allocate_borrowed_slots("folder1", 2).await;

        // 有借调位时
        let folder = pool.find_folder_with_borrowed_slots().await;
        assert!(folder.is_some());
        assert_eq!(folder.unwrap(), "folder1");
    }

    #[tokio::test]
    async fn test_get_task_slot() {
        let pool = TaskSlotPool::new(3);

        // 分配固定槽位
        pool.allocate_fixed_slot("task1", false).await;

        // 检查任务槽位
        let slot_info = pool.get_task_slot("task1").await;
        assert!(slot_info.is_some());
        let (slot_id, slot_type) = slot_info.unwrap();
        assert_eq!(slot_id, 0);
        assert_eq!(slot_type, TaskSlotType::Fixed);

        // 不存在的任务
        assert!(pool.get_task_slot("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_borrowed_slots_limit() {
        let pool = TaskSlotPool::new(3);

        // 分配固定槽位
        pool.allocate_fixed_slot("folder1", true).await;

        // 尝试借调超过可用数量的槽位
        let borrowed = pool.allocate_borrowed_slots("folder1", 5).await;
        assert_eq!(borrowed.len(), 2); // 只能借到2个（3-1=2）
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

        // 只有10个槽位，所以只有10个能成功分配
        assert_eq!(success_count, 10);
        assert_eq!(pool.used_slots().await, 10);
    }

    #[tokio::test]
    async fn test_resize_expand() {
        // 测试扩容：从3个槽位扩展到5个
        let pool = TaskSlotPool::new(3);

        // 初始状态：3个槽位
        assert_eq!(pool.max_slots(), 3);
        assert_eq!(pool.available_borrow_slots().await, 3);

        // 分配2个槽位
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        assert_eq!(pool.used_slots().await, 2);
        assert_eq!(pool.available_borrow_slots().await, 1);

        // 扩容到5个槽位
        pool.resize(5).await;
        assert_eq!(pool.max_slots(), 5);

        // 验证：已占用的槽位不变，新增2个空闲槽位
        assert_eq!(pool.used_slots().await, 2);
        assert_eq!(pool.available_borrow_slots().await, 3);

        // 应该能继续分配新槽位
        let slot3 = pool.allocate_fixed_slot("task3", false).await;
        assert!(slot3.is_some());
        let slot4 = pool.allocate_fixed_slot("task4", false).await;
        assert!(slot4.is_some());
        assert_eq!(pool.used_slots().await, 4);
    }

    #[tokio::test]
    async fn test_resize_shrink_with_free_slots() {
        // 测试缩容：从5个缩减到3个（有空闲槽位）
        let pool = TaskSlotPool::new(5);

        // 只分配2个槽位
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        assert_eq!(pool.used_slots().await, 2);

        // 缩容到3个槽位（应该成功，因为只占用了2个）
        pool.resize(3).await;
        assert_eq!(pool.max_slots(), 3);

        // 验证：已占用的槽位不变
        assert_eq!(pool.used_slots().await, 2);
        assert_eq!(pool.available_borrow_slots().await, 1);

        // 应该只能再分配1个槽位
        let slot3 = pool.allocate_fixed_slot("task3", false).await;
        assert!(slot3.is_some());
        
        let slot4 = pool.allocate_fixed_slot("task4", false).await;
        assert!(slot4.is_none()); // 超过上限，无法分配
    }

    #[tokio::test]
    async fn test_resize_shrink_with_occupied_slots() {
        // 测试缩容：从5个缩减到3个（有超出上限的占用槽位）
        let pool = TaskSlotPool::new(5);

        // 分配所有5个槽位
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        pool.allocate_fixed_slot("task3", false).await;
        pool.allocate_fixed_slot("task4", false).await;
        pool.allocate_fixed_slot("task5", false).await;
        assert_eq!(pool.used_slots().await, 5);

        // 缩容到3个槽位
        pool.resize(3).await;
        assert_eq!(pool.max_slots(), 3);

        // 验证：由于槽位4和5仍被占用，used_slots只计算前3个的占用情况
        assert_eq!(pool.used_slots().await, 3);

        // 新的分配应该失败（前3个槽位都被占用）
        let slot_new = pool.allocate_fixed_slot("task_new", false).await;
        assert!(slot_new.is_none());

        // 释放 task1（slot 0），应该可以重新分配
        pool.release_fixed_slot("task1").await;
        assert_eq!(pool.used_slots().await, 2);

        let slot_new2 = pool.allocate_fixed_slot("task_new2", false).await;
        assert!(slot_new2.is_some());
        assert_eq!(slot_new2.unwrap(), 0); // 应该分配到 slot 0
    }

    #[tokio::test]
    async fn test_resize_no_change() {
        // 测试无需调整的情况
        let pool = TaskSlotPool::new(5);

        pool.allocate_fixed_slot("task1", false).await;
        assert_eq!(pool.used_slots().await, 1);

        // 调整为相同大小
        pool.resize(5).await;
        assert_eq!(pool.max_slots(), 5);

        // 状态应该不变
        assert_eq!(pool.used_slots().await, 1);
        assert_eq!(pool.available_borrow_slots().await, 4);
    }

    #[tokio::test]
    async fn test_resize_expand_then_shrink() {
        // 测试先扩容再缩容
        let pool = TaskSlotPool::new(3);

        // 分配2个槽位
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;

        // 扩容到7个
        pool.resize(7).await;
        assert_eq!(pool.max_slots(), 7);
        assert_eq!(pool.available_borrow_slots().await, 5);

        // 再分配3个槽位
        pool.allocate_fixed_slot("task3", false).await;
        pool.allocate_fixed_slot("task4", false).await;
        pool.allocate_fixed_slot("task5", false).await;
        assert_eq!(pool.used_slots().await, 5);

        // 缩容到4个
        pool.resize(4).await;
        assert_eq!(pool.max_slots(), 4);
        // 前4个槽位中有4个被占用，第5个（slot 4）超出新上限
        assert_eq!(pool.used_slots().await, 4);
        assert_eq!(pool.available_borrow_slots().await, 0);

        // 释放 task1，应该可以分配新任务
        pool.release_fixed_slot("task1").await;
        assert_eq!(pool.used_slots().await, 3);

        let slot_new = pool.allocate_fixed_slot("task_new", false).await;
        assert!(slot_new.is_some());
    }

    #[tokio::test]
    async fn test_available_slots_basic() {
        let pool = TaskSlotPool::new(5);
        
        // 初始状态：5个空闲槽位
        assert_eq!(pool.available_slots().await, 5);
        
        // 分配2个固定槽位
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        assert_eq!(pool.available_slots().await, 3);
        
        // 分配1个借调槽位
        pool.allocate_borrowed_slots("folder1", 1).await;
        assert_eq!(pool.available_slots().await, 2);
        
        // 释放1个固定槽位
        pool.release_fixed_slot("task1").await;
        assert_eq!(pool.available_slots().await, 3);
    }

    #[tokio::test]
    async fn test_available_slots_edge_cases() {
        let pool = TaskSlotPool::new(3);
        
        // 全部占用
        pool.allocate_fixed_slot("task1", false).await;
        pool.allocate_fixed_slot("task2", false).await;
        pool.allocate_fixed_slot("task3", false).await;
        assert_eq!(pool.available_slots().await, 0);
        
        // 释放全部
        pool.release_fixed_slot("task1").await;
        pool.release_fixed_slot("task2").await;
        pool.release_fixed_slot("task3").await;
        assert_eq!(pool.available_slots().await, 3);
    }

    #[tokio::test]
    async fn test_available_slots_concurrent() {
        let pool = Arc::new(TaskSlotPool::new(10));
        let mut handles = vec![];
        
        // 并发查询和分配
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
        
        // 等待所有任务完成
        let mut success = 0;
        for handle in handles {
            if handle.await.unwrap().is_some() {
                success += 1;
            }
        }
        
        // 应该只有10个成功（槽位数限制）
        assert_eq!(success, 10);
        assert_eq!(pool.available_slots().await, 0);
    }
}
