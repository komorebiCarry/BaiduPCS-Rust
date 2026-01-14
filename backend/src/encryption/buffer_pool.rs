//! 缓冲区池
//!
//! 用于复用加密/解密操作中的缓冲区，避免频繁分配大块内存

use parking_lot::Mutex;
use std::sync::Arc;

/// 默认缓冲区大小：16MB（与加密分块大小一致）
pub const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// 默认池容量（最多缓存的缓冲区数量）
const DEFAULT_POOL_CAPACITY: usize = 4;

/// 缓冲区池
/// 
/// 用于复用大型缓冲区，避免每次加密/解密操作都分配新的内存。
/// 线程安全，可在多个加密任务之间共享。
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
}

struct BufferPoolInner {
    /// 缓冲区存储
    buffers: Mutex<Vec<Vec<u8>>>,
    /// 单个缓冲区大小
    buffer_size: usize,
    /// 池容量（最多缓存多少个缓冲区）
    capacity: usize,
    /// 统计：获取次数
    acquire_count: std::sync::atomic::AtomicU64,
    /// 统计：命中次数（从池中获取）
    hit_count: std::sync::atomic::AtomicU64,
}

impl BufferPool {
    /// 创建新的缓冲区池
    pub fn new(buffer_size: usize, capacity: usize) -> Self {
        Self {
            inner: Arc::new(BufferPoolInner {
                buffers: Mutex::new(Vec::with_capacity(capacity)),
                buffer_size,
                capacity,
                acquire_count: std::sync::atomic::AtomicU64::new(0),
                hit_count: std::sync::atomic::AtomicU64::new(0),
            }),
        }
    }

    /// 使用默认配置创建缓冲区池
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_BUFFER_SIZE, DEFAULT_POOL_CAPACITY)
    }

    /// 获取一个缓冲区
    /// 
    /// 如果池中有可用缓冲区，则复用；否则分配新的。
    /// 返回的缓冲区已清零并调整到正确大小。
    pub fn acquire(&self) -> PooledBuffer {
        self.inner.acquire_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let buffer = {
            let mut buffers = self.inner.buffers.lock();
            buffers.pop()
        };

        let buffer = match buffer {
            Some(mut buf) => {
                self.inner.hit_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // 确保大小正确
                buf.resize(self.inner.buffer_size, 0);
                buf
            }
            None => {
                // 分配新缓冲区
                vec![0u8; self.inner.buffer_size]
            }
        };

        PooledBuffer {
            buffer: Some(buffer),
            pool: self.clone(),
        }
    }

    /// 获取指定大小的缓冲区
    /// 
    /// 如果请求大小与池缓冲区大小一致，则尝试复用；否则分配新的。
    pub fn acquire_sized(&self, size: usize) -> PooledBuffer {
        if size == self.inner.buffer_size {
            self.acquire()
        } else {
            // 大小不一致，直接分配（不放回池中）
            PooledBuffer {
                buffer: Some(vec![0u8; size]),
                pool: self.clone(),
            }
        }
    }

    /// 归还缓冲区到池中
    fn release(&self, mut buffer: Vec<u8>) {
        // 只接受大小匹配的缓冲区
        if buffer.capacity() >= self.inner.buffer_size {
            let mut buffers = self.inner.buffers.lock();
            if buffers.len() < self.inner.capacity {
                // 清零敏感数据
                buffer.fill(0);
                buffer.truncate(self.inner.buffer_size);
                buffers.push(buffer);
            }
            // 如果池满了，直接丢弃（会被 Drop）
        }
    }

    /// 获取缓冲区大小
    pub fn buffer_size(&self) -> usize {
        self.inner.buffer_size
    }

    /// 获取池容量
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// 获取当前池中的缓冲区数量
    pub fn available(&self) -> usize {
        self.inner.buffers.lock().len()
    }

    /// 获取统计信息
    pub fn stats(&self) -> BufferPoolStats {
        let acquire_count = self.inner.acquire_count.load(std::sync::atomic::Ordering::Relaxed);
        let hit_count = self.inner.hit_count.load(std::sync::atomic::Ordering::Relaxed);
        
        BufferPoolStats {
            acquire_count,
            hit_count,
            hit_rate: if acquire_count > 0 {
                hit_count as f64 / acquire_count as f64
            } else {
                0.0
            },
            available: self.available(),
            capacity: self.inner.capacity,
            buffer_size: self.inner.buffer_size,
        }
    }

    /// 清空池中所有缓冲区
    pub fn clear(&self) {
        self.inner.buffers.lock().clear();
    }

    /// 预热池（预分配缓冲区）
    pub fn warm_up(&self, count: usize) {
        let count = count.min(self.inner.capacity);
        let mut buffers = self.inner.buffers.lock();
        
        while buffers.len() < count {
            buffers.push(vec![0u8; self.inner.buffer_size]);
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// 池化缓冲区
/// 
/// 自动归还缓冲区到池中（RAII 模式）
pub struct PooledBuffer {
    buffer: Option<Vec<u8>>,
    pool: BufferPool,
}

impl PooledBuffer {
    /// 获取缓冲区的可变切片
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut().map(|b| b.as_mut_slice()).unwrap_or(&mut [])
    }

    /// 获取缓冲区的不可变切片
    pub fn as_slice(&self) -> &[u8] {
        self.buffer.as_ref().map(|b| b.as_slice()).unwrap_or(&[])
    }

    /// 获取缓冲区长度
    pub fn len(&self) -> usize {
        self.buffer.as_ref().map(|b| b.len()).unwrap_or(0)
    }

    /// 检查缓冲区是否为空
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 取出缓冲区（不归还到池中）
    pub fn take(mut self) -> Vec<u8> {
        self.buffer.take().unwrap_or_default()
    }

    /// 调整缓冲区大小
    pub fn resize(&mut self, new_len: usize) {
        if let Some(buf) = self.buffer.as_mut() {
            buf.resize(new_len, 0);
        }
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.release(buffer);
        }
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// 缓冲区池统计信息
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    /// 获取次数
    pub acquire_count: u64,
    /// 命中次数
    pub hit_count: u64,
    /// 命中率
    pub hit_rate: f64,
    /// 当前可用数量
    pub available: usize,
    /// 池容量
    pub capacity: usize,
    /// 单个缓冲区大小
    pub buffer_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::new(1024, 2);
        
        // 获取第一个缓冲区
        let buf1 = pool.acquire();
        assert_eq!(buf1.len(), 1024);
        assert_eq!(pool.available(), 0);
        
        // 归还后应该在池中
        drop(buf1);
        assert_eq!(pool.available(), 1);
        
        // 再次获取应该复用
        let _buf2 = pool.acquire();
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn test_buffer_pool_capacity() {
        let pool = BufferPool::new(1024, 2);
        
        // 获取并归还3个缓冲区
        let buf1 = pool.acquire();
        let buf2 = pool.acquire();
        let buf3 = pool.acquire();
        
        drop(buf1);
        drop(buf2);
        drop(buf3);
        
        // 池容量是2，只能保留2个
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_buffer_pool_stats() {
        let pool = BufferPool::new(1024, 2);
        
        let _buf1 = pool.acquire(); // miss
        drop(_buf1);
        
        let _buf2 = pool.acquire(); // hit
        
        let stats = pool.stats();
        assert_eq!(stats.acquire_count, 2);
        assert_eq!(stats.hit_count, 1);
        assert!((stats.hit_rate - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_pooled_buffer_take() {
        let pool = BufferPool::new(1024, 2);
        
        let buf = pool.acquire();
        let vec = buf.take();
        
        assert_eq!(vec.len(), 1024);
        // 因为使用了 take，缓冲区不会归还到池中
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn test_buffer_pool_warm_up() {
        let pool = BufferPool::new(1024, 4);
        
        pool.warm_up(3);
        assert_eq!(pool.available(), 3);
        
        // 预热超过容量不会超出
        pool.warm_up(10);
        assert_eq!(pool.available(), 4);
    }
}
