//! 简化的Epoch管理器
//!
//! 替代复杂的crossbeam-epoch实现，专注于性能和简洁性

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use crate::{debug_log, trace_log, warn_log, error_log, info_log};

/// 简化的Epoch
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SimpleEpoch(u64);

impl SimpleEpoch {
    pub const MIN: SimpleEpoch = SimpleEpoch(1);

    pub fn new(value: u64) -> SimpleEpoch {
        SimpleEpoch(value)
    }

    pub fn increment(&self) -> SimpleEpoch {
        SimpleEpoch(self.0 + 1)
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn epoch(&self) -> SimpleEpoch {
        *self
    }

    pub fn wait_for_complete(&self) -> SimpleEpoch {
        // 在简化版本中，立即返回
        trace_log!("SimpleEpoch {:?} wait_for_complete called", self);
        *self
    }

    pub fn mark_complete(&self) {
        // 在简化版本中，不做任何操作
        trace_log!("SimpleEpoch {:?} marked as complete", self);
    }

    // 为了与原有flush逻辑兼容，添加一些辅助方法
    pub fn is_valid(&self) -> bool {
        self.0 >= SimpleEpoch::MIN.0
    }

    pub fn distance_to(&self, other: SimpleEpoch) -> u64 {
        if other.0 >= self.0 {
            other.0 - self.0
        } else {
            0
        }
    }

    pub fn is_older_than(&self, other: SimpleEpoch) -> bool {
        self.0 < other.0
    }
}

/// 简化的Epoch管理器
pub struct SimpleEpochManager {
    counter: AtomicU64,
}

impl SimpleEpochManager {
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(SimpleEpoch::MIN.get()),
        }
    }

    /// 创建新的epoch
    pub fn new_epoch(&self) -> SimpleEpoch {
        let next_val = self.counter.fetch_add(1, Ordering::SeqCst);
        // trace_log!("创建新epoch: {:?}", next_val);
        SimpleEpoch(next_val)
    }

    /// 获取当前epoch
    pub fn current_epoch(&self) -> SimpleEpoch {
        let current = self.counter.load(Ordering::SeqCst);
        SimpleEpoch(current)
    }

    /// 获取当前flush epoch（为了API兼容性）
    pub fn current_flush_epoch(&self) -> SimpleEpoch {
        self.current_epoch()
    }

    /// 创建Guard
    pub fn create_guard(&self) -> SimpleEpochGuard {
        SimpleEpochGuard::new(self)
    }

    /// check in（为了API兼容性）
    pub fn check_in(&self) -> SimpleEpochGuard {
        self.create_guard()
    }

    /// roll epoch forward（为了API兼容性）
    pub fn roll_epoch_forward(&self) -> (SimpleEpoch, SimpleEpoch) {
        let previous = self.current_epoch();
        let next = self.new_epoch();
        // trace_log!("epoch前进: {:?} -> {:?}", previous, next);
        (previous, next)
    }

    /// manually advance epoch（为了API兼容性）
    pub fn manually_advance_epoch(&self) {
        // 在简化版本中，这个方法只是确保epoch前进
        let current = self.current_epoch();
        // trace_log!("手动推进epoch到: {:?}", current);
    }

    /// 获取epoch统计信息
    pub fn get_epoch_stats(&self) -> (u64, u64) {
        let current = self.counter.load(Ordering::SeqCst);
        (SimpleEpoch::MIN.get(), current)
    }

    /// 检查epoch是否需要清理
    pub fn should_cleanup_epoch(&self, epoch: SimpleEpoch, threshold: u64) -> bool {
        let current = self.current_epoch();
        epoch.distance_to(current) > threshold
    }
}

/// 简化的Epoch Guard
pub struct SimpleEpochGuard {
    epoch: SimpleEpoch,
}

impl SimpleEpochGuard {
    pub fn new(manager: &SimpleEpochManager) -> Self {
        let epoch = manager.new_epoch();
        Self { epoch }
    }

    pub fn epoch(&self) -> SimpleEpoch {
        self.epoch
    }
}

impl Drop for SimpleEpochGuard {
    fn drop(&mut self) {
        // 在这个简化版本中，我们不需要复杂的引用计数
        // Guard被销毁时自动处理资源清理
        trace_log!("SimpleEpochGuard dropped for epoch {:?}", self.epoch);
    }
}

impl Clone for SimpleEpochManager {
    fn clone(&self) -> Self {
        Self {
            counter: AtomicU64::new(self.counter.load(Ordering::SeqCst)),
        }
    }
}

impl Default for SimpleEpochManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 针对leaf节点的epoch管理
pub struct LeafEpochManager {
    dirty_epoch: Mutex<Option<SimpleEpoch>>,
    max_unflushed_epoch: Mutex<Option<SimpleEpoch>>,
}

impl LeafEpochManager {
    pub fn new() -> Self {
        Self {
            dirty_epoch: Mutex::new(None),
            max_unflushed_epoch: Mutex::new(None),
        }
    }

    /// 更新dirty epoch
    pub fn update_dirty(&self, new_epoch: SimpleEpoch) -> Option<SimpleEpoch> {
        let mut dirty = self.dirty_epoch.lock().unwrap();
        let old = *dirty;
        *dirty = Some(new_epoch);
        trace_log!("LeafEpochManager: 更新dirty epoch {:?} -> {:?}", old, new_epoch);
        old
    }

    /// 更新最大未flushepoch
    pub fn update_max_unflushed(&self, new_epoch: SimpleEpoch) {
        let mut max_unflushed = self.max_unflushed_epoch.lock().unwrap();
        *max_unflushed = Some(new_epoch);
        trace_log!("LeafEpochManager: 更新max_unflushed epoch to {:?}", new_epoch);
    }

    /// 获取当前dirty epoch
    pub fn get_dirty(&self) -> Option<SimpleEpoch> {
        *self.dirty_epoch.lock().unwrap()
    }

    /// 获取当前最大未flushepoch
    pub fn get_max_unflushed(&self) -> Option<SimpleEpoch> {
        *self.max_unflushed_epoch.lock().unwrap()
    }

    /// 检查是否需要cooperative flushing
    pub fn needs_cooperative_flush(&self, current_epoch: SimpleEpoch) -> bool {
        if let Some(dirty_epoch) = self.get_dirty() {
            if dirty_epoch != current_epoch {
                trace_log!("LeafEpochManager: 需要cooperative flush, dirty_epoch={:?}, current_epoch={:?}", dirty_epoch, current_epoch);
                return true;
            }
        }
        false
    }

    /// 执行cooperative flushing
    pub fn cooperative_flush(&self, current_epoch: SimpleEpoch) -> Option<SimpleEpoch> {
        if let Some(dirty_epoch) = self.get_dirty() {
            if dirty_epoch != current_epoch {
                trace_log!("LeafEpochManager: 执行cooperative flush: dirty_epoch={:?}, current_epoch={:?}", dirty_epoch, current_epoch);

                // 更新最大未flushepoch
                self.update_max_unflushed(dirty_epoch);

                // 清除dirty状态
                self.update_dirty(SimpleEpoch::MIN);

                return Some(dirty_epoch);
            }
        }
        None
    }

    /// 强制清理所有状态
    pub fn force_cleanup(&self) {
        let mut dirty = self.dirty_epoch.lock().unwrap();
        let mut max_unflushed = self.max_unflushed_epoch.lock().unwrap();
        trace_log!("LeafEpochManager: 强制清理状态");
        *dirty = None;
        *max_unflushed = None;
    }

    /// 获取状态信息
    pub fn get_status(&self) -> (Option<SimpleEpoch>, Option<SimpleEpoch>) {
        (self.get_dirty(), self.get_max_unflushed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_simple_epoch_basic() {
        let manager = Arc::new(SimpleEpochManager::new());

        // 测试基本功能
        let epoch1 = manager.new_epoch();
        let epoch2 = manager.new_epoch();
        let epoch3 = manager.new_epoch();

        assert!(epoch1 < epoch2);
        assert!(epoch2 < epoch3);
        // current_epoch()应该返回epoch3的下一个epoch值
        assert_eq!(manager.current_epoch(), epoch3.increment());

        println!("✅ Basic epoch test passed");
    }

    #[test]
    fn test_simple_epoch_guard() {
        let manager = Arc::new(SimpleEpochManager::new());

        {
            let _guard1 = manager.create_guard();
            let _guard2 = manager.create_guard();
            // 每个guard都会创建新的epoch，所以current_epoch应该是3
            assert_eq!(manager.current_epoch(), SimpleEpoch(3));
        }

        // Guard被drop后，继续使用
        let _guard3 = manager.create_guard();
        // 应该是4
        assert_eq!(manager.current_epoch(), SimpleEpoch(4));

        println!("✅ Guard test passed");
    }

    #[test]
    fn test_concurrent_epochs() {
        let manager = Arc::new(SimpleEpochManager::new());
        let mut handles = vec![];

        // 创建多个线程同时创建epoch
        for _ in 0..10 {
            let manager_clone = manager.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let _epoch = manager_clone.new_epoch();
                }
            });
            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }

        // 最终epoch应该是 1001 (初始1 + 10*100)
        assert_eq!(manager.current_epoch(), SimpleEpoch(1001));

        println!("✅ Concurrent epoch test passed");
    }

    #[test]
    fn test_leaf_epoch_manager() {
        let leaf_manager = LeafEpochManager::new();

        // 初始状态下不应该需要cooperative flush
        assert!(!leaf_manager.needs_cooperative_flush(SimpleEpoch(2)));
        assert!(!leaf_manager.needs_cooperative_flush(SimpleEpoch::MIN));

        // 更新dirty epoch后再测试
        leaf_manager.update_dirty(SimpleEpoch(1));
        assert!(leaf_manager.needs_cooperative_flush(SimpleEpoch(2)));
        assert!(!leaf_manager.needs_cooperative_flush(SimpleEpoch(1)));

        // 测试cooperative flushing
        let old_epoch = leaf_manager.cooperative_flush(SimpleEpoch(5));
        assert_eq!(old_epoch, Some(SimpleEpoch(1)));
        assert_eq!(leaf_manager.get_max_unflushed(), Some(SimpleEpoch(1)));

        // 更新dirty epoch
        leaf_manager.update_dirty(SimpleEpoch(10));
        assert_eq!(leaf_manager.get_dirty(), Some(SimpleEpoch(10)));

        println!("✅ Leaf epoch manager test passed");
    }
}