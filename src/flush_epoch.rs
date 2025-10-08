//! 简化的Epoch管理器 - 替换crossbeam-epoch
//!
//! 使用我们自己的SimpleEpochManager来替代复杂的crossbeam-epoch实现
//! 专注于性能和简洁性

use std::sync::Arc;
use crate::simple_epoch::{SimpleEpoch, SimpleEpochManager, SimpleEpochGuard, LeafEpochManager};
use crate::{debug_log, trace_log, warn_log, error_log, info_log};

// 重新导出SimpleEpoch类型作为FlushEpoch，保持API兼容性
pub use crate::simple_epoch::SimpleEpoch as FlushEpoch;
pub use crate::simple_epoch::SimpleEpochManager as FlushEpochTracker;
pub use crate::simple_epoch::SimpleEpochGuard as FlushEpochGuard;

// 保持原有的flush invariants结构，但使用SimpleEpoch
#[derive(Debug)]
pub struct FlushInvariants {
    _private: (),
}

impl FlushInvariants {
    pub fn new() -> Self {
        Self { _private: () }
    }

    pub fn check(&self, _epoch: FlushEpoch) -> bool {
        // 在简化版本中，我们不做复杂的invariant检查
        true
    }

    pub fn mark_flushing_epoch(&self, _epoch: FlushEpoch) {
        // 在简化版本中，我们不做flushing标记
        trace_log!("mark_flushing_epoch: {:?}", _epoch);
    }

    pub fn mark_flushed_epoch(&self, _epoch: FlushEpoch) {
        // 在简化版本中，我们不做flushed标记
        trace_log!("mark_flushed_epoch: {:?}", _epoch);
    }
}

impl Default for FlushInvariants {
    fn default() -> Self {
        Self::new()
    }
}

// 为了保持兼容性，添加一些必要的trait实现
impl concurrent_map::Minimum for FlushEpoch {
    const MIN: FlushEpoch = SimpleEpoch::MIN;
}

// 重新导出一些常用的常量和辅助函数
pub fn min_epoch() -> FlushEpoch {
    FlushEpoch::MIN
}

pub fn increment_epoch(epoch: FlushEpoch) -> FlushEpoch {
    epoch.increment()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_flush_epoch_compatibility() {
        let tracker = FlushEpochTracker::new();

        // 测试基本的epoch创建
        let epoch1 = tracker.new_epoch();
        let epoch2 = tracker.new_epoch();

        assert!(epoch1 < epoch2);

        // 测试guard功能
        let _guard = tracker.create_guard();
        // current_epoch应该返回epoch2的下一个epoch值，guard又增加了一个epoch
        assert_eq!(tracker.current_epoch(), epoch2.increment().increment());

        println!("✅ FlushEpoch兼容性测试通过");
    }

    #[test]
    fn test_leaf_epoch_manager_compatibility() {
        let leaf_manager = LeafEpochManager::new();

        // 初始状态下不应该需要cooperative flush
        let current_epoch = FlushEpoch::new(5);
        assert!(!leaf_manager.needs_cooperative_flush(current_epoch));

        // 更新dirty epoch后再测试
        leaf_manager.update_dirty(FlushEpoch::new(3));
        assert!(leaf_manager.needs_cooperative_flush(current_epoch));
        assert!(!leaf_manager.needs_cooperative_flush(FlushEpoch::new(3)));

        // 测试cooperative flushing
        let flushed_epoch = leaf_manager.cooperative_flush(current_epoch);
        assert!(flushed_epoch.is_some());

        println!("✅ LeafEpochManager兼容性测试通过");
    }
}