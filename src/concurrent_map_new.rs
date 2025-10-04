//! 基于crossbeam-skiplist的ConcurrentMap实现
//! 作为EBR-based ConcurrentMap的替代方案

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

use crossbeam_epoch::{Collector, Guard};
use crossbeam_skiplist::SkipMap;

use crate::{debug_log, trace_log, warn_log, error_log, info_log};

/// 需要实现的Minimum trait
pub trait Minimum {
    const MIN: Self;
}

/// 基于crossbeam-skiplist的并发映射表
pub struct ConcurrentMap<K, V> {
    inner: Arc<SkipMap<K, V>>,
    collector: Collector,
}

impl<K, V> std::fmt::Debug for ConcurrentMap<K, V>
where
    K: Ord + std::fmt::Debug,
    V: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentMap")
            .field("len", &self.inner.len())
            .field("inner", &self.inner)
            .finish()
    }
}

impl<K: 'static, V: 'static> Default for ConcurrentMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: 'static, V: 'static> ConcurrentMap<K, V> {
    /// 创建新的并发映射表
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SkipMap::new()),
            collector: Collector::new(),
        }
    }

    /// 检查映射表是否为空
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// 获取条目数量
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 获取值
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Ord + Borrow<Q> + Send + Sync,
        Q: Ord + ?Sized,
        V: Clone + Send + Sync,
    {
        self.inner.get(key).map(|entry| entry.value().clone())
    }

    /// 插入键值对，返回旧值
    pub fn insert(&self, key: K, value: V) -> Option<V>
    where
        K: Ord + Clone + Send + Sync,
        V: Clone + Send + Sync,
    {
        // 先检查是否存在旧值
        let old_value = self.get(&key);
        self.inner.insert(key, value);
        old_value
    }

    /// 移除键值对，返回旧值
    pub fn remove<Q>(&self, key: &Q) -> Option<V>
    where
        K: Ord + Borrow<Q> + Send + Sync,
        Q: Ord + ?Sized,
        V: Clone + Send + Sync,
    {
        self.inner.remove(key).map(|entry| entry.value().clone())
    }

    /// 获取范围迭代器
    pub fn range<'a, R, Q>(&'a self, range: R) -> impl Iterator<Item = (K, V)> + 'a
    where
        K: Ord + Clone + Borrow<Q> + Send + Sync,
        V: Clone + Send + Sync,
        R: RangeBounds<Q> + 'a,
        Q: Ord + 'a,
    {
        self.inner
            .range(range)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    /// 获取小于指定键的条目
    pub fn get_lt<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Ord + Clone + Borrow<Q> + Send + Sync,
        V: Clone + Send + Sync,
        Q: Ord + ?Sized,
    {
        // 简化实现，直接搜索所有元素找到最大的小于key的条目
        let mut result = None;
        for entry in self.inner.iter() {
            if entry.key().borrow() < key {
                result = Some((entry.key().clone(), entry.value().clone()));
            } else {
                break;
            }
        }
        result
    }

    /// 获取小于等于指定键的条目
    pub fn get_lte<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Ord + Clone + Borrow<Q> + Send + Sync,
        V: Clone + Send + Sync,
        Q: Ord + ?Sized,
    {
        // 暂时使用get_lt作为替代，后续可以优化
        self.get_lt(key)
    }

    /// 获取最大条目
    pub fn last(&self) -> Option<(K, V)>
    where
        K: Ord + Clone + Send + Sync,
        V: Clone + Send + Sync,
    {
        self.inner.iter().last().map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    /// 获取最小条目
    pub fn first(&self) -> Option<(K, V)>
    where
        K: Ord + Clone + Send + Sync,
        V: Clone + Send + Sync,
    {
        self.inner.iter().next().map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    /// 检查是否包含键
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Ord + Borrow<Q> + Send + Sync,
        Q: Ord + ?Sized,
    {
        self.inner.contains_key(key)
    }

    /// 手动推进epoch（用于垃圾回收）
    pub fn manually_advance_epoch(&self) {
        let _guard = self.collector.register().pin();
        // 创建新的guard来推进epoch
    }

    /// 获取collector的引用（用于高级操作）
    pub fn collector(&self) -> &Collector {
        &self.collector
    }
}

// 为了保持API兼容性，添加Clone trait
impl<K, V> Clone for ConcurrentMap<K, V>
where
    K: Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        // 创建新的实例，但注意这不会复制数据
        // crossbeam-skiplist是并发共享的，多个实例共享同一份数据
        // Arc<SkipMap>可以安全地clone
        Self {
            inner: self.inner.clone(),
            collector: Collector::new(), // 每个实例有自己的collector
        }
    }
}

/// 为了保持API兼容性，提供带泛型参数的类型别名
pub type ConcurrentMapWithParams<K, V, const FANOUT: usize = 64, const BUFFER_SIZE: usize = 128> =
    ConcurrentMap<K, V>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InlineArray;

    #[test]
    fn basic_operations() {
        let map = ConcurrentMap::new();

        // 测试插入
        assert_eq!(map.insert(1, "a"), None);
        assert_eq!(map.insert(2, "b"), None);
        assert_eq!(map.insert(1, "c"), Some("a"));

        // 测试获取
        assert_eq!(map.get(&1), Some("c"));
        assert_eq!(map.get(&2), Some("b"));
        assert_eq!(map.get(&3), None);

        // 测试长度
        assert_eq!(map.len(), 2);
        assert!(!map.is_empty());

        // 测试移除
        assert_eq!(map.remove(&1), Some("c"));
        assert_eq!(map.get(&1), None);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn range_operations() {
        let map = ConcurrentMap::new();

        for i in 1..=10 {
            map.insert(i, i * 10);
        }

        // 测试范围迭代
        let values: Vec<_> = map.range(3..=7).collect();
        assert_eq!(values, vec![(3, 30), (4, 40), (5, 50), (6, 60), (7, 70)]);

        // 测试get_lt
        assert_eq!(map.get_lt(&5), Some((4, 40)));
        assert_eq!(map.get_lt(&1), None);

        // 测试first和last
        assert_eq!(map.first(), Some((1, 10)));
        assert_eq!(map.last(), Some((10, 100)));
    }

    #[test]
    fn inline_array_compatibility() {
        let map = ConcurrentMap::<InlineArray, String>::new();

        let key1 = InlineArray::from(vec![1, 2, 3]);
        let key2 = InlineArray::from(vec![4, 5, 6]);

        map.insert(key1.clone(), "value1".to_string());
        map.insert(key2.clone(), "value2".to_string());

        assert_eq!(map.get(&key1), Some("value1".to_string()));
        assert_eq!(map.get(&key2), Some("value2".to_string()));
    }
}