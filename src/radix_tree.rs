//! 专为InlineArray优化的Radix Tree实现
//! 替代crossbeam-skiplist，解决死循环问题

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use crate::InlineArray;

/// 基于BTreeMap的简单Radix Map实现
/// 专门为InlineArray键优化
#[derive(Debug, Clone)]
pub struct InlineArrayRadixMap<V: Clone> {
    inner: Arc<RwLock<BTreeMap<InlineArray, V>>>,
}

impl<V: Clone> Default for InlineArrayRadixMap<V> {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl<V: Clone + Send + Sync + 'static> InlineArrayRadixMap<V> {
    /// 创建新的映射表
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// 插入键值对
    pub fn insert(&self, key: InlineArray, value: V) -> Option<V> {
        let mut map = self.inner.write().unwrap();
        map.insert(key, value)
    }

    /// 获取值
    pub fn get(&self, key: &InlineArray) -> Option<V> {
        let map = self.inner.read().unwrap();
        map.get(key).cloned()
    }

    /// 获取小于等于指定键的最大条目
    pub fn get_lte(&self, key: &InlineArray) -> Option<(InlineArray, V)> {
        let map = self.inner.read().unwrap();

        // 首先尝试精确匹配
        if let Some(value) = map.get(key) {
            return Some((key.clone(), value.clone()));
        }

        // 如果没有精确匹配，查找小于key的最大条目
        let range = (std::ops::Bound::Unbounded, std::ops::Bound::Excluded(key));
        for (k, v) in map.range::<InlineArray, _>(range).rev() {
            return Some((k.clone(), v.clone()));
        }

        None
    }

    /// 获取小于等于指定键的最大条目（支持字节数组）
    pub fn get_lte_from_bytes(&self, key: &[u8]) -> Option<(InlineArray, V)> {
        let inline_key = InlineArray::try_from(key).ok()?;
        self.get_lte(&inline_key)
    }

    /// 获取小于指定键的最大条目
    pub fn get_lt(&self, key: &InlineArray) -> Option<(InlineArray, V)> {
        let map = self.inner.read().unwrap();

        let range = (std::ops::Bound::Unbounded, std::ops::Bound::Excluded(key));
        for (k, v) in map.range::<InlineArray, _>(range).rev() {
            return Some((k.clone(), v.clone()));
        }

        None
    }

    /// 获取小于指定键的最大条目（支持字节数组）
    pub fn get_lt_from_bytes(&self, key: &[u8]) -> Option<(InlineArray, V)> {
        let inline_key = InlineArray::try_from(key).ok()?;
        self.get_lt(&inline_key)
    }

    /// 获取范围查询结果
    pub fn range<R>(&self, range: R) -> Vec<(InlineArray, V)>
    where
        R: std::ops::RangeBounds<InlineArray>,
    {
        let map = self.inner.read().unwrap();
        map.range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// 移除键值对
    pub fn remove(&self, key: &InlineArray) -> Option<V> {
        let mut map = self.inner.write().unwrap();
        map.remove(key)
    }

    /// 获取条目数量
    pub fn len(&self) -> usize {
        let map = self.inner.read().unwrap();
        map.len()
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        let map = self.inner.read().unwrap();
        map.is_empty()
    }

    /// 获取最大条目
    pub fn last(&self) -> Option<(InlineArray, V)> {
        let map = self.inner.read().unwrap();
        map.iter().last().map(|(k, v)| (k.clone(), v.clone()))
    }

    /// 获取最小条目
    pub fn first(&self) -> Option<(InlineArray, V)> {
        let map = self.inner.read().unwrap();
        map.iter().next().map(|(k, v)| (k.clone(), v.clone()))
    }

    /// 检查是否包含指定键
    pub fn contains_key(&self, key: &InlineArray) -> bool {
        let map = self.inner.read().unwrap();
        map.contains_key(key)
    }

    /// 手动推进epoch（为了API兼容性）
    pub fn manually_advance_epoch(&self) {
        // BTreeMap不需要epoch管理，空实现
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_operations() {
        let map = InlineArrayRadixMap::new();

        // 测试插入
        let key1 = InlineArray::from(vec![1, 2, 3]);
        let key2 = InlineArray::from(vec![4, 5, 6]);

        assert_eq!(map.insert(key1.clone(), "value1"), None);
        assert_eq!(map.insert(key2.clone(), "value2"), None);
        assert_eq!(map.insert(key1.clone(), "value1_updated"), Some("value1"));

        // 测试获取
        assert_eq!(map.get(&key1), Some("value1_updated"));
        assert_eq!(map.get(&key2), Some("value2"));
        assert_eq!(map.get(&InlineArray::from(vec![7, 8, 9])), None);

        // 测试长度
        assert_eq!(map.len(), 2);
        assert!(!map.is_empty());

        // 测试移除
        assert_eq!(map.remove(&key1), Some("value1_updated"));
        assert_eq!(map.get(&key1), None);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn get_lte_operations() {
        let map = InlineArrayRadixMap::new();

        map.insert(InlineArray::from(vec![1, 0, 0]), "a");
        map.insert(InlineArray::from(vec![2, 0, 0]), "b");
        map.insert(InlineArray::from(vec![3, 0, 0]), "c");

        // 测试精确匹配
        let key = InlineArray::from(vec![2, 0, 0]);
        assert_eq!(map.get_lte(&key), Some((key.clone(), "b")));

        // 测试小于等于
        let key = InlineArray::from(vec![2, 5, 0]);
        assert_eq!(map.get_lte(&key), Some((InlineArray::from(vec![2, 0, 0]), "b")));

        let key = InlineArray::from(vec![1, 5, 0]);
        assert_eq!(map.get_lte(&key), Some((InlineArray::from(vec![1, 0, 0]), "a")));

        let key = InlineArray::from(vec![0, 5, 0]);
        assert_eq!(map.get_lte(&key), None);
    }

    #[test]
    fn first_last_operations() {
        let map = InlineArrayRadixMap::new();

        map.insert(InlineArray::from(vec![5, 0]), "middle");
        map.insert(InlineArray::from(vec![1, 0]), "first");
        map.insert(InlineArray::from(vec![9, 0]), "last");

        assert_eq!(map.first(), Some((InlineArray::from(vec![1, 0]), "first")));
        assert_eq!(map.last(), Some((InlineArray::from(vec![9, 0]), "last")));
    }
}