//! SimpleEpochManager基础功能测试
//!
//! 验证新的简化epoch管理系统的基本功能

use melange_db::*;
use std::sync::Arc;
use std::thread;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 SimpleEpochManager基础功能测试");

    // 创建简单的数据库实例
    let db = open("simple_epoch_basic_test")?;
    let tree = db.open_tree("test_tree")?;

    // 基本插入测试
    println!("\n📝 基本功能测试:");
    tree.insert("key1", "value1")?;
    tree.insert("key2", "value2")?;
    tree.insert("key3", "value3")?;

    assert_eq!(tree.get("key1")?, Some("value1".into()));
    assert_eq!(tree.get("key2")?, Some("value2".into()));
    assert_eq!(tree.get("key3")?, Some("value3".into()));
    println!("✅ 基本插入和读取功能正常");

    // 更新测试
    tree.insert("key1", "new_value1")?;
    assert_eq!(tree.get("key1")?, Some("new_value1".into()));
    println!("✅ 更新功能正常");

    // 删除测试
    tree.remove("key2")?;
    assert_eq!(tree.get("key2")?, None);
    println!("✅ 删除功能正常");

    // 简单多线程测试
    println!("\n🔀 多线程测试:");
    let db = Arc::new(open("simple_epoch_basic_test_mt")?);
    let mut handles = vec![];

    for thread_id in 0..4 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            let tree = db_clone.open_tree("thread_test").unwrap();
            for i in 0..100 {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("thread_{}_value_{}", thread_id, i);
                tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let tree = db.open_tree("thread_test")?;
    let mut count = 0;
    for item in tree.iter() {
        let (key, _) = item?;
        if key.starts_with(b"thread_") {
            count += 1;
        }
    }
    assert_eq!(count, 400); // 4 threads * 100 items each
    println!("✅ 多线程插入功能正常，共插入 {} 条记录", count);

    println!("\n🎉 SimpleEpochManager基础功能测试完成！");

    // 清理
    drop(tree);
    drop(db);
    std::fs::remove_dir_all("simple_epoch_basic_test")?;
    std::fs::remove_dir_all("simple_epoch_basic_test_mt")?;

    Ok(())
}