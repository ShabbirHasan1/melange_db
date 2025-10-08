//! SimpleEpochManager性能测试
//!
//! 验证新的简化epoch管理系统的性能表现

use melange_db::*;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 SimpleEpochManager性能测试开始");

    // 测试1: 单线程epoch创建性能
    test_single_thread_epoch_creation()?;

    // 测试2: 多线程epoch创建性能
    test_multi_thread_epoch_creation()?;

    // 测试3: Guard创建性能
    test_guard_creation_performance()?;

    // 测试4: 内存使用效率
    test_memory_efficiency()?;

    println!("✅ SimpleEpochManager性能测试完成");
    Ok(())
}

fn test_single_thread_epoch_creation() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试1: 单线程epoch创建性能");

    let db = open("epoch_perf_test_single")?;
    let tree = db.open_tree("test_tree")?;

    let iterations = 100_000;
    let start = Instant::now();

    for i in 0..iterations {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
    }

    let duration = start.elapsed();
    let ops_per_sec = iterations as f64 / duration.as_secs_f64();

    println!("✅ 单线程epoch创建性能:");
    println!("   操作数: {}", iterations);
    println!("   耗时: {:?}", duration);
    println!("   吞吐量: {:.0} ops/sec", ops_per_sec);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("epoch_perf_test_single")?;

    Ok(())
}

fn test_multi_thread_epoch_creation() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试2: 多线程epoch创建性能");

    let db = open("epoch_perf_test_multi")?;
    let db = Arc::new(db);
    let tree = db.open_tree("test_tree")?;
    let tree = Arc::new(tree);

    let thread_count = 8;
    let operations_per_thread = 10_000;
    let total_operations = thread_count * operations_per_thread;

    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..thread_count {
        let tree_clone = tree.clone();
        let handle = thread::spawn(move || {
            for i in 0..operations_per_thread {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("thread_{}_value_{}", thread_id, i);
                tree_clone.insert(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let ops_per_sec = total_operations as f64 / duration.as_secs_f64();

    println!("✅ 多线程epoch创建性能:");
    println!("   线程数: {}", thread_count);
    println!("   每线程操作数: {}", operations_per_thread);
    println!("   总操作数: {}", total_operations);
    println!("   耗时: {:?}", duration);
    println!("   吞吐量: {:.0} ops/sec", ops_per_sec);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("epoch_perf_test_multi")?;

    Ok(())
}

fn test_guard_creation_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试3: Guard创建性能");

    let db = open("epoch_perf_test_guard")?;
    let tree = db.open_tree("test_tree")?;

    let iterations = 50_000;
    let start = Instant::now();

    for i in 0..iterations {
        let key = format!("guard_key_{}", i);
        let value = format!("guard_value_{}", i);

        // 这里会自动创建epoch guard
        tree.insert(key.as_bytes(), value.as_bytes())?;

        // 模拟一些读取操作
        let _ = tree.get(key.as_bytes());
    }

    let duration = start.elapsed();
    let ops_per_sec = iterations as f64 / duration.as_secs_f64();

    println!("✅ Guard创建性能:");
    println!("   操作数: {}", iterations);
    println!("   耗时: {:?}", duration);
    println!("   吞吐量: {:.0} ops/sec", ops_per_sec);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("epoch_perf_test_guard")?;

    Ok(())
}

fn test_memory_efficiency() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试4: 内存使用效率");

    let db = open("epoch_perf_test_memory")?;
    let tree = db.open_tree("test_tree")?;

    // 插入大量数据测试内存效率
    let iterations = 20_000;
    let value_size = 1024; // 1KB value
    let value = "x".repeat(value_size);

    println!("   插入 {} 条 {} 字节的记录...", iterations, value_size);

    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("memory_key_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
    }
    let insert_duration = start.elapsed();

    println!("✅ 内存效率测试:");
    println!("   插入耗时: {:?}", insert_duration);
    println!("   平均插入时间: {:.2} µs/条", insert_duration.as_micros() as f64 / iterations as f64);
    println!("   数据总量: {:.2} MB", (iterations * value_size) as f64 / 1024.0 / 1024.0);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("epoch_perf_test_memory")?;

    Ok(())
}