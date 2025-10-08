//! SimpleEpochManager POC测试
//!
//! 测试新的SimpleEpochManager的epoch管理功能

use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use melange_db::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 开始SimpleEpochManager POC测试");

    // 测试1: 单线程epoch管理
    test_single_thread_epoch()?;

    // 测试2: 多线程epoch管理（不检查epoch关系）
    test_multi_thread_epoch_simple()?;

    // 测试3: 多线程epoch管理（检查epoch关系）
    test_multi_thread_epoch_with_checks()?;

    println!("✅ 所有POC测试完成");
    Ok(())
}

fn test_single_thread_epoch() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试1: 单线程epoch管理");

    let config = Config::new()
        .path("poc_single_thread")
        .flush_every_ms(None)
        .cache_capacity_bytes(64 * 1024 * 1024);

    if std::path::Path::new("poc_single_thread").exists() {
        std::fs::remove_dir_all("poc_single_thread")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    println!("  创建数据库连接成功");

    // 测试epoch递增
    let mut epochs = Vec::new();
    for i in 0..10 {
        let key = format!("single_key_{}", i);
        let value = format!("single_value_{}", i);

        let start = Instant::now();
        tree.insert(key.as_bytes(), value.as_bytes())?;
        let duration = start.elapsed();

        // 记录epoch信息 (通过tree的方法访问epoch)
        // 注意：tree.cache是私有的，我们通过其他方式验证epoch功能
        epochs.push(current_epoch.get());

        println!("    第{}次插入: epoch={}, 耗时={:.2} µs",
                 i, current_epoch.get(), duration.as_micros());
    }

    // 验证epoch是否递增
    println!("  Epoch序列: {:?}", epochs);
    let is_increasing = epochs.windows(2).all(|w| w[0] <= w[1]);
    println!("  Epoch递增检查: {}", if is_increasing { "✅ 通过" } else { "❌ 失败" });

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("poc_single_thread")?;
    Ok(())
}

fn test_multi_thread_epoch_simple() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试2: 多线程epoch管理（简单模式）");

    let config = Config::new()
        .path("poc_multi_thread_simple")
        .flush_every_ms(None)
        .cache_capacity_bytes(128 * 1024 * 1024);

    if std::path::Path::new("poc_multi_thread_simple").exists() {
        std::fs::remove_dir_all("poc_multi_thread_simple")?;
    }

    let db = Arc::new(config.open::<1024>()?);
    let results = Arc::new(Mutex::new(Vec::new()));
    let thread_count = 4;

    println!("  启动{}个并发线程进行插入操作...", thread_count);

    let mut handles = vec![];
    let start_time = Instant::now();

    for thread_id in 0..thread_count {
        let db_clone = db.clone();
        let results_clone = results.clone();

        let handle = thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
            let tree = db_clone.open_tree("test_tree")?;
            let mut thread_epochs = Vec::new();

            for i in 0..100 {
                let key = format!("multi_key_{}_{}", thread_id, i);
                let value = format!("multi_value_{}_{}", thread_id, i);

                let start = Instant::now();
                tree.insert(key.as_bytes(), value.as_bytes())?;
                let duration = start.elapsed();

                let current_epoch = tree.cache.current_flush_epoch();
                thread_epochs.push((current_epoch.get(), duration.as_micros()));
            }

            let mut results = results_clone.lock().unwrap();
            results.push((thread_id, thread_epochs));

            Ok(())
        });

        handles.push(handle);
    }

    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap()?;
    }

    let total_duration = start_time.elapsed();
    println!("  总耗时: {:.2} ms", total_duration.as_millis());

    // 分析结果
    let results = results.lock().unwrap();
    for (thread_id, epochs) in results.iter() {
        let avg_time = epochs.iter().map(|(_, time)| *time).sum::<u128>() / epochs.len() as u128;
        let (first_epoch, _) = epochs[0];
        let (last_epoch, _) = epochs[epochs.len() - 1];

        println!("    线程{}: epoch范围=[{}, {}], 平均耗时={:.2} µs/条",
                 thread_id, first_epoch, last_epoch, avg_time);
    }

    drop(db);
    std::fs::remove_dir_all("poc_multi_thread_simple")?;
    Ok(())
}

fn test_multi_thread_epoch_with_checks() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试3: 多线程epoch管理（带epoch关系检查）");

    let config = Config::new()
        .path("poc_multi_thread_checks")
        .flush_every_ms(None)
        .cache_capacity_bytes(256 * 1024 * 1024);

    if std::path::Path::new("poc_multi_thread_checks").exists() {
        std::fs::remove_dir_all("poc_multi_thread_checks")?;
    }

    let db = Arc::new(config.open::<1024>()?);
    let shared_data = Arc::new(Mutex::new(SharedData::new()));
    let thread_count = 3;

    println!("  启动{}个线程进行复杂的epoch操作...", thread_count);

    let mut handles = vec![];
    let start_time = Instant::now();

    for thread_id in 0..thread_count {
        let db_clone = db.clone();
        let shared_data_clone = shared_data.clone();

        let handle = thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
            let tree = db_clone.open_tree("test_tree")?;
            let mut operations = Vec::new();

            // 每个线程执行多种操作
            for operation_id in 0..50 {
                match operation_id % 4 {
                    0 => {
                        // 插入操作
                        let key = format!("complex_key_{}_{}", thread_id, operation_id);
                        let value = format!("complex_value_{}_{}", thread_id, operation_id);

                        let start = Instant::now();
                        tree.insert(key.as_bytes(), value.as_bytes())?;
                        let duration = start.elapsed();

                        let current_epoch = tree.cache.current_flush_epoch();
                        operations.push((format!("insert_{}", operation_id), current_epoch.get(), duration.as_micros()));
                    }
                    1 => {
                        // 读取操作
                        if operation_id > 0 {
                            let key = format!("complex_key_{}_{}", thread_id, operation_id - 1);
                            let start = Instant::now();
                            let _value = tree.get(key.as_bytes())?;
                            let duration = start.elapsed();

                            let current_epoch = tree.cache.current_flush_epoch();
                            operations.push((format!("read_{}", operation_id), current_epoch.get(), duration.as_micros()));
                        }
                    }
                    2 => {
                        // 更新操作
                        if operation_id > 0 {
                            let key = format!("complex_key_{}_{}", thread_id, operation_id / 2);
                            let value = format!("updated_value_{}_{}", thread_id, operation_id);

                            let start = Instant::now();
                            tree.insert(key.as_bytes(), value.as_bytes())?;
                            let duration = start.elapsed();

                            let current_epoch = tree.cache.current_flush_epoch();
                            operations.push((format!("update_{}", operation_id), current_epoch.get(), duration.as_micros()));
                        }
                    }
                    3 => {
                        // 清理测试
                        if operation_id > 10 {
                            let key = format!("complex_key_{}_{}", thread_id, operation_id - 10);
                            let start = Instant::now();
                            tree.remove(key.as_bytes())?;
                            let duration = start.elapsed();

                            let current_epoch = tree.cache.current_flush_epoch();
                            operations.push((format!("remove_{}", operation_id), current_epoch.get(), duration.as_micros()));
                        }
                    }
                    _ => unreachable!(),
                }
            }

            // 记录线程结果
            let mut shared = shared_data_clone.lock().unwrap();
            shared.thread_results.push((thread_id, operations));

            Ok(())
        });

        handles.push(handle);
    }

    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap()?;
    }

    let total_duration = start_time.elapsed();
    println!("  总耗时: {:.2} ms", total_duration.as_millis());

    // 分析结果
    let shared = shared_data.lock().unwrap();
    let mut all_epochs = Vec::new();
    let mut operation_times = Vec::new();

    for (thread_id, operations) in &shared.thread_results {
        let mut thread_time_total = 0u128;
        let mut thread_epoch_min = u64::MAX;
        let mut thread_epoch_max = 0u64;

        for (op_name, epoch, time) in operations {
            thread_time_total += *time;
            thread_epoch_min = thread_epoch_min.min(*epoch);
            thread_epoch_max = thread_epoch_max.max(*epoch);
            all_epochs.push(*epoch);
            operation_times.push(*time);
        }

        println!("    线程{}: epoch范围=[{}, {}], 总耗时={:.2} ms, 平均耗时={:.2} µs/操作",
                 thread_id, thread_epoch_min, thread_epoch_max,
                 thread_time_total / 1000,
                 thread_time_total as f64 / operations.len() as f64);
    }

    // Epoch一致性检查
    if !all_epochs.is_empty() {
        let min_epoch = all_epochs.iter().min().unwrap();
        let max_epoch = all_epochs.iter().max().unwrap();
        println!("  全局epoch范围: [{} , {}]", min_epoch, max_epoch);

        // 检查是否有奇怪的epoch跳跃
        let mut sorted_epochs = all_epochs.clone();
        sorted_epochs.sort();
        sorted_epochs.dedup();

        println!("  唯一epoch数量: {}", sorted_epochs.len());
        println!("  Epoch序列: {:?}", sorted_epochs.iter().take(10).collect::<Vec<_>>());
    }

    // 性能统计
    if !operation_times.is_empty() {
        let avg_time = operation_times.iter().sum::<u128>() as f64 / operation_times.len() as f64;
        operation_times.sort();
        let p50 = operation_times[operation_times.len() / 2];
        let p95 = operation_times[(operation_times.len() * 95) / 100];

        println!("  性能统计:");
        println!("    平均耗时: {:.2} µs/操作", avg_time);
        println!("    P50: {} µs", p50);
        println!("    P95: {} µs", p95);
    }

    drop(db);
    std::fs::remove_dir_all("poc_multi_thread_checks")?;
    Ok(())
}

#[derive(Debug)]
struct SharedData {
    thread_results: Vec<(usize, Vec<(String, u64, u128)>)>,
}

impl SharedData {
    fn new() -> Self {
        Self {
            thread_results: Vec::new(),
        }
    }
}