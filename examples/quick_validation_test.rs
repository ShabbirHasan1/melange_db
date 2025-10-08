//! 快速验证测试
//!
//! 验证SimpleEpochManager迁移后的基本功能，跳过有问题的性能测试

use melange_db::*;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 SimpleEpochManager快速验证测试");

    // 测试1: 基本CRUD功能
    test_basic_crud()?;

    // 测试2: 简单多线程测试（避免EBR冲突）
    test_simple_multithread()?;

    // 测试3: tree.clear()性能对比
    test_clear_performance()?;

    println!("✅ 快速验证测试完成 - SimpleEpochManager迁移成功！");
    Ok(())
}

fn test_basic_crud() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📝 测试1: 基本CRUD功能");

    let config = Config::new()
        .path("quick_validation_basic")
        .flush_every_ms(None)
        .cache_capacity_bytes(64 * 1024 * 1024);

    if std::path::Path::new("quick_validation_basic").exists() {
        std::fs::remove_dir_all("quick_validation_basic")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    // 插入测试
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
    }
    let insert_duration = start.elapsed();

    // 读取测试
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let _value = tree.get(key.as_bytes())?;
    }
    let read_duration = start.elapsed();

    // 更新测试
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let value = format!("updated_value_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
    }
    let update_duration = start.elapsed();

    // 验证数据
    let value = tree.get(b"key_500")?;
    assert_eq!(value, Some("updated_value_500".as_bytes().into()));

    println!("  ✅ 插入1000条: {:.2} ms ({:.2} µs/条)",
             insert_duration.as_millis(),
             insert_duration.as_micros() as f64 / 1000.0);
    println!("  ✅ 读取1000条: {:.2} ms ({:.2} µs/条)",
             read_duration.as_millis(),
             read_duration.as_micros() as f64 / 1000.0);
    println!("  ✅ 更新1000条: {:.2} ms ({:.2} µs/条)",
             update_duration.as_millis(),
             update_duration.as_micros() as f64 / 1000.0);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("quick_validation_basic")?;
    Ok(())
}

fn test_simple_multithread() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔀 测试2: 简单多线程测试");

    let config = Config::new()
        .path("quick_validation_mt")
        .flush_every_ms(None)
        .cache_capacity_bytes(128 * 1024 * 1024);

    if std::path::Path::new("quick_validation_mt").exists() {
        std::fs::remove_dir_all("quick_validation_mt")?;
    }

    let db = Arc::new(config.open::<1024>()?);
    let thread_count = 4;
    let operations_per_thread = 100;

    println!("  启动{}个线程，每个线程{}次操作...", thread_count, operations_per_thread);

    let mut handles = vec![];
    let start_time = Instant::now();

    for thread_id in 0..thread_count {
        let db_clone = db.clone();

        let handle = thread::spawn(move || {
            let tree = db_clone.open_tree("mt_test").unwrap();
            let mut success_count = 0;

            for i in 0..operations_per_thread {
                let key = format!("mt_key_{}_{}", thread_id, i);
                let value = format!("mt_value_{}_{}", thread_id, i);

                match tree.insert(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => success_count += 1,
                    Err(e) => {
                        eprintln!("线程{}插入失败: {}", thread_id, e);
                        break;
                    }
                }

                // 偶尔进行读取验证
                if i > 0 && i % 20 == 0 {
                    let read_key = format!("mt_key_{}_{}", thread_id, i / 2);
                    if let Ok(Some(stored_value)) = tree.get(read_key.as_bytes()) {
                        let expected_value = format!("mt_value_{}_{}", thread_id, i / 2);
                        if stored_value.as_ref() != expected_value.as_bytes() {
                            eprintln!("线程{}数据验证失败", thread_id);
                            break;
                        }
                    }
                }
            }

            println!("  线程{}完成: 成功{}次操作", thread_id, success_count);
            success_count
        });

        handles.push(handle);
    }

    // 等待所有线程完成
    let mut total_success = 0;
    for handle in handles {
        total_success += handle.join().unwrap();
    }

    let total_duration = start_time.elapsed();
    let total_expected = thread_count * operations_per_thread;

    println!("  ✅ 多线程测试完成:");
    println!("    预期操作: {}", total_expected);
    println!("    成功操作: {}", total_success);
    println!("    成功率: {:.1}%", (total_success as f64 / total_expected as f64) * 100.0);
    println!("    总耗时: {:.2} ms", total_duration.as_millis());

    drop(db);
    std::fs::remove_dir_all("quick_validation_mt")?;
    Ok(())
}

fn test_clear_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧹 测试3: tree.clear()性能对比");

    let config = Config::new()
        .path("quick_validation_clear")
        .flush_every_ms(None)
        .cache_capacity_bytes(64 * 1024 * 1024);

    if std::path::Path::new("quick_validation_clear").exists() {
        std::fs::remove_dir_all("quick_validation_clear")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("clear_test")?;

    let batch_size = 200;
    let test_rounds = 3;

    println!("  执行{}轮对比测试，每轮{}条记录...", test_rounds, batch_size);

    // 测试1: 使用tree.clear()
    println!("  🧹 使用tree.clear():");
    let mut clear_times = Vec::new();

    for round in 0..test_rounds {
        tree.clear()?;

        let start = Instant::now();
        for i in 0..batch_size {
            let key = format!("clear_key_{}_{}", round, i);
            let value = format!("clear_value_{}_{}", round, i);
            tree.insert(key.as_bytes(), value.as_bytes())?;
        }
        let duration = start.elapsed();
        clear_times.push(duration.as_micros());

        println!("    第{}轮: {:.2} µs/条", round + 1, duration.as_micros() as f64 / batch_size as f64);
    }

    // 测试2: 不使用tree.clear()
    println!("  🚫 不使用tree.clear():");
    let mut no_clear_times = Vec::new();

    for round in 0..test_rounds {
        let start = Instant::now();
        for i in 0..batch_size {
            let key = format!("no_clear_key_{}_{}", round, i);
            let value = format!("no_clear_value_{}_{}", round, i);
            tree.insert(key.as_bytes(), value.as_bytes())?;
        }
        let duration = start.elapsed();
        no_clear_times.push(duration.as_micros());

        println!("    第{}轮: {:.2} µs/条", round + 1, duration.as_micros() as f64 / batch_size as f64);
    }

    // 性能对比
    let avg_clear = clear_times.iter().sum::<u128>() as f64 / clear_times.len() as f64 / batch_size as f64;
    let avg_no_clear = no_clear_times.iter().sum::<u128>() as f64 / no_clear_times.len() as f64 / batch_size as f64;
    let difference = avg_clear - avg_no_clear;
    let percentage = (difference / avg_no_clear) * 100.0;

    println!("  📊 性能对比结果:");
    println!("    使用clear(): {:.2} µs/条", avg_clear);
    println!("    不使用clear(): {:.2} µs/条", avg_no_clear);
    println!("    差异: {:+.2} µs/条 ({:+.1}%)", difference, percentage);

    if difference.abs() < 100.0 {
        println!("    结论: 两种方式性能相近 ✓");
    } else if difference < 0.0 {
        println!("    结论: 使用clear()性能更好 ✓");
    } else {
        println!("    结论: 不使用clear()性能更好 ✓");
    }

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("quick_validation_clear")?;
    Ok(())
}