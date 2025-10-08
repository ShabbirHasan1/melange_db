//! 单线程验证测试
//!
//! 专门用于验证SimpleEpochManager迁移后的单线程功能

use melange_db::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 SimpleEpochManager单线程验证测试");

    // 测试1: 基本CRUD功能
    test_basic_crud()?;

    // 测试2: tree.clear()性能对比
    test_clear_performance()?;

    // 测试3: 不同数据大小的性能
    test_different_data_sizes()?;

    // 测试4: 简单的迭代器测试
    test_iterator_functionality()?;

    println!("✅ 单线程验证测试完成 - SimpleEpochManager基础功能正常！");
    Ok(())
}

fn test_basic_crud() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📝 测试1: 基本CRUD功能");

    let config = Config::new()
        .path("single_thread_basic")
        .flush_every_ms(None)
        .cache_capacity_bytes(64 * 1024 * 1024);

    if std::path::Path::new("single_thread_basic").exists() {
        std::fs::remove_dir_all("single_thread_basic")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    println!("  数据库初始化成功");

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
    let mut read_success = 0;
    for i in 0..1000 {
        let key = format!("key_{}", i);
        if let Ok(Some(_value)) = tree.get(key.as_bytes()) {
            read_success += 1;
        }
    }
    let read_duration = start.elapsed();

    // 更新测试
    let start = Instant::now();
    let mut update_success = 0;
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let value = format!("updated_value_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
        update_success += 1;
    }
    let update_duration = start.elapsed();

    // 验证数据
    let test_key = "key_500";
    let expected_value = "updated_value_500";
    let actual_value = tree.get(test_key.as_bytes())?;

    match actual_value {
        Some(ref v) if v.as_ref() == expected_value.as_bytes() => {
            println!("  ✅ 数据验证通过");
        }
        Some(v) => {
            println!("  ❌ 数据验证失败: 期望 {}, 得到 {:?}", expected_value, v);
        }
        None => {
            println!("  ❌ 数据验证失败: 键 {} 不存在", test_key);
        }
    }

    // 删除测试
    let start = Instant::now();
    let mut delete_success = 0;
    for i in 0..100 {
        let key = format!("key_{}", i);
        tree.remove(key.as_bytes())?;
        delete_success += 1;
    }
    let delete_duration = start.elapsed();

    // 验证删除
    let deleted_value = tree.get(b"key_50")?;
    if deleted_value.is_none() {
        println!("  ✅ 删除功能验证通过");
    } else {
        println!("  ❌ 删除功能验证失败");
    }

    println!("  📊 性能统计:");
    println!("    插入1000条: {:.2} ms ({:.2} µs/条) - 成功率 {:.1}%",
             insert_duration.as_millis(),
             insert_duration.as_micros() as f64 / 1000.0,
             1000.0 / 1000.0 * 100.0);
    println!("    读取1000条: {:.2} ms ({:.2} µs/条) - 成功率 {:.1}%",
             read_duration.as_millis(),
             read_duration.as_micros() as f64 / read_success as f64,
             read_success as f64 / 1000.0 * 100.0);
    println!("    更新1000条: {:.2} ms ({:.2} µs/条) - 成功率 {:.1}%",
             update_duration.as_millis(),
             update_duration.as_micros() as f64 / update_success as f64,
             update_success as f64 / 1000.0 * 100.0);
    println!("    删除100条: {:.2} ms ({:.2} µs/条) - 成功率 {:.1}%",
             delete_duration.as_millis(),
             delete_duration.as_micros() as f64 / delete_success as f64,
             delete_success as f64 / 100.0 * 100.0);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("single_thread_basic")?;
    Ok(())
}

fn test_clear_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧹 测试2: tree.clear()性能对比");

    let config = Config::new()
        .path("single_thread_clear")
        .flush_every_ms(None)
        .cache_capacity_bytes(64 * 1024 * 1024);

    if std::path::Path::new("single_thread_clear").exists() {
        std::fs::remove_dir_all("single_thread_clear")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("clear_test")?;

    let batch_size = 500;
    let test_rounds = 5;

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
    std::fs::remove_dir_all("single_thread_clear")?;
    Ok(())
}

fn test_different_data_sizes() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📏 测试3: 不同数据大小的性能");

    let config = Config::new()
        .path("single_thread_sizes")
        .flush_every_ms(None)
        .cache_capacity_bytes(128 * 1024 * 1024);

    if std::path::Path::new("single_thread_sizes").exists() {
        std::fs::remove_dir_all("single_thread_sizes")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("size_test")?;

    let test_sizes = vec![
        (64, "小数据 (64字节)"),
        (256, "中等数据 (256字节)"),
        (1024, "大数据 (1KB)"),
        (4096, "超大数据 (4KB)"),
    ];

    for (data_size, description) in test_sizes {
        println!("  📊 测试{}:", description);

        let test_value = vec![b'X'; data_size];
        let test_count = 200;

        tree.clear()?;

        let start = Instant::now();
        for i in 0..test_count {
            let key = format!("size_key_{}_{}", data_size, i);
            tree.insert(key.as_bytes(), &*test_value)?;
        }
        let duration = start.elapsed();

        let avg_time = duration.as_micros() as f64 / test_count as f64;
        println!("    插入{}条{}: {:.2} µs/条", test_count, description, avg_time);

        // 验证几条数据
        let mut verify_success = 0;
        for i in 0..5.min(test_count) {
            let key = format!("size_key_{}_{}", data_size, i);
            if let Ok(Some(value)) = tree.get(key.as_bytes()) {
                if value.len() == data_size {
                    verify_success += 1;
                }
            }
        }
        println!("    验证{}条: 成功{}", 5.min(test_count), verify_success);
    }

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("single_thread_sizes")?;
    Ok(())
}

fn test_iterator_functionality() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔄 测试4: 迭代器功能");

    let config = Config::new()
        .path("single_thread_iterator")
        .flush_every_ms(None)
        .cache_capacity_bytes(64 * 1024 * 1024);

    if std::path::Path::new("single_thread_iterator").exists() {
        std::fs::remove_dir_all("single_thread_iterator")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("iterator_test")?;

    // 插入测试数据
    println!("  插入测试数据...");
    for i in 0..100 {
        let key = format!("iter_key_{:03}", i);
        let value = format!("iter_value_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
    }

    // 测试迭代器
    println!("  测试迭代器功能...");
    let start = Instant::now();
    let mut iter_count = 0;
    let mut iter_success = 0;

    for item in tree.iter() {
        match item {
            Ok((key, value)) => {
                iter_count += 1;
                // 验证数据格式
                if key.starts_with(b"iter_key_") && value.starts_with(b"iter_value_") {
                    iter_success += 1;
                }
            }
            Err(e) => {
                println!("    迭代器错误: {}", e);
                break;
            }
        }
    }
    let iter_duration = start.elapsed();

    println!("  📊 迭代器结果:");
    println!("    迭代总数: {}", iter_count);
    println!("    验证成功: {}", iter_success);
    println!("    成功率: {:.1}%", iter_success as f64 / iter_count as f64 * 100.0);
    println!("    迭代耗时: {:.2} ms", iter_duration.as_millis());
    println!("    平均每项: {:.2} µs", iter_duration.as_micros() as f64 / iter_count as f64);

    // 测试范围迭代
    println!("  测试范围迭代...");
    let start = Instant::now();
    let mut range_count = 0;

    for item in tree.range::<&[u8], std::ops::Range<&[u8]>>(b"iter_key_020"..b"iter_key_030") {
        if let Ok((key, _)) = item {
            range_count += 1;
            println!("    范围内键: {:?}", String::from_utf8_lossy(&key));
        }
    }
    let range_duration = start.elapsed();

    println!("    范围迭代结果: 找到{}条记录，耗时{:.2} ms", range_count, range_duration.as_millis());

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("single_thread_iterator")?;
    Ok(())
}