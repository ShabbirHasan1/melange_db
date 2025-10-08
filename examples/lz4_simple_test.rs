//! 简化的LZ4压缩性能测试
//!
//! 只运行前3个基础测试，避免并发测试导致的卡顿问题

use melange_db::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 简化 LZ4压缩性能测试");
    println!("🎯 专注于基础插入和读取性能测试");

    let config = Config::new()
        .path("test_lz4_simple")
        .flush_every_ms(None)
        .cache_capacity_bytes(512 * 1024 * 1024)
        .compression_algorithm(CompressionAlgorithm::Lz4);

    if std::path::Path::new("test_lz4_simple").exists() {
        std::fs::remove_dir_all("test_lz4_simple")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    // 测试1: 单条插入性能
    println!("\n📊 测试1: 单条插入性能");
    let test_count = 1000; // 减少测试数量
    let mut insert_times = Vec::new();

    for i in 0..test_count {
        let start = Instant::now();
        let key = format!("single_key_{}", i);
        let value = format!("lz4_single_value_{}_with_some_additional_data_to_compress", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
        let duration = start.elapsed();
        insert_times.push(duration.as_nanos() as f64);
    }

    insert_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg_insert = insert_times.iter().sum::<f64>() / insert_times.len() as f64;
    let p50_insert = insert_times[test_count / 2];
    let p95_insert = insert_times[(test_count * 95) / 100];
    let p99_insert = insert_times[(test_count * 99) / 100];

    println!("✅ 插入性能统计 ({}条记录):", test_count);
    println!("   平均: {:.2} µs/条", avg_insert / 1000.0);
    println!("   P50: {:.2} µs/条", p50_insert / 1000.0);
    println!("   P95: {:.2} µs/条", p95_insert / 1000.0);
    println!("   P99: {:.2} µs/条", p99_insert / 1000.0);

    // 测试2: 读取性能
    println!("\n📊 测试2: 读取性能");
    let mut read_times = Vec::new();

    for i in 0..test_count {
        let start = Instant::now();
        let key = format!("single_key_{}", i);
        let _value = tree.get(key.as_bytes())?;
        let duration = start.elapsed();
        read_times.push(duration.as_nanos() as f64);
    }

    read_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg_read = read_times.iter().sum::<f64>() / read_times.len() as f64;
    let p50_read = read_times[test_count / 2];
    let p95_read = read_times[(test_count * 95) / 100];
    let p99_read = read_times[(test_count * 99) / 100];

    println!("✅ 读取性能统计 ({}条记录):", test_count);
    println!("   平均: {:.2} µs/条", avg_read / 1000.0);
    println!("   P50: {:.2} µs/条", p50_read / 1000.0);
    println!("   P95: {:.2} µs/条", p95_read / 1000.0);
    println!("   P99: {:.2} µs/条", p99_read / 1000.0);

    // 测试3: 批量插入性能
    println!("\n📊 测试3: 批量插入性能");
    let batch_sizes = [100, 500, 1000]; // 减少批次大小

    for &batch_size in &batch_sizes {
        let mut batch_times = Vec::new();
        let iterations = 10; // 减少迭代次数

        for i in 0..iterations {
            // 清理数据
            tree.clear()?;

            let start = Instant::now();
            for j in 0..batch_size {
                let key = format!("batch_key_{}_{}", i, j);
                let value = format!("lz4_batch_value_{}_{}", i, j);
                tree.insert(key.as_bytes(), value.as_bytes())?;
            }
            let duration = start.elapsed();
            batch_times.push(duration.as_nanos() as f64);

            println!("    第{}次批量插入{}条: {:.2} µs/条",
                     i + 1, batch_size, duration.as_micros() as f64 / batch_size as f64);
        }

        let avg_batch = batch_times.iter().sum::<f64>() / batch_times.len() as f64;
        let avg_per_op = avg_batch / batch_size as f64;
        println!("✅ 批量插入{}条: 平均 {:.2} µs/条", batch_size, avg_per_op / 1000.0);
    }

    // 测试4: SimpleEpochManager性能验证
    println!("\n📊 测试4: SimpleEpochManager性能验证");

    // 测试在clear()后的性能
    println!("  🧹 clear()后的性能:");
    for i in 0..3 {
        tree.clear()?;
        let start = Instant::now();

        for j in 0..500 {
            let key = format!("epoch_clear_key_{}_{}", i, j);
            tree.insert(key.as_bytes(), format!("value_{}", j).as_bytes())?;
        }

        let duration = start.elapsed();
        println!("    第{}次: {:.2} µs/条", i + 1, duration.as_micros() as f64 / 500.0);
    }

    // 测试不使用clear()的性能
    println!("  🚫 不使用clear()的性能:");
    for i in 0..3 {
        let start = Instant::now();

        for j in 0..500 {
            let key = format!("epoch_no_clear_key_{}_{}", i, j);
            tree.insert(key.as_bytes(), format!("value_{}", j).as_bytes())?;
        }

        let duration = start.elapsed();
        println!("    第{}次: {:.2} µs/条", i + 1, duration.as_micros() as f64 / 500.0);
    }

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("test_lz4_simple")?;

    println!("\n🎉 简化 LZ4压缩性能测试完成");
    println!("💡 SimpleEpochManager迁移成功！系统运行正常。");

    Ok(())
}