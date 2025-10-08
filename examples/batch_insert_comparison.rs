//! 批量插入性能对比测试
//!
//! 对比使用tree.clear()和不使用tree.clear()的性能差异

use melange_db::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 批量插入性能对比测试");

    // 测试1: 使用tree.clear()（像LZ4示例那样）
    test_with_clear()?;

    // 测试2: 不使用tree.clear()（使用不同的key）
    test_without_clear()?;

    println!("🎉 批量插入性能对比测试完成");
    Ok(())
}

fn test_with_clear() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试1: 使用tree.clear()");

    let config = Config::new()
        .path("test_clear")
        .flush_every_ms(None)
        .cache_capacity_bytes(512 * 1024 * 1024)
        .compression_algorithm(CompressionAlgorithm::Lz4);

    if std::path::Path::new("test_clear").exists() {
        std::fs::remove_dir_all("test_clear")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    let batch_size = 1000;
    let iterations = 10;
    let mut times = Vec::new();

    println!("  执行{}次，每次{}条记录...", iterations, batch_size);

    for i in 0..iterations {
        // 清理数据（这就是导致性能问题的原因）
        tree.clear()?;

        let start = Instant::now();
        for j in 0..batch_size {
            let key = format!("key_{}", j);
            let value = format!("value_{}_{}", i, j);
            tree.insert(key.as_bytes(), value.as_bytes())?;
        }
        let duration = start.elapsed();
        let duration_micros = duration.as_micros() as f64;
        times.push(duration_micros);

        println!("    第{}次: {:.2} ms, {:.2} µs/条",
                 i + 1, duration.as_millis(), duration_micros / batch_size as f64);
    }

    let avg_time = times.iter().sum::<f64>() / times.len() as f64;
    println!("  📈 平均耗时: {:.2} ms ({:.2} µs/条)", avg_time / 1000.0, avg_time / batch_size as f64);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("test_clear")?;

    Ok(())
}

fn test_without_clear() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 测试2: 不使用tree.clear()");

    let config = Config::new()
        .path("test_no_clear")
        .flush_every_ms(None)
        .cache_capacity_bytes(512 * 1024 * 1024)
        .compression_algorithm(CompressionAlgorithm::Lz4);

    if std::path::Path::new("test_no_clear").exists() {
        std::fs::remove_dir_all("test_no_clear")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    let batch_size = 1000;
    let iterations = 10;
    let mut times = Vec::new();

    println!("  执行{}次，每次{}条记录...", iterations, batch_size);

    for i in 0..iterations {
        let start = Instant::now();
        for j in 0..batch_size {
            // 使用不同的key，避免重复
            let key = format!("batch_{}_key_{}", i, j);
            let value = format!("value_{}_{}", i, j);
            tree.insert(key.as_bytes(), value.as_bytes())?;
        }
        let duration = start.elapsed();
        let duration_micros = duration.as_micros() as f64;
        times.push(duration_micros);

        println!("    第{}次: {:.2} ms, {:.2} µs/条",
                 i + 1, duration.as_millis(), duration_micros / batch_size as f64);
    }

    let avg_time = times.iter().sum::<f64>() / times.len() as f64;
    println!("  📈 平均耗时: {:.2} ms ({:.2} µs/条)", avg_time / 1000.0, avg_time / batch_size as f64);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("test_no_clear")?;

    Ok(())
}