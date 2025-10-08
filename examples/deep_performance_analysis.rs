//! 深度性能分析测试
//!
//! 分析tree.clear()性能悖论的详细原因

use melange_db::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 深度性能分析测试");

    // 测试不同场景下的性能表现
    test_clear_vs_no_clear()?;
    test_memory_allocation_pattern()?;
    test_cache_behavior()?;
    test_epoch_overhead()?;

    println!("🎉 深度性能分析完成");
    Ok(())
}

fn test_clear_vs_no_clear() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 基础对比测试");

    let config = Config::new()
        .path("test_basic")
        .flush_every_ms(None)
        .cache_capacity_bytes(256 * 1024 * 1024)
        .compression_algorithm(CompressionAlgorithm::Lz4);

    if std::path::Path::new("test_basic").exists() {
        std::fs::remove_dir_all("test_basic")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    let batch_size = 500;
    let rounds = 5;

    // 测试1: 使用clear()
    println!("  🧹 使用tree.clear():");
    for round in 0..rounds {
        tree.clear()?;

        let start = Instant::now();
        for i in 0..batch_size {
            let key = format!("key_{}", i);
            let value = format!("value_{}_{}", round, i);
            tree.insert(key.as_bytes(), value.as_bytes())?;
        }
        let duration = start.elapsed();
        println!("    第{}轮: {:.2} µs/条", round + 1, duration.as_micros() as f64 / batch_size as f64);
    }

    // 测试2: 不使用clear()
    println!("  🚫 不使用tree.clear():");
    for round in 0..rounds {
        let start = Instant::now();
        for i in 0..batch_size {
            let key = format!("round_{}_key_{}", round, i);
            let value = format!("value_{}_{}", round, i);
            tree.insert(key.as_bytes(), value.as_bytes())?;
        }
        let duration = start.elapsed();
        println!("    第{}轮: {:.2} µs/条", round + 1, duration.as_micros() as f64 / batch_size as f64);
    }

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("test_basic")?;
    Ok(())
}

fn test_memory_allocation_pattern() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧠 内存分配模式测试");

    // 测试不同value大小的影响
    for value_size in [64, 256, 1024, 4096] {
        println!("  📏 测试value大小: {} bytes", value_size);

        let config = Config::new()
            .path(&format!("test_memory_{}", value_size))
            .flush_every_ms(None)
            .cache_capacity_bytes(128 * 1024 * 1024)
            .compression_algorithm(CompressionAlgorithm::Lz4);

        if std::path::Path::new(&format!("test_memory_{}", value_size)).exists() {
            std::fs::remove_dir_all(&format!("test_memory_{}", value_size))?;
        }

        let db = config.open::<1024>()?;
        let tree = db.open_tree("test_tree")?;

        let batch_size = 200;
        let value_data = vec![0u8; value_size];

        // 使用clear()
        tree.clear()?;
        let start_clear = Instant::now();
        for i in 0..batch_size {
            let key = format!("key_{}", i);
            tree.insert(key.as_bytes(), &*value_data)?;
        }
        let clear_time = start_clear.elapsed();

        // 不使用clear()
        let start_no_clear = Instant::now();
        for i in 0..batch_size {
            let key = format!("no_clear_key_{}", i);
            tree.insert(key.as_bytes(), &*value_data)?;
        }
        let no_clear_time = start_no_clear.elapsed();

        let clear_micros = clear_time.as_micros() as f64 / batch_size as f64;
        let no_clear_micros = no_clear_time.as_micros() as f64 / batch_size as f64;
        let diff = clear_micros - no_clear_micros;

        println!("    clear(): {:.2} µs/条", clear_micros);
        println!("    no_clear(): {:.2} µs/条", no_clear_micros);
        println!("    差异: {:+.2} µs/条 ({:+.1}%)", diff, diff / no_clear_micros * 100.0);

        drop(tree);
        drop(db);
        std::fs::remove_dir_all(&format!("test_memory_{}", value_size))?;
    }

    Ok(())
}

fn test_cache_behavior() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n💾 缓存行为测试");

    let config = Config::new()
        .path("test_cache")
        .flush_every_ms(None)
        .cache_capacity_bytes(64 * 1024 * 1024) // 较小的缓存
        .compression_algorithm(CompressionAlgorithm::Lz4);

    if std::path::Path::new("test_cache").exists() {
        std::fs::remove_dir_all("test_cache")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    let batch_size = 1000;

    // 填满缓存
    println!("  🔵 填充缓存...");
    for i in 0..batch_size * 2 {
        let key = format!("fill_key_{}", i);
        let value = format!("fill_value_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
    }

    // 测试在缓存压力下的性能
    println!("  🔄 缓存压力测试:");

    // 使用clear()
    tree.clear()?;
    let start = Instant::now();
    for i in 0..batch_size {
        let key = format!("pressure_clear_key_{}", i);
        let value = format!("pressure_clear_value_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
    }
    let clear_time = start.elapsed();

    // 不使用clear()
    let start = Instant::now();
    for i in 0..batch_size {
        let key = format!("pressure_no_clear_key_{}", i);
        let value = format!("pressure_no_clear_value_{}", i);
        tree.insert(key.as_bytes(), value.as_bytes())?;
    }
    let no_clear_time = start.elapsed();

    println!("    clear(): {:.2} µs/条", clear_time.as_micros() as f64 / batch_size as f64);
    println!("    no_clear(): {:.2} µs/条", no_clear_time.as_micros() as f64 / batch_size as f64);

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("test_cache")?;
    Ok(())
}

fn test_epoch_overhead() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n⏰ Epoch开销测试");

    let config = Config::new()
        .path("test_epoch")
        .flush_every_ms(None)
        .cache_capacity_bytes(128 * 1024 * 1024)
        .compression_algorithm(CompressionAlgorithm::Lz4);

    if std::path::Path::new("test_epoch").exists() {
        std::fs::remove_dir_all("test_epoch")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("test_tree")?;

    let batch_size = 500;
    let iterations = 20;

    println!("  📈 测试SimpleEpochManager在不同状态下的性能:");

    // 测试在"干净"状态下的性能
    println!("    干净状态下的性能:");
    for i in 0..5 {
        tree.clear()?;
        let start = Instant::now();
        for j in 0..batch_size {
            let key = format!("clean_key_{}_{}", i, j);
            tree.insert(key.as_bytes(), format!("value_{}", j).as_bytes())?;
        }
        let duration = start.elapsed();
        println!("      第{}次: {:.2} µs/条", i + 1, duration.as_micros() as f64 / batch_size as f64);
    }

    // 测试在"脏"状态下的性能
    println!("    脏状态下的性能:");
    for i in 0..5 {
        let start = Instant::now();
        for j in 0..batch_size {
            let key = format!("dirty_key_{}_{}", i, j);
            tree.insert(key.as_bytes(), format!("value_{}", j).as_bytes())?;
        }
        let duration = start.elapsed();
        println!("      第{}次: {:.2} µs/条", i + 1, duration.as_micros() as f64 / batch_size as f64);
    }

    drop(tree);
    drop(db);
    std::fs::remove_dir_all("test_epoch")?;
    Ok(())
}