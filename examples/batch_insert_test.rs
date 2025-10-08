//! 批量插入性能测试
//!
//! 单独测试批量插入的性能，排除其他因素影响

use melange_db::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 批量插入性能测试开始");

    // 配置数据库
    let config = Config::new()
        .path("batch_insert_test")
        .flush_every_ms(None)
        .cache_capacity_bytes(512 * 1024 * 1024);

    // 清理旧的测试数据库
    if std::path::Path::new("batch_insert_test").exists() {
        std::fs::remove_dir_all("batch_insert_test")?;
    }

    let db = config.open::<1024>()?;
    let tree = db.open_tree("batch_test")?;

    // 测试1: 批量插入100条记录
    test_batch_insert(&tree, 100, "小批量")?;

    // 测试2: 批量插入1000条记录
    test_batch_insert(&tree, 1000, "中批量")?;

    // 测试3: 批量插入5000条记录
    test_batch_insert(&tree, 5000, "大批量")?;

    // 测试4: 重复插入测试（模拟实际使用场景）
    test_repeated_batch_insert(&tree, 100, 50)?;

    println!("🎉 批量插入性能测试完成");

    // 清理
    drop(tree);
    drop(db);
    std::fs::remove_dir_all("batch_insert_test")?;

    Ok(())
}

fn test_batch_insert(
    tree: &Tree,
    batch_size: usize,
    test_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 {} - 批量插入{}条记录", test_name, batch_size);

    let mut times = Vec::new();
    let iterations = 10; // 减少迭代次数以便快速测试

    for iteration in 0..iterations {
        println!("  迭代 {}/{}", iteration + 1, iterations);

        let start = Instant::now();

        for i in 0..batch_size {
            let key = format!("batch_key_{}", i);
            let value = format!("batch_value_{}", i);
            tree.insert(key.as_bytes(), value.as_bytes())?;
        }

        let duration = start.elapsed();
        let duration_micros = duration.as_micros() as f64;
        times.push(duration_micros);

        println!("    耗时: {:.2} ms, 平均: {:.2} µs/条",
                 duration.as_millis(),
                 duration_micros / batch_size as f64);

        // 验证数据
        if iteration == 0 {
            let test_key = format!("batch_key_0");
            assert_eq!(tree.get(test_key.as_bytes())?, Some("batch_value_0".into()));
            println!("    ✅ 数据验证通过");
        }
    }

    let avg_time = times.iter().sum::<f64>() / times.len() as f64;
    let min_time = times.iter().fold(f64::MAX, |a, &b| a.min(b));
    let max_time = times.iter().fold(f64::MIN, |a, &b| a.max(b));

    println!("  📈 统计结果:");
    println!("     平均耗时: {:.2} ms ({:.2} µs/条)", avg_time / 1000.0, avg_time / batch_size as f64);
    println!("     最快耗时: {:.2} ms ({:.2} µs/条)", min_time / 1000.0, min_time / batch_size as f64);
    println!("     最慢耗时: {:.2} ms ({:.2} µs/条)", max_time / 1000.0, max_time / batch_size as f64);

    Ok(())
}

fn test_repeated_batch_insert(
    tree: &Tree,
    batch_size: usize,
    iterations: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📊 重复批量插入测试 - {}条 × {}次", batch_size, iterations);

    let start_total = Instant::now();
    let mut total_ops = 0;

    for i in 0..iterations {
        let start = Instant::now();

        for j in 0..batch_size {
            let key = format!("repeat_key_{}_{}", i, j);
            let value = format!("repeat_value_{}_{}", i, j);
            tree.insert(key.as_bytes(), value.as_bytes())?;
        }

        let duration = start.elapsed();
        total_ops += batch_size;

        if i % 10 == 0 {
            println!("  第{}次: {:.2} ms, {:.2} µs/条",
                     i + 1,
                     duration.as_millis(),
                     duration.as_micros() as f64 / batch_size as f64);
        }
    }

    let total_duration = start_total.elapsed();
    let avg_per_op = total_duration.as_micros() as f64 / total_ops as f64;

    println!("  📈 总体统计:");
    println!("     总操作数: {}", total_ops);
    println!("     总耗时: {:.2} 秒", total_duration.as_secs_f64());
    println!("     平均每条: {:.2} µs", avg_per_op);
    println!("     吞吐量: {:.0} ops/sec", total_ops as f64 / total_duration.as_secs_f64());

    // 验证最后一批数据
    let last_key = format!("repeat_key_{}_{}", iterations - 1, batch_size - 1);
    assert_eq!(tree.get(last_key.as_bytes())?,
               Some(format!("repeat_value_{}_{}", iterations - 1, batch_size - 1).into()));
    println!("  ✅ 数据验证通过");

    Ok(())
}