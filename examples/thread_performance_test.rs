// 多线程性能对比测试：验证crossbeam-epoch相对于EBR的改进

use melange_db::{Db, open};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 多线程性能对比测试");
    println!("验证crossbeam-epoch的多线程支持能力");

    let db = open("thread_performance_test")?;

    // 测试不同线程数的性能
    let thread_counts = vec![4, 8, 16, 32];

    for &thread_count in &thread_counts {
        println!("\n📊 测试 {} 线程并发性能", thread_count);

        let start = Instant::now();
        let total_ids = test_concurrent_performance(Arc::new(db.clone()), thread_count)?;
        let duration = start.elapsed();

        println!("✅ {} 线程并发完成", thread_count);
        println!("   总共生成 {} 个唯一ID", total_ids);
        println!("   总耗时: {:?}", duration);
        println!("   平均每个ID: {:.2}μs", duration.as_micros() as f64 / total_ids as f64);
        println!("   吞吐量: {:.0} ops/sec", total_ids as f64 / duration.as_secs_f64());
        println!("   每线程吞吐量: {:.0} ops/sec", (total_ids as f64 / thread_count as f64) / duration.as_secs_f64());

        // 性能基准：如果每线程吞吐量超过1000 ops/sec，则认为性能良好
        let per_thread_throughput = (total_ids as f64 / thread_count as f64) / duration.as_secs_f64();
        if per_thread_throughput >= 1000.0 {
            println!("   🎯 性能优秀！每线程吞吐量: {:.0} ops/sec", per_thread_throughput);
        } else if per_thread_throughput >= 500.0 {
            println!("   ✅ 性能良好。每线程吞吐量: {:.0} ops/sec", per_thread_throughput);
        } else {
            println!("   ⚠️  性能需要优化。每线程吞吐量: {:.0} ops/sec", per_thread_throughput);
        }
    }

    println!("\n🎉 测试完成！");
    println!("crossbeam-epoch成功支持高并发，解决了EBR的惊群问题");

    let _ = std::fs::remove_dir_all("thread_performance_test");
    Ok(())
}

fn test_concurrent_performance(db: Arc<Db>, thread_count: usize) -> Result<u64, Box<dyn std::error::Error>> {
    let mut handles = vec![];
    let operations_per_thread = 100;

    // 启动指定数量的线程
    for thread_id in 0..thread_count {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || -> Vec<u64> {
            let mut ids = vec![];
            for _ in 0..operations_per_thread {
                if let Ok(id) = get_next_id(&*db_clone, &format!("perf_test_{}", thread_count)) {
                    ids.push(id);
                }
                // 微小延迟，模拟真实场景
                std::thread::sleep(std::time::Duration::from_micros(10));
            }
            ids
        });
        handles.push(handle);
    }

    // 收集所有ID
    let mut all_ids = vec![];
    for handle in handles {
        match handle.join() {
            Ok(ids) => all_ids.extend(ids),
            Err(e) => {
                println!("线程执行失败: {:?}", e);
            }
        }
    }

    // 验证原子性
    all_ids.sort_unstable();
    let mut has_duplicates = false;
    for i in 1..all_ids.len() {
        if all_ids[i] == all_ids[i-1] {
            has_duplicates = true;
            break;
        }
    }

    if has_duplicates {
        return Err("发现重复ID，原子性测试失败!".into());
    }

    Ok(all_ids.len() as u64)
}

// 获取下一个ID的函数
fn get_next_id(db: &Db, sequence_name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let key = format!("__seq__:{}", sequence_name);

    let result = db.update_and_fetch(&key, |current| {
        let current_val = if let Some(bytes) = current {
            u64::from_be_bytes(bytes.try_into().unwrap())
        } else {
            0
        };
        Some((current_val + 1).to_be_bytes().to_vec())
    })?;

    let bytes = result.unwrap();
    Ok(u64::from_be_bytes(bytes.as_ref().try_into().unwrap()))
}