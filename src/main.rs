use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use thread_safe_queue::blocking::BoundedQueue;
use std::time::Duration;

fn bounded_queue_present() {
    let q = Arc::new(BoundedQueue::<i32>::with_capacity(10));
    let mut producers: Vec<thread::JoinHandle<()>> = Vec::new();
    let mut consumers: Vec<thread::JoinHandle<()>> = Vec::new();
    let consumed = Arc::new(AtomicUsize::new(0));
    let produced = Arc::new(AtomicUsize::new(0));

    for p in 0..4 {
        let q = Arc::clone(&q);
        let produced = Arc::clone(&produced);
        producers.push(thread::spawn(move || {
            for i in 0..100 {
                q.push(p * 100 + i);
                produced.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(50));
            }
        }));
    }

    for _ in 0..5 {
        let q = Arc::clone(&q);
        let consumed = Arc::clone(&consumed);
        consumers.push(thread::spawn(move || {
            while let Some(_val) = q.pop() {
                consumed.fetch_add(1, Ordering::Relaxed);
                // println!("{}", val);
            }
        }));
    }

    for prod in producers {
        prod.join().unwrap();
    }
    q.stop();
    for cons in consumers {
        cons.join().unwrap();
    }

    assert!(produced.load(Ordering::Relaxed) == consumed.load(Ordering::Relaxed));
    println!(
        "produced: {}; consumed: {}",
        produced.load(Ordering::Relaxed),
        consumed.load(Ordering::Relaxed)
    );
}

pub fn main() {
    bounded_queue_present();
}
