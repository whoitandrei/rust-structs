use atomic_wait::{wait, wake_one};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct Semaphore {
    permits: AtomicU32,
}

impl Semaphore {
    pub const fn new(permits: u32) -> Self {
        Semaphore {
            permits: AtomicU32::new(permits),
        }
    }

    pub fn acquire(&self) {
        loop {
            let cur_state = self.permits.load(Ordering::Relaxed);
            if cur_state == 0 {
                wait(&self.permits, 0);
                continue;
            }
            match self.permits.compare_exchange(
                cur_state,
                cur_state - 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }

    pub fn try_acquire(&self) -> bool {
        let mut cur = self.permits.load(Ordering::Relaxed);
        loop {
            if cur == 0 {
                return false;
            }
            match self.permits.compare_exchange_weak(
                cur,
                cur - 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn release(&self) {
        let mut cur = self.permits.load(Ordering::Relaxed);
        loop {
            let next = cur.checked_add(1).expect("semaphore permits overflow");
            match self.permits.compare_exchange_weak(
                cur,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        wake_one(&self.permits);
    }

    pub fn available(&self) -> u32 {
        self.permits.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    fn run_with_timeout<F: FnOnce() + Send + 'static>(secs: u64, f: F) {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            f();
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(secs))
            .expect("timeout: maybe deadlock / lost wakeup");
    }

    #[test]
    fn try_acquire_exactly_n_times() {
        let s = Semaphore::new(3);
        assert!(s.try_acquire());
        assert!(s.try_acquire());
        assert!(s.try_acquire());
        assert!(!s.try_acquire());
        s.release();
        assert!(s.try_acquire());
    }

    #[test]
    fn release_before_acquire_is_remembered() {
        run_with_timeout(2, || {
            let s = Semaphore::new(0);
            s.release();
            s.acquire();
        });
    }

    #[test]
    fn limits_concurrency() {
        let limit = 3;
        let sem = Semaphore::new(limit);
        let inside = AtomicUsize::new(0);
        let max_inside = AtomicUsize::new(0);

        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|| {
                    for _ in 0..20 {
                        sem.acquire();
                        let now = inside.fetch_add(1, Ordering::SeqCst) + 1;
                        max_inside.fetch_max(now, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(2));
                        inside.fetch_sub(1, Ordering::SeqCst);
                        sem.release();
                    }
                });
            }
        });

        let max = max_inside.load(Ordering::SeqCst);
        assert!(max <= limit as usize, "inside at once: {max}");
        assert!(max >= 2, "nothing to check, no parallel");
    }

    #[test]
    fn signal_wakes_waiter() {
        run_with_timeout(3, || {
            let s = Arc::new(Semaphore::new(0));
            let s2 = Arc::clone(&s);
            let h = thread::spawn(move || s2.acquire());

            thread::sleep(Duration::from_millis(100));
            s.release();
            h.join().unwrap();
        });
    }

    #[test]
    fn all_waiters_wake_up() {
        run_with_timeout(5, || {
            let s = Arc::new(Semaphore::new(0));
            let hs: Vec<_> = (0..8)
                .map(|_| {
                    let s = Arc::clone(&s);
                    thread::spawn(move || s.acquire())
                })
                .collect();

            thread::sleep(Duration::from_millis(200));
            for _ in 0..8 {
                s.release();
            }
            for h in hs {
                h.join().unwrap();
            }
            assert!(!s.try_acquire());
        });
    }

    #[test]
    fn binary_semaphore_is_mutual_exclusion() {
        let sem = Semaphore::new(1);
        let counter = AtomicUsize::new(0);

        thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for _ in 0..10_000 {
                        sem.acquire();
                        let v = counter.load(Ordering::Relaxed);
                        counter.store(v + 1, Ordering::Relaxed);
                        sem.release();
                    }
                });
            }
        });

        assert_eq!(counter.load(Ordering::Relaxed), 80_000);
    }

    #[test]
    fn permits_are_conserved_under_stress() {
        run_with_timeout(30, || {
            let n = 4;
            let s = Arc::new(Semaphore::new(n));
            let hs: Vec<_> = (0..8)
                .map(|_| {
                    let s = Arc::clone(&s);
                    thread::spawn(move || {
                        for _ in 0..20_000 {
                            s.acquire();
                            s.release();
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            let mut got = 0;
            while s.try_acquire() {
                got += 1;
            }
            assert_eq!(got, n);
        });
    }

    #[test]
    fn available_reflects_permits() {
        let s = Semaphore::new(2);
        assert_eq!(s.available(), 2);
        assert!(s.try_acquire());
        assert_eq!(s.available(), 1);
        s.acquire();
        assert_eq!(s.available(), 0);
        s.release();
        assert_eq!(s.available(), 1);
    }

    #[test]
    fn available_is_stable_after_threads_finish() {
        let s = Semaphore::new(4);
        thread::scope(|sc| {
            for _ in 0..8 {
                sc.spawn(|| {
                    for _ in 0..5_000 {
                        s.acquire();
                        s.release();
                    }
                });
            }
        });
        assert_eq!(s.available(), 4);
    }

    #[test]
    #[should_panic(expected = "overflow")]
    fn release_overflow_panics() {
        let s = Semaphore::new(u32::MAX);
        s.release();
    }

    #[test]
    fn release_at_max_minus_one_ok() {
        let s = Semaphore::new(u32::MAX - 1);
        s.release();
        assert_eq!(s.available(), u32::MAX);
    }
}
