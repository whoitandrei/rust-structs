// needs atomic-wait to cross-platform

use atomic_wait::{wait, wake_one};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU32, Ordering};

const UNLOCKED: u32 = 0;
const LOCKED: u32 = 1;
const CONTENDED: u32 = 2;

// 'state' conditions:
// 0 - free
// 1 - locked
// 2 - locked, someone waits
pub struct MyMutex<T> {
    state: AtomicU32,
    data: UnsafeCell<T>,
}

impl<T> MyMutex<T> {
    pub const fn new(value: T) -> Self {
        MyMutex {
            state: AtomicU32::new(UNLOCKED),
            data: UnsafeCell::new(value),
        }
    }

    pub fn lock(&self) -> MyMutexGuard<'_, T> {
        if self
            .state
            .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return MyMutexGuard { mutex: self };
        }

        while self.state.swap(CONTENDED, Ordering::Acquire) != UNLOCKED {
            wait(&self.state, CONTENDED);
        }
        MyMutexGuard { mutex: self }
    }

    pub fn try_lock(&self) -> Option<MyMutexGuard<'_, T>> {
        match self
            .state
            .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(_) => Some(MyMutexGuard { mutex: self }),
            Err(_) => None,
        }
    }
}

unsafe impl<T: Send> Sync for MyMutex<T> {}

unsafe impl<T: Sync> Sync for MyMutexGuard<'_, T> {}

pub struct MyMutexGuard<'a, T> {
    mutex: &'a MyMutex<T>,
}

// Deref, DerefMut, Drop
impl<T> Deref for MyMutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: locked while guard lives
        // and we can guarantee that we have this resource
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T> DerefMut for MyMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: locked == true while guard lives
        // and we can guarantee that we have this resource
        // so there are no other &mut
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T> Drop for MyMutexGuard<'_, T> {
    fn drop(&mut self) {
        if self.mutex.state.swap(UNLOCKED, Ordering::Release) == CONTENDED {
            wake_one(&self.mutex.state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::{Duration, Instant};

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
    fn deref_and_deref_mut() {
        let m = MyMutex::new(10);
        {
            let mut g = m.lock();
            *g += 5;
        }
        assert_eq!(*m.lock(), 15);
    }

    #[test]
    fn auto_deref_methods() {
        let m = MyMutex::new(Vec::new());
        {
            let mut g = m.lock();
            g.push(1);
            g.push(2);
        }
        assert_eq!(m.lock().len(), 2);
    }

    #[test]
    fn try_lock_semantics() {
        let m = MyMutex::new(0);
        let g = m.try_lock();
        assert!(g.is_some());
        assert!(m.try_lock().is_none());
        drop(g);
        assert!(m.try_lock().is_some());
    }

    #[test]
    fn mutual_exclusion() {
        let m = MyMutex::new(0usize);
        let threads = 8;
        let iters = 10_000;

        thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    for _ in 0..iters {
                        let mut g = m.lock();
                        let v = *g;
                        *g = v + 1;
                    }
                });
            }
        });

        assert_eq!(*m.lock(), threads * iters);
    }

    #[test]
    fn lock_released_after_panic() {
        let m = Arc::new(MyMutex::new(0));
        let m2 = Arc::clone(&m);

        let res = thread::spawn(move || {
            let mut g = m2.lock();
            *g = 42;
            panic!("boom");
        })
        .join();

        assert!(res.is_err());
        run_with_timeout(2, move || {
            assert_eq!(*m.lock(), 42);
        });
    }

    #[test]
    fn all_waiters_wake_up_in_chain() {
        run_with_timeout(5, || {
            let m = Arc::new(MyMutex::new(0));
            let guard = m.lock();

            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let m = Arc::clone(&m);
                    thread::spawn(move || {
                        *m.lock() += 1;
                    })
                })
                .collect();

            thread::sleep(Duration::from_millis(200));
            drop(guard);

            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(*m.lock(), 8);
        });
    }

    #[test]
    fn stress_short_critical_sections() {
        run_with_timeout(30, || {
            for _ in 0..200 {
                let m = Arc::new(MyMutex::new(0usize));
                let hs: Vec<_> = (0..8)
                    .map(|_| {
                        let m = Arc::clone(&m);
                        thread::spawn(move || {
                            for _ in 0..500 {
                                *m.lock() += 1;
                            }
                        })
                    })
                    .collect();
                for h in hs {
                    h.join().unwrap();
                }
                assert_eq!(*m.lock(), 8 * 500);
            }
        });
    }

    #[test]
    fn waiting_thread_sleeps_not_spins() {
        let m = Arc::new(MyMutex::new(()));
        let guard = m.lock();

        let m2 = Arc::clone(&m);
        let h = thread::spawn(move || {
            let t = Instant::now();
            let _g = m2.lock();
            t.elapsed()
        });

        thread::sleep(Duration::from_millis(500));
        drop(guard);
        let waited = h.join().unwrap();
        assert!(waited >= Duration::from_millis(400));
    }

    #[test]
    fn is_sync_for_send_types() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<MyMutex<i32>>();
        assert_sync::<MyMutex<std::cell::Cell<i32>>>();
    }
}
