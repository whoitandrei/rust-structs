use atomic_wait::{wait, wake_all, wake_one};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU32, Ordering};

const WRITE_LOCKED: u32 = u32::MAX;

pub struct MyRwLock<T> {
    /// 0 - free, n - n readres, u32::MAX - writer
    state: AtomicU32,
    data: UnsafeCell<T>,
}

impl<T> MyRwLock<T> {
    pub fn new(value: T) -> Self {
        MyRwLock {
            state: AtomicU32::new(0),
            data: UnsafeCell::new(value),
        }
    }

    pub fn read(&self) -> ReadGuard<'_, T> {
        loop {
            let cur_state = self.state.load(Ordering::Relaxed);
            if cur_state == WRITE_LOCKED {
                wait(&self.state, WRITE_LOCKED);
                continue;
            }
            assert!(cur_state < WRITE_LOCKED - 1);
            if self
                .state
                .compare_exchange(
                    cur_state,
                    cur_state + 1,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return ReadGuard { lock: self };
            }
        }
    }

    pub fn write(&self) -> WriteGuard<'_, T> {
        loop {
            match self
                .state
                .compare_exchange(0, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
            {
                Ok(_) => return WriteGuard { lock: self },
                Err(actual) => wait(&self.state, actual),
            }
        }
    }

    pub fn try_read(&self) -> Option<ReadGuard<'_, T>> {
        let mut cur = self.state.load(Ordering::Relaxed);
        loop {
            if cur == WRITE_LOCKED {
                return None;
            }
            assert!(cur < WRITE_LOCKED - 1);
            match self.state.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(ReadGuard { lock: self }),
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn try_write(&self) -> Option<WriteGuard<'_, T>> {
        match self
            .state
            .compare_exchange(0, WRITE_LOCKED, Ordering::Acquire, Ordering::Relaxed)
        {
            Ok(_) => Some(WriteGuard { lock: self }),
            Err(_) => None,
        }
    }
}

unsafe impl<T: Send + Sync> Sync for MyRwLock<T> {}
unsafe impl<T: Sync> Sync for ReadGuard<'_, T> {}
unsafe impl<T: Sync> Sync for WriteGuard<'_, T> {}

pub struct ReadGuard<'a, T> {
    lock: &'a MyRwLock<T>,
}
pub struct WriteGuard<'a, T> {
    lock: &'a MyRwLock<T>,
}

// ReadGuard: Deref, Drop
impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        if self.lock.state.fetch_sub(1, Ordering::Release) == 1 {
            wake_one(&self.lock.state);
        }
    }
}

// WriteGuard: Deref, DerefMut, Drop
impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.state.store(0, Ordering::Release);
        wake_all(&self.lock.state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier, mpsc};
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
    fn basic_read_write() {
        let l = MyRwLock::new(10);
        {
            let mut w = l.write();
            *w += 5;
        }
        assert_eq!(*l.read(), 15);
    }

    #[test]
    fn readers_run_concurrently() {
        run_with_timeout(5, || {
            let n = 4;
            let l = Arc::new(MyRwLock::new(0));
            let b = Arc::new(Barrier::new(n));
            let hs: Vec<_> = (0..n)
                .map(|_| {
                    let (l, b) = (Arc::clone(&l), Arc::clone(&b));
                    thread::spawn(move || {
                        let _g = l.read();
                        b.wait();
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
        });
    }

    #[test]
    fn writer_is_exclusive() {
        let l = MyRwLock::new(0);
        let w = l.write();
        assert!(l.try_read().is_none());
        assert!(l.try_write().is_none());
        drop(w);
        assert!(l.try_read().is_some());
    }

    #[test]
    fn reader_blocks_writer_but_not_readers() {
        let l = MyRwLock::new(0);
        let r1 = l.read();
        assert!(l.try_write().is_none());
        let r2 = l.try_read();
        assert!(r2.is_some());
        drop(r1);
        assert!(l.try_write().is_none());
        drop(r2);
        assert!(l.try_write().is_some());
    }

    #[test]
    fn writers_mutual_exclusion() {
        let l = MyRwLock::new(0usize);
        thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for _ in 0..10_000 {
                        let mut g = l.write();
                        let v = *g;
                        *g = v + 1;
                    }
                });
            }
        });
        assert_eq!(*l.read(), 80_000);
    }

    #[test]
    fn waiting_writer_wakes_after_last_reader() {
        run_with_timeout(5, || {
            let l = Arc::new(MyRwLock::new(0));
            let done = Arc::new(AtomicBool::new(false));
            let r = l.read();

            let (l2, d2) = (Arc::clone(&l), Arc::clone(&done));
            let h = thread::spawn(move || {
                *l2.write() += 1;
                d2.store(true, Ordering::SeqCst);
            });

            thread::sleep(Duration::from_millis(200));
            assert!(!done.load(Ordering::SeqCst), "writer gone thru reader");
            drop(r);
            h.join().unwrap();
            assert_eq!(*l.read(), 1);
        });
    }

    #[test]
    fn waiting_readers_wake_after_writer() {
        run_with_timeout(5, || {
            let l = Arc::new(MyRwLock::new(0));
            let mut w = l.write();

            let hs: Vec<_> = (0..4)
                .map(|_| {
                    let l = Arc::clone(&l);
                    thread::spawn(move || *l.read())
                })
                .collect();

            thread::sleep(Duration::from_millis(200));
            *w = 7;
            drop(w);

            for h in hs {
                assert_eq!(h.join().unwrap(), 7);
            }
        });
    }

    #[test]
    fn lock_released_after_writer_panic() {
        let l = Arc::new(MyRwLock::new(0));
        let l2 = Arc::clone(&l);
        let res = thread::spawn(move || {
            let mut g = l2.write();
            *g = 42;
            panic!("boom");
        })
        .join();
        assert!(res.is_err());
        run_with_timeout(2, move || assert_eq!(*l.read(), 42));
    }

    #[test]
    fn mixed_stress() {
        run_with_timeout(30, || {
            let l = Arc::new(MyRwLock::new(0usize));
            let mut hs = Vec::new();
            for _ in 0..4 {
                let l = Arc::clone(&l);
                hs.push(thread::spawn(move || {
                    for _ in 0..5_000 {
                        *l.write() += 1;
                    }
                }));
            }
            for _ in 0..4 {
                let l = Arc::clone(&l);
                hs.push(thread::spawn(move || {
                    for _ in 0..20_000 {
                        let _ = *l.read();
                    }
                }));
            }
            for h in hs {
                h.join().unwrap();
            }
            assert_eq!(*l.read(), 20_000);
        });
    }

    #[test]
    fn sync_bounds() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<MyRwLock<i32>>();
    }
}
