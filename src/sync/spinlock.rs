use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread::yield_now,
};

pub struct MySpinlock {
    locked: AtomicBool,
    // TODO: add data in SpinLock to guarantee resources
    // data: UnsafeCell<T>,
}

impl MySpinlock {
    pub const fn get_entity() -> Self {
        MySpinlock {
            locked: AtomicBool::new(false),
        }
    }

    pub fn lock(&self) {
        while self.locked.swap(true, Ordering::Acquire) {
            yield_now();
        }
    }

    pub fn try_lock(&self) -> bool {
        !self.locked.swap(true, Ordering::Acquire)
    }

    pub fn unlock(&self) {
        if !self.locked.swap(false, Ordering::Release) {
            println!("error! unlock without lock");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    #[test]
    fn try_lock_semantics() {
        let l = MySpinlock::get_entity();

        assert!(l.try_lock());
        assert!(!l.try_lock());
        l.unlock();
        assert!(l.try_lock());
        l.unlock();
    }

    #[test]
    fn mutual_exclusion() {
        let lock = MySpinlock::get_entity();
        let counter = AtomicUsize::new(0);
        let threads = 8;
        let iters = 10_000;

        thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    for _ in 0..iters {
                        lock.lock();
                        let v = counter.load(Ordering::Relaxed);
                        counter.store(v + 1, Ordering::Relaxed);
                        lock.unlock();
                    }
                });
            }
        });

        assert_eq!(counter.load(Ordering::Relaxed), threads * iters);
    }
}