use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
    thread::yield_now,
};

pub struct MySpinLock<T> {
    locked: AtomicBool,
    data: UnsafeCell<T>,
}

// SAFETY: &Guard gives only &T -> safe only when T: Sync
unsafe impl<T: Sync> Sync for SpinLockGuard<'_, T> {}

// SAFETY: &Guard gives only &T
unsafe impl<T: Send> Sync for MySpinLock<T> {}

impl<T> MySpinLock<T> {
    pub const fn new(value: T) -> Self {
        MySpinLock {
            locked: AtomicBool::new(false),
            data: UnsafeCell::new(value),
        }
    }

    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        while self.locked.swap(true, Ordering::Acquire) {
            yield_now();
        }
        SpinLockGuard { lock: self }
    }

    pub fn try_lock(&self) -> Option<SpinLockGuard<'_, T>> {
        if !self.locked.swap(true, Ordering::Acquire) {
            return Option::Some(SpinLockGuard { lock: self });
        }
        None
    }
}

pub struct SpinLockGuard<'a, T> {
    lock: &'a MySpinLock<T>,
}

impl<T> Deref for SpinLockGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: locked == true while guard lives
        // and we can guarantee that we have this resource
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for SpinLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: locked == true while guard lives
        // and we can guarantee that we have this resource
        // so there are other &mut
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for SpinLockGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn deref_and_deref_mut() {
        let lock = MySpinLock::new(10);
        {
            let mut g = lock.lock();
            *g += 5;
        }
        let g = lock.lock();
        assert_eq!(*g, 15);
    }

    #[test]
    fn auto_deref_methods() {
        let lock = MySpinLock::new(Vec::new());
        {
            let mut g = lock.lock();
            g.push(1);
            g.push(2);
        }
        assert_eq!(lock.lock().len(), 2);
    }

    #[test]
    fn mutual_exclusion() {
        let lock = MySpinLock::new(0usize);
        let threads = 8;
        let iters = 10_000;

        thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    for _ in 0..iters {
                        let mut g = lock.lock();
                        let v = *g;
                        *g = v + 1;
                    }
                });
            }
        });

        assert_eq!(*lock.lock(), threads * iters);
    }

    #[test]
    fn lock_released_after_panic() {
        let lock = Arc::new(MySpinLock::new(0));
        let l2 = Arc::clone(&lock);

        let result = thread::spawn(move || {
            let mut g = l2.lock();
            *g = 42;
            panic!("boom");
        })
        .join();

        assert!(result.is_err());
        assert_eq!(*lock.lock(), 42);
    }

    #[test]
    fn data_visible_across_threads() {
        let lock = Arc::new(MySpinLock::new(Vec::new()));
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let lock = Arc::clone(&lock);
                thread::spawn(move || lock.lock().push(i))
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let mut v = lock.lock().clone();
        v.sort();
        assert_eq!(v, vec![0, 1, 2, 3]);
    }

    #[test]
    fn is_sync_for_send_types() {
        fn assert_sync<T: Sync>() {}
        assert_sync::<MySpinLock<i32>>();
        assert_sync::<MySpinLock<std::cell::Cell<i32>>>();
    }

    #[test]
    fn try_lock_semantics() {
        let lock = MySpinLock::new(0);
        let g = lock.try_lock();
        assert!(g.is_some());
        assert!(lock.try_lock().is_none());
        drop(g);
        assert!(lock.try_lock().is_some());
    }
}
