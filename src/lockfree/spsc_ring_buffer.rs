use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[repr(align(128))]
struct CachePadded<T>(T);

struct Inner<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

// SAFETY: T write in producer and read in concumer - here is Send trait
// Sync - buffer in UnsafeCell but one slot have one владелец прикиньте forget как это на eng будет
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

pub struct Producer<T> {
    inner: Arc<Inner<T>>,
}
pub struct Consumer<T> {
    inner: Arc<Inner<T>>,
}

pub fn channel<T>(cap: usize) -> (Producer<T>, Consumer<T>) {
    if cap == 0 {
        panic!("cannot init channel with capacity 0")
    }

    let cap_pow_of_two = cap.checked_next_power_of_two().expect("cap is too large");
    let buf: Box<[UnsafeCell<MaybeUninit<T>>]> = (0..cap_pow_of_two)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect::<Vec<_>>()
        .into_boxed_slice();

    let inner = Arc::new(Inner {
        buf: buf,
        mask: cap_pow_of_two - 1,
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
    });

    (
        Producer {
            inner: Arc::clone(&inner),
        },
        Consumer { inner: inner },
    )
}

impl<T> Producer<T> {
    pub fn push(&mut self, value: T) -> Result<(), T> {
        let inner = &*self.inner;
        let tail = inner.tail.0.load(Ordering::Relaxed);
        let head = inner.head.0.load(Ordering::Acquire);

        if tail.wrapping_sub(head) == inner.mask + 1 {
            return Err(value);
        }

        // SAFETY: cell belongs only for producer ecause of last check
        unsafe {
            (*inner.buf[tail & inner.mask].get()).write(value);
        }

        inner.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }
}

impl<T> Consumer<T> {
    pub fn pop(&mut self) -> Option<T> {
        let inner = &*self.inner;
        let head = inner.head.0.load(Ordering::Relaxed);
        let tail = inner.tail.0.load(Ordering::Acquire);

        if head == tail {
            return None;
        }

        // SAFETY: cell belongs only for consumer because of last check
        // producer have wrote cell and go away
        let val = unsafe { (*inner.buf[head & inner.mask].get()).assume_init_read() };

        inner.head.0.store(head.wrapping_add(1), Ordering::Release);
        Some(val)
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();

        let mut i = head;
        while i != tail {
            unsafe {
                (*self.buf[i & self.mask].get()).assume_init_drop();
            }
            i = i.wrapping_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    const N: usize = if cfg!(miri) { 200 } else { 100_000 };

    #[test]
    fn basic_full_empty_wraparound() {
        let (mut p, mut c) = channel::<String>(3);
        assert_eq!(c.pop(), None);
        for i in 0..4 {
            p.push(i.to_string()).unwrap();
        }
        assert_eq!(p.push("x".into()), Err("x".to_string()));

        for i in 4..1000 {
            assert_eq!(c.pop(), Some((i - 4).to_string()));
            p.push(i.to_string()).unwrap();
        }
    }

    #[test]
    fn fifo_two_threads() {
        let (mut p, mut c) = channel::<String>(4);

        let producer = thread::spawn(move || {
            for i in 0..N {
                let mut v = i.to_string();
                while let Err(back) = p.push(v) {
                    v = back;
                    thread::yield_now();
                }
            }
        });

        let consumer = thread::spawn(move || {
            for i in 0..N {
                let got = loop {
                    if let Some(v) = c.pop() {
                        break v;
                    }
                    thread::yield_now();
                };
                assert_eq!(got, i.to_string(), "broked FIFO landing");
            }
            assert_eq!(c.pop(), None);
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }
}
