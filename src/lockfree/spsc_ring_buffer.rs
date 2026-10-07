use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

#[repr(align(128))]
struct CachePadded<T>(T);

struct Inner<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

// SAFETY: todo
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
        todo!()
    }
}

impl<T> Consumer<T> {
    pub fn pop(&mut self) -> Option<T> {
        todo!()
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        todo!()
    }
}
