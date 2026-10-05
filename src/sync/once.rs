use atomic_wait::{wait, wake_all};
use std::sync::atomic::{AtomicU32, Ordering};

const INCOMPLETE: u32 = 0;
const RUNNING: u32 = 1;
const COMPLETE: u32 = 2;

pub struct Once {
    state: AtomicU32,
}

impl Once {
    pub const fn new() -> Self {
        Once {
            state: AtomicU32::new(INCOMPLETE),
        }
    }

    pub fn is_completed(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMPLETE
    }

    // BUG
    // TODO
    // if f() panic waiters fails into deadlock
    pub fn call_once<F: FnOnce()>(&self, f: F) {
        if self.state.load(Ordering::Acquire) == COMPLETE {
            return;
        }
        loop {
            match self.state.compare_exchange(
                INCOMPLETE,
                RUNNING,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    f();
                    self.state.store(COMPLETE, Ordering::Release);
                    wake_all(&self.state);
                    return;
                }
                Err(COMPLETE) => return,
                Err(RUNNING) => wait(&self.state, RUNNING),
                Err(_) => unreachable!(),
            }
        }
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
    fn runs_exactly_once_sequential() {
        let once = Once::new();
        let mut count = 0;
        assert!(!once.is_completed());
        once.call_once(|| count += 1);
        once.call_once(|| count += 1);
        once.call_once(|| count += 1);
        assert_eq!(count, 1);
        assert!(once.is_completed());
    }

    #[test]
    fn runs_exactly_once_many_threads() {
        run_with_timeout(10, || {
            let once = Arc::new(Once::new());
            let counter = Arc::new(AtomicUsize::new(0));
            let hs: Vec<_> = (0..16)
                .map(|_| {
                    let (once, counter) = (Arc::clone(&once), Arc::clone(&counter));
                    thread::spawn(move || {
                        once.call_once(|| {
                            counter.fetch_add(1, Ordering::SeqCst);
                        });
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            assert_eq!(counter.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn waiters_see_initialized_data() {
        run_with_timeout(10, || {
            let once = Arc::new(Once::new());
            let data = Arc::new(AtomicUsize::new(0));
            let hs: Vec<_> = (0..8)
                .map(|_| {
                    let (once, data) = (Arc::clone(&once), Arc::clone(&data));
                    thread::spawn(move || {
                        once.call_once(|| {
                            thread::sleep(Duration::from_millis(200));
                            data.store(42, Ordering::Relaxed);
                        });
                        assert_eq!(data.load(Ordering::Relaxed), 42);
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
        });
    }

    #[test]
    fn waiters_wake_up() {
        run_with_timeout(5, || {
            let once = Arc::new(Once::new());
            let started = Arc::new(AtomicUsize::new(0));

            let o = Arc::clone(&once);
            let first = thread::spawn(move || {
                o.call_once(|| thread::sleep(Duration::from_millis(300)));
            });
            thread::sleep(Duration::from_millis(50));

            let hs: Vec<_> = (0..4)
                .map(|_| {
                    let (once, started) = (Arc::clone(&once), Arc::clone(&started));
                    thread::spawn(move || {
                        started.fetch_add(1, Ordering::SeqCst);
                        once.call_once(|| panic!("f must do not running twice"));
                    })
                })
                .collect();

            first.join().unwrap();
            for h in hs {
                h.join().unwrap();
            }
            assert!(once.is_completed());
        });
    }

    #[test]
    fn fast_path_does_not_call_f() {
        let once = Once::new();
        let counter = AtomicUsize::new(0);
        for _ in 0..1000 {
            once.call_once(|| {
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn works_in_static() {
        static ONCE: Once = Once::new();
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        for _ in 0..3 {
            ONCE.call_once(|| {
                COUNTER.fetch_add(1, Ordering::SeqCst);
            });
        }
        assert_eq!(COUNTER.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn many_fresh_onces_stress() {
        run_with_timeout(30, || {
            for _ in 0..500 {
                let once = Arc::new(Once::new());
                let counter = Arc::new(AtomicUsize::new(0));
                let hs: Vec<_> = (0..8)
                    .map(|_| {
                        let (once, counter) = (Arc::clone(&once), Arc::clone(&counter));
                        thread::spawn(move || {
                            once.call_once(|| {
                                counter.fetch_add(1, Ordering::Relaxed);
                            });
                            assert_eq!(counter.load(Ordering::Relaxed), 1);
                        })
                    })
                    .collect();
                for h in hs {
                    h.join().unwrap();
                }
            }
        });
    }
}
