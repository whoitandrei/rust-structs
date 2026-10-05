use std::sync::{Condvar, Mutex};

struct State {
    arrived: u32,
    generation: u64,
}

pub struct Barrier {
    n: u32,
    state: Mutex<State>,
    cv: Condvar,
}

impl Barrier {
    pub fn new(n: u32) -> Self {
        assert!(n > 0);
        Barrier {
            n,
            state: Mutex::new(State {
                arrived: 0,
                generation: 0,
            }),
            cv: Condvar::new(),
        }
    }

    pub fn wait(&self) -> bool {
        let mut st = self.state.lock().unwrap();
        let my_gen = st.generation;
        st.arrived += 1;

        if st.arrived == self.n {
            st.arrived = 0;
            st.generation = my_gen + 1;
            drop(st);
            self.cv.notify_all();
            true
        } else {
            let _st = self.cv.wait_while(st, |s| s.generation == my_gen).unwrap();
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::Duration;

    fn run_with_timeout<F: FnOnce() + Send + 'static>(secs: u64, f: F) {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            f();
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(secs))
            .expect("timeout: maybe deadlock");
    }

    #[test]
    fn single_thread_barrier_does_not_block() {
        run_with_timeout(2, || {
            let b = Barrier::new(1);
            assert!(b.wait());
            assert!(b.wait());
        });
    }

    #[test]
    #[should_panic]
    fn zero_threads_panics() {
        let _ = Barrier::new(0);
    }

    #[test]
    fn exactly_one_leader() {
        run_with_timeout(5, || {
            let n = 8;
            let b = Arc::new(Barrier::new(n));
            let leaders = Arc::new(AtomicUsize::new(0));
            let hs: Vec<_> = (0..n)
                .map(|_| {
                    let (b, leaders) = (Arc::clone(&b), Arc::clone(&leaders));
                    thread::spawn(move || {
                        if b.wait() {
                            leaders.fetch_add(1, Ordering::SeqCst);
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            assert_eq!(leaders.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn nobody_passes_early() {
        run_with_timeout(5, || {
            let n = 8;
            let b = Arc::new(Barrier::new(n));
            let arrived = Arc::new(AtomicUsize::new(0));
            let hs: Vec<_> = (0..n)
                .map(|i| {
                    let (b, arrived) = (Arc::clone(&b), Arc::clone(&arrived));
                    thread::spawn(move || {
                        thread::sleep(Duration::from_millis(10 * i as u64));
                        arrived.fetch_add(1, Ordering::SeqCst);
                        b.wait();
                        assert_eq!(arrived.load(Ordering::SeqCst), n as usize);
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
        });
    }

    #[test]
    fn reusable_many_rounds() {
        run_with_timeout(30, || {
            let n = 4u32;
            let rounds = 1000;
            let b = Arc::new(Barrier::new(n));
            let counter = Arc::new(AtomicUsize::new(0));
            let hs: Vec<_> = (0..n)
                .map(|_| {
                    let (b, counter) = (Arc::clone(&b), Arc::clone(&counter));
                    thread::spawn(move || {
                        for round in 1..=rounds {
                            counter.fetch_add(1, Ordering::SeqCst);
                            b.wait();
                            assert!(counter.load(Ordering::SeqCst) >= round * n as usize);
                            b.wait();
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            assert_eq!(counter.load(Ordering::SeqCst), rounds * n as usize);
        });
    }

    #[test]
    fn one_leader_per_round() {
        run_with_timeout(30, || {
            let n = 4u32;
            let rounds = 500;
            let b = Arc::new(Barrier::new(n));
            let leaders = Arc::new(AtomicUsize::new(0));
            let hs: Vec<_> = (0..n)
                .map(|_| {
                    let (b, leaders) = (Arc::clone(&b), Arc::clone(&leaders));
                    thread::spawn(move || {
                        for _ in 0..rounds {
                            if b.wait() {
                                leaders.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            assert_eq!(leaders.load(Ordering::SeqCst), rounds);
        });
    }
}