use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

struct Inner<T> {
    deque: VecDeque<T>,
    stopped: bool,
}

pub struct BoundedQueue<T> {
    state: Mutex<Inner<T>>,
    capacity: usize,
    not_empty: Condvar,
    not_full: Condvar,
}

impl<T> BoundedQueue<T> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            state: Mutex::new(Inner {
                deque: VecDeque::<T>::new(),
                stopped: false,
            }),
            capacity: cap,
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        }
    }

    pub fn push(&self, item: T) {
        let mut guard = self
            .not_full
            .wait_while(self.state.lock().unwrap(), |inner| {
                inner.deque.len() >= self.capacity
            })
            .unwrap();
        guard.deque.push_back(item);
        drop(guard);
        self.not_empty.notify_one();
    }

    pub fn pop(&self) -> Option<T> {
        let mut guard = self
            .not_empty
            .wait_while(self.state.lock().unwrap(), |inner| {
                inner.deque.len() == 0 && !inner.stopped
            })
            .unwrap();

        if guard.deque.is_empty() && guard.stopped {
            return None;
        }

        let val = guard.deque.pop_front();
        drop(guard);
        self.not_full.notify_one();

        val
    }

    pub fn stop(&self) {
        {
            let mut guard = self.state.lock().unwrap();
            guard.stopped = true;
            drop(guard);
        }

        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    pub fn size(&self) -> usize {
        let guard = self.state.lock().unwrap();
        guard.deque.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::time::Duration;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn all_tasks_are_consumed() {
        let queue = Arc::new(BoundedQueue::<i32>::with_capacity(10));

        let producers_count = 4;
        let consumers_count = 5;
        let tasks_per_producer = 100;

        let consumed = Arc::new(Mutex::new(Vec::new()));

        let mut producers = Vec::new();
        let mut consumers = Vec::new();

        for producer_id in 0..producers_count {
            let queue = Arc::clone(&queue);

            producers.push(thread::spawn(move || {
                for task_id in 0..tasks_per_producer {
                    let value = producer_id * tasks_per_producer + task_id;
                    queue.push(value);
                }
            }));
        }

        for _ in 0..consumers_count {
            let queue = Arc::clone(&queue);
            let consumed = Arc::clone(&consumed);

            consumers.push(thread::spawn(move || {
                while let Some(value) = queue.pop() {
                    consumed.lock().unwrap().push(value);
                }
            }));
        }

        for producer in producers {
            producer.join().unwrap();
        }

        queue.stop();

        for consumer in consumers {
            consumer.join().unwrap();
        }

        let values = consumed.lock().unwrap();

        let expected_count = producers_count * tasks_per_producer;

        assert_eq!(values.len(), expected_count as usize);

        let unique_values: HashSet<i32> = values.iter().copied().collect();

        assert_eq!(unique_values.len(), expected_count as usize);

        for expected_value in 0..expected_count {
            assert!(unique_values.contains(&expected_value));
        }
    }

    #[test]
    fn consumers_finish_after_stop_without_deadlock() {
        let queue = Arc::new(BoundedQueue::<i32>::with_capacity(10));

        let consumers_count = 5;
        let mut consumers = Vec::new();

        for _ in 0..consumers_count {
            let queue = Arc::clone(&queue);

            consumers.push(thread::spawn(move || while queue.pop().is_some() {}));
        }

        let (finished_tx, finished_rx) = mpsc::channel();

        thread::spawn(move || {
            for consumer in consumers {
                consumer.join().unwrap();
            }

            finished_tx.send(()).unwrap();
        });

        queue.stop();

        finished_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("consumers not finished: maybe deadlock");
    }
}
