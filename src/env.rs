// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, Builder as ThreadBuilder, JoinHandle, ThreadId};

use crate::grpc_sys;

use crate::cq::{CompletionQueue, CompletionQueueHandle, EventType};
use crate::task::CallTag;

pub use crate::grpc_sys::GrpcEvent as Event;
use std::sync::mpsc;

// poll_queue will create WAIT_THREAD_COUNT_DEFAULT threads in begin.
// If wait thread count < WAIT_THREAD_COUNT_MIN, create number to WAIT_THREAD_COUNT_DEFAULT.
// If wait thread count > WAIT_THREAD_COUNT_MAX, wait thread will quit to WAIT_THREAD_COUNT_DEFAULT.
const DEFAULT_WAIT_THREAD_COUNT_DEFAULT: usize = 30;
const DEFAULT_WAIT_THREAD_COUNT_MIN: usize = 10;
const DEFAULT_WAIT_THREAD_COUNT_MAX: usize = 50;

fn new_thread(oid: ThreadId, cq: CompletionQueue, wtc: Arc<AtomicUsize>, tx: mpsc::Sender<bool>, min: usize, max: usize) {
    thread::spawn(move || {
        let id = thread::current().id();
        loop {
            let mut tag: Option<Box<CallTag>> = None;
            let mut success = false;

            let c = wtc.fetch_add(1, Ordering::SeqCst);
            if c > max {
                debug!("cq {:?}:{:?} quit, wtc is {}", oid, id, c + 1);
                wtc.fetch_sub(1, Ordering::SeqCst);
                break;
            }
            trace!("cq {:?}:{:?} is waiting, wtc is {}", oid, id, c + 1);
            loop {
                let e = cq.next();
                match e.event_type {
                    EventType::QueueShutdown => break,
                    // timeout should not happen in theory.
                    EventType::QueueTimeout => {},
                    EventType::OpComplete => {
                        tag = Some(unsafe { Box::from_raw(e.tag as _) });
                        success = e.success != 0;
                        break},
                }
            }

            if let Some(tag) = tag {
                let tag_str = format!("{:?}", tag);
                let c = wtc.fetch_sub(1, Ordering::SeqCst);
                trace!("cq {:?}:{:?} is working on {}, wtc is {}", oid, id, tag_str, c);
                if c < min {
                    tx.send(false).unwrap();
                }
                tag.resolve(&cq, success);
                trace!("cq {:?}:{:?} is done with {:?}", oid, id, tag_str);
            } else {
                trace!("cq {:?}:{:?} shutdown", oid, id);
                tx.send(true).unwrap();
                break;
            }
        }
    });
}

// event loop
fn poll_queue(cq: Arc<CompletionQueueHandle>, default: usize, min: usize, max: usize) {
    let oid = thread::current().id();
    let cq = CompletionQueue::new(cq, oid);
    let wtc : Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let (tx, rx): (mpsc::Sender<bool>, mpsc::Receiver<bool>) = mpsc::channel();

    for _ in 0..default {
        new_thread(oid, cq.clone(), wtc.clone(), tx.clone(), min, max);
    }

    loop {
        trace!("poll_queue {:?} is waiting", oid);
        let mut shutdown = rx.recv().unwrap();
        for r in rx.try_iter() {
            if r {
                shutdown = r;
            }
        }
        if shutdown {
            trace!("poll_queue {:?} shutdown", oid);
            break;
        }
        let c = wtc.load(Ordering::SeqCst);
        if c < min {
            debug!("poll_queue {:?} create thread, wtc is {}", oid, c);
            for _ in 0..(default - c) {
                new_thread(oid, cq.clone(), wtc.clone(), tx.clone(), min, max);
            }
        }
    }
}

/// [`Environment`] factory in order to configure the properties.
pub struct EnvBuilder {
    cq_count: usize,
    name_prefix: Option<String>,
    wait_thread_count_default: usize,
    wait_thread_count_min: usize,
    wait_thread_count_max: usize,
}

impl EnvBuilder {
    /// Initialize a new [`EnvBuilder`].
    pub fn new() -> EnvBuilder {
        EnvBuilder {
            cq_count: unsafe { grpc_sys::gpr_cpu_num_cores() as usize },
            name_prefix: None,
            wait_thread_count_default: DEFAULT_WAIT_THREAD_COUNT_DEFAULT,
            wait_thread_count_min: DEFAULT_WAIT_THREAD_COUNT_MIN,
            wait_thread_count_max: DEFAULT_WAIT_THREAD_COUNT_MAX,
        }
    }

    /// Set the number of completion queues and polling threads. Each thread polls
    /// one completion queue.
    ///
    /// # Panics
    ///
    /// This method will panic if `count` is 0.
    pub fn cq_count(mut self, count: usize) -> EnvBuilder {
        assert!(count > 0);
        self.cq_count = count;
        self
    }

    pub fn wait_thread_count_default(mut self, count: usize) -> EnvBuilder {
        assert!(count > 0);
        self.wait_thread_count_default = count;
        self
    }

    pub fn wait_thread_count_min(mut self, count: usize) -> EnvBuilder {
        assert!(count > 0);
        self.wait_thread_count_min = count;
        self
    }

    pub fn wait_thread_count_max(mut self, count: usize) -> EnvBuilder {
        assert!(count > 0);
        self.wait_thread_count_max = count;
        self
    }

    /// Set the thread name prefix of each polling thread.
    pub fn name_prefix<S: Into<String>>(mut self, prefix: S) -> EnvBuilder {
        self.name_prefix = Some(prefix.into());
        self
    }

    /// Finalize the [`EnvBuilder`], build the [`Environment`] and initialize the gRPC library.
    pub fn build(self) -> Environment {
        unsafe {
            grpc_sys::grpc_init();
        }
        let mut cqs = Vec::with_capacity(self.cq_count);
        let mut handles = Vec::with_capacity(self.cq_count);
        let default = self.wait_thread_count_default;
        let min = self.wait_thread_count_min;
        let max = self.wait_thread_count_max;
        for i in 0..self.cq_count {
            let cq = Arc::new(CompletionQueueHandle::new());
            let cq_ = cq.clone();
            let mut builder = ThreadBuilder::new();
            if let Some(ref prefix) = self.name_prefix {
                builder = builder.name(format!("{}-{}", prefix, i));
            }
            let handle = builder.spawn(move || poll_queue(cq_, default, min, max)).unwrap();
            cqs.push(CompletionQueue::new(cq, handle.thread().id()));
            handles.push(handle);
        }

        Environment {
            cqs,
            idx: AtomicUsize::new(0),
            _handles: handles,
        }
    }
}

/// An object that used to control concurrency and start gRPC event loop.
pub struct Environment {
    cqs: Vec<CompletionQueue>,
    idx: AtomicUsize,
    _handles: Vec<JoinHandle<()>>,
}

impl Environment {
    /// Initialize gRPC and create a thread pool to poll completion queue. The thread pool size
    /// and the number of completion queue is specified by `cq_count`. Each thread polls one
    /// completion queue.
    ///
    /// # Panics
    ///
    /// This method will panic if `cq_count` is 0.
    pub fn new(cq_count: usize) -> Environment {
        assert!(cq_count > 0);
        EnvBuilder::new()
            .name_prefix("grpc-poll")
            .cq_count(cq_count)
            .build()
    }

    /// Get all the created completion queues.
    pub fn completion_queues(&self) -> &[CompletionQueue] {
        self.cqs.as_slice()
    }

    /// Pick an arbitrary completion queue.
    pub fn pick_cq(&self) -> CompletionQueue {
        let idx = self.idx.fetch_add(1, Ordering::Relaxed);
        self.cqs[idx % self.cqs.len()].clone()
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        for cq in self.completion_queues() {
            // it's safe to shutdown more than once.
            cq.shutdown()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_loop() {
        let mut env = Environment::new(2);

        let q1 = env.pick_cq();
        let q2 = env.pick_cq();
        let q3 = env.pick_cq();
        let cases = vec![(&q1, &q3, true), (&q1, &q2, false)];
        for (lq, rq, is_eq) in cases {
            let lq_ref = lq.borrow().unwrap();
            let rq_ref = rq.borrow().unwrap();
            if is_eq {
                assert_eq!(lq_ref.as_ptr(), rq_ref.as_ptr());
            } else {
                assert_ne!(lq_ref.as_ptr(), rq_ref.as_ptr());
            }
        }

        assert_eq!(env.completion_queues().len(), 2);
        for cq in env.completion_queues() {
            cq.shutdown();
        }

        for handle in env._handles.drain(..) {
            handle.join().unwrap();
        }
    }
}
