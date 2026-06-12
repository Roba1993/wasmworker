use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::oneshot;

use crate::{channel::Channel, convert::from_bytes, error::TaskError};

type LifecycleCallback = Box<dyn FnOnce()>;

/// A handle to a running channel task on a WebWorker.
///
/// `ChannelTask` combines a bidirectional [`Channel`] for sending and receiving
/// messages with the worker, and a future that resolves to the task's final result.
///
/// This type is returned by [`crate::WebWorker::run_channel`] and
/// [`crate::pool::WebWorkerPool::run_channel`]. It allows you to exchange messages
/// with the worker (e.g., for progress reporting) and then consume the final result.
///
/// # Example
///
/// ```ignore
/// let task = worker
///     .run_channel(webworker_channel!(process_with_progress), &data)
///     .await;
///
/// let progress: Progress = task.recv().await.expect("progress");
/// task.send(&Continue { should_continue: true });
///
/// let result: ProcessResult = task.result().await.expect("worker terminated");
/// ```
pub struct ChannelTask<R> {
    channel: Channel,
    result_rx: Option<oneshot::Receiver<Vec<u8>>>,
    on_complete: Option<LifecycleCallback>,
    on_terminate: Option<LifecycleCallback>,
    _phantom: PhantomData<R>,
}

impl<R: DeserializeOwned> ChannelTask<R> {
    /// Create a new `ChannelTask` from a channel and a result receiver.
    #[doc(hidden)]
    pub fn new(channel: Channel, result_rx: oneshot::Receiver<Vec<u8>>) -> Self {
        Self::with_lifecycle(channel, result_rx, None, None)
    }

    #[doc(hidden)]
    pub(crate) fn with_lifecycle(
        channel: Channel,
        result_rx: oneshot::Receiver<Vec<u8>>,
        on_complete: Option<LifecycleCallback>,
        on_terminate: Option<LifecycleCallback>,
    ) -> Self {
        Self {
            channel,
            result_rx: Some(result_rx),
            on_complete,
            on_terminate,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn with_callbacks(
        mut self,
        on_complete: LifecycleCallback,
        on_terminate: LifecycleCallback,
    ) -> Self {
        self.on_complete = Some(on_complete);
        self.on_terminate = Some(on_terminate);
        self
    }

    /// Receive the next deserialized message from the worker.
    ///
    /// Returns `None` if the channel's sender side has been dropped
    /// (i.e., the worker has finished and closed the channel).
    pub async fn recv<T: DeserializeOwned>(&self) -> Option<T> {
        self.channel.recv().await
    }

    /// Receive raw bytes from the worker.
    ///
    /// Returns `None` if the channel's sender side has been dropped.
    pub async fn recv_bytes(&self) -> Option<Box<[u8]>> {
        self.channel.recv_bytes().await
    }

    /// Send a serialized message to the worker.
    pub fn send<T: Serialize>(&self, msg: &T) {
        self.channel.send(msg);
    }

    /// Send raw bytes to the worker.
    pub fn send_bytes(&self, bytes: &[u8]) {
        self.channel.send_bytes(bytes);
    }

    /// Await the task's final result, consuming the `ChannelTask`.
    pub async fn result(mut self) -> Result<R, TaskError> {
        let result_rx = self
            .result_rx
            .take()
            .ok_or(TaskError::ResultAlreadyConsumed)?;
        let result = result_rx.await.map_err(|_| TaskError::WorkerTerminated);

        match result {
            Ok(bytes) => {
                self.on_terminate.take();
                if let Some(on_complete) = self.on_complete.take() {
                    on_complete();
                }
                Ok(from_bytes(&bytes))
            }
            Err(error) => {
                if let Some(on_terminate) = self.on_terminate.take() {
                    on_terminate();
                }
                Err(error)
            }
        }
    }

    /// Terminate the worker running this task.
    ///
    /// Pool tasks exclusively lease their worker. The pool replaces the terminated
    /// worker in the same slot before making that slot schedulable again.
    pub fn terminate(mut self) {
        if let Some(on_terminate) = self.on_terminate.take() {
            on_terminate();
        }
    }
}

impl<R> Drop for ChannelTask<R> {
    fn drop(&mut self) {
        if let Some(on_terminate) = self.on_terminate.take() {
            on_terminate();
        }
    }
}
