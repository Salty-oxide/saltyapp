use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;

/// Forwards a fetch's messages to the UI in batches, and reports how many it
/// actually sent.
///
/// The command layer used to do this one message at a time: a 1,000-message
/// fetch cost 1,000 IPC hops, each with its own serialization and its own
/// dispatch into the webview, feeding a frontend that coalesces arrivals into
/// at most ten renders a second regardless. Batching changes none of what
/// arrives or in what order — only how many hops it takes.
///
/// Lives here rather than in `src-tauri` so it can be tested: it is the piece
/// with the interesting behaviour (when a partial batch goes, what a Stop
/// drops, what the count means), and `src-tauri` needs a desktop toolchain
/// and a running Tauri app to exercise at all.
///
/// * `emit` sends one batch. It is called only with a non-empty batch.
/// * `progress` is called with the running total each time it passes another
///   `progress_every` messages — the Logs panel's "Fetched N so far" line.
/// * The returned count is how many messages were handed to `emit`, which the
///   caller uses to work out which messages the stream did *not* deliver.
///
/// Generic over the item because the batching has nothing to do with what is
/// being batched: it was written for fetched messages and is now also what
/// carries ksqlDB result rows to the same grid. Nothing in the body ever
/// looked at a field, so this is the same function with a wider signature —
/// the existing message tests exercise it unchanged.
pub async fn forward_in_batches<T, E, P>(
    mut messages: UnboundedReceiver<T>,
    cancelled: Arc<AtomicBool>,
    batch_size: usize,
    batch_interval: Duration,
    progress_every: usize,
    mut emit: E,
    mut progress: P,
) -> usize
where
    E: FnMut(Vec<T>),
    P: FnMut(usize),
{
    // A batch size of zero would mean a batch is never full, so nothing would
    // ever be sent except on the interval — treat it as "send each message".
    let batch_size = batch_size.max(1);
    let mut emitted = 0usize;
    let mut batch: Vec<T> = Vec::with_capacity(batch_size);
    let mut reported_at = 0usize;

    loop {
        // A full batch goes as soon as it is full; a partial one waits for
        // company, but never longer than `batch_interval` — otherwise a slow
        // topic (or the tail of any fetch) would hold rows back until enough
        // of them existed, and a fetch returning fewer than `batch_size`
        // messages would never stream at all.
        let received = if batch.is_empty() {
            messages.recv().await
        } else {
            match tokio::time::timeout(batch_interval, messages.recv()).await {
                Ok(received) => received,
                Err(_elapsed) => {
                    emitted += batch.len();
                    emit(std::mem::take(&mut batch));
                    continue;
                }
            }
        };

        let Some(message) = received else {
            // The fetch dropped its sender: it is finished.
            break;
        };

        // Stopping the poll loop isn't the whole of stopping: whatever it had
        // already queued here would still be serialized and sent to a Data tab
        // that is discarding it, so Stop looked ignored for as long as the
        // backlog took to drain. Anything buffered here is dropped with it.
        if cancelled.load(Ordering::Relaxed) {
            return emitted;
        }

        batch.push(message);
        if batch.len() >= batch_size {
            emitted += batch.len();
            emit(std::mem::take(&mut batch));
        }

        // Counted over messages rather than batches, so the interval still
        // means what its name says.
        let seen = emitted + batch.len();
        if progress_every > 0 && seen - reported_at >= progress_every {
            reported_at = seen;
            progress(seen);
        }
    }

    // Whatever the last batch was short of a full one.
    emitted += batch.len();
    if !batch.is_empty() {
        emit(batch);
    }
    emitted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TopicMessage;
    use std::sync::Mutex;
    use tokio::sync::mpsc::unbounded_channel;

    fn message(offset: i64) -> TopicMessage {
        TopicMessage {
            partition: 0,
            offset,
            timestamp_ms: None,
            key_base64: None,
            payload_base64: None,
            payload_size_bytes: None,
            headers: Vec::new(),
        }
    }

    /// Collects the batches `forward_in_batches` emits, for assertions on
    /// both what arrived and how it was grouped.
    #[derive(Default)]
    struct Collector {
        batches: Arc<Mutex<Vec<Vec<i64>>>>,
    }

    impl Collector {
        fn emitter(&self) -> impl FnMut(Vec<TopicMessage>) + use<> {
            let batches = Arc::clone(&self.batches);
            move |batch: Vec<TopicMessage>| {
                assert!(!batch.is_empty(), "an empty batch is a wasted IPC hop");
                batches.lock().unwrap().push(batch.iter().map(|m| m.offset).collect());
            }
        }

        fn batches(&self) -> Vec<Vec<i64>> {
            self.batches.lock().unwrap().clone()
        }

        fn offsets(&self) -> Vec<i64> {
            self.batches().into_iter().flatten().collect()
        }
    }

    const NEVER: Duration = Duration::from_secs(3600);

    #[tokio::test]
    async fn groups_messages_into_full_batches() {
        let (tx, rx) = unbounded_channel();
        for offset in 0..10 {
            tx.send(message(offset)).unwrap();
        }
        drop(tx);
        let collector = Collector::default();

        let emitted = forward_in_batches(
            rx,
            Arc::new(AtomicBool::new(false)),
            4,
            NEVER,
            0,
            collector.emitter(),
            |_| {},
        )
        .await;

        assert_eq!(emitted, 10);
        // 4 + 4 + the remaining 2 as the tail.
        assert_eq!(collector.batches(), vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9]]);
    }

    #[tokio::test]
    async fn delivers_every_message_exactly_once_and_in_order() {
        let (tx, rx) = unbounded_channel();
        for offset in 0..1000 {
            tx.send(message(offset)).unwrap();
        }
        drop(tx);
        let collector = Collector::default();

        let emitted =
            forward_in_batches(rx, Arc::new(AtomicBool::new(false)), 64, NEVER, 0, collector.emitter(), |_| {}).await;

        assert_eq!(emitted, 1000);
        assert_eq!(collector.offsets(), (0..1000).collect::<Vec<_>>());
    }

    /// The count is what the command layer uses to decide which messages the
    /// response still has to carry, so it must be exactly the number handed
    /// to `emit` — never the number received.
    #[tokio::test]
    async fn reports_the_number_of_messages_it_emitted() {
        let (tx, rx) = unbounded_channel();
        for offset in 0..7 {
            tx.send(message(offset)).unwrap();
        }
        drop(tx);
        let collector = Collector::default();

        let emitted =
            forward_in_batches(rx, Arc::new(AtomicBool::new(false)), 64, NEVER, 0, collector.emitter(), |_| {}).await;

        assert_eq!(emitted, 7);
        assert_eq!(collector.offsets().len(), 7);
    }

    /// A fetch smaller than one batch — and the tail of every fetch — must
    /// still reach the grid, rather than waiting for messages that will never
    /// come.
    #[tokio::test]
    async fn sends_a_partial_batch_once_the_interval_passes() {
        let (tx, rx) = unbounded_channel();
        let collector = Collector::default();
        let emitter = collector.emitter();

        let handle = tokio::spawn(async move {
            forward_in_batches(
                rx,
                Arc::new(AtomicBool::new(false)),
                64,
                Duration::from_millis(20),
                0,
                emitter,
                |_| {},
            )
            .await
        });

        tx.send(message(1)).unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        // Still open, so only the interval can have flushed this.
        assert_eq!(collector.offsets(), vec![1], "a partial batch waited past its interval");

        tx.send(message(2)).unwrap();
        drop(tx);
        let emitted = handle.await.unwrap();
        assert_eq!(emitted, 2);
        assert_eq!(collector.offsets(), vec![1, 2]);
    }

    /// Mid-stream Stop: what was already sent stays sent, and everything
    /// still queued behind the flag is dropped rather than serialized for a
    /// grid that is discarding it.
    #[tokio::test]
    async fn stops_emitting_as_soon_as_the_fetch_is_cancelled() {
        let (tx, rx) = unbounded_channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let collector = Collector::default();
        let emitter = collector.emitter();
        let flag = Arc::clone(&cancelled);

        let handle =
            tokio::spawn(async move { forward_in_batches(rx, flag, 2, NEVER, 0, emitter, |_| {}).await });

        for offset in 0..4 {
            tx.send(message(offset)).unwrap();
        }
        // Let the two full batches go out before the Stop lands.
        while collector.offsets().len() < 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        cancelled.store(true, Ordering::Relaxed);
        for offset in 4..100 {
            tx.send(message(offset)).unwrap();
        }
        drop(tx);

        let emitted = handle.await.unwrap();
        assert_eq!(emitted, 4);
        assert_eq!(collector.offsets(), vec![0, 1, 2, 3]);
    }

    /// A fetch cancelled before the forwarder ever reads a message sends
    /// nothing at all.
    #[tokio::test]
    async fn emits_nothing_when_the_fetch_was_already_cancelled() {
        let (tx, rx) = unbounded_channel();
        for offset in 0..10 {
            tx.send(message(offset)).unwrap();
        }
        drop(tx);
        let collector = Collector::default();

        let emitted =
            forward_in_batches(rx, Arc::new(AtomicBool::new(true)), 2, NEVER, 0, collector.emitter(), |_| {}).await;

        assert_eq!(emitted, 0);
        assert!(collector.batches().is_empty());
    }

    /// Cancellation drops a partly-filled batch too: those rows belong to a
    /// fetch the user asked to stop.
    #[tokio::test]
    async fn drops_a_partial_batch_on_cancellation() {
        let (tx, rx) = unbounded_channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        tx.send(message(0)).unwrap();
        cancelled.store(true, Ordering::Relaxed);
        tx.send(message(1)).unwrap();
        drop(tx);
        let collector = Collector::default();

        let emitted =
            forward_in_batches(rx, Arc::clone(&cancelled), 64, NEVER, 0, collector.emitter(), |_| {}).await;

        assert_eq!(emitted, 0);
        assert!(collector.batches().is_empty());
    }

    #[tokio::test]
    async fn reports_progress_by_messages_rather_than_batches() {
        let (tx, rx) = unbounded_channel();
        for offset in 0..100 {
            tx.send(message(offset)).unwrap();
        }
        drop(tx);
        let reported = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&reported);

        forward_in_batches(
            rx,
            Arc::new(AtomicBool::new(false)),
            8,
            NEVER,
            25,
            |_| {},
            move |count| seen.lock().unwrap().push(count),
        )
        .await;

        // Every 25 messages, not every 25 batches — the same points the
        // command layer's `count % PROGRESS_LOG_INTERVAL` reported before the
        // batching went in.
        assert_eq!(*reported.lock().unwrap(), vec![25, 50, 75, 100]);
    }

    #[tokio::test]
    async fn a_fetch_that_returns_nothing_emits_nothing() {
        let (tx, rx) = unbounded_channel::<TopicMessage>();
        drop(tx);
        let collector = Collector::default();

        let emitted =
            forward_in_batches(rx, Arc::new(AtomicBool::new(false)), 64, NEVER, 0, collector.emitter(), |_| {}).await;

        assert_eq!(emitted, 0);
        assert!(collector.batches().is_empty(), "an empty fetch must not send an empty batch");
    }

    /// Defensive: a zero batch size must not mean "never full", which would
    /// leave every message waiting on the interval.
    #[tokio::test]
    async fn treats_a_zero_batch_size_as_one_message_per_batch() {
        let (tx, rx) = unbounded_channel();
        tx.send(message(0)).unwrap();
        tx.send(message(1)).unwrap();
        drop(tx);
        let collector = Collector::default();

        let emitted =
            forward_in_batches(rx, Arc::new(AtomicBool::new(false)), 0, NEVER, 0, collector.emitter(), |_| {}).await;

        assert_eq!(emitted, 2);
        assert_eq!(collector.batches(), vec![vec![0], vec![1]]);
    }
}
