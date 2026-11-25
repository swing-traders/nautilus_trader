// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Redis-backed message bus backing for the system.
//!
//! # Architecture
//!
//! Runs background tasks on `get_runtime()` for publishing, stream reading,
//! and heartbeats. Messages are sent via an unbounded `tokio::sync::mpsc`
//! channel to the publish task, which buffers and writes them to Redis
//! streams. Each background task owns its own Redis connection created on
//! the Nautilus runtime.
//!
//! Handles are stored as `Option<JoinHandle>` for idempotent shutdown via
//! `close_async()`. The synchronous `close()` uses `block_in_place` to
//! bridge into the async shutdown path and must be called from outside any
//! `current_thread` Tokio runtime.

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::stream::Stream;
use nautilus_common::{
    enums::SerializationEncoding,
    live::get_runtime,
    logging::{log_task_error, log_task_started, log_task_stopped},
    msgbus::{
        BusMessage, BusPayloadType, MessageBusBacking, MessageBusBackingFactory, MessageBusConfig,
        switchboard::CLOSE_TOPIC,
    },
};
use nautilus_core::{
    UUID4,
    time::{duration_since_unix_epoch, get_atomic_clock_realtime},
};
use nautilus_cryptography::providers::install_cryptographic_provider;
use nautilus_model::identifiers::TraderId;
use redis::{AsyncCommands, aio::ConnectionManager, streams};
use serde::{Deserialize, Serialize};
use streams::StreamReadOptions;
use ustr::Ustr;

use super::{
    CountRequest, REDIS_MINID, REDIS_XTRIM, WriterState, await_handle, await_publisher,
    check_redis_version, connect_redis, is_retryable_error, retry_delay,
};
use crate::redis::{RedisConnectionConfig, create_redis_connection, get_stream_key};

const MSGBUS_PUBLISH: &str = "msgbus-publish";
const MSGBUS_STREAM: &str = "msgbus-stream";
const MSGBUS_HEARTBEAT: &str = "msgbus-heartbeat";
const HEARTBEAT_TOPIC: &str = "health:heartbeat";
const TRIM_BUFFER_SECS: u64 = 60;

type RedisStreamBulk = Vec<HashMap<String, Vec<HashMap<String, redis::Value>>>>;

/// Configuration for a Redis-backed message bus backing.
///
/// Redis 6.2 or higher is required for correct operation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.core.nautilus_pyo3.infrastructure",
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.infrastructure")
)]
pub struct RedisMessageBusConfig {
    /// The Redis host address. If `None`, `127.0.0.1` is used.
    pub host: Option<String>,
    /// The Redis port. If `None`, `6379` is used.
    pub port: Option<u16>,
    /// The Redis account username.
    pub username: Option<String>,
    /// The Redis account password.
    pub password: Option<String>,
    /// If Redis should use an SSL-enabled connection.
    pub ssl: bool,
    /// The timeout (in seconds) to wait for a new connection.
    pub connection_timeout: u16,
    /// The timeout (in seconds) to wait for a response.
    pub response_timeout: u16,
    /// The number of retry attempts with exponential backoff for connection attempts.
    pub number_of_retries: usize,
    /// The base value for exponential backoff calculation.
    pub exponent_base: u64,
    /// The maximum delay between retry attempts (in seconds).
    pub max_delay: u64,
    /// The multiplication factor for retry delay calculation.
    pub factor: u64,
}

impl Debug for RedisMessageBusConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = self.password.as_ref().map(|_| "***");
        f.debug_struct(stringify!(RedisMessageBusConfig))
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &redacted)
            .field("ssl", &self.ssl)
            .field("connection_timeout", &self.connection_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("number_of_retries", &self.number_of_retries)
            .field("exponent_base", &self.exponent_base)
            .field("max_delay", &self.max_delay)
            .field("factor", &self.factor)
            .finish()
    }
}

impl Default for RedisMessageBusConfig {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            username: None,
            password: None,
            ssl: false,
            connection_timeout: 20,
            response_timeout: 20,
            number_of_retries: 100,
            exponent_base: 2,
            max_delay: 1000,
            factor: 2,
        }
    }
}

impl RedisConnectionConfig for RedisMessageBusConfig {
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn port(&self) -> Option<u16> {
        self.port
    }

    fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    fn ssl(&self) -> bool {
        self.ssl
    }

    fn connection_timeout(&self) -> u16 {
        self.connection_timeout
    }

    fn response_timeout(&self) -> u16 {
        self.response_timeout
    }

    fn number_of_retries(&self) -> usize {
        self.number_of_retries
    }

    fn exponent_base(&self) -> u64 {
        self.exponent_base
    }

    fn max_delay(&self) -> u64 {
        self.max_delay
    }

    fn factor(&self) -> u64 {
        self.factor
    }
}

/// Factory for constructing Redis message bus backings.
#[derive(Debug, Clone)]
pub struct RedisMessageBusFactory {
    config: RedisMessageBusConfig,
}

impl RedisMessageBusFactory {
    /// Creates a new [`RedisMessageBusFactory`] from the given Redis configuration.
    #[must_use]
    pub const fn new(config: RedisMessageBusConfig) -> Self {
        Self { config }
    }
}

impl MessageBusBackingFactory for RedisMessageBusFactory {
    fn create(
        &self,
        trader_id: TraderId,
        instance_id: UUID4,
        config: MessageBusConfig,
    ) -> anyhow::Result<Box<dyn MessageBusBacking>> {
        Ok(Box::new(RedisMessageBusBacking::new(
            trader_id,
            instance_id,
            config,
            self.config.clone(),
        )?))
    }
}

pub struct RedisMessageBusBacking {
    /// The trader ID for this message bus backing.
    pub trader_id: TraderId,
    /// The instance ID for this message bus backing.
    pub instance_id: UUID4,
    pub_tx: tokio::sync::mpsc::UnboundedSender<BusMessage>,
    pub_handle: Option<tokio::task::JoinHandle<()>>,
    pub_state: Arc<WriterState>,
    stream_rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
    stream_handle: Option<tokio::task::JoinHandle<()>>,
    stream_signal: Arc<AtomicBool>,
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
    heartbeat_signal: Arc<AtomicBool>,
}

impl Debug for RedisMessageBusBacking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RedisMessageBusBacking))
            .field("trader_id", &self.trader_id)
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl RedisMessageBusBacking {
    /// Creates a new [`RedisMessageBusBacking`] instance for the given `trader_id`, `instance_id`, and `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if the heartbeat interval is configured as zero seconds.
    pub fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        config: MessageBusConfig,
        backing: RedisMessageBusConfig,
    ) -> anyhow::Result<Self> {
        install_cryptographic_provider();

        if config.heartbeat_interval_secs == Some(0) {
            anyhow::bail!("heartbeat_interval_secs must be greater than 0");
        }

        let external_streams = config.external_streams.clone().unwrap_or_default();
        let heartbeat_interval_secs = config.heartbeat_interval_secs;
        let publish = backing.clone();

        let (pub_tx, pub_rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
        let (pub_state, count_requests) = WriterState::new();
        let publish_state = pub_state.clone();

        // Create publish task (start the runtime here for now)
        let pub_handle = Some(get_runtime().spawn(async move {
            if let Err(e) = publish_reporting(
                pub_rx,
                trader_id,
                instance_id,
                config,
                publish,
                publish_state,
                count_requests,
            )
            .await
            {
                log_task_error(MSGBUS_PUBLISH, &e);
            }
        }));

        // Conditionally create stream task and channel if external streams configured
        let stream_signal = Arc::new(AtomicBool::new(false));
        let (stream_rx, stream_handle) = if external_streams.is_empty() {
            (None, None)
        } else {
            let stream_signal_clone = stream_signal.clone();
            let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(100_000);
            (
                Some(stream_rx),
                Some(get_runtime().spawn(async move {
                    if let Err(e) = stream_messages(
                        stream_tx,
                        backing.clone(),
                        external_streams,
                        stream_signal_clone,
                    )
                    .await
                    {
                        log_task_error(MSGBUS_STREAM, &e);
                    }
                })),
            )
        };

        // Create heartbeat task
        let heartbeat_signal = Arc::new(AtomicBool::new(false));
        let heartbeat_handle = if let Some(heartbeat_interval_secs) = heartbeat_interval_secs {
            let signal = heartbeat_signal.clone();
            let pub_tx_clone = pub_tx.clone();

            Some(get_runtime().spawn(async move {
                run_heartbeat(heartbeat_interval_secs, signal, pub_tx_clone).await;
            }))
        } else {
            None
        };

        Ok(Self {
            trader_id,
            instance_id,
            pub_tx,
            pub_handle,
            pub_state,
            stream_rx,
            stream_handle,
            stream_signal,
            heartbeat_handle,
            heartbeat_signal,
        })
    }
}

impl MessageBusBacking for RedisMessageBusBacking {
    /// Returns whether the message bus backing publishing channel is closed.
    fn is_closed(&self) -> bool {
        self.pub_tx.is_closed()
    }

    /// Queues a serialized bus message for external publication.
    fn publish(&self, message: BusMessage) {
        if let Err(e) = self.pub_tx.send(message) {
            log::error!("Failed to send message: {e}");
        }
    }

    fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.get_stream_receiver()
    }

    /// Closes the message bus backing.
    fn close(&mut self) {
        log::debug!("Closing");

        self.stream_signal.store(true, Ordering::Relaxed);
        self.heartbeat_signal.store(true, Ordering::Relaxed);

        if !self.pub_tx.is_closed() {
            let msg = BusMessage::new_close();

            if let Err(e) = self.pub_tx.send(msg) {
                log::warn!("Failed to send close message: {e:?}");
            }
        }

        // Keep close sync for now to avoid async trait method
        tokio::task::block_in_place(|| {
            get_runtime().block_on(async {
                self.close_async().await;
            });
        });

        log::debug!("Closed");
    }
}

impl RedisMessageBusBacking {
    /// Retrieves the Redis stream receiver for this message bus instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream receiver has already been taken.
    pub fn get_stream_receiver(
        &mut self,
    ) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
        self.stream_rx
            .take()
            .ok_or_else(|| anyhow::anyhow!("Stream receiver already taken"))
    }

    /// Streams messages arriving on the stream receiver channel.
    pub fn stream(
        mut stream_rx: tokio::sync::mpsc::Receiver<BusMessage>,
    ) -> impl Stream<Item = BusMessage> + 'static {
        async_stream::stream! {
            while let Some(msg) = stream_rx.recv().await {
                yield msg;
            }
        }
    }

    pub async fn close_async(&mut self) {
        await_publisher(self.pub_handle.take(), MSGBUS_PUBLISH, &self.pub_state).await;
        await_handle(self.stream_handle.take(), MSGBUS_STREAM).await;
        await_handle(self.heartbeat_handle.take(), MSGBUS_HEARTBEAT).await;
    }
}

/// Publishes messages received on `rx` to Redis streams for the given `trader_id` and `instance_id`, using `config`.
///
/// A write Redis does not answer is retried until it does, before any newer message is written,
/// so every message reaches its stream at least once and in order.
///
/// # Errors
///
/// Returns an error if:
/// - The backing configuration is missing in `config`.
/// - The server's version cannot be read.
/// - Redis answers a write with an error that retrying cannot clear.
pub async fn publish_messages(
    rx: tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
    trader_id: TraderId,
    instance_id: UUID4,
    config: MessageBusConfig,
    backing: RedisMessageBusConfig,
) -> anyhow::Result<()> {
    let (state, count_requests) = WriterState::new();
    publish_reporting(
        rx,
        trader_id,
        instance_id,
        config,
        backing,
        state,
        count_requests,
    )
    .await
}

/// Publishes as [`publish_messages`] does, telling its closer through `state` whether a delivery
/// is failing and answering `count_requests` with what it holds.
pub(crate) async fn publish_reporting(
    rx: tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
    trader_id: TraderId,
    instance_id: UUID4,
    config: MessageBusConfig,
    backing: RedisMessageBusConfig,
    state: Arc<WriterState>,
    count_requests: tokio::sync::mpsc::UnboundedReceiver<CountRequest>,
) -> anyhow::Result<()> {
    log_task_started(MSGBUS_PUBLISH);

    let mut outbox = Outbox::new(rx, state, count_requests);
    let (mut con, info) = outbox
        .until_answered(&backing, || connect_redis(MSGBUS_PUBLISH, &backing))
        .await?;
    check_redis_version(&info)?;
    let stream_key = get_stream_key(trader_id, instance_id, &config);

    // Auto-trimming
    let autotrim_duration = config
        .autotrim_mins
        .filter(|&mins| mins > 0)
        .map(|mins| Duration::from_secs(u64::from(mins) * 60));
    let autotrim_maxlen = config
        .autotrim_maxlen
        .filter(|&maxlen| maxlen > 0)
        .map(usize::try_from)
        .transpose()?;
    let mut last_trim_index: HashMap<String, usize> = HashMap::new();

    // Buffering
    let buffer_interval = Duration::from_millis(u64::from(config.buffer_interval_ms.unwrap_or(0)));

    // A sleep used to trigger periodic flushing of the buffer.
    // When `buffer_interval` is zero we skip using the timer and flush immediately
    // after every message.
    let flush_timer = tokio::time::sleep(buffer_interval);
    tokio::pin!(flush_timer);

    // What was taken in while the setup was retried; holding nothing, nothing is failing
    if outbox.buffer.is_empty() {
        outbox.delivered();
    } else {
        let pipe = drain_buffer(
            &mut outbox,
            &mut con,
            &stream_key,
            config.stream_per_topic,
            autotrim_duration,
            autotrim_maxlen,
            &mut last_trim_index,
        )
        .await;
        outbox.write(&con, &pipe, &backing).await?;
    }

    while !outbox.closing {
        tokio::select! {
            maybe_msg = outbox.rx.recv() => {
                if let Some(msg) = maybe_msg {
                    if msg.topic == CLOSE_TOPIC {
                        log::debug!("Received close message");
                        outbox.closing = true;
                    } else {
                        outbox.buffer.push_back(msg);

                        // Immediate flush mode, which also flushes what was taken in while a
                        // write was retried
                        while buffer_interval.is_zero() && !outbox.buffer.is_empty() {
                            let pipe = drain_buffer(
                                &mut outbox,
                                &mut con,
                                &stream_key,
                                config.stream_per_topic,
                                autotrim_duration,
                                autotrim_maxlen,
                                &mut last_trim_index,
                            ).await;
                            outbox.write(&con, &pipe, &backing).await?;
                        }
                    }
                } else {
                    log::debug!("Channel hung up");
                    outbox.closing = true;
                }
            }
            // Only poll the timer when the interval is non-zero. This avoids
            // unnecessarily waking the task when immediate flushing is enabled.
            () = &mut flush_timer, if !buffer_interval.is_zero() => {
                if !outbox.buffer.is_empty() {
                    let pipe = drain_buffer(
                        &mut outbox,
                        &mut con,
                        &stream_key,
                        config.stream_per_topic,
                        autotrim_duration,
                        autotrim_maxlen,
                        &mut last_trim_index,
                    ).await;
                    outbox.write(&con, &pipe, &backing).await?;
                }

                // Schedule the next tick
                flush_timer.as_mut().reset(tokio::time::Instant::now() + buffer_interval);
            }
        }
    }

    // Drain any remaining messages
    if !outbox.buffer.is_empty() {
        let pipe = drain_buffer(
            &mut outbox,
            &mut con,
            &stream_key,
            config.stream_per_topic,
            autotrim_duration,
            autotrim_maxlen,
            &mut last_trim_index,
        )
        .await;
        outbox.write(&con, &pipe, &backing).await?;
    }

    log_task_stopped(MSGBUS_PUBLISH);
    Ok(())
}

/// What the publisher has taken from its channel and not yet written. Once a write has failed,
/// newer messages keep being taken in behind it while it is retried.
struct Outbox {
    rx: tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
    count_requests: tokio::sync::mpsc::UnboundedReceiver<CountRequest>,
    buffer: VecDeque<BusMessage>,
    in_flight: usize,
    /// Consecutive unanswered requests, for the backoff.
    failures: usize,
    /// Whether newer messages are taken in while the publisher waits: from an unanswered request
    /// until a delivery.
    retrying: bool,
    closing: bool,
    state: Arc<WriterState>,
}

impl Outbox {
    fn new(
        rx: tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
        state: Arc<WriterState>,
        count_requests: tokio::sync::mpsc::UnboundedReceiver<CountRequest>,
    ) -> Self {
        Self {
            rx,
            count_requests,
            buffer: VecDeque::new(),
            in_flight: 0,
            failures: 0,
            retrying: false,
            closing: false,
            state,
        }
    }

    fn held(&self) -> usize {
        self.in_flight + self.buffer.len() + self.rx.len()
    }

    /// Marks a delivery failing: a sent request failed, by an error or a timeout.
    fn failed(&mut self) {
        self.retrying = true;
        self.state.set_failing(true);
    }

    fn delivered(&mut self) {
        self.retrying = false;
        self.state.set_failing(false);
    }

    /// Writes `pipe`, carrying the messages in flight, until Redis answers it. A write retried
    /// after only its reply was lost lands twice, which every stream reader absorbs by its key.
    async fn write(
        &mut self,
        con: &ConnectionManager,
        pipe: &redis::Pipeline,
        backing: &RedisMessageBusConfig,
    ) -> anyhow::Result<()> {
        let written = self
            .until_answered(backing, || {
                let mut con = con.clone();
                async move { pipe.query_async::<()>(&mut con).await }
            })
            .await;
        self.in_flight = 0;
        written?;
        self.delivered();
        Ok(())
    }

    /// Runs `op` until it succeeds or fails with an error retrying cannot clear, waiting the
    /// config's backoff after each retryable failure.
    async fn until_answered<T, F, Fut>(
        &mut self,
        backing: &RedisMessageBusConfig,
        mut op: F,
    ) -> redis::RedisResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = redis::RedisResult<T>>,
    {
        loop {
            match self.draining(op()).await {
                Err(e) if is_retryable_error(&e) => {
                    log::error!(
                        "Error in task '{MSGBUS_PUBLISH}', retrying until Redis answers: {e}"
                    );
                    self.failed();
                    let delay = retry_delay(backing, self.failures);
                    self.failures = self.failures.saturating_add(1);
                    self.draining(tokio::time::sleep(delay)).await;
                }
                result => {
                    self.failures = 0;
                    return result;
                }
            }
        }
    }

    /// Awaits `future`, answering the closer's count requests meanwhile and, once a write has
    /// failed, taking newer messages in.
    async fn draining<F: Future>(&mut self, future: F) -> F::Output {
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                output = &mut future => return output,
                Some(reply) = self.count_requests.recv() => {
                    let _ = reply.send((self.held(), self.closing));
                }
                maybe_msg = self.rx.recv(), if self.retrying && !self.closing => {
                    match maybe_msg {
                        Some(msg) if msg.topic != CLOSE_TOPIC => self.buffer.push_back(msg),
                        _ => self.closing = true,
                    }
                }
            }
        }
    }
}

/// Drains the outbox's buffer into one atomic pipeline of stream writes, its messages in flight
/// from then, trimming streams as configured.
async fn drain_buffer(
    outbox: &mut Outbox,
    conn: &mut redis::aio::ConnectionManager,
    stream_key: &str,
    stream_per_topic: bool,
    autotrim_duration: Option<Duration>,
    autotrim_maxlen: Option<usize>,
    last_trim_index: &mut HashMap<String, usize>,
) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    pipe.atomic();

    let messages = std::mem::take(&mut outbox.buffer);
    outbox.in_flight = messages.len();
    for msg in messages {
        let encoding = msg.encoding.to_string();
        let items: Vec<(&str, &[u8])> = vec![
            ("topic", msg.topic.as_ref()),
            ("type", msg.payload_type.as_str().as_bytes()),
            ("payload", msg.payload.as_ref()),
            ("encoding", encoding.as_bytes()),
        ];
        let stream_key = if stream_per_topic {
            format!("{stream_key}:{}", msg.topic)
        } else {
            stream_key.to_string()
        };

        if let Some(maxlen) = autotrim_maxlen {
            pipe.xadd_maxlen(
                &stream_key,
                streams::StreamMaxlen::Approx(maxlen),
                "*",
                &items,
            );
        } else {
            pipe.xadd(&stream_key, "*", &items);
        }

        if autotrim_duration.is_none() {
            continue; // Nothing else to do
        }

        // Autotrim stream
        let last_trim_ms = last_trim_index.entry(stream_key.clone()).or_insert(0); // Remove clone
        let unix_duration_now = duration_since_unix_epoch();
        let trim_buffer = Duration::from_secs(TRIM_BUFFER_SECS);

        // Improve efficiency of this by batching
        if *last_trim_ms < unix_duration_now.saturating_sub(trim_buffer).as_millis() as usize {
            let min_timestamp_ms = unix_duration_now
                .saturating_sub(autotrim_duration.unwrap())
                .as_millis() as usize;
            let mut trim = redis::cmd(REDIS_XTRIM);
            trim.arg(stream_key.clone())
                .arg(REDIS_MINID)
                .arg(min_timestamp_ms);
            let result: Result<(), redis::RedisError> =
                outbox.draining(trim.query_async(conn)).await;

            if let Err(e) = result {
                log::error!("Error trimming stream '{stream_key}': {e}");
                outbox.failed();
            } else {
                last_trim_index.insert(stream_key.clone(), unix_duration_now.as_millis() as usize);
            }
        }
    }

    pipe
}

/// Streams messages from Redis streams and sends them over the provided `tx` channel.
///
/// # Errors
///
/// Returns an error if:
/// - Establishing the Redis connection fails before the terminate signal is received.
/// - A Redis read operation returns a non-retryable error.
pub async fn stream_messages(
    tx: tokio::sync::mpsc::Sender<BusMessage>,
    config: RedisMessageBusConfig,
    stream_keys: Vec<String>,
    stream_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    log_task_started(MSGBUS_STREAM);

    let Some(mut con) = connect_stream_connection(&config, &stream_signal).await? else {
        log_task_stopped(MSGBUS_STREAM);
        return Ok(());
    };

    let mut read_error_count = 0;

    let stream_keys = &stream_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<&str>>();

    log::debug!("Listening to streams: [{}]", stream_keys.join(", "));

    // Start streaming from current timestamp
    let clock = get_atomic_clock_realtime();
    let timestamp_ms = clock.get_time_ms();
    let initial_id = timestamp_ms.to_string();

    let mut last_ids: HashMap<String, String> = stream_keys
        .iter()
        .map(|&key| (key.to_string(), initial_id.clone()))
        .collect();

    let opts = StreamReadOptions::default().block(100);

    'outer: loop {
        if stream_signal.load(Ordering::Relaxed) {
            log::debug!("Received streaming terminate signal");
            break;
        }

        let ids: Vec<String> = stream_keys
            .iter()
            .map(|&key| last_ids[key].clone())
            .collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();

        let result: Result<RedisStreamBulk, _> =
            con.xread_options(&[&stream_keys], &[&id_refs], &opts).await;

        match result {
            Ok(stream_bulk) => {
                read_error_count = 0;

                if stream_bulk.is_empty() {
                    // Timeout occurred: no messages received
                    continue;
                }

                for entry in &stream_bulk {
                    for (stream_key, stream_msgs) in entry {
                        for stream_msg in stream_msgs {
                            for (id, array) in stream_msg {
                                last_ids.insert(stream_key.clone(), id.clone());

                                match decode_bus_message(array) {
                                    Ok(msg) => {
                                        if let Err(e) = tx.send(msg).await {
                                            log::debug!("Channel closed: {e:?}");
                                            break 'outer; // End streaming
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("{e:?}");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if !is_retryable_error(&e) {
                    anyhow::bail!("Error reading from stream: {e:?}");
                }

                log::error!("Error reading from stream: {e:?}");

                let Some(reconnected) =
                    reconnect_stream_connection(&config, &stream_signal, &mut read_error_count)
                        .await?
                else {
                    break;
                };
                con = reconnected;
            }
        }
    }

    log_task_stopped(MSGBUS_STREAM);
    Ok(())
}

async fn connect_stream_connection(
    config: &RedisMessageBusConfig,
    stream_signal: &Arc<AtomicBool>,
) -> anyhow::Result<Option<ConnectionManager>> {
    let connect = create_redis_connection(MSGBUS_STREAM, config);
    let terminate = wait_for_stream_signal(stream_signal);

    tokio::pin!(connect);
    tokio::pin!(terminate);

    tokio::select! {
        result = &mut connect => result.map(Some),
        () = &mut terminate => Ok(None),
    }
}

async fn reconnect_stream_connection(
    config: &RedisMessageBusConfig,
    stream_signal: &Arc<AtomicBool>,
    read_error_count: &mut usize,
) -> anyhow::Result<Option<ConnectionManager>> {
    loop {
        let delay = retry_delay(config, *read_error_count);
        *read_error_count = (*read_error_count).saturating_add(1);

        if !wait_for_retry_delay(delay, stream_signal).await {
            return Ok(None);
        }

        match connect_stream_connection(config, stream_signal).await {
            Ok(Some(con)) => return Ok(Some(con)),
            Ok(None) => return Ok(None),
            Err(e) => {
                log::error!("Error reconnecting to stream: {e:?}");
            }
        }
    }
}

async fn wait_for_retry_delay(retry_delay: Duration, stream_signal: &Arc<AtomicBool>) -> bool {
    let retry_timer = tokio::time::sleep(retry_delay);
    let terminate = wait_for_stream_signal(stream_signal);

    tokio::pin!(retry_timer);
    tokio::pin!(terminate);

    tokio::select! {
        () = &mut retry_timer => true,
        () = &mut terminate => false,
    }
}

async fn wait_for_stream_signal(stream_signal: &Arc<AtomicBool>) {
    let check_timer = tokio::time::interval(Duration::from_millis(100));

    tokio::pin!(check_timer);

    while !stream_signal.load(Ordering::Relaxed) {
        check_timer.tick().await;
    }
}

// Redis fields are unordered, and older streams may omit type or encoding headers
fn decode_bus_message(stream_msg: &redis::Value) -> anyhow::Result<BusMessage> {
    let redis::Value::Array(fields) = stream_msg else {
        anyhow::bail!("Invalid stream message format: {stream_msg:?}");
    };

    if fields.len() < 4 || fields.len() % 2 != 0 {
        anyhow::bail!("Invalid stream message format: {stream_msg:?}");
    }

    let mut topic: Option<String> = None;
    let mut payload_type = BusPayloadType::Custom(Ustr::default());
    let mut encoding = SerializationEncoding::default();
    let mut payload: Option<Bytes> = None;

    for pair in fields.as_chunks::<2>().0 {
        let redis::Value::BulkString(key) = &pair[0] else {
            anyhow::bail!("Invalid stream field key: {stream_msg:?}");
        };

        match key.as_slice() {
            b"topic" => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid topic format: {stream_msg:?}");
                };
                topic = Some(
                    String::from_utf8(bytes.clone())
                        .map_err(|e| anyhow::anyhow!("Error parsing topic: {e}"))?,
                );
            }
            b"type" => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid type format: {stream_msg:?}");
                };
                let type_name = std::str::from_utf8(bytes)
                    .map_err(|e| anyhow::anyhow!("Error parsing type: {e}"))?;
                payload_type = BusPayloadType::from_name(type_name);
            }
            b"encoding" => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid encoding format: {stream_msg:?}");
                };
                let value = std::str::from_utf8(bytes)
                    .map_err(|e| anyhow::anyhow!("Error parsing encoding: {e}"))?;
                encoding = value
                    .parse()
                    .map_err(|e| anyhow::anyhow!("Error parsing encoding: {e}"))?;
            }
            b"payload" => {
                let redis::Value::BulkString(bytes) = &pair[1] else {
                    anyhow::bail!("Invalid payload format: {stream_msg:?}");
                };
                payload = Some(Bytes::copy_from_slice(bytes));
            }
            _ => {}
        }
    }

    let Some(topic) = topic else {
        anyhow::bail!("Stream message missing topic: {stream_msg:?}");
    };
    let Some(payload) = payload else {
        anyhow::bail!("Stream message missing payload: {stream_msg:?}");
    };

    Ok(BusMessage::with_str_topic(
        topic,
        payload_type,
        payload,
        encoding,
    ))
}

async fn run_heartbeat(
    heartbeat_interval_secs: u16,
    signal: Arc<AtomicBool>,
    pub_tx: tokio::sync::mpsc::UnboundedSender<BusMessage>,
) {
    log_task_started("heartbeat");
    log::debug!("Heartbeat at {heartbeat_interval_secs} second intervals");

    let heartbeat_interval = Duration::from_secs(u64::from(heartbeat_interval_secs));
    let heartbeat_timer = tokio::time::interval(heartbeat_interval);

    let check_interval = Duration::from_millis(100);
    let check_timer = tokio::time::interval(check_interval);

    tokio::pin!(heartbeat_timer);
    tokio::pin!(check_timer);

    loop {
        if signal.load(Ordering::Relaxed) {
            log::debug!("Received heartbeat terminate signal");
            break;
        }

        tokio::select! {
            _ = heartbeat_timer.tick() => {
                let heartbeat = create_heartbeat_msg();
                if let Err(e) = pub_tx.send(heartbeat) {
                    // We expect an error if the channel is closed during shutdown
                    log::debug!("Error sending heartbeat: {e}");
                }
            },
            _ = check_timer.tick() => {}
        }
    }

    log_task_stopped("heartbeat");
}

fn create_heartbeat_msg() -> BusMessage {
    let payload = Bytes::from(chrono::Utc::now().to_rfc3339().into_bytes());
    BusMessage::with_str_topic(
        HEARTBEAT_TOPIC,
        BusPayloadType::Custom(Ustr::default()),
        payload,
        SerializationEncoding::default(),
    )
}

#[cfg(test)]
mod tests {
    use nautilus_common::{msgbus::external_io_from_backing, testing::wait_until_async};
    use redis::Value;
    use rstest::*;
    use serde_json::json;

    use super::*;

    #[rstest]
    fn test_default_redis_message_bus_config() {
        let config = RedisMessageBusConfig::default();

        assert_eq!(config.host, None);
        assert_eq!(config.port, None);
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
        assert!(!config.ssl);
        assert_eq!(config.connection_timeout, 20);
        assert_eq!(config.response_timeout, 20);
        assert_eq!(config.number_of_retries, 100);
        assert_eq!(config.exponent_base, 2);
        assert_eq!(config.max_delay, 1000);
        assert_eq!(config.factor, 2);
    }

    #[rstest]
    fn test_deserialize_redis_message_bus_config() {
        let config_json = json!({
            "host": "localhost",
            "port": 6379,
            "username": "user",
            "password": "pass",
            "ssl": true,
            "connection_timeout": 30,
            "response_timeout": 10,
            "number_of_retries": 3,
            "exponent_base": 2,
            "max_delay": 10,
            "factor": 2
        });

        let config: RedisMessageBusConfig = serde_json::from_value(config_json).unwrap();

        assert_eq!(config.host, Some("localhost".to_string()));
        assert_eq!(config.port, Some(6379));
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
        assert!(config.ssl);
        assert_eq!(config.connection_timeout, 30);
        assert_eq!(config.response_timeout, 10);
        assert_eq!(config.number_of_retries, 3);
        assert_eq!(config.exponent_base, 2);
        assert_eq!(config.max_delay, 10);
        assert_eq!(config.factor, 2);
    }

    #[rstest]
    fn test_deserialize_redis_message_bus_config_rejects_type_selector() {
        let config_json = json!({
            "type": "redis",
        });

        let error = serde_json::from_value::<RedisMessageBusConfig>(config_json).unwrap_err();

        assert!(error.to_string().contains("unknown field `type`"));
    }

    #[rstest]
    fn test_decode_bus_message_valid() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::BulkString(b"QuoteTick".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::BulkString(b"msgpack".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload_type, BusPayloadType::QuoteTick);
        assert_eq!(msg.encoding, SerializationEncoding::MsgPack);
        assert_eq!(msg.payload, Bytes::from("data1"));
    }

    #[rstest]
    fn test_decode_bus_message_defaults_legacy_headers() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload_type, BusPayloadType::Custom(Ustr::default()));
        assert_eq!(msg.encoding, SerializationEncoding::Json);
        assert_eq!(msg.payload, Bytes::from("data1"));
    }

    #[rstest]
    fn test_decode_bus_message_unknown_type_is_custom() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::BulkString(b"UnknownPayload".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_ok());
        let msg = result.unwrap();
        assert_eq!(
            msg.payload_type,
            BusPayloadType::Custom(Ustr::from("UnknownPayload"))
        );
        assert_eq!(msg.encoding, SerializationEncoding::Json);
    }

    #[rstest]
    fn test_decode_bus_message_accepts_unordered_metadata_fields() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::BulkString(b"msgpack".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::BulkString(b"TradeTick".to_vec()),
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
        ]);

        let msg = decode_bus_message(&stream_msg).unwrap();

        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload_type, BusPayloadType::TradeTick);
        assert_eq!(msg.encoding, SerializationEncoding::MsgPack);
        assert_eq!(msg.payload, Bytes::from("data1"));
    }

    #[rstest]
    fn test_decode_bus_message_rejects_invalid_encoding_header() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::BulkString(b"invalid".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let error = decode_bus_message(&stream_msg).unwrap_err();

        assert!(
            error.to_string().contains("Error parsing encoding"),
            "{error:?}"
        );
    }

    #[rstest]
    fn test_decode_bus_message_missing_fields() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"0".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid stream message format: array([bulk-string('\"0\"'), bulk-string('\"topic1\"')])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_topic_format() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::Int(42),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid topic format: array([bulk-string('\"topic\"'), int(42), bulk-string('\"payload\"'), bulk-string('\"data1\"')])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_type_format() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"type".to_vec()),
            Value::Int(42),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid type format: array([bulk-string('\"topic\"'), bulk-string('\"topic1\"'), bulk-string('\"type\"'), int(42), bulk-string('\"payload\"'), bulk-string('\"data1\"')])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_encoding_format() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"encoding".to_vec()),
            Value::Int(42),
            Value::BulkString(b"payload".to_vec()),
            Value::BulkString(b"data1".to_vec()),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid encoding format: array([bulk-string('\"topic\"'), bulk-string('\"topic1\"'), bulk-string('\"encoding\"'), int(42), bulk-string('\"payload\"'), bulk-string('\"data1\"')])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_payload_format() {
        let stream_msg = Value::Array(vec![
            Value::BulkString(b"topic".to_vec()),
            Value::BulkString(b"topic1".to_vec()),
            Value::BulkString(b"payload".to_vec()),
            Value::Int(42),
        ]);

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid payload format: array([bulk-string('\"topic\"'), bulk-string('\"topic1\"'), bulk-string('\"payload\"'), int(42)])"
        );
    }

    #[rstest]
    fn test_decode_bus_message_invalid_stream_msg_format() {
        let stream_msg = Value::BulkString(b"not an array".to_vec());

        let result = decode_bus_message(&stream_msg);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "Invalid stream message format: bulk-string('\"not an array\"')"
        );
    }

    #[rstest]
    fn test_new_rejects_zero_heartbeat_interval() {
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            heartbeat_interval_secs: Some(0),
            ..Default::default()
        };

        let result = RedisMessageBusBacking::new(
            trader_id,
            instance_id,
            config,
            RedisMessageBusConfig::default(),
        );

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "heartbeat_interval_secs must be greater than 0"
        );
    }

    #[rstest]
    fn test_stream_retry_delay_uses_config_bounds() {
        let config = RedisMessageBusConfig {
            factor: 10,
            exponent_base: 2,
            max_delay: 1,
            ..Default::default()
        };

        assert_eq!(retry_delay(&config, 0), Duration::from_millis(10));
        assert_eq!(retry_delay(&config, 1), Duration::from_millis(20));
        assert_eq!(retry_delay(&config, 10), Duration::from_secs(1));
    }

    #[rstest]
    fn test_stream_error_retry_classification() {
        let dropped =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        let client: redis::RedisError = (redis::ErrorKind::Client, "client error").into();

        assert!(is_retryable_error(&dropped));
        assert!(!is_retryable_error(&client));
    }

    #[tokio::test]
    async fn test_wait_for_retry_delay_returns_false_when_signaled() {
        let stream_signal = Arc::new(AtomicBool::new(true));
        let signal = stream_signal.clone();
        let fut = async move { wait_for_retry_delay(Duration::from_secs(30), &signal).await };

        let handle = tokio::spawn(fut);

        wait_until_async(|| async { handle.is_finished() }, Duration::from_secs(1)).await;

        assert!(!handle.await.unwrap());
    }

    #[rstest]
    fn test_external_io_from_backing_takes_stream_receiver() {
        let (stream_tx, stream_rx) = tokio::sync::mpsc::channel::<BusMessage>(1);
        let backing = backing_with_stream_receiver(stream_rx);
        let message = BusMessage::with_str_topic(
            "events/data",
            BusPayloadType::QuoteTick,
            Bytes::from_static(b"payload"),
            SerializationEncoding::Json,
        );

        let (_egress, mut ingress) = external_io_from_backing(Box::new(backing));
        stream_tx.try_send(message.clone()).unwrap();
        let mut receiver = ingress.take_receiver().unwrap();
        let received = receiver.try_recv().unwrap();

        assert_eq!(received.topic, message.topic);
        assert_eq!(received.payload, message.payload);
        assert!(ingress.take_receiver().is_err());
    }

    fn backing_with_stream_receiver(
        stream_rx: tokio::sync::mpsc::Receiver<BusMessage>,
    ) -> RedisMessageBusBacking {
        let (pub_tx, _pub_rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
        RedisMessageBusBacking {
            trader_id: TraderId::from("tester-001"),
            instance_id: UUID4::new(),
            pub_tx,
            pub_handle: None,
            pub_state: WriterState::new().0,
            stream_rx: Some(stream_rx),
            stream_handle: None,
            stream_signal: Arc::new(AtomicBool::new(false)),
            heartbeat_handle: None,
            heartbeat_signal: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[cfg(target_os = "linux")] // Run Redis tests on Linux platforms only
#[cfg(test)]
mod serial_tests {
    use std::{sync::mpsc, thread};

    use nautilus_common::{
        enums::Environment,
        msgbus::{self, TypedHandler},
        testing::wait_until_async,
    };
    use nautilus_live::{
        builder::LiveNodeBuilder,
        config::{LiveExecEngineConfig, LiveNodeConfig},
    };
    use nautilus_model::data::{QuoteTick, TradeTick};
    use redis::aio::ConnectionManager;
    use rstest::*;

    use super::{test_proxy::RedisProxy, *};

    #[fixture]
    async fn redis_connection() -> ConnectionManager {
        let config = RedisMessageBusConfig {
            connection_timeout: 1,
            number_of_retries: 0,
            ..Default::default()
        };
        create_redis_connection(MSGBUS_STREAM, &config)
            .await
            .expect("A running Redis service is required for this test")
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_terminate_signal(#[future] redis_connection: ConnectionManager) {
        let _con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = stream_signal.clone();

        // Start the message streaming task
        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        stream_signal.store(true, Ordering::Relaxed);
        let _ = rx.recv().await; // Wait for the tx to close

        // Shutdown and cleanup
        rx.close();
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_when_receiver_closed(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = stream_signal.clone();

        // Use a message ID in the future, as streaming begins
        // around the timestamp the task is spawned.
        let clock = get_atomic_clock_realtime();
        let future_id = (clock.get_time_ms() + 1_000_000).to_string();

        // Publish test message
        let _: () = con
            .xadd(
                stream_key,
                future_id,
                &[("topic", "topic1"), ("payload", "data1")],
            )
            .await
            .unwrap();

        // Immediately close channel
        rx.close();

        // Start the message streaming task
        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        // Shutdown and cleanup
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages(#[future] redis_connection: ConnectionManager) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = stream_signal.clone();

        // Use a message ID in the future, as streaming begins
        // around the timestamp the task is spawned.
        let clock = get_atomic_clock_realtime();
        let future_id = (clock.get_time_ms() + 1_000_000).to_string();

        // Publish test message
        let _: () = con
            .xadd(
                stream_key,
                future_id,
                &[("topic", "topic1"), ("payload", "data1")],
            )
            .await
            .unwrap();

        // Start the message streaming task
        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        // Receive and verify the message
        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "topic1");
        assert_eq!(msg.payload, Bytes::from("data1"));

        // Shutdown and cleanup
        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_skips_preexisting_entries(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let suffix = UUID4::new();
        let stream_key = format!("test:stream:no-backlog:{suffix}");
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = stream_signal.clone();

        let _: () = con
            .xadd(
                &stream_key,
                "1-0",
                &[("topic", "preexisting"), ("payload", "old")],
            )
            .await
            .unwrap();

        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        let future_id = (get_atomic_clock_realtime().get_time_ms() + 1_000_000).to_string();
        let _: () = con
            .xadd(
                &stream_key,
                future_id,
                &[("topic", "live"), ("payload", "new")],
            )
            .await
            .unwrap();

        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;

        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
        let _: usize = con.del(stream_key).await.unwrap();

        assert_eq!(msg.topic, "live");
        assert_eq!(msg.payload, Bytes::from("new"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_skips_malformed_entry(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let suffix = UUID4::new();
        let stream_key = format!("test:stream:malformed:{suffix}");
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = stream_signal.clone();

        let clock = get_atomic_clock_realtime();
        let base_id = clock.get_time_ms() + 1_000_000;

        let _: () = con
            .xadd(
                &stream_key,
                format!("{}", base_id + 1),
                &[("topic", "missing-payload")],
            )
            .await
            .unwrap();
        let _: () = con
            .xadd(
                &stream_key,
                format!("{}", base_id + 2),
                &[("topic", "valid"), ("payload", "data")],
            )
            .await
            .unwrap();

        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;

        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();

        assert_eq!(msg.topic, "valid");
        assert_eq!(msg.payload, Bytes::from("data"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_returns_unrecoverable_read_error(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, _rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        let suffix = UUID4::new();
        let stream_key = format!("test:stream:wrong-type:{suffix}");
        let external_streams = vec![stream_key.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));

        let _: () = con.set(&stream_key, "not-a-stream").await.unwrap();

        let result = stream_messages(
            tx,
            RedisMessageBusConfig::default(),
            external_streams,
            stream_signal,
        )
        .await;

        let _: () = con.del(&stream_key).await.unwrap();

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Error reading from stream")
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_connection_returns_none_when_signaled() {
        let config = RedisMessageBusConfig {
            port: Some(1),
            connection_timeout: 20,
            ..Default::default()
        };
        let stream_signal = Arc::new(AtomicBool::new(true));
        let signal = stream_signal.clone();
        let handle = tokio::spawn(async move { connect_stream_connection(&config, &signal).await });

        wait_until_async(|| async { handle.is_finished() }, Duration::from_secs(1)).await;

        assert!(handle.await.unwrap().unwrap().is_none());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages(#[future] redis_connection: ConnectionManager) {
        let mut con = redis_connection.await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();

        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        // Start the publish_messages task
        let handle = tokio::spawn(async move {
            publish_messages(
                rx,
                trader_id,
                instance_id,
                config,
                RedisMessageBusConfig::default(),
            )
            .await
            .unwrap();
        });

        // Send a test message
        let msg = BusMessage::with_str_topic(
            "test_topic",
            BusPayloadType::QuoteTick,
            Bytes::from("test_payload"),
            SerializationEncoding::Json,
        );
        tx.send(msg).unwrap();

        // Wait until the message is published to Redis
        wait_until_async(
            || {
                let mut con = con.clone();
                let stream_key = stream_key.clone();
                async move {
                    let messages: RedisStreamBulk =
                        con.xread(&[&stream_key], &["0"]).await.unwrap();
                    !messages.is_empty()
                }
            },
            Duration::from_secs(3),
        )
        .await;

        // Verify the message was published to Redis
        let messages: RedisStreamBulk = con.xread(&[&stream_key], &["0"]).await.unwrap();
        assert_eq!(messages.len(), 1);
        let stream_msgs = messages[0].get(&stream_key).unwrap();
        let stream_msg_array = &stream_msgs[0].values().next().unwrap();
        let decoded_message = decode_bus_message(stream_msg_array).unwrap();
        assert_eq!(decoded_message.topic, "test_topic");
        assert_eq!(decoded_message.payload_type, BusPayloadType::QuoteTick);
        assert_eq!(decoded_message.encoding, SerializationEncoding::Json);
        assert_eq!(decoded_message.payload, Bytes::from("test_payload"));

        // Stop publishing task
        let msg = BusMessage::new_close();
        tx.send(msg).unwrap();

        // Shutdown and cleanup
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages_applies_autotrim_maxlen(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            buffer_interval_ms: Some(10),
            autotrim_mins: Some(60),
            autotrim_maxlen: Some(10),
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        let handle = tokio::spawn(async move {
            publish_messages(
                rx,
                trader_id,
                instance_id,
                config,
                RedisMessageBusConfig::default(),
            )
            .await
            .unwrap();
        });
        let msg = BusMessage::with_str_topic(
            "test_topic",
            BusPayloadType::QuoteTick,
            Bytes::from_static(b"test_payload"),
            SerializationEncoding::Json,
        );

        // The integration service uses Redis's default stream node limits,
        // so these writes span multiple macro nodes.
        let messages_sent = 250;

        for _ in 0..messages_sent {
            tx.send(msg.clone()).unwrap();
        }
        tx.send(BusMessage::new_close()).unwrap();
        handle.await.unwrap();

        let stream_len: usize = con.xlen(&stream_key).await.unwrap();
        let _: usize = con.del(stream_key).await.unwrap();

        assert!(stream_len >= 10);
        assert!(stream_len < messages_sent);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_messages_retries_until_redis_answers(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let proxy = RedisProxy::start().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let backing = RedisMessageBusConfig {
            port: Some(proxy.port),
            connection_timeout: 1,
            response_timeout: 1,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let handle = tokio::spawn(async move {
            publish_messages(rx, trader_id, instance_id, config, backing)
                .await
                .unwrap();
        });
        let numbered = |seq: u32| {
            BusMessage::with_str_topic(
                "test_topic",
                BusPayloadType::Custom(Ustr::default()),
                Bytes::from(seq.to_string()),
                SerializationEncoding::Json,
            )
        };

        tx.send(numbered(1)).unwrap();
        wait_until_async(
            || {
                let mut con = con.clone();
                let stream_key = stream_key.clone();
                async move { con.xlen::<_, usize>(&stream_key).await.unwrap() == 1 }
            },
            Duration::from_secs(3),
        )
        .await;
        proxy.pause();
        for seq in 2..=4 {
            tx.send(numbered(seq)).unwrap();
        }
        // A second held write is the retry of the first, sent after it timed out
        wait_until_async(
            || async { proxy.held_writes() >= 2 },
            Duration::from_secs(10),
        )
        .await;
        proxy.resume();
        tx.send(numbered(5)).unwrap();
        tx.send(BusMessage::new_close()).unwrap();
        handle.await.unwrap();

        let messages: RedisStreamBulk = con.xread(&[&stream_key], &["0"]).await.unwrap();
        let _: usize = con.del(&stream_key).await.unwrap();
        let mut published: Vec<u32> = messages[0][&stream_key]
            .iter()
            .flat_map(|entry| entry.values())
            .map(|fields| {
                let payload = decode_bus_message(fields).unwrap().payload;
                std::str::from_utf8(&payload).unwrap().parse().unwrap()
            })
            .collect();
        let mut seen = std::collections::HashSet::new();
        published.retain(|seq| seen.insert(*seq));

        assert_eq!(published, vec![1, 2, 3, 4, 5]);
    }

    fn numbered(seq: u32) -> BusMessage {
        BusMessage::with_str_topic(
            "test_topic",
            BusPayloadType::Custom(Ustr::default()),
            Bytes::from(seq.to_string()),
            SerializationEncoding::Json,
        )
    }

    async fn published_numbers(con: &mut ConnectionManager, stream_key: &str) -> Vec<u32> {
        let messages: RedisStreamBulk = con.xread(&[stream_key], &["0"]).await.unwrap();
        let Some(streams) = messages.first() else {
            return Vec::new();
        };
        streams[stream_key]
            .iter()
            .flat_map(|entry| entry.values())
            .map(|fields| {
                let payload = decode_bus_message(fields).unwrap().payload;
                std::str::from_utf8(&payload).unwrap().parse().unwrap()
            })
            .collect()
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_publisher_whose_setup_goes_unanswered_delivers(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let proxy = RedisProxy::start().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let backing = RedisMessageBusConfig {
            port: Some(proxy.port),
            connection_timeout: 1,
            response_timeout: 1,
            number_of_retries: 0,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        proxy.lose_next_request();
        let handle = tokio::spawn(async move {
            publish_messages(rx, trader_id, instance_id, config, backing)
                .await
                .unwrap();
        });
        tx.send(numbered(1)).unwrap();
        tx.send(BusMessage::new_close()).unwrap();
        handle.await.unwrap();

        let published = published_numbers(&mut con, &stream_key).await;
        let _: usize = con.del(&stream_key).await.unwrap();

        assert_eq!(published, vec![1]);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_live_nodes_publish_and_ingest_external_redis_stream(
        #[future] redis_connection: ConnectionManager,
    ) {
        let _con = redis_connection.await;
        let redis_config = RedisMessageBusConfig::default();
        let trader_a = TraderId::from("NODEA-001");
        let instance_a = UUID4::new();
        let node_a_msgbus = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_a, instance_a, &node_a_msgbus);
        let node_b_msgbus = MessageBusConfig {
            external_streams: Some(vec![stream_key]),
            stream_per_topic: false,
            ..Default::default()
        };
        let quote = QuoteTick::default();
        let trade = TradeTick::default();
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (quote_tx, quote_rx) = mpsc::channel::<QuoteTick>();
        let (trade_tx, trade_rx) = mpsc::channel::<TradeTick>();

        let node_b = thread::spawn({
            let redis_config = redis_config.clone();
            move || -> anyhow::Result<()> {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()?;

                runtime.block_on(async move {
                    let config = LiveNodeConfig {
                        environment: Environment::Sandbox,
                        trader_id: TraderId::from("NODEB-001"),
                        msgbus: Some(node_b_msgbus),
                        exec_engine: LiveExecEngineConfig {
                            reconciliation: false,
                            ..Default::default()
                        },
                        delay_post_stop: Duration::ZERO,
                        timeout_connection: Duration::from_millis(500),
                        timeout_disconnection: Duration::from_millis(500),
                        ..Default::default()
                    };
                    let mut node = LiveNodeBuilder::from_config(config)?
                        .with_external_msgbus_factory(Box::new(RedisMessageBusFactory::new(
                            redis_config,
                        )))
                        .build()?;
                    let handle = node.handle();
                    let quote_handler = TypedHandler::from({
                        let quote_tx = quote_tx.clone();
                        let handle = handle.clone();
                        move |quote: &QuoteTick| {
                            let _ = quote_tx.send(*quote);
                            handle.stop();
                        }
                    });
                    let trade_handler = TypedHandler::from(move |trade: &TradeTick| {
                        let _ = trade_tx.send(*trade);
                    });

                    msgbus::subscribe_quotes("data.quotes.*".into(), quote_handler, None);
                    msgbus::subscribe_trades("data.trades.*".into(), trade_handler, None);
                    msgbus::get_message_bus()
                        .borrow_mut()
                        .add_streaming_type(BusPayloadType::QuoteTick);
                    let result = tokio::time::timeout(Duration::from_secs(10), async {
                        let run = node.run();
                        tokio::pin!(run);

                        let announce_ready = async {
                            for _ in 0..100 {
                                if handle.is_running() {
                                    ready_tx.send(())?;
                                    return Ok(());
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }

                            anyhow::bail!("node B did not reach running state")
                        };

                        tokio::select! {
                            result = &mut run => result,
                            ready = announce_ready => {
                                ready?;
                                run.await
                            }
                        }
                    })
                    .await;
                    msgbus::get_message_bus().borrow_mut().dispose();

                    match result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(e) => anyhow::bail!("node B timed out: {e}"),
                    }
                })
            }
        });

        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("node B should start Redis ingress");

        let node_a = thread::spawn(move || -> anyhow::Result<()> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;

            runtime.block_on(async move {
                let config = LiveNodeConfig {
                    environment: Environment::Sandbox,
                    trader_id: trader_a,
                    instance_id: Some(instance_a),
                    msgbus: Some(node_a_msgbus),
                    exec_engine: LiveExecEngineConfig {
                        reconciliation: false,
                        ..Default::default()
                    },
                    delay_post_stop: Duration::ZERO,
                    timeout_connection: Duration::from_millis(500),
                    timeout_disconnection: Duration::from_millis(500),
                    ..Default::default()
                };
                let _node = LiveNodeBuilder::from_config(config)?
                    .with_external_msgbus_factory(Box::new(RedisMessageBusFactory::new(
                        redis_config,
                    )))
                    .build()?;

                msgbus::publish_trade("data.trades.TEST".into(), &trade);
                msgbus::publish_quote("data.quotes.TEST".into(), &quote);
                msgbus::get_message_bus().borrow_mut().dispose();

                Ok(())
            })
        });

        node_a
            .join()
            .expect("node A thread should not panic")
            .expect("node A should publish externally");
        let received_quote = quote_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("node B should republish the registered quote type");
        node_b
            .join()
            .expect("node B thread should not panic")
            .expect("node B should ingest and stop cleanly");

        assert_eq!(received_quote, quote);
        assert!(
            trade_rx.try_recv().is_err(),
            "unregistered trade type should not republish internally"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_multiple_streams(#[future] redis_connection: ConnectionManager) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        // Setup multiple stream keys
        let suffix = UUID4::new();
        let stream_key1 = format!("test:stream:{suffix}:1");
        let stream_key2 = format!("test:stream:{suffix}:2");
        let external_streams = vec![stream_key1.clone(), stream_key2.clone()];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = stream_signal.clone();

        let clock = get_atomic_clock_realtime();
        let base_id = clock.get_time_ms() + 1_000_000;

        // Start streaming task
        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        // Publish to stream 1 at higher ID
        let _: () = con
            .xadd(
                &stream_key1,
                format!("{}", base_id + 100),
                &[("topic", "stream1-first"), ("payload", "data")],
            )
            .await
            .unwrap();

        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "stream1-first");

        // Publish to stream 2 at lower ID (tests independent cursor tracking)
        let _: () = con
            .xadd(
                &stream_key2,
                format!("{}", base_id + 50),
                &[("topic", "stream2-second"), ("payload", "data")],
            )
            .await
            .unwrap();

        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "stream2-second");

        // Shutdown and cleanup
        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_messages_interleaved_at_different_rates(
        #[future] redis_connection: ConnectionManager,
    ) {
        let mut con = redis_connection.await;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<BusMessage>(100);

        // Setup multiple stream keys
        let suffix = UUID4::new();
        let stream_key1 = format!("test:stream:interleaved:{suffix}:1");
        let stream_key2 = format!("test:stream:interleaved:{suffix}:2");
        let stream_key3 = format!("test:stream:interleaved:{suffix}:3");
        let external_streams = vec![
            stream_key1.clone(),
            stream_key2.clone(),
            stream_key3.clone(),
        ];
        let stream_signal = Arc::new(AtomicBool::new(false));
        let stream_signal_clone = stream_signal.clone();

        let clock = get_atomic_clock_realtime();
        let base_id = clock.get_time_ms() + 1_000_000;

        let handle = tokio::spawn(async move {
            stream_messages(
                tx,
                RedisMessageBusConfig::default(),
                external_streams,
                stream_signal_clone,
            )
            .await
            .unwrap();
        });

        // Stream 1 advances with high ID
        let _: () = con
            .xadd(
                &stream_key1,
                format!("{}", base_id + 100),
                &[("topic", "s1m1"), ("payload", "data")],
            )
            .await
            .unwrap();
        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "s1m1");

        // Stream 2 gets message at lower ID - would be skipped with global cursor
        let _: () = con
            .xadd(
                &stream_key2,
                format!("{}", base_id + 50),
                &[("topic", "s2m1"), ("payload", "data")],
            )
            .await
            .unwrap();
        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "s2m1");

        // Stream 3 gets message at even lower ID
        let _: () = con
            .xadd(
                &stream_key3,
                format!("{}", base_id + 25),
                &[("topic", "s3m1"), ("payload", "data")],
            )
            .await
            .unwrap();
        let msg = receive_bus_message(&mut rx, Duration::from_secs(2)).await;
        assert_eq!(msg.topic, "s3m1");

        // Shutdown and cleanup
        rx.close();
        stream_signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close() {
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let mut db = RedisMessageBusBacking::new(
            trader_id,
            instance_id,
            config,
            RedisMessageBusConfig::default(),
        )
        .unwrap();

        // Close the message bus backing (test should not hang)
        MessageBusBacking::close(&mut db);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_heartbeat_task() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<BusMessage>();
        let signal = Arc::new(AtomicBool::new(false));

        // Start the heartbeat task with a short interval
        let handle = tokio::spawn(run_heartbeat(1, signal.clone(), tx));

        let heartbeat = receive_unbounded_bus_message(&mut rx, Duration::from_secs(2)).await;

        // Stop the heartbeat task
        signal.store(true, Ordering::Relaxed);
        handle.await.unwrap();

        // Ensure heartbeats were sent
        assert_eq!(heartbeat.topic, HEARTBEAT_TOPIC);
    }

    async fn receive_bus_message(
        rx: &mut tokio::sync::mpsc::Receiver<BusMessage>,
        timeout: Duration,
    ) -> BusMessage {
        let mut received = None;

        wait_until_async(
            || {
                if received.is_none() {
                    received = rx.try_recv().ok();
                }

                let has_received = received.is_some();
                async move { has_received }
            },
            timeout,
        )
        .await;

        received.unwrap()
    }

    async fn receive_unbounded_bus_message(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<BusMessage>,
        timeout: Duration,
    ) -> BusMessage {
        let mut received = None;

        wait_until_async(
            || {
                if received.is_none() {
                    received = rx.try_recv().ok();
                }

                let has_received = received.is_some();
                async move { has_received }
            },
            timeout,
        )
        .await;

        received.unwrap()
    }
}

/// A TCP proxy in front of the test Redis that holds traffic as a paused server does, severs its
/// connections as a restarting server does, loses one reply after Redis has acted on it, or holds
/// one connection's requests apart while the client connects again.
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod test_proxy {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{
            TcpListener, TcpStream,
            tcp::{OwnedReadHalf, OwnedWriteHalf},
        },
        sync::{oneshot, watch},
    };

    const REDIS_ADDR: &str = "127.0.0.1:6379";
    const EXEC: &[u8] = b"EXEC\r\n";
    const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";
    const PONG: &[u8] = b"+PONG\r\n";

    #[derive(Clone)]
    struct Controls {
        pause: Arc<watch::Sender<bool>>,
        paused: watch::Receiver<bool>,
        severed: watch::Receiver<usize>,
        lose_next_reply: Arc<AtomicBool>,
        lose_next_request: Arc<AtomicBool>,
        pause_at_exec: Arc<AtomicBool>,
        pause_after_exec: Arc<AtomicBool>,
        sever_on_exec: Arc<AtomicBool>,
        held_writes: Arc<AtomicUsize>,
    }

    /// One relayed connection's own controls.
    struct Connection {
        stalled: AtomicBool,
        stalled_requests: Mutex<Vec<u8>>,
        drop_client: watch::Sender<bool>,
        release: Mutex<Option<oneshot::Sender<oneshot::Sender<()>>>>,
    }

    pub(crate) struct RedisProxy {
        pub(crate) port: u16,
        controls: Controls,
        severed: watch::Sender<usize>,
        connections: Arc<Mutex<Vec<Arc<Connection>>>>,
    }

    impl RedisProxy {
        pub(crate) async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (pause, paused) = watch::channel(false);
            let (severed, severed_rx) = watch::channel(0);
            let controls = Controls {
                pause: Arc::new(pause),
                paused,
                severed: severed_rx,
                lose_next_reply: Arc::new(AtomicBool::new(false)),
                lose_next_request: Arc::new(AtomicBool::new(false)),
                pause_at_exec: Arc::new(AtomicBool::new(false)),
                pause_after_exec: Arc::new(AtomicBool::new(false)),
                sever_on_exec: Arc::new(AtomicBool::new(false)),
                held_writes: Arc::new(AtomicUsize::new(0)),
            };
            let connections: Arc<Mutex<Vec<Arc<Connection>>>> = Arc::default();
            let proxy = Self {
                port,
                controls: controls.clone(),
                severed,
                connections: connections.clone(),
            };

            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    let server = TcpStream::connect(REDIS_ADDR).await.unwrap();
                    let (release, released) = oneshot::channel();
                    let connection = Arc::new(Connection {
                        stalled: AtomicBool::new(false),
                        stalled_requests: Mutex::new(Vec::new()),
                        drop_client: watch::Sender::new(false),
                        release: Mutex::new(Some(release)),
                    });
                    connections.lock().unwrap().push(connection.clone());
                    tokio::spawn(relay(
                        client,
                        server,
                        controls.clone(),
                        connection,
                        released,
                    ));
                }
            });

            proxy
        }

        /// Holds every byte in both directions until `resume`, as a paused server does.
        pub(crate) fn pause(&self) {
            self.controls.pause.send_replace(true);
        }

        pub(crate) fn resume(&self) {
            self.controls.pause.send_replace(false);
        }

        /// Pauses as `pause` does on the next request holding an `EXEC`, holding that request too.
        pub(crate) fn pause_at_next_exec(&self) {
            self.controls.pause_at_exec.store(true, Ordering::SeqCst);
        }

        /// Pauses as `pause` does right after forwarding the next request holding an `EXEC`.
        pub(crate) fn pause_after_next_exec(&self) {
            self.controls.pause_after_exec.store(true, Ordering::SeqCst);
        }

        /// Closes every open connection, discarding what it holds, as a restarting server does.
        pub(crate) fn sever(&self) {
            self.severed.send_modify(|generation| *generation += 1);
        }

        /// Discards the next request holding an `EXEC` and closes its connection, so Redis never
        /// runs that transaction.
        pub(crate) fn sever_on_next_exec(&self) {
            self.controls.sever_on_exec.store(true, Ordering::SeqCst);
        }

        /// Discards the next request a client sends, leaving its connection open, so the client
        /// waits for an answer that never comes.
        pub(crate) fn lose_next_request(&self) {
            self.controls
                .lose_next_request
                .store(true, Ordering::SeqCst);
        }

        /// Discards the next reply Redis sends and closes the client's connection, after Redis
        /// has acted on the request.
        pub(crate) fn lose_next_reply(&self) {
            self.controls.lose_next_reply.store(true, Ordering::SeqCst);
        }

        /// The number of client writes that arrived while traffic was held.
        pub(crate) fn held_writes(&self) -> usize {
            self.controls.held_writes.load(Ordering::SeqCst)
        }

        /// Holds the requests of every connection now open, apart from any connection opened
        /// later, until `release_stalled`.
        pub(crate) fn stall(&self) {
            for connection in self.connections.lock().unwrap().iter() {
                connection.stalled.store(true, Ordering::SeqCst);
            }
        }

        /// The number of request bytes the stalled connections hold.
        pub(crate) fn stalled_bytes(&self) -> usize {
            self.connections
                .lock()
                .unwrap()
                .iter()
                .map(|connection| connection.stalled_requests.lock().unwrap().len())
                .sum()
        }

        /// Closes the stalled connections towards the client only: Redis keeps serving each, and
        /// is yet to receive what it holds.
        pub(crate) fn drop_stalled_clients(&self) {
            for connection in self.connections.lock().unwrap().iter() {
                if connection.stalled.load(Ordering::SeqCst) {
                    connection.drop_client.send_replace(true);
                }
            }
        }

        /// Forwards to Redis what each stalled connection holds, followed by a `PING`, and
        /// returns once Redis has answered it or closed the connection.
        pub(crate) async fn release_stalled(&self) {
            let releases: Vec<oneshot::Sender<oneshot::Sender<()>>> = self
                .connections
                .lock()
                .unwrap()
                .iter()
                .filter(|connection| connection.stalled.load(Ordering::SeqCst))
                .filter_map(|connection| connection.release.lock().unwrap().take())
                .collect();
            for release in releases {
                let (done, released) = oneshot::channel();
                if release.send(done).is_ok() {
                    let _ = released.await;
                }
            }
        }
    }

    async fn relay(
        client: TcpStream,
        server: TcpStream,
        controls: Controls,
        connection: Arc<Connection>,
        released: oneshot::Receiver<oneshot::Sender<()>>,
    ) {
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let mut severed = controls.severed.clone();
        severed.mark_unchanged();
        let mut drop_client = connection.drop_client.subscribe();

        tokio::select! {
            () = forward_requests(&mut client_read, &mut server_write, controls.clone(), &connection) => return,
            () = forward_replies(&mut server_read, &mut client_write, controls) => return,
            _ = severed.changed() => return,
            _ = drop_client.wait_for(|dropped| *dropped) => {}
        }

        drop(client_read);
        drop(client_write);
        if let Ok(done) = released.await {
            let mut requests = std::mem::take(&mut *connection.stalled_requests.lock().unwrap());
            requests.extend_from_slice(PING);
            if server_write.write_all(&requests).await.is_ok() {
                await_pong(&mut server_read).await;
            }
            let _ = done.send(());
        }
    }

    async fn await_pong(from: &mut OwnedReadHalf) {
        let mut replies = Vec::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match from.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    replies.extend_from_slice(&buf[..n]);
                    if replies.ends_with(PONG) {
                        return;
                    }
                }
            }
        }
    }

    async fn forward_requests(
        from: &mut OwnedReadHalf,
        to: &mut OwnedWriteHalf,
        mut controls: Controls,
        connection: &Connection,
    ) {
        let paused = controls.paused.clone();
        let mut buf = vec![0u8; 64 * 1024];
        let mut held: Vec<u8> = Vec::new();
        loop {
            tokio::select! {
                read = from.read(&mut buf) => {
                    let n = match read {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    let request = &buf[..n];
                    if connection.stalled.load(Ordering::SeqCst) {
                        connection.stalled_requests.lock().unwrap().extend_from_slice(request);
                        continue;
                    }
                    if controls.lose_next_request.swap(false, Ordering::SeqCst) {
                        continue;
                    }
                    let holds_exec = request.windows(EXEC.len()).any(|window| window == EXEC);
                    if holds_exec && controls.sever_on_exec.swap(false, Ordering::SeqCst) {
                        return;
                    }
                    if holds_exec && controls.pause_at_exec.swap(false, Ordering::SeqCst) {
                        controls.pause.send_replace(true);
                    }
                    if *paused.borrow() || !held.is_empty() {
                        controls.held_writes.fetch_add(1, Ordering::SeqCst);
                        held.extend_from_slice(request);
                    } else if to.write_all(request).await.is_err() {
                        return;
                    }
                    if holds_exec && controls.pause_after_exec.swap(false, Ordering::SeqCst) {
                        controls.pause.send_replace(true);
                    }
                }
                Ok(()) = controls.paused.changed(), if !held.is_empty() => {
                    let resumed = !*paused.borrow();
                    if resumed {
                        if to.write_all(&held).await.is_err() {
                            return;
                        }
                        held.clear();
                    }
                }
            }
        }
    }

    async fn forward_replies(
        from: &mut OwnedReadHalf,
        to: &mut OwnedWriteHalf,
        mut controls: Controls,
    ) {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match from.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            let _ = controls.paused.wait_for(|paused| !paused).await;
            if controls.lose_next_reply.swap(false, Ordering::SeqCst) {
                return;
            }
            if to.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    }
}
