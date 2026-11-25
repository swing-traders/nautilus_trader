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

//! Redis-backed cache database for the system.
//!
//! # Architecture
//!
//! Uses two Redis connections with distinct roles:
//! - **READ** (`self.con`): synchronous queries (`keys`, `read`, `load_all`),
//!   owned by the main struct.
//! - **WRITE**: owned by a background task on `get_runtime()`, receives
//!   commands via an unbounded `tokio::sync::mpsc` channel.
//!
//! All write operations (`insert`, `update`, `delete`, `flush`) are routed
//! through the command channel so they execute on the WRITE connection. This
//! avoids cross-runtime I/O issues since the WRITE connection is always
//! created on the Nautilus runtime.
//!
//! Synchronous callers (`close`, `flushdb_sync`) use `std::sync::mpsc` reply
//! channels to block until the background task confirms completion. When
//! called from the Nautilus runtime itself, `block_in_place` is used
//! automatically to avoid stalling the worker thread.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::{Debug, Write as _},
    pin::Pin,
    sync::{
        Arc,
        mpsc::{self, SyncSender},
    },
    time::Duration,
};

use ahash::AHashMap;
use anyhow::Context;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use nautilus_common::{
    cache::{
        CacheConfig,
        database::{CacheDatabaseAdapter, CacheDatabaseFactory, CacheMap},
    },
    enums::SerializationEncoding,
    live::get_runtime,
    logging::{log_task_awaiting, log_task_started, log_task_stopped},
    signal::Signal,
};
use nautilus_core::{UUID4, UnixNanos, correctness::check_slice_not_empty};
use nautilus_cryptography::providers::install_cryptographic_provider;
use nautilus_model::{
    accounts::AccountAny,
    data::{Bar, CustomData, DataType, FundingRateUpdate, HasTsInit, QuoteTick, TradeTick},
    enums::TriggerType,
    events::{
        AccountState, OrderEventAny, OrderFilled, OrderSnapshot,
        position::snapshot::PositionSnapshot,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, ComponentId, InstrumentId, PositionId, StrategyId,
        TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny, SyntheticInstrument},
    orderbook::OrderBook,
    orders::{Order, OrderAny},
    position::Position,
    types::{Currency, Money},
};
use redis::{
    AsyncCommands, AsyncConnectionConfig, Pipeline,
    aio::{ConnectionManager, MultiplexedConnection},
};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{
    CountRequest, REDIS_DELIMITER, REDIS_FLUSHDB, WriterState, await_writer, check_redis_version,
    get_index_key, info_field, is_retryable_error, retry_delay,
};
use crate::redis::{
    RedisConnectionConfig, create_redis_connection, get_redis_url, queries::DatabaseQueries,
};

// Task and connection names
const CACHE_READ: &str = "cache-read";
const CACHE_WRITE: &str = "cache-write";
const CACHE_PROCESS: &str = "cache-process";

// Error constants
const FAILED_TX_CHANNEL: &str = "Failed to send to channel";

// Collection keys
const INDEX: &str = "index";
const GENERAL: &str = "general";
const CURRENCIES: &str = "currencies";
const INSTRUMENTS: &str = "instruments";
const SYNTHETICS: &str = "synthetics";
const ACCOUNTS: &str = "accounts";
const ORDERS: &str = "orders";
const POSITIONS: &str = "positions";
const ACTORS: &str = "actors";
const STRATEGIES: &str = "strategies";
const SNAPSHOTS: &str = "snapshots";
const HEALTH: &str = "health";
const CUSTOM: &str = "custom";

// Index keys
const INDEX_ORDER_IDS: &str = "index:order_ids";
const INDEX_ORDER_POSITION: &str = "index:order_position";
const INDEX_ORDER_CLIENT: &str = "index:order_client";
const INDEX_ORDERS: &str = "index:orders";
const INDEX_ORDERS_OPEN: &str = "index:orders_open";
const INDEX_ORDERS_CLOSED: &str = "index:orders_closed";
const INDEX_ORDERS_EMULATED: &str = "index:orders_emulated";
const INDEX_ORDERS_INFLIGHT: &str = "index:orders_inflight";
const INDEX_POSITIONS: &str = "index:positions";
const INDEX_POSITIONS_OPEN: &str = "index:positions_open";
const INDEX_POSITIONS_CLOSED: &str = "index:positions_closed";

/// Configuration for a Redis-backed cache database.
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
pub struct RedisCacheConfig {
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

impl Debug for RedisCacheConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = self.password.as_ref().map(|_| "***");
        f.debug_struct(stringify!(RedisCacheConfig))
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

impl Default for RedisCacheConfig {
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

impl RedisConnectionConfig for RedisCacheConfig {
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

/// A type of database operation.
#[derive(Clone, Debug)]
pub enum DatabaseOperation {
    Insert,
    Update,
    UpdateOrder,
    ReplaceList,
    Delete,
    Flush(SyncSender<()>),
    Close,
}

/// Represents a database command to be performed which may be executed in a task.
#[derive(Clone, Debug)]
pub struct DatabaseCommand {
    /// The database operation type.
    pub op_type: DatabaseOperation,
    /// The primary key for the operation.
    pub key: Option<String>,
    /// The data payload for the operation.
    pub payload: Option<Vec<Bytes>>,
}

impl DatabaseCommand {
    /// Creates a new [`DatabaseCommand`] instance.
    #[must_use]
    pub const fn new(op_type: DatabaseOperation, key: String, payload: Option<Vec<Bytes>>) -> Self {
        Self {
            op_type,
            key: Some(key),
            payload,
        }
    }

    /// Initialize a `Close` database command, this is meant to close the database cache channel.
    #[must_use]
    pub const fn close() -> Self {
        Self {
            op_type: DatabaseOperation::Close,
            key: None,
            payload: None,
        }
    }
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.infrastructure")
)]
pub struct RedisCacheDatabase {
    pub con: ConnectionManager,
    pub trader_id: TraderId,
    pub trader_key: String,
    pub encoding: SerializationEncoding,
    pub bulk_read_batch_size: Option<usize>,
    tx: tokio::sync::mpsc::UnboundedSender<DatabaseCommand>,
    handle: Option<tokio::task::JoinHandle<()>>,
    writer: Arc<WriterState>,
}

impl Debug for RedisCacheDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RedisCacheDatabase))
            .field("trader_id", &self.trader_id)
            .field("encoding", &self.encoding)
            .finish_non_exhaustive()
    }
}

impl RedisCacheDatabase {
    /// Creates a new [`RedisCacheDatabase`] instance for the given `trader_id`, `instance_id`, and `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database configuration is missing in `config`.
    /// - Establishing the Redis connection fails.
    /// - The command processing task cannot be spawned.
    pub async fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        config: CacheConfig,
        database: RedisCacheConfig,
    ) -> anyhow::Result<Self> {
        install_cryptographic_provider();

        let con = create_redis_connection(CACHE_READ, &database).await?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<DatabaseCommand>();
        let trader_key = get_trader_key(trader_id, instance_id, &config);
        let trader_key_clone = trader_key.clone();
        let encoding = config.encoding;
        let bulk_read_batch_size = config.bulk_read_batch_size;
        let (writer, count_requests) = WriterState::new();
        let writer_state = writer.clone();

        let handle = get_runtime().spawn(async move {
            if let Err(e) = process_commands(
                rx,
                trader_key_clone,
                config.clone(),
                database.clone(),
                writer_state,
                count_requests,
            )
            .await
            {
                log::error!("Error in task '{CACHE_PROCESS}': {e}");
            }
        });

        Ok(Self {
            con,
            trader_id,
            trader_key,
            encoding,
            bulk_read_batch_size,
            tx,
            handle: Some(handle),
            writer,
        })
    }

    #[must_use]
    pub const fn get_encoding(&self) -> SerializationEncoding {
        self.encoding
    }

    #[must_use]
    pub fn get_trader_key(&self) -> &str {
        &self.trader_key
    }

    pub fn close(&mut self) {
        log::debug!("Closing");

        let Some(handle) = self.handle.take() else {
            log::debug!("Already closed");
            return;
        };

        if let Err(e) = self.tx.send(DatabaseCommand::close()) {
            log::debug!("Error sending close command: {e:?}");
        }

        log_task_awaiting(CACHE_PROCESS);

        let (tx, rx) = mpsc::sync_channel(1);
        let writer = self.writer.clone();

        get_runtime().spawn(async move {
            await_writer(handle, CACHE_PROCESS, &writer).await;
            let _ = tx.send(());
        });
        let _ = blocking_recv(&rx);

        log::debug!("Closed");
    }

    pub async fn flushdb(&mut self) {
        if let Err(e) = redis::cmd(REDIS_FLUSHDB)
            .query_async::<()>(&mut self.con)
            .await
        {
            log::error!("Failed to flush database: {e:?}");
        }
    }

    /// Sends a flush command through the background task channel and blocks
    /// until it completes. Safe to call from any runtime context.
    ///
    /// # Errors
    ///
    /// Returns an error if the command channel is closed or the reply is lost.
    pub fn flushdb_sync(&self) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let cmd = DatabaseCommand {
            op_type: DatabaseOperation::Flush(reply_tx),
            key: None,
            payload: None,
        };
        self.tx
            .send(cmd)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))?;
        blocking_recv(&reply_rx).map_err(|e| anyhow::anyhow!("Failed to flush database: {e}"))?;
        Ok(())
    }

    /// Retrieves all keys matching the given `pattern` from Redis for this trader.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis scan operation fails.
    pub async fn keys(&mut self, pattern: &str) -> anyhow::Result<Vec<String>> {
        let pattern = format!("{}{REDIS_DELIMITER}{pattern}", self.trader_key);
        DatabaseQueries::scan_keys(&mut self.con, pattern).await
    }

    /// Reads the value(s) associated with `key` for this trader from Redis.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis read operation fails.
    pub async fn read(&mut self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        DatabaseQueries::read(&self.con, &self.trader_key, key).await
    }

    /// Reads multiple values using bulk operations for efficiency.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis read operation fails.
    pub async fn read_bulk(&mut self, keys: &[String]) -> anyhow::Result<Vec<Option<Bytes>>> {
        match self.bulk_read_batch_size {
            Some(batch_size) => {
                DatabaseQueries::read_bulk_batched(&self.con, keys, batch_size).await
            }
            None => DatabaseQueries::read_bulk(&self.con, keys).await,
        }
    }

    /// Loads custom data from Redis matching the given `data_type` (blocking).
    ///
    /// Spawns the async query on the global Nautilus runtime and blocks until
    /// the result arrives via a channel. Safe from any thread context (Python,
    /// test runtimes, plain threads).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or the reply channel is closed.
    pub fn load_custom_data(&self, data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        let con = self.con.clone();
        let trader_key = self.trader_key.clone();
        let data_type = data_type.clone();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = DatabaseQueries::load_custom_data(&con, &trader_key, &data_type).await;
            if let Err(e) = tx.send(result) {
                log::error!("Failed to send custom data result for '{data_type}': {e:?}");
            }
        });

        blocking_recv(&rx).map_err(|e| anyhow::anyhow!("load_custom_data channel closed: {e}"))?
    }

    /// Sends an insert command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn insert(&self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Insert, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Stores custom data in Redis (key format: `custom:<ts_init_020>:<uuid>`, value: full JSON).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the insert command cannot be sent.
    pub fn add_custom_data(&self, data: &CustomData) -> anyhow::Result<()> {
        let json_bytes = serde_json::to_vec(data)
            .map_err(|e| anyhow::anyhow!("CustomData serialization failed: {e}"))?;
        let ts_init = data.ts_init().as_u64();
        let key = format!(
            "{CUSTOM}{REDIS_DELIMITER}{:020}{REDIS_DELIMITER}{}",
            ts_init,
            UUID4::new()
        );
        self.insert(key, Some(vec![Bytes::from(json_bytes)]))
    }

    /// Sends an update command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn update(&mut self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Update, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Sends a delete command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete(&mut self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Delete the given order from the database with full index cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<()> {
        let order_id_bytes = Bytes::from(client_order_id.to_string());

        // Delete the order itself
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete order command: {e}"))?;

        // Delete from all order indexes
        let index_keys = [
            INDEX_ORDER_IDS,
            INDEX_ORDERS,
            INDEX_ORDERS_OPEN,
            INDEX_ORDERS_CLOSED,
            INDEX_ORDERS_EMULATED,
            INDEX_ORDERS_INFLIGHT,
        ];

        for index_key in &index_keys {
            let key = (*index_key).to_string();
            let payload = vec![order_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send delete order index command: {e}"))?;
        }

        // Delete from hash indexes
        let hash_indexes = [INDEX_ORDER_POSITION, INDEX_ORDER_CLIENT];
        for index_key in &hash_indexes {
            let key = (*index_key).to_string();
            let payload = vec![order_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx.send(op).map_err(|e| {
                anyhow::anyhow!("Failed to send delete order hash index command: {e}")
            })?;
        }

        Ok(())
    }

    /// Delete the given position from the database with full index cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_position(&self, position_id: &PositionId) -> anyhow::Result<()> {
        let position_id_bytes = Bytes::from(position_id.to_string());

        // Delete the position itself
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete position command: {e}"))?;

        // Delete from all position indexes
        let index_keys = [
            INDEX_POSITIONS,
            INDEX_POSITIONS_OPEN,
            INDEX_POSITIONS_CLOSED,
        ];

        for index_key in &index_keys {
            let key = (*index_key).to_string();
            let payload = vec![position_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx.send(op).map_err(|e| {
                anyhow::anyhow!("Failed to send delete position index command: {e}")
            })?;
        }

        Ok(())
    }

    /// Delete the given account event from the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_account_event(
        &self,
        _account_id: &AccountId,
        _event_id: &str,
    ) -> anyhow::Result<()> {
        log::warn!("Deleting account events currently a no-op (pending redesign)");
        Ok(())
    }
}

fn blocking_recv<T>(rx: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvError> {
    let on_nautilus_runtime =
        tokio::runtime::Handle::try_current().is_ok_and(|h| h.id() == get_runtime().handle().id());

    if on_nautilus_runtime {
        tokio::task::block_in_place(|| rx.recv())
    } else {
        rx.recv()
    }
}

async fn process_commands(
    rx: tokio::sync::mpsc::UnboundedReceiver<DatabaseCommand>,
    trader_key: String,
    config: CacheConfig,
    database: RedisCacheConfig,
    state: Arc<WriterState>,
    count_requests: tokio::sync::mpsc::UnboundedReceiver<CountRequest>,
) -> anyhow::Result<()> {
    log_task_started(CACHE_PROCESS);

    let mut writer = CacheWriter::new(
        rx,
        count_requests,
        trader_key,
        config.encoding,
        database,
        state,
    )?;
    writer.set_up().await?;

    // Buffering
    let mut buffer: VecDeque<DatabaseCommand> = VecDeque::new();
    let buffer_interval = Duration::from_millis(config.buffer_interval_ms.unwrap_or(0) as u64);

    // A sleep used to trigger periodic flushing of the buffer.
    // When `buffer_interval` is zero we skip using the timer and flush immediately
    // after every message.
    let flush_timer = tokio::time::sleep(buffer_interval);
    tokio::pin!(flush_timer);

    // Continue to receive and handle messages until closed or the channel is hung up
    while !writer.closing {
        tokio::select! {
            maybe_cmd = writer.rx.recv() => {
                handle_command(&mut writer, maybe_cmd, &mut buffer, buffer_interval).await;
            }
            () = &mut flush_timer, if !buffer_interval.is_zero() => {
                flush_buffer(&mut writer, &mut buffer, &mut flush_timer, buffer_interval).await;
            }
        }
    }

    // Drain any remaining messages
    if !buffer.is_empty() {
        writer.drain_buffer(&mut buffer).await;
    }

    log_task_stopped(CACHE_PROCESS);
    Ok(())
}

async fn handle_command(
    writer: &mut CacheWriter,
    maybe_cmd: Option<DatabaseCommand>,
    buffer: &mut VecDeque<DatabaseCommand>,
    buffer_interval: Duration,
) {
    let Some(cmd) = maybe_cmd else {
        log::debug!("Command channel closed");
        writer.closing = true;
        return;
    };

    log::trace!("Received {cmd:?}");

    match cmd.op_type {
        DatabaseOperation::Close => writer.closing = true,
        DatabaseOperation::Flush(_) => {
            buffer.push_back(cmd);
            writer.drain_buffer(buffer).await;
        }
        _ => {
            buffer.push_back(cmd);
            if buffer_interval.is_zero() {
                writer.drain_buffer(buffer).await;
            }
        }
    }
}

async fn flush_buffer(
    writer: &mut CacheWriter,
    buffer: &mut VecDeque<DatabaseCommand>,
    flush_timer: &mut Pin<&mut tokio::time::Sleep>,
    buffer_interval: Duration,
) {
    if !buffer.is_empty() {
        writer.drain_buffer(buffer).await;
    }
    flush_timer
        .as_mut()
        .reset(tokio::time::Instant::now() + buffer_interval);
}

/// The cache writer's connection, owned by the writer so that a connection it replaces can be
/// fenced off.
struct WriteConnection {
    con: MultiplexedConnection,
    run_id: String,
    client_id: i64,
}

/// A trader key's one cache writer: it delivers its commands in order, retrying until Redis
/// answers. While a delivery fails it takes newer commands in as they arrive, holding for each
/// replacing write only the latest to its target, and every append.
struct CacheWriter {
    rx: tokio::sync::mpsc::UnboundedReceiver<DatabaseCommand>,
    count_requests: tokio::sync::mpsc::UnboundedReceiver<CountRequest>,
    trader_key: String,
    encoding: SerializationEncoding,
    database: RedisCacheConfig,
    state: Arc<WriterState>,
    client: redis::Client,
    connection: Option<WriteConnection>,
    /// The server run and client id of every connection replaced since the last fence.
    replaced: Vec<(String, i64)>,
    held: Held,
    /// The transaction last sent whose answer never came.
    unanswered: Option<Attempt>,
    /// Consecutive unanswered requests, for the backoff.
    failures: usize,
    /// Whether a delivery is failing: from the start until the first delivery, and from any
    /// unanswered request until a delivery.
    failing: bool,
    /// Whether newer commands are taken in while the writer waits: from an unanswered request
    /// until a delivery.
    retrying: bool,
    closing: bool,
}

impl CacheWriter {
    fn new(
        rx: tokio::sync::mpsc::UnboundedReceiver<DatabaseCommand>,
        count_requests: tokio::sync::mpsc::UnboundedReceiver<CountRequest>,
        trader_key: String,
        encoding: SerializationEncoding,
        database: RedisCacheConfig,
        state: Arc<WriterState>,
    ) -> anyhow::Result<Self> {
        let (redis_url, _) = get_redis_url(&database);
        let client = redis::Client::open(redis_url)?;
        Ok(Self {
            rx,
            count_requests,
            held: Held::new(trader_key.clone()),
            trader_key,
            encoding,
            database,
            state,
            client,
            connection: None,
            replaced: Vec::new(),
            unanswered: None,
            failures: 0,
            failing: true,
            retrying: false,
            closing: false,
        })
    }

    fn held_count(&self) -> usize {
        self.held.len() + self.rx.len()
    }

    /// Connects, then delivers what was taken in while connecting.
    async fn set_up(&mut self) -> redis::RedisResult<()> {
        self.connected().await?;
        self.deliver_held().await;
        Ok(())
    }

    /// Delivers the buffered commands in order, each run of writes as one transaction.
    async fn drain_buffer(&mut self, buffer: &mut VecDeque<DatabaseCommand>) {
        for cmd in buffer.drain(..) {
            self.held.push_command(cmd, false);
        }
        self.deliver_held().await;
    }

    /// Delivers everything held, in order; holding nothing, no delivery is failing.
    async fn deliver_held(&mut self) {
        while let Some((&id, op)) = self.held.ops.first_key_value() {
            match op {
                Op::Write(_) => self.deliver_writes().await,
                Op::UpdateOrder { .. } => self.read_order(id).await,
                Op::IndexOrder { .. } => self.index_order(id),
                Op::Flush(_) => self.flush(id).await,
            }
        }
        self.delivered();
    }

    /// Delivers the front run of writes as one transaction until Redis answers it. After a failure
    /// it first asks whether the unanswered transaction landed, then sends what it still holds,
    /// which includes what was taken in meanwhile.
    async fn deliver_writes(&mut self) {
        loop {
            if self.held.front_run().next().is_none() {
                self.unanswered = None;
                return;
            }

            // A transaction appending no event cannot tell whether it landed, and is sent again
            let provable = self
                .unanswered
                .take()
                .filter(|attempt| !attempt.proofs.is_empty());
            if let Some(attempt) = provable {
                match self.landed(&attempt).await {
                    Ok(landed) => {
                        self.answered();
                        if landed {
                            self.held.remove_through(attempt.last_id);
                            self.delivered();
                        }
                    }
                    Err(e) if is_retryable_error(&e) => {
                        self.unanswered = Some(attempt);
                        self.retry_after(&e).await;
                    }
                    Err(e) => {
                        log::error!("{e}");
                        self.held.remove_through(attempt.last_id);
                        self.failed();
                    }
                }
            } else {
                let (pipe, attempt) = self.held.transaction();
                match self.send::<()>(pipe).await {
                    Ok(()) => {
                        self.answered();
                        self.held.remove_through(attempt.last_id);
                        self.delivered();
                    }
                    Err(e) if is_retryable_error(&e) => {
                        self.unanswered = Some(attempt);
                        self.retry_after(&e).await;
                    }
                    Err(e) => {
                        log::error!("{e}");
                        self.held.remove_through(attempt.last_id);
                        self.failed();
                    }
                }
            }
        }
    }

    /// Whether `attempt` landed: true when any list it appended an event to, and did not replace
    /// after, ends with that event, since no event repeats another and the writer is its trader
    /// key's only one.
    async fn landed(&mut self, attempt: &Attempt) -> redis::RedisResult<bool> {
        let mut pipe = redis::pipe();
        for (key, _) in &attempt.proofs {
            pipe.lrange(key, -1, -1);
        }
        let tails: Vec<Vec<Bytes>> = self.send(pipe).await?;
        Ok(attempt
            .proofs
            .iter()
            .zip(&tails)
            .any(|((_, event), tail)| tail.first() == Some(event)))
    }

    /// Reads the stored events of the order update `id`, then holds in its place its event's
    /// append and the order's re-index.
    async fn read_order(&mut self, id: i64) {
        let Some(Op::UpdateOrder { key, payload }) = self.held.ops.get(&id) else {
            panic!("held op {id} is not an order update");
        };
        let key = key.clone();
        let payload = payload.clone();
        if let Err(e) = check_slice_not_empty(&payload, stringify!(payload)) {
            log::error!("{e}");
            self.held.remove(id);
            return;
        }

        let mut pipe = redis::pipe();
        pipe.lrange(&key, 0, -1);
        let read = self.until_answered::<(Vec<Bytes>,)>(pipe).await;
        self.held.remove(id);
        match read {
            Ok((stored,)) if stored.is_empty() => {
                log::warn!("Cannot update order in Redis, no existing state at {key}");
            }
            Ok((stored,)) => {
                let appended = payload[0].clone();
                let append = Write::Append {
                    key: key.clone(),
                    value: appended.clone(),
                    if_exists: true,
                    unique: true,
                };
                self.held.push_front(Op::IndexOrder {
                    key,
                    stored,
                    appended,
                });
                self.held.push_front(Op::Write(append));
            }
            Err(e) => {
                log::error!("{e}");
                self.failed();
            }
        }
    }

    fn index_order(&mut self, id: i64) {
        let Some(Op::IndexOrder {
            key,
            stored,
            appended,
        }) = self.held.remove(id)
        else {
            panic!("held op {id} is not an order re-index");
        };
        match order_from_events(self.encoding, &key, &stored, &appended) {
            Ok(order) => {
                let mut writes = Vec::new();
                update_order_indexes(&mut writes, &self.trader_key, &order);
                for write in writes.into_iter().rev() {
                    self.held.push_front(Op::Write(write));
                }
            }
            Err(e) => log::error!("{e}"),
        }
    }

    async fn flush(&mut self, id: i64) {
        let mut pipe = redis::pipe();
        pipe.cmd(REDIS_FLUSHDB);
        let flushed = self.until_answered::<()>(pipe).await;
        let Some(Op::Flush(reply_tx)) = self.held.remove(id) else {
            panic!("held op {id} is not a flush");
        };
        match flushed {
            Ok(()) => self.delivered(),
            Err(e) => {
                log::error!("Failed to flush database: {e:?}");
                self.failed();
            }
        }
        let _ = reply_tx.send(());
    }

    /// Sends `pipe`, which is safe to repeat, until Redis answers it.
    async fn until_answered<T: redis::FromRedisValue>(
        &mut self,
        pipe: Pipeline,
    ) -> redis::RedisResult<T> {
        loop {
            match self.send(pipe.clone()).await {
                Err(e) if is_retryable_error(&e) => self.retry_after(&e).await,
                result => {
                    self.answered();
                    return result;
                }
            }
        }
    }

    /// Sends `pipe` once on the write connection.
    async fn send<T: redis::FromRedisValue>(&mut self, pipe: Pipeline) -> redis::RedisResult<T> {
        let mut con = self.connected().await?;
        self.draining(async move { pipe.query_async::<T>(&mut con).await })
            .await
    }

    /// The write connection, connected first when there is none. A new connection kills every
    /// connection replaced since the last fence before it carries anything, so a write still in
    /// flight on one either ran before the kill, where the new connection's reads see it, or never
    /// runs.
    async fn connected(&mut self) -> redis::RedisResult<MultiplexedConnection> {
        loop {
            if let Some(connection) = &self.connection {
                return Ok(connection.con.clone());
            }

            let opened = open_write_connection(
                self.client.clone(),
                write_connection_config(&self.database),
                self.replaced.clone(),
            );
            match self.draining(opened).await {
                Ok(connection) => {
                    self.answered();
                    self.replaced.clear();
                    self.connection = Some(connection);
                }
                Err(e) if is_retryable_error(&e) => self.retry_after(&e).await,
                Err(e) => return Err(e),
            }
        }
    }

    /// Waits the config's backoff after a retryable failure, first replacing the connection when
    /// the failure is one a connection does not recover from. Entering retry compacts what is
    /// held to the latest value of each target.
    async fn retry_after(&mut self, error: &redis::RedisError) {
        log::error!("Error in task '{CACHE_PROCESS}', retrying until Redis answers: {error}");
        if error.is_unrecoverable_error()
            && let Some(replaced) = self.connection.take()
        {
            self.replaced.push((replaced.run_id, replaced.client_id));
        }
        if !self.retrying {
            self.retrying = true;
            self.held.compact();
        }
        self.failed();
        let delay = retry_delay(&self.database, self.failures);
        self.failures = self.failures.saturating_add(1);
        self.draining(tokio::time::sleep(delay)).await;
    }

    fn answered(&mut self) {
        self.failures = 0;
    }

    /// Marks a delivery failing: a sent request failed, by an error or a timeout.
    fn failed(&mut self) {
        self.failing = true;
        self.state.set_failing(true);
    }

    fn delivered(&mut self) {
        self.retrying = false;
        self.failing = false;
        self.state.set_failing(false);
    }

    /// Awaits `future`, answering the closer's count requests meanwhile and, while a delivery
    /// fails, taking newer commands in.
    async fn draining<F: Future>(&mut self, future: F) -> F::Output {
        tokio::pin!(future);
        loop {
            tokio::select! {
                biased;
                output = &mut future => return output,
                Some(reply) = self.count_requests.recv() => {
                    let _ = reply.send((self.held_count(), self.closing));
                }
                maybe_cmd = self.rx.recv(), if self.retrying && !self.closing => {
                    match maybe_cmd {
                        Some(cmd) if !matches!(cmd.op_type, DatabaseOperation::Close) => {
                            self.held.push_command(cmd, true);
                        }
                        _ => self.closing = true,
                    }
                }
            }
        }
    }
}

fn write_connection_config(database: &RedisCacheConfig) -> AsyncConnectionConfig {
    AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(u64::from(
            database.connection_timeout,
        ))))
        .set_response_timeout(Some(Duration::from_secs(u64::from(
            database.response_timeout,
        ))))
}

/// Opens a write connection, learning the server's run and the connection's client id, and kills
/// each connection in `replaced` that the same server run still serves; a connection of an
/// earlier run died with it.
async fn open_write_connection(
    client: redis::Client,
    config: AsyncConnectionConfig,
    replaced: Vec<(String, i64)>,
) -> redis::RedisResult<WriteConnection> {
    log::debug!("Creating {CACHE_WRITE} redis connection");
    let mut con = client
        .get_multiplexed_async_connection_with_config(&config)
        .await?;

    let mut handshake = redis::pipe();
    handshake.cmd("INFO").cmd("CLIENT").arg("ID");
    let (info, client_id): (String, i64) = handshake.query_async(&mut con).await?;
    check_redis_version(&info).map_err(|e| {
        redis::RedisError::from((redis::ErrorKind::Client, "Redis version", e.to_string()))
    })?;
    let Some(run_id) = info_field(&info, "run_id") else {
        return Err((redis::ErrorKind::Client, "Redis run_id not available").into());
    };

    let fenced: Vec<i64> = replaced
        .iter()
        .filter(|(run, _)| run == run_id)
        .map(|(_, replaced_id)| *replaced_id)
        .collect();
    if !fenced.is_empty() {
        let mut fence = redis::pipe();
        for replaced_id in &fenced {
            fence.cmd("CLIENT").arg("KILL").arg("ID").arg(*replaced_id);
        }
        fence.query_async::<()>(&mut con).await?;
    }

    Ok(WriteConnection {
        con,
        run_id: run_id.to_string(),
        client_id,
    })
}

/// One Redis write a cache command makes, kept as data so that a writer retrying while Redis
/// isn't answering can hold only the latest write to each target.
#[derive(Debug)]
enum Write {
    Set {
        key: String,
        value: Bytes,
    },
    Delete {
        key: String,
    },
    ReplaceList {
        key: String,
        value: Bytes,
    },
    /// `unique` when the entry is an event, which no other entry repeats.
    Append {
        key: String,
        value: Bytes,
        if_exists: bool,
        unique: bool,
    },
    AddMember {
        key: String,
        member: Bytes,
    },
    RemoveMember {
        key: String,
        member: Bytes,
    },
    SetField {
        key: String,
        field: Bytes,
        value: Bytes,
    },
    DeleteField {
        key: String,
        field: Bytes,
    },
}

/// What a replacing write replaces: a whole key, or one member or field of it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Target {
    Key(String),
    Member(String, Bytes),
}

impl Write {
    fn queue(&self, pipe: &mut Pipeline) {
        match self {
            Self::Set { key, value } => {
                pipe.set(key.as_str(), value.as_ref());
            }
            Self::Delete { key } => {
                pipe.del(key.as_str());
            }
            Self::ReplaceList { key, value } => {
                pipe.del(key.as_str());
                pipe.rpush(key.as_str(), value.as_ref());
            }
            Self::Append {
                key,
                value,
                if_exists: true,
                ..
            } => {
                pipe.rpush_exists(key.as_str(), value.as_ref());
            }
            Self::Append {
                key,
                value,
                if_exists: false,
                ..
            } => {
                pipe.rpush(key.as_str(), value.as_ref());
            }
            Self::AddMember { key, member } => {
                pipe.sadd(key.as_str(), member.as_ref());
            }
            Self::RemoveMember { key, member } => {
                pipe.srem(key.as_str(), member.as_ref());
            }
            Self::SetField { key, field, value } => {
                pipe.hset(key.as_str(), field.as_ref(), value.as_ref());
            }
            Self::DeleteField { key, field } => {
                pipe.hdel(key.as_str(), field.as_ref());
            }
        }
    }

    /// What this write replaces; `None` for an append, which replaces nothing.
    fn target(&self) -> Option<Target> {
        match self {
            Self::Set { key, .. } | Self::Delete { key } | Self::ReplaceList { key, .. } => {
                Some(Target::Key(key.clone()))
            }
            Self::Append { .. } => None,
            Self::AddMember { key, member } | Self::RemoveMember { key, member } => {
                Some(Target::Member(key.clone(), member.clone()))
            }
            Self::SetField { key, field, .. } | Self::DeleteField { key, field } => {
                Some(Target::Member(key.clone(), field.clone()))
            }
        }
    }
}

/// What the cache writer holds and has not yet delivered, by id in order. Consecutive writes are
/// sent as one transaction; an order update or a flush is delivered on its own.
struct Held {
    trader_key: String,
    ops: BTreeMap<i64, Op>,
    next_id: i64,
    next_front_id: i64,
    /// The id of the latest write to each target.
    latest: AHashMap<Target, i64>,
    /// The id of the latest op reading each key.
    read: AHashMap<String, i64>,
}

enum Op {
    Write(Write),
    UpdateOrder {
        key: String,
        payload: Vec<Bytes>,
    },
    /// Re-indexes the order stored as `stored`, once its event `appended` has been written.
    IndexOrder {
        key: String,
        stored: Vec<Bytes>,
        appended: Bytes,
    },
    Flush(SyncSender<()>),
}

/// A transaction sent: the id of its last write, and for each list it appended an event to and did
/// not replace after, that list's last event.
#[derive(Debug)]
struct Attempt {
    last_id: i64,
    proofs: Vec<(String, Bytes)>,
}

/// Whether an order, rebuilt from its events, belongs in an index; `None` leaves its membership
/// as it is.
type Belongs = fn(&OrderAny) -> Option<bool>;

/// Every order index an order update writes, each with whether the order belongs in it.
const ORDER_INDEXES: [(&str, Belongs); 6] = [
    (INDEX_ORDERS, |_| Some(true)),
    (INDEX_ORDER_IDS, |order| {
        order.venue_order_id().map(|_| true)
    }),
    (INDEX_ORDERS_INFLIGHT, |order| Some(order.is_inflight())),
    (INDEX_ORDERS_CLOSED, |order| {
        if order.is_open() {
            Some(false)
        } else if order.is_closed() {
            Some(true)
        } else {
            None
        }
    }),
    (INDEX_ORDERS_OPEN, |order| {
        if order.is_open() {
            Some(true)
        } else if order.is_closed() {
            Some(false)
        } else {
            None
        }
    }),
    (INDEX_ORDERS_EMULATED, |order| {
        let emulated = order
            .emulation_trigger()
            .is_some_and(|trigger| trigger != TriggerType::NoTrigger);
        Some(emulated && !order.is_closed())
    }),
];

impl Held {
    fn new(trader_key: String) -> Self {
        Self {
            trader_key,
            ops: BTreeMap::new(),
            next_id: 0,
            next_front_id: -1,
            latest: AHashMap::new(),
            read: AHashMap::new(),
        }
    }

    /// The number of commands held; a re-index is the remainder of an order update already
    /// counted by its append.
    fn len(&self) -> usize {
        self.ops
            .values()
            .filter(|op| !matches!(op, Op::IndexOrder { .. }))
            .count()
    }

    /// Holds the writes `cmd` makes after everything already held. When `coalesce`, a replacing
    /// write drops the earlier write to its target it supersedes.
    fn push_command(&mut self, cmd: DatabaseCommand, coalesce: bool) {
        let op_type = match cmd.op_type {
            DatabaseOperation::Flush(reply_tx) => {
                self.push(Op::Flush(reply_tx), coalesce);
                return;
            }
            DatabaseOperation::Close => panic!("Close command should not be held"),
            op_type => op_type,
        };
        let Some(key) = cmd.key else {
            log::error!("Null key found for {op_type:?} command");
            return;
        };
        let collection = match get_collection_key(&key) {
            Ok(collection) => collection,
            Err(e) => {
                log::error!("{e}");
                return;
            }
        };

        let key = full_redis_key(&self.trader_key, &key);
        let mut writes = Vec::new();

        match op_type {
            DatabaseOperation::Insert => {
                if let Some(payload) = cmd.payload {
                    log::debug!("Processing INSERT for collection: {collection}, key: {key}");
                    if let Err(e) = insert(&mut writes, collection, &key, &payload) {
                        log::error!("{e}");
                    }
                } else {
                    log::error!("Null `payload` for `insert`");
                }
            }
            DatabaseOperation::Update => {
                if let Some(payload) = cmd.payload {
                    log::debug!("Processing UPDATE for collection: {collection}, key: {key}");
                    if let Err(e) = update(&mut writes, collection, &key, &payload) {
                        log::error!("{e}");
                    }
                } else {
                    log::error!("Null `payload` for `update`");
                }
            }
            DatabaseOperation::UpdateOrder => {
                if let Some(payload) = cmd.payload {
                    log::debug!("Processing UPDATE_ORDER for key: {key}");
                    self.push(Op::UpdateOrder { key, payload }, coalesce);
                } else {
                    log::error!("Null `payload` for `update_order`");
                }
            }
            DatabaseOperation::ReplaceList => {
                if let Some(payload) = cmd.payload {
                    log::debug!("Processing REPLACE_LIST for key: {key}");
                    if let Err(e) = replace_list_operation(&mut writes, collection, &key, &payload)
                    {
                        log::error!("{e}");
                    }
                } else {
                    log::error!("Null `payload` for `replace_list`");
                }
            }
            DatabaseOperation::Delete => {
                log::debug!(
                    "Processing DELETE for collection: {}, key: {}, payload: {:?}",
                    collection,
                    key,
                    cmd.payload.as_ref().map(std::vec::Vec::len)
                );
                // `payload` can be `None` for a delete operation
                if let Err(e) = delete(&mut writes, collection, &key, cmd.payload) {
                    log::error!("{e}");
                }
            }
            DatabaseOperation::Flush(_) | DatabaseOperation::Close => {
                unreachable!("handled before the key is read")
            }
        }

        for write in writes {
            self.push(Op::Write(write), coalesce);
        }
    }

    fn push(&mut self, op: Op, coalesce: bool) {
        let id = self.next_id;
        self.next_id += 1;
        self.insert(id, op, coalesce);
    }

    /// Holds `op` before everything already held, as the remainder of the order update being
    /// delivered.
    fn push_front(&mut self, op: Op) {
        let id = self.next_front_id;
        self.next_front_id -= 1;
        self.ops.insert(id, op);
    }

    /// Holds `op` as `id`, recording what it reads. When `coalesce`, a replacing write drops the
    /// earlier write to its target unless an op between them reads that target.
    fn insert(&mut self, id: i64, op: Op, coalesce: bool) {
        if let Op::Write(write) = &op
            && let Some(target) = write.target()
        {
            if coalesce
                && let Some(&earlier) = self.latest.get(&target)
                && self.supersedes(&target, earlier)
            {
                self.ops.remove(&earlier);
            }
            self.latest.insert(target, id);
        }
        for key in self.reads(&op) {
            self.read.insert(key, id);
        }
        self.ops.insert(id, op);
    }

    /// The keys `op` reads: an order update reads its order and rewrites every order index from
    /// it; nothing else reads.
    fn reads(&self, op: &Op) -> Vec<String> {
        match op {
            Op::UpdateOrder { key, .. } | Op::IndexOrder { key, .. } => {
                let indexes = ORDER_INDEXES
                    .iter()
                    .map(|(index, _)| full_redis_key(&self.trader_key, index));
                std::iter::once(key.clone()).chain(indexes).collect()
            }
            Op::Write(_) | Op::Flush(_) => Vec::new(),
        }
    }

    /// Drops the held op `id` once delivered, with what was recorded for it.
    fn remove(&mut self, id: i64) -> Option<Op> {
        let op = self.ops.remove(&id)?;
        self.retire(id, &op);
        Some(op)
    }

    /// Forgets what was recorded for the delivered op `id`, keeping what later ops recorded.
    fn retire(&mut self, id: i64, op: &Op) {
        if let Op::Write(write) = op
            && let Some(target) = write.target()
            && self.latest.get(&target) == Some(&id)
        {
            self.latest.remove(&target);
        }
        for key in self.reads(op) {
            if self.read.get(&key) == Some(&id) {
                self.read.remove(&key);
            }
        }
    }

    /// Whether a write to `target` held after the write `earlier` supersedes it: true unless an op
    /// between them reads the target's key.
    fn supersedes(&self, target: &Target, earlier: i64) -> bool {
        let (Target::Key(key) | Target::Member(key, _)) = target;
        self.read.get(key).is_none_or(|&read| read < earlier)
    }

    /// Drops every held write a later held write supersedes, as coalescing would have.
    fn compact(&mut self) {
        let ops = std::mem::take(&mut self.ops);
        self.latest.clear();
        self.read.clear();
        for (id, op) in ops {
            self.insert(id, op, true);
        }
    }

    /// The writes held before any other op, by id. Writes held in front, the remainder of an
    /// order update, are a transaction of their own, as the order update's are when healthy.
    fn front_run(&self) -> impl Iterator<Item = (&i64, &Write)> {
        let in_front = self.ops.keys().next().is_some_and(|&id| id < 0);
        self.ops.iter().map_while(move |(id, op)| match op {
            Op::Write(write) if (*id < 0) == in_front => Some((id, write)),
            _ => None,
        })
    }

    fn transaction(&self) -> (Pipeline, Attempt) {
        let mut pipe = redis::pipe();
        pipe.atomic();
        let mut proofs: AHashMap<&str, &Bytes> = AHashMap::new();
        let mut last_id = None;
        for (id, write) in self.front_run() {
            last_id = Some(*id);
            write.queue(&mut pipe);
            match write {
                Write::Append {
                    key,
                    value,
                    unique: true,
                    ..
                } => {
                    proofs.insert(key, value);
                }
                Write::Set { key, .. } | Write::Delete { key } | Write::ReplaceList { key, .. } => {
                    proofs.remove(key.as_str());
                }
                _ => {}
            }
        }

        let last_id = last_id.expect("a transaction is built from held writes");
        let proofs = proofs
            .into_iter()
            .map(|(key, event)| (key.to_string(), event.clone()))
            .collect();
        (pipe, Attempt { last_id, proofs })
    }

    /// Drops the held ops up to and including `last_id`, the front run a transaction delivered.
    fn remove_through(&mut self, last_id: i64) {
        let pending = self.ops.split_off(&(last_id + 1));
        let delivered = std::mem::replace(&mut self.ops, pending);
        for (id, op) in &delivered {
            self.retire(*id, op);
        }
    }
}

fn order_from_events(
    encoding: SerializationEncoding,
    key: &str,
    stored: &[Bytes],
    appended: &Bytes,
) -> anyhow::Result<OrderAny> {
    let mut events: Vec<OrderEventAny> = stored
        .iter()
        .map(|payload| DatabaseQueries::deserialize_payload(encoding, payload))
        .collect::<anyhow::Result<_>>()
        .with_context(|| {
            format!(
                "Order event append succeeded for {key}, but index replay failed decoding history"
            )
        })?;
    let event: OrderEventAny = DatabaseQueries::deserialize_payload(encoding, appended.as_ref())
        .with_context(|| {
            format!(
                "Order event append succeeded for {key}, but index replay failed decoding appended event"
            )
        })?;
    events.push(event);
    OrderAny::from_events(events).with_context(|| {
        format!("Order event append succeeded for {key}, but index replay failed rebuilding order")
    })
}

fn insert(
    writes: &mut Vec<Write>,
    collection: &str,
    key: &str,
    value: &[Bytes],
) -> anyhow::Result<()> {
    check_slice_not_empty(value, stringify!(value))?;

    match collection {
        INDEX => insert_index(writes, key, value),
        GENERAL | CURRENCIES | INSTRUMENTS | SYNTHETICS | ACTORS | STRATEGIES | HEALTH | CUSTOM => {
            writes.push(Write::Set {
                key: key.to_string(),
                value: value[0].clone(),
            });
            Ok(())
        }
        ACCOUNTS | ORDERS | POSITIONS => {
            writes.push(Write::Append {
                key: key.to_string(),
                value: value[0].clone(),
                if_exists: false,
                unique: true,
            });
            Ok(())
        }
        // A snapshot repeats the state it captures, so two can be identical
        SNAPSHOTS => {
            writes.push(Write::Append {
                key: key.to_string(),
                value: value[0].clone(),
                if_exists: false,
                unique: false,
            });
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `insert` for collection '{collection}'"),
    }
}

fn insert_index(writes: &mut Vec<Write>, key: &str, value: &[Bytes]) -> anyhow::Result<()> {
    let index_key = get_index_key(key)?;
    match index_key {
        INDEX_ORDER_IDS
        | INDEX_ORDERS
        | INDEX_ORDERS_OPEN
        | INDEX_ORDERS_CLOSED
        | INDEX_ORDERS_EMULATED
        | INDEX_ORDERS_INFLIGHT
        | INDEX_POSITIONS
        | INDEX_POSITIONS_OPEN
        | INDEX_POSITIONS_CLOSED => {
            writes.push(Write::AddMember {
                key: key.to_string(),
                member: value[0].clone(),
            });
            Ok(())
        }
        INDEX_ORDER_POSITION | INDEX_ORDER_CLIENT => {
            writes.push(Write::SetField {
                key: key.to_string(),
                field: value[0].clone(),
                value: value[1].clone(),
            });
            Ok(())
        }
        _ => anyhow::bail!("Index unknown '{index_key}' on insert"),
    }
}

fn replace_list_operation(
    writes: &mut Vec<Write>,
    collection: &str,
    key: &str,
    value: &[Bytes],
) -> anyhow::Result<()> {
    check_slice_not_empty(value, stringify!(value))?;

    match collection {
        ACCOUNTS | ORDERS | POSITIONS => {
            writes.push(Write::ReplaceList {
                key: key.to_string(),
                value: value[0].clone(),
            });
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `replace_list` for collection '{collection}'"),
    }
}

fn update(
    writes: &mut Vec<Write>,
    collection: &str,
    key: &str,
    value: &[Bytes],
) -> anyhow::Result<()> {
    check_slice_not_empty(value, stringify!(value))?;

    match collection {
        ACCOUNTS | ORDERS | POSITIONS => {
            writes.push(Write::Append {
                key: key.to_string(),
                value: value[0].clone(),
                if_exists: true,
                unique: true,
            });
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `update` for collection '{collection}'"),
    }
}

fn delete(
    writes: &mut Vec<Write>,
    collection: &str,
    key: &str,
    value: Option<Vec<Bytes>>,
) -> anyhow::Result<()> {
    log::debug!(
        "delete: collection={}, key={}, has_payload={}",
        collection,
        key,
        value.is_some()
    );

    match collection {
        INDEX => delete_from_index(writes, key, value),
        ORDERS | POSITIONS | ACCOUNTS | ACTORS | STRATEGIES => {
            writes.push(Write::Delete {
                key: key.to_string(),
            });
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `delete` for collection '{collection}'"),
    }
}

fn delete_from_index(
    writes: &mut Vec<Write>,
    key: &str,
    value: Option<Vec<Bytes>>,
) -> anyhow::Result<()> {
    let value = value.ok_or_else(|| anyhow::anyhow!("Empty `payload` for `delete` '{key}'"))?;
    let index_key = get_index_key(key)?;

    match index_key {
        INDEX_ORDER_IDS
        | INDEX_ORDERS
        | INDEX_ORDERS_OPEN
        | INDEX_ORDERS_CLOSED
        | INDEX_ORDERS_EMULATED
        | INDEX_ORDERS_INFLIGHT
        | INDEX_POSITIONS
        | INDEX_POSITIONS_OPEN
        | INDEX_POSITIONS_CLOSED => {
            writes.push(Write::RemoveMember {
                key: key.to_string(),
                member: value[0].clone(),
            });
            Ok(())
        }
        INDEX_ORDER_POSITION | INDEX_ORDER_CLIENT => {
            writes.push(Write::DeleteField {
                key: key.to_string(),
                field: value[0].clone(),
            });
            Ok(())
        }
        _ => anyhow::bail!("Unsupported index operation: remove from '{index_key}'"),
    }
}

fn full_redis_key(trader_key: &str, key: &str) -> String {
    format!("{trader_key}{REDIS_DELIMITER}{key}")
}

fn update_order_indexes(writes: &mut Vec<Write>, trader_key: &str, order: &OrderAny) {
    let member = Bytes::from(order.client_order_id().to_string());
    for (index, belongs) in ORDER_INDEXES {
        let key = full_redis_key(trader_key, index);
        match belongs(order) {
            Some(true) => writes.push(Write::AddMember {
                key,
                member: member.clone(),
            }),
            Some(false) => writes.push(Write::RemoveMember {
                key,
                member: member.clone(),
            }),
            None => {}
        }
    }
}

fn format_timestamp(timestamp: UnixNanos) -> String {
    let dt = DateTime::<Utc>::from_timestamp_nanos(timestamp.as_u64().cast_signed());
    dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

fn get_trader_key(trader_id: TraderId, instance_id: UUID4, config: &CacheConfig) -> String {
    let mut key = String::new();

    if config.use_trader_prefix {
        key.push_str("trader-");
    }

    key.push_str(trader_id.as_str());

    if config.use_instance_id {
        key.push(REDIS_DELIMITER);
        write!(key, "{instance_id}").expect("writing to String cannot fail");
    }

    key
}

fn get_collection_key(key: &str) -> anyhow::Result<&str> {
    key.split_once(REDIS_DELIMITER)
        .map(|(collection, _)| collection)
        .ok_or_else(|| {
            anyhow::anyhow!("Invalid `key`, missing a '{REDIS_DELIMITER}' delimiter, was {key}")
        })
}

#[derive(Debug)]
pub struct RedisCacheDatabaseAdapter {
    pub database: RedisCacheDatabase,
}

impl RedisCacheDatabaseAdapter {
    fn encoding(&self) -> SerializationEncoding {
        self.database.get_encoding()
    }

    fn send_command(
        &self,
        op_type: DatabaseOperation,
        key: String,
        payload: Option<Vec<Bytes>>,
    ) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(op_type, key, payload);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))
    }

    fn append_list(&self, key: String, payload: Bytes) -> anyhow::Result<()> {
        self.send_command(DatabaseOperation::Update, key, Some(vec![payload]))
    }

    fn serialize_account_event(&self, account: &AccountAny) -> anyhow::Result<Bytes> {
        let event: AccountState = account.last_event().ok_or_else(|| {
            anyhow::anyhow!("Cannot persist account with no events: {}", account.id())
        })?;
        let payload = DatabaseQueries::serialize_payload(self.encoding(), &event)?;
        Ok(Bytes::from(payload))
    }

    fn serialize_order_event(&self, order_event: &OrderEventAny) -> anyhow::Result<Bytes> {
        let payload = DatabaseQueries::serialize_payload(self.encoding(), order_event)?;
        Ok(Bytes::from(payload))
    }

    fn serialize_position_event(&self, position: &Position) -> anyhow::Result<Bytes> {
        let event: OrderFilled = position.last_event().ok_or_else(|| {
            anyhow::anyhow!("Cannot persist position with no events: {}", position.id)
        })?;
        let payload = DatabaseQueries::serialize_payload(self.encoding(), &event)?;
        Ok(Bytes::from(payload))
    }

    fn load_state(&self, key: String) -> anyhow::Result<AHashMap<String, Bytes>> {
        let mut con = self.database.con.clone();
        let trader_key = self.database.trader_key.clone();
        let encoding = self.encoding();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = async {
                let full_key = format!("{trader_key}{REDIS_DELIMITER}{key}");
                let value: Option<Bytes> = con.get(&full_key).await?;
                let Some(value) = value else {
                    return Ok(AHashMap::new());
                };

                DatabaseQueries::deserialize_payload(encoding, &value)
            }
            .await;

            if let Err(e) = tx.send(result) {
                log::error!("Failed to send state load result for '{key}': {e:?}");
            }
        });

        blocking_recv(&rx).map_err(|e| anyhow::anyhow!("load_state channel closed: {e}"))?
    }

    fn update_state(&self, key: String, state: &AHashMap<String, Bytes>) -> anyhow::Result<()> {
        let payload = DatabaseQueries::serialize_payload(self.encoding(), state)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn replace_list(&self, key: String, payload: Bytes) -> anyhow::Result<()> {
        self.send_command(DatabaseOperation::ReplaceList, key, Some(vec![payload]))
    }
}

#[async_trait::async_trait]
impl CacheDatabaseFactory for RedisCacheConfig {
    async fn create(
        &self,
        trader_id: TraderId,
        instance_id: UUID4,
        config: CacheConfig,
    ) -> anyhow::Result<Box<dyn CacheDatabaseAdapter>> {
        let database =
            RedisCacheDatabase::new(trader_id, instance_id, config, self.clone()).await?;
        Ok(Box::new(RedisCacheDatabaseAdapter { database }))
    }
}

#[async_trait::async_trait]
impl CacheDatabaseAdapter for RedisCacheDatabaseAdapter {
    fn close(&mut self) -> anyhow::Result<()> {
        self.database.close();
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.database.flushdb_sync()
    }

    async fn load_all(&self) -> anyhow::Result<CacheMap> {
        log::debug!("Loading all data");

        let (
            currencies,
            instruments,
            synthetics,
            accounts,
            orders,
            positions,
            greeks,
            yield_curves,
        ) = tokio::try_join!(
            self.load_currencies(),
            self.load_instruments(),
            self.load_synthetics(),
            self.load_accounts(),
            self.load_orders(),
            self.load_positions(),
            self.load_greeks(),
            self.load_yield_curves()
        )
        .map_err(|e| anyhow::anyhow!("Error loading cache data: {e}"))?;

        Ok(CacheMap {
            currencies,
            instruments,
            synthetics,
            accounts,
            orders,
            positions,
            greeks,
            yield_curves,
        })
    }

    fn load(&self) -> anyhow::Result<AHashMap<String, Bytes>> {
        let con = self.database.con.clone();
        let trader_key = self.database.trader_key.clone();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = async {
                let pattern = format!("{trader_key}{REDIS_DELIMITER}{GENERAL}:*");
                let mut con_scan = con.clone();
                let keys = DatabaseQueries::scan_keys(&mut con_scan, pattern).await?;
                if keys.is_empty() {
                    return Ok(AHashMap::new());
                }

                let values = DatabaseQueries::read_bulk(&con, &keys).await?;
                let prefix = format!("{trader_key}{REDIS_DELIMITER}{GENERAL}{REDIS_DELIMITER}");
                let mut general = AHashMap::new();

                for (key, value) in keys.into_iter().zip(values) {
                    let Some(value) = value else {
                        continue;
                    };

                    if let Some(clean_key) = key.strip_prefix(&prefix) {
                        general.insert(clean_key.to_string(), value);
                    }
                }

                Ok(general)
            }
            .await;

            if let Err(e) = tx.send(result) {
                log::error!("Failed to send general load result: {e:?}");
            }
        });

        blocking_recv(&rx).map_err(|e| anyhow::anyhow!("load channel closed: {e}"))?
    }

    async fn load_currencies(&self) -> anyhow::Result<AHashMap<Ustr, Currency>> {
        DatabaseQueries::load_currencies(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_instruments(&self) -> anyhow::Result<AHashMap<InstrumentId, InstrumentAny>> {
        DatabaseQueries::load_instruments(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_synthetics(&self) -> anyhow::Result<AHashMap<InstrumentId, SyntheticInstrument>> {
        DatabaseQueries::load_synthetics(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_accounts(&self) -> anyhow::Result<AHashMap<AccountId, AccountAny>> {
        DatabaseQueries::load_accounts(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_orders(&self) -> anyhow::Result<AHashMap<ClientOrderId, OrderAny>> {
        DatabaseQueries::load_orders(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_positions(&self) -> anyhow::Result<AHashMap<PositionId, Position>> {
        DatabaseQueries::load_positions(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    fn load_index_order_position(&self) -> anyhow::Result<AHashMap<ClientOrderId, PositionId>> {
        let con = self.database.con.clone();
        let trader_key = self.database.trader_key.clone();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = DatabaseQueries::load_index_order_position(&con, &trader_key).await;
            if let Err(e) = tx.send(result) {
                log::error!("Failed to send load_index_order_position result: {e:?}");
            }
        });

        blocking_recv(&rx)
            .map_err(|e| anyhow::anyhow!("load_index_order_position channel closed: {e}"))?
    }

    fn load_index_order_client(&self) -> anyhow::Result<AHashMap<ClientOrderId, ClientId>> {
        let con = self.database.con.clone();
        let trader_key = self.database.trader_key.clone();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = DatabaseQueries::load_index_order_client(&con, &trader_key).await;
            if let Err(e) = tx.send(result) {
                log::error!("Failed to send load_index_order_client result: {e:?}");
            }
        });

        blocking_recv(&rx)
            .map_err(|e| anyhow::anyhow!("load_index_order_client channel closed: {e}"))?
    }

    async fn load_currency(&self, code: &Ustr) -> anyhow::Result<Option<Currency>> {
        DatabaseQueries::load_currency(
            &self.database.con,
            &self.database.trader_key,
            code,
            self.encoding(),
        )
        .await
    }

    async fn load_instrument(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<InstrumentAny>> {
        DatabaseQueries::load_instrument(
            &self.database.con,
            &self.database.trader_key,
            instrument_id,
            self.encoding(),
        )
        .await
    }

    async fn load_synthetic(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<SyntheticInstrument>> {
        DatabaseQueries::load_synthetic(
            &self.database.con,
            &self.database.trader_key,
            instrument_id,
            self.encoding(),
        )
        .await
    }

    async fn load_account(&self, account_id: &AccountId) -> anyhow::Result<Option<AccountAny>> {
        DatabaseQueries::load_account(
            &self.database.con,
            &self.database.trader_key,
            account_id,
            self.encoding(),
        )
        .await
    }

    async fn load_order(
        &self,
        client_order_id: &ClientOrderId,
    ) -> anyhow::Result<Option<OrderAny>> {
        DatabaseQueries::load_order(
            &self.database.con,
            &self.database.trader_key,
            client_order_id,
            self.encoding(),
        )
        .await
    }

    async fn load_position(&self, position_id: &PositionId) -> anyhow::Result<Option<Position>> {
        DatabaseQueries::load_position(
            &self.database.con,
            &self.database.trader_key,
            position_id,
            self.encoding(),
        )
        .await
    }

    fn load_actor(&self, component_id: &ComponentId) -> anyhow::Result<AHashMap<String, Bytes>> {
        let key = format!("{ACTORS}{REDIS_DELIMITER}{component_id}{REDIS_DELIMITER}state");
        self.load_state(key)
    }

    fn load_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<AHashMap<String, Bytes>> {
        let key = format!("{STRATEGIES}{REDIS_DELIMITER}{strategy_id}{REDIS_DELIMITER}state");
        self.load_state(key)
    }

    fn load_signals(&self, _name: &str) -> anyhow::Result<Vec<Signal>> {
        anyhow::bail!("Loading signals from Redis cache adapter not supported")
    }

    fn load_custom_data(&self, data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        self.database.load_custom_data(data_type)
    }

    fn load_order_snapshot(
        &self,
        _client_order_id: &ClientOrderId,
    ) -> anyhow::Result<Option<OrderSnapshot>> {
        anyhow::bail!("Loading order snapshots from Redis cache adapter not supported")
    }

    fn load_position_snapshot(
        &self,
        _position_id: &PositionId,
    ) -> anyhow::Result<Option<PositionSnapshot>> {
        anyhow::bail!("Loading position snapshots from Redis cache adapter not supported")
    }

    fn load_quotes(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<QuoteTick>> {
        anyhow::bail!("Loading quote data for Redis cache adapter not supported")
    }

    fn load_trades(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<TradeTick>> {
        anyhow::bail!("Loading market data for Redis cache adapter not supported")
    }

    fn load_funding_rates(
        &self,
        _instrument_id: &InstrumentId,
    ) -> anyhow::Result<Vec<FundingRateUpdate>> {
        anyhow::bail!("Loading market data for Redis cache adapter not supported")
    }

    fn load_bars(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<Bar>> {
        anyhow::bail!("Loading market data for Redis cache adapter not supported")
    }

    fn add(&self, key: String, value: Bytes) -> anyhow::Result<()> {
        let key = format!("{GENERAL}{REDIS_DELIMITER}{key}");
        self.database.insert(key, Some(vec![value]))
    }

    fn add_currency(&self, currency: &Currency) -> anyhow::Result<()> {
        let key = format!("{CURRENCIES}{REDIS_DELIMITER}{}", currency.code);
        let payload = DatabaseQueries::serialize_payload(self.encoding(), currency)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_instrument(&self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        let key = format!("{INSTRUMENTS}{REDIS_DELIMITER}{}", instrument.id());
        let payload = DatabaseQueries::serialize_payload(self.encoding(), instrument)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_synthetic(&self, synthetic: &SyntheticInstrument) -> anyhow::Result<()> {
        let key = format!("{SYNTHETICS}{REDIS_DELIMITER}{}", synthetic.id);
        let payload = DatabaseQueries::serialize_payload(self.encoding(), synthetic)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let account_id = account.id();
        let key = format!("{ACCOUNTS}{REDIS_DELIMITER}{account_id}");

        let payload = self.serialize_account_event(account)?;
        self.database.insert(key, Some(vec![payload]))
    }

    fn add_order(&self, order: &OrderAny, client_id: Option<ClientId>) -> anyhow::Result<()> {
        let client_order_id = order.client_order_id();
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");

        let event = OrderEventAny::Initialized(order.init_event().clone());
        let payload = self.serialize_order_event(&event)?;
        self.replace_list(key, payload)?;

        let order_id_bytes = Bytes::from(client_order_id.to_string());
        self.database
            .insert(INDEX_ORDERS.to_string(), Some(vec![order_id_bytes.clone()]))?;

        if order
            .emulation_trigger()
            .is_some_and(|trigger| trigger != TriggerType::NoTrigger)
        {
            self.database.insert(
                INDEX_ORDERS_EMULATED.to_string(),
                Some(vec![order_id_bytes.clone()]),
            )?;
        }

        if let Some(client_id) = client_id {
            self.database.insert(
                INDEX_ORDER_CLIENT.to_string(),
                Some(vec![order_id_bytes, Bytes::from(client_id.to_string())]),
            )?;
        }

        Ok(())
    }

    fn add_order_snapshot(&self, snapshot: &OrderSnapshot) -> anyhow::Result<()> {
        let key = format!(
            "{SNAPSHOTS}{REDIS_DELIMITER}{ORDERS}{REDIS_DELIMITER}{}",
            snapshot.client_order_id
        );
        let payload = DatabaseQueries::serialize_payload(self.encoding(), snapshot)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_position(&self, position: &Position) -> anyhow::Result<()> {
        let position_id = position.id;
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");

        let payload = self.serialize_position_event(position)?;
        self.replace_list(key, payload)?;

        let position_id_bytes = Bytes::from(position_id.to_string());
        self.database.insert(
            INDEX_POSITIONS.to_string(),
            Some(vec![position_id_bytes.clone()]),
        )?;
        self.database.insert(
            INDEX_POSITIONS_OPEN.to_string(),
            Some(vec![position_id_bytes.clone()]),
        )?;
        self.send_command(
            DatabaseOperation::Delete,
            INDEX_POSITIONS_CLOSED.to_string(),
            Some(vec![position_id_bytes]),
        )?;

        Ok(())
    }

    fn add_position_snapshot(&self, snapshot: &PositionSnapshot) -> anyhow::Result<()> {
        let key = format!(
            "{SNAPSHOTS}{REDIS_DELIMITER}{POSITIONS}{REDIS_DELIMITER}{}",
            snapshot.position_id
        );
        let payload = DatabaseQueries::serialize_payload(self.encoding(), snapshot)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_order_book(&self, _order_book: &OrderBook) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_signal(&self, _signal: &Signal) -> anyhow::Result<()> {
        anyhow::bail!("Saving signals for Redis cache adapter not supported")
    }

    fn add_custom_data(&self, data: &CustomData) -> anyhow::Result<()> {
        let json_bytes = serde_json::to_vec(data)
            .map_err(|e| anyhow::anyhow!("CustomData serialization failed: {e}"))?;
        let ts_init = data.ts_init().as_u64();
        let key = format!(
            "{CUSTOM}{REDIS_DELIMITER}{:020}{REDIS_DELIMITER}{}",
            ts_init,
            UUID4::new()
        );
        self.database
            .insert(key, Some(vec![Bytes::from(json_bytes)]))
    }

    fn add_quote(&self, _quote: &QuoteTick) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_trade(&self, _trade: &TradeTick) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_funding_rate(&self, _funding_rate: &FundingRateUpdate) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_bar(&self, _bar: &Bar) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn delete_actor(&self, component_id: &ComponentId) -> anyhow::Result<()> {
        let key = format!("{ACTORS}{REDIS_DELIMITER}{component_id}{REDIS_DELIMITER}state");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))
    }

    fn delete_strategy(&self, component_id: &StrategyId) -> anyhow::Result<()> {
        let key = format!("{STRATEGIES}{REDIS_DELIMITER}{component_id}{REDIS_DELIMITER}state");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))
    }

    fn delete_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<()> {
        let order_id_bytes = Bytes::from(client_order_id.to_string());

        log::debug!("Deleting order: {client_order_id} from Redis");
        log::debug!("Trader key: {}", self.database.trader_key);

        // Delete the order itself
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        log::debug!("Deleting order key: {key}");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete order command: {e}"))?;

        // Delete from all order indexes
        let index_keys = [
            INDEX_ORDER_IDS,
            INDEX_ORDERS,
            INDEX_ORDERS_OPEN,
            INDEX_ORDERS_CLOSED,
            INDEX_ORDERS_EMULATED,
            INDEX_ORDERS_INFLIGHT,
        ];

        for index_key in &index_keys {
            let key = (*index_key).to_string();
            log::debug!("Deleting from index: {key} (order_id: {client_order_id})");
            let payload = vec![order_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send delete order index command: {e}"))?;
        }

        // Delete from hash indexes
        let hash_indexes = [INDEX_ORDER_POSITION, INDEX_ORDER_CLIENT];
        for index_key in &hash_indexes {
            let key = (*index_key).to_string();
            log::debug!("Deleting from hash index: {key} (order_id: {client_order_id})");
            let payload = vec![order_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.database.tx.send(op).map_err(|e| {
                anyhow::anyhow!("Failed to send delete order hash index command: {e}")
            })?;
        }

        log::debug!("Sent all delete commands for order: {client_order_id}");
        Ok(())
    }

    fn delete_position(&self, position_id: &PositionId) -> anyhow::Result<()> {
        let position_id_bytes = Bytes::from(position_id.to_string());

        // Delete the position itself
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete position command: {e}"))?;

        // Delete from all position indexes
        let index_keys = [
            INDEX_POSITIONS,
            INDEX_POSITIONS_OPEN,
            INDEX_POSITIONS_CLOSED,
        ];

        for index_key in &index_keys {
            let key = (*index_key).to_string();
            let payload = vec![position_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.database.tx.send(op).map_err(|e| {
                anyhow::anyhow!("Failed to send delete position index command: {e}")
            })?;
        }

        Ok(())
    }

    fn delete_account_event(&self, account_id: &AccountId, event_id: &str) -> anyhow::Result<()> {
        log::warn!(
            "Deleting account events currently a no-op (pending redesign), {account_id}: {event_id}"
        );
        Ok(())
    }

    fn index_venue_order_id(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> anyhow::Result<()> {
        self.database.insert(
            INDEX_ORDER_IDS.to_string(),
            Some(vec![Bytes::from(client_order_id.to_string())]),
        )?;
        log::debug!("Indexed {client_order_id:?} -> {venue_order_id:?}");
        Ok(())
    }

    fn index_order_position(
        &self,
        client_order_id: ClientOrderId,
        position_id: PositionId,
    ) -> anyhow::Result<()> {
        self.database.insert(
            INDEX_ORDER_POSITION.to_string(),
            Some(vec![
                Bytes::from(client_order_id.to_string()),
                Bytes::from(position_id.to_string()),
            ]),
        )
    }

    fn update_actor(
        &self,
        component_id: &ComponentId,
        state: &AHashMap<String, Bytes>,
    ) -> anyhow::Result<()> {
        let key = format!("{ACTORS}{REDIS_DELIMITER}{component_id}{REDIS_DELIMITER}state");
        self.update_state(key, state)
    }

    fn update_strategy(
        &self,
        strategy_id: &StrategyId,
        state: &AHashMap<String, Bytes>,
    ) -> anyhow::Result<()> {
        let key = format!("{STRATEGIES}{REDIS_DELIMITER}{strategy_id}{REDIS_DELIMITER}state");
        self.update_state(key, state)
    }

    fn update_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let account_id = account.id();
        let key = format!("{ACCOUNTS}{REDIS_DELIMITER}{account_id}");
        let payload = self.serialize_account_event(account)?;
        self.append_list(key, payload)
    }

    fn update_order(&self, order_event: &OrderEventAny) -> anyhow::Result<()> {
        let client_order_id = order_event.client_order_id();
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let payload = DatabaseQueries::serialize_payload(self.encoding(), order_event)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::UpdateOrder,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))
    }

    fn update_position(&self, position: &Position) -> anyhow::Result<()> {
        let position_id = position.id;
        if position.fill_voids.is_empty() {
            let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");
            let payload = self.serialize_position_event(position)?;
            self.append_list(key, payload)?;
        } else {
            self.add_position_snapshot(&PositionSnapshot::from_replay_state(position, None))?;
        }

        let position_id_bytes = Bytes::from(position_id.to_string());

        if position.is_open() {
            self.database.insert(
                INDEX_POSITIONS_OPEN.to_string(),
                Some(vec![position_id_bytes.clone()]),
            )?;
            self.send_command(
                DatabaseOperation::Delete,
                INDEX_POSITIONS_CLOSED.to_string(),
                Some(vec![position_id_bytes]),
            )?;
        } else if position.is_closed() {
            self.database.insert(
                INDEX_POSITIONS_CLOSED.to_string(),
                Some(vec![position_id_bytes.clone()]),
            )?;
            self.send_command(
                DatabaseOperation::Delete,
                INDEX_POSITIONS_OPEN.to_string(),
                Some(vec![position_id_bytes]),
            )?;
        }

        Ok(())
    }

    fn snapshot_order_state(&self, order: &OrderAny) -> anyhow::Result<()> {
        let snapshot = OrderSnapshot::from(order.clone());
        self.add_order_snapshot(&snapshot)
    }

    fn snapshot_position_state(
        &self,
        position: &Position,
        ts_snapshot: UnixNanos,
        unrealized_pnl: Option<Money>,
    ) -> anyhow::Result<()> {
        let mut snapshot = PositionSnapshot::from(position, unrealized_pnl);
        snapshot.ts_init = ts_snapshot;
        self.add_position_snapshot(&snapshot)
    }

    fn heartbeat(&self, timestamp: UnixNanos) -> anyhow::Result<()> {
        let timestamp = format_timestamp(timestamp);
        self.database.insert(
            format!("{HEALTH}{REDIS_DELIMITER}heartbeat"),
            Some(vec![Bytes::from(timestamp)]),
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_get_trader_key_with_prefix_and_instance_id() {
        let trader_id = TraderId::from("tester-123");
        let instance_id = UUID4::new();
        let config = CacheConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let key = get_trader_key(trader_id, instance_id, &config);
        assert!(key.starts_with("trader-tester-123:"));
        assert!(key.ends_with(&instance_id.to_string()));
    }

    #[rstest]
    fn test_get_collection_key_valid() {
        let key = "collection:123";
        assert_eq!(get_collection_key(key).unwrap(), "collection");
    }

    #[rstest]
    fn test_get_collection_key_invalid() {
        let key = "no_delimiter";
        assert!(get_collection_key(key).is_err());
    }
}

#[cfg(target_os = "linux")] // Run Redis tests on Linux platforms only
#[cfg(test)]
mod serial_tests {
    use nautilus_common::testing::wait_until_async;
    use nautilus_model::events::{OrderDenied, OrderInitialized};
    use rstest::rstest;
    use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};

    use super::*;
    use crate::redis::{CLOSE_TIMEOUT, msgbus::test_proxy::RedisProxy};

    async fn redis_connection() -> ConnectionManager {
        let config = RedisCacheConfig {
            connection_timeout: 1,
            number_of_retries: 0,
            ..Default::default()
        };
        create_redis_connection(CACHE_READ, &config)
            .await
            .expect("A running Redis service is required for this test")
    }

    fn database_through(proxy: &RedisProxy) -> RedisCacheConfig {
        RedisCacheConfig {
            port: Some(proxy.port),
            connection_timeout: 1,
            response_timeout: 1,
            ..Default::default()
        }
    }

    /// Buffers commands until closed.
    fn buffered() -> CacheConfig {
        CacheConfig {
            buffer_interval_ms: Some(3_600_000),
            ..Default::default()
        }
    }

    fn new_trader_key() -> String {
        format!("trader-tester-{}", UUID4::new())
    }

    fn spawn_writer(
        rx: tokio::sync::mpsc::UnboundedReceiver<DatabaseCommand>,
        trader_key: &str,
        config: CacheConfig,
        database: RedisCacheConfig,
    ) -> (JoinHandle<()>, Arc<WriterState>) {
        let (state, count_requests) = WriterState::new();
        let handle = tokio::spawn({
            let trader_key = trader_key.to_string();
            let state = state.clone();
            async move {
                process_commands(rx, trader_key, config, database, state, count_requests)
                    .await
                    .unwrap();
            }
        });
        (handle, state)
    }

    /// Waits until a delivery of the writer behind `state` is failing while it holds `held`
    /// writes.
    async fn wait_until_holding(state: &Arc<WriterState>, held: usize) {
        wait_until_async(
            || {
                let state = state.clone();
                async move {
                    state.is_failing() && state.held().await.is_some_and(|(count, _)| count == held)
                }
            },
            Duration::from_secs(10),
        )
        .await;
    }

    /// Starts a cache writer for a fresh trader key whose connection runs through the proxy.
    fn writer_through(
        config: CacheConfig,
        database: RedisCacheConfig,
    ) -> (
        UnboundedSender<DatabaseCommand>,
        JoinHandle<()>,
        String,
        Arc<WriterState>,
    ) {
        let trader_key = new_trader_key();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (handle, state) = spawn_writer(rx, &trader_key, config, database);
        (tx, handle, trader_key, state)
    }

    fn command(op_type: DatabaseOperation, key: &str, value: impl Into<Bytes>) -> DatabaseCommand {
        DatabaseCommand::new(op_type, key.to_string(), Some(vec![value.into()]))
    }

    async fn wait_for_list_len(con: &ConnectionManager, key: &str, len: usize) {
        wait_until_async(
            || {
                let mut con = con.clone();
                let key = key.to_string();
                async move { con.llen::<_, usize>(&key).await.unwrap() == len }
            },
            Duration::from_secs(3),
        )
        .await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_lost_with_its_connection_is_retried() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let (tx, handle, trader_key, _) =
            writer_through(CacheConfig::default(), database_through(&proxy));
        let ready = format!("{trader_key}{REDIS_DELIMITER}orders:O-1");
        let ledger = format!("{trader_key}{REDIS_DELIMITER}general:ledger");

        tx.send(command(
            DatabaseOperation::ReplaceList,
            "orders:O-1",
            &b"initialized"[..],
        ))
        .unwrap();
        wait_for_list_len(&con, &ready, 1).await;
        proxy.pause();
        tx.send(command(
            DatabaseOperation::Insert,
            "general:ledger",
            &b"ledger"[..],
        ))
        .unwrap();
        wait_until_async(
            || async { proxy.held_writes() >= 1 },
            Duration::from_secs(3),
        )
        .await;
        proxy.sever();
        proxy.resume();
        tx.send(DatabaseCommand::close()).unwrap();
        handle.await.unwrap();

        let stored: Option<Vec<u8>> = con.get(&ledger).await.unwrap();
        let _: usize = con.del(&[&ready, &ledger]).await.unwrap();

        assert_eq!(stored.as_deref(), Some(&b"ledger"[..]));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_append_whose_reply_is_lost_lands_once() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let (tx, handle, trader_key, _) =
            writer_through(CacheConfig::default(), database_through(&proxy));
        let key = format!("{trader_key}{REDIS_DELIMITER}orders:O-1");

        tx.send(command(
            DatabaseOperation::ReplaceList,
            "orders:O-1",
            &b"initialized"[..],
        ))
        .unwrap();
        wait_for_list_len(&con, &key, 1).await;
        proxy.lose_next_reply();
        tx.send(command(
            DatabaseOperation::Update,
            "orders:O-1",
            &b"filled"[..],
        ))
        .unwrap();
        tx.send(DatabaseCommand::close()).unwrap();
        handle.await.unwrap();

        let events: Vec<Vec<u8>> = con.lrange(&key, 0, -1).await.unwrap();
        let _: usize = con.del(&key).await.unwrap();

        assert_eq!(events, vec![b"initialized".to_vec(), b"filled".to_vec()]);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_an_append_delayed_on_a_replaced_connection_never_lands() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let (tx, handle, trader_key, _) =
            writer_through(CacheConfig::default(), database_through(&proxy));
        let key = format!("{trader_key}{REDIS_DELIMITER}orders:O-1");

        tx.send(command(
            DatabaseOperation::ReplaceList,
            "orders:O-1",
            &b"initialized"[..],
        ))
        .unwrap();
        wait_for_list_len(&con, &key, 1).await;
        proxy.stall();
        tx.send(command(
            DatabaseOperation::Update,
            "orders:O-1",
            &b"filled"[..],
        ))
        .unwrap();
        wait_until_async(
            || async { proxy.stalled_bytes() > 0 },
            Duration::from_secs(3),
        )
        .await;
        // The writer loses the connection; Redis still serves it, the append not yet received
        proxy.drop_stalled_clients();
        wait_for_list_len(&con, &key, 2).await;
        tx.send(command(
            DatabaseOperation::Update,
            "orders:O-1",
            &b"settled"[..],
        ))
        .unwrap();
        wait_for_list_len(&con, &key, 3).await;
        proxy.release_stalled().await;
        tx.send(DatabaseCommand::close()).unwrap();
        handle.await.unwrap();

        let events: Vec<Vec<u8>> = con.lrange(&key, 0, -1).await.unwrap();
        let _: usize = con.del(&key).await.unwrap();

        assert_eq!(
            events,
            vec![
                b"initialized".to_vec(),
                b"filled".to_vec(),
                b"settled".to_vec()
            ]
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_lost_transaction_holding_a_repeated_snapshot_is_sent_again() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let trader_key = new_trader_key();
        let snapshots = format!("{trader_key}{REDIS_DELIMITER}snapshots:O-1");
        let ledger = format!("{trader_key}{REDIS_DELIMITER}general:ledger");
        let _: usize = con.rpush(&snapshots, &b"snapshot"[..]).await.unwrap();
        let _: () = con.set(&ledger, &b"ledger-1"[..]).await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Queued before the writer starts, so its one buffered transaction holds both
        tx.send(command(
            DatabaseOperation::Insert,
            "general:ledger",
            &b"ledger-2"[..],
        ))
        .unwrap();
        tx.send(command(
            DatabaseOperation::Insert,
            "snapshots:O-1",
            &b"snapshot"[..],
        ))
        .unwrap();
        tx.send(DatabaseCommand::close()).unwrap();
        proxy.sever_on_next_exec();
        let (handle, _) = spawn_writer(rx, &trader_key, buffered(), database_through(&proxy));
        handle.await.unwrap();

        let stored: Option<Vec<u8>> = con.get(&ledger).await.unwrap();
        let stored_snapshots: Vec<Vec<u8>> = con.lrange(&snapshots, 0, -1).await.unwrap();
        let _: usize = con.del(&[&snapshots, &ledger]).await.unwrap();

        assert_eq!(stored.as_deref(), Some(&b"ledger-2"[..]));
        assert_eq!(
            stored_snapshots,
            vec![b"snapshot".to_vec(), b"snapshot".to_vec()]
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_writer_whose_setup_goes_unanswered_delivers() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let database = RedisCacheConfig {
            number_of_retries: 0,
            ..database_through(&proxy)
        };

        proxy.lose_next_request();
        let (tx, handle, trader_key, _) = writer_through(CacheConfig::default(), database);
        let ledger = format!("{trader_key}{REDIS_DELIMITER}general:ledger");
        tx.send(command(
            DatabaseOperation::Insert,
            "general:ledger",
            &b"ledger"[..],
        ))
        .unwrap();
        tx.send(DatabaseCommand::close()).unwrap();
        handle.await.unwrap();

        let stored: Option<Vec<u8>> = con.get(&ledger).await.unwrap();
        let _: usize = con.del(&ledger).await.unwrap();

        assert_eq!(stored.as_deref(), Some(&b"ledger"[..]));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_buffered_transaction_becomes_visible_at_once() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let trader_key = new_trader_key();
        let order = format!("{trader_key}{REDIS_DELIMITER}orders:O-1");
        let open = format!("{trader_key}{REDIS_DELIMITER}{INDEX_ORDERS_OPEN}");
        let closed = format!("{trader_key}{REDIS_DELIMITER}{INDEX_ORDERS_CLOSED}");
        let _: usize = con.rpush(&order, &b"accepted"[..]).await.unwrap();
        let _: usize = con.sadd(&open, &b"O-1"[..]).await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // An order's fill and its index moves, queued before the writer starts so that its one
        // buffered transaction holds them all
        tx.send(command(
            DatabaseOperation::Update,
            "orders:O-1",
            &b"filled"[..],
        ))
        .unwrap();
        tx.send(command(
            DatabaseOperation::Delete,
            INDEX_ORDERS_OPEN,
            &b"O-1"[..],
        ))
        .unwrap();
        tx.send(command(
            DatabaseOperation::Insert,
            INDEX_ORDERS_CLOSED,
            &b"O-1"[..],
        ))
        .unwrap();
        tx.send(DatabaseCommand::close()).unwrap();
        proxy.pause_after_next_exec();
        let (handle, _) = spawn_writer(rx, &trader_key, buffered(), database_through(&proxy));
        wait_for_list_len(&con, &order, 2).await;
        let open_once_filled: bool = con.sismember(&open, &b"O-1"[..]).await.unwrap();
        let closed_once_filled: bool = con.sismember(&closed, &b"O-1"[..]).await.unwrap();
        proxy.resume();
        handle.await.unwrap();
        let _: usize = con.del(&[&order, &open, &closed]).await.unwrap();

        assert_eq!((open_once_filled, closed_once_filled), (false, true));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_paused_writer_holds_the_latest_value_and_every_event() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let (tx, handle, trader_key, state) =
            writer_through(CacheConfig::default(), database_through(&proxy));
        let order = format!("{trader_key}{REDIS_DELIMITER}orders:O-1");
        let ledger = format!("{trader_key}{REDIS_DELIMITER}general:ledger");

        tx.send(command(
            DatabaseOperation::ReplaceList,
            "orders:O-1",
            &b"initialized"[..],
        ))
        .unwrap();
        wait_for_list_len(&con, &order, 1).await;
        proxy.pause();
        for seq in 1..=1_000 {
            tx.send(command(
                DatabaseOperation::Insert,
                "general:ledger",
                format!("ledger-{seq}"),
            ))
            .unwrap();
        }
        for seq in 1..=10 {
            tx.send(command(
                DatabaseOperation::Update,
                "orders:O-1",
                format!("event-{seq}"),
            ))
            .unwrap();
        }
        wait_until_holding(&state, 1 + 10).await;
        proxy.resume();
        tx.send(DatabaseCommand::close()).unwrap();
        handle.await.unwrap();

        let stored: Option<String> = con.get(&ledger).await.unwrap();
        let events: Vec<String> = con.lrange(&order, 0, -1).await.unwrap();
        let _: usize = con.del(&[&order, &ledger]).await.unwrap();

        let mut expected = vec!["initialized".to_string()];
        expected.extend((1..=10).map(|seq| format!("event-{seq}")));
        assert_eq!(stored.as_deref(), Some("ledger-1000"));
        assert_eq!(events, expected);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_failed_buffered_batch_holds_only_the_latest_value() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let trader_key = new_trader_key();
        let ledger = format!("{trader_key}{REDIS_DELIMITER}general:ledger");
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Queued before the writer starts, so its one buffered transaction holds them all
        for seq in 1..=1_000 {
            tx.send(command(
                DatabaseOperation::Insert,
                "general:ledger",
                format!("ledger-{seq}"),
            ))
            .unwrap();
        }
        tx.send(DatabaseCommand::close()).unwrap();
        proxy.pause_at_next_exec();
        let (handle, state) = spawn_writer(rx, &trader_key, buffered(), database_through(&proxy));
        wait_until_holding(&state, 1).await;
        proxy.resume();
        handle.await.unwrap();

        let stored: Option<String> = con.get(&ledger).await.unwrap();
        let _: usize = con.del(&ledger).await.unwrap();

        assert_eq!(stored.as_deref(), Some("ledger-1000"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_order_updates_between_writes_of_another_key_keep_only_its_latest_value() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let (tx, handle, trader_key, state) =
            writer_through(CacheConfig::default(), database_through(&proxy));
        let order = format!("{trader_key}{REDIS_DELIMITER}orders:O-1");
        let ledger = format!("{trader_key}{REDIS_DELIMITER}general:ledger");
        let fills = 100;

        tx.send(command(
            DatabaseOperation::ReplaceList,
            "orders:O-1",
            &b"initialized"[..],
        ))
        .unwrap();
        wait_for_list_len(&con, &order, 1).await;
        proxy.pause();
        for seq in 1..=fills {
            tx.send(command(
                DatabaseOperation::Insert,
                "general:ledger",
                format!("ledger-{seq}"),
            ))
            .unwrap();
            tx.send(command(
                DatabaseOperation::UpdateOrder,
                "orders:O-1",
                format!("fill-{seq}"),
            ))
            .unwrap();
        }
        wait_until_holding(&state, 1 + fills).await;
        proxy.resume();
        tx.send(DatabaseCommand::close()).unwrap();
        handle.await.unwrap();

        let stored: Option<String> = con.get(&ledger).await.unwrap();
        let events: Vec<String> = con.lrange(&order, 0, -1).await.unwrap();
        let _: usize = con.del(&[&order, &ledger]).await.unwrap();

        let mut expected = vec!["initialized".to_string()];
        expected.extend((1..=fills).map(|seq| format!("fill-{seq}")));
        assert_eq!(stored.as_deref(), Some(format!("ledger-{fills}").as_str()));
        assert_eq!(events, expected);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_an_order_update_between_replacements_of_its_order_keeps_the_earlier_one() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let config = CacheConfig::default();
        let encoding = config.encoding;
        let (tx, handle, trader_key, _) = writer_through(config, database_through(&proxy));
        let initialized = OrderInitialized::default();
        let reinitialized = OrderInitialized {
            event_id: UUID4::new(),
            ..initialized.clone()
        };
        let denied = OrderDenied::new(
            initialized.trader_id,
            initialized.strategy_id,
            initialized.instrument_id,
            initialized.client_order_id,
            Ustr::from("denied"),
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
        );
        let client_order_id = initialized.client_order_id.to_string();
        let order_key = format!("orders:{client_order_id}");
        let order = format!("{trader_key}{REDIS_DELIMITER}{order_key}");
        let closed = format!("{trader_key}{REDIS_DELIMITER}{INDEX_ORDERS_CLOSED}");
        let ledger = format!("{trader_key}{REDIS_DELIMITER}general:ledger");
        let payload = |event: OrderEventAny| {
            Bytes::from(DatabaseQueries::serialize_payload(encoding, &event).unwrap())
        };
        let second = payload(OrderEventAny::Initialized(reinitialized));

        proxy.pause();
        tx.send(command(
            DatabaseOperation::Insert,
            "general:ledger",
            &b"ledger"[..],
        ))
        .unwrap();
        tx.send(command(
            DatabaseOperation::ReplaceList,
            &order_key,
            payload(OrderEventAny::Initialized(initialized)),
        ))
        .unwrap();
        tx.send(command(
            DatabaseOperation::UpdateOrder,
            &order_key,
            payload(OrderEventAny::Denied(denied)),
        ))
        .unwrap();
        tx.send(command(
            DatabaseOperation::ReplaceList,
            &order_key,
            second.clone(),
        ))
        .unwrap();
        // A second held write is the ledger's retry, sent once the writer took the rest in
        wait_until_async(
            || async { proxy.held_writes() >= 2 },
            Duration::from_secs(10),
        )
        .await;
        proxy.resume();
        tx.send(DatabaseCommand::close()).unwrap();
        handle.await.unwrap();

        let events: Vec<Vec<u8>> = con.lrange(&order, 0, -1).await.unwrap();
        let is_closed: bool = con.sismember(&closed, &client_order_id).await.unwrap();
        let _: usize = con.del(&[&order, &closed, &ledger]).await.unwrap();
        let _: usize = con
            .del(
                ORDER_INDEXES
                    .iter()
                    .map(|(index, _)| format!("{trader_key}{REDIS_DELIMITER}{index}"))
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();

        assert_eq!(events, vec![second.to_vec()]);
        assert!(is_closed);
    }

    async fn cache_through(
        proxy: &RedisProxy,
        config: CacheConfig,
        database: RedisCacheConfig,
    ) -> RedisCacheDatabase {
        let config = CacheConfig {
            use_instance_id: true,
            ..config
        };
        let cache = RedisCacheDatabase::new(
            TraderId::from("tester-001"),
            UUID4::new(),
            config,
            RedisCacheConfig {
                port: Some(proxy.port),
                ..database
            },
        )
        .await
        .unwrap();
        let state = cache.writer.clone();
        wait_until_async(|| async { !state.is_failing() }, Duration::from_secs(3)).await;
        cache
    }

    /// Closes `cache` on a thread of its own, sending how long the close took.
    fn close_on_thread(mut cache: RedisCacheDatabase) -> std::sync::mpsc::Receiver<Duration> {
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            cache.close();
            let _ = closed_tx.send(started.elapsed());
        });
        closed_rx
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_healthy_close_waits_for_a_large_buffered_drain() {
        let mut con = redis_connection().await;
        let proxy = RedisProxy::start().await;
        let database = RedisCacheConfig {
            response_timeout: 30,
            ..database_through(&proxy)
        };
        let cache = cache_through(&proxy, buffered(), database).await;
        let keys: Vec<String> = (0..10_000)
            .map(|seq| format!("{}{REDIS_DELIMITER}general:key-{seq}", cache.trader_key))
            .collect();
        for seq in 0..10_000 {
            cache
                .tx
                .send(command(
                    DatabaseOperation::Insert,
                    &format!("general:key-{seq}"),
                    seq.to_string(),
                ))
                .unwrap();
        }

        // Redis answers late but within the response timeout: the writer is delivering, not
        // retrying, so its close outlasts the bound a retrying writer's close keeps
        proxy.pause();
        let closed = close_on_thread(cache);
        tokio::time::sleep(CLOSE_TIMEOUT + Duration::from_millis(500)).await;
        let closed_while_paused = closed.try_recv().is_ok();
        proxy.resume();
        closed
            .recv_timeout(Duration::from_secs(30))
            .expect("the close returns once its drain is answered");
        let stored: usize = con.exists(&keys).await.unwrap();
        let _: usize = con.del(&keys).await.unwrap();

        assert!(!closed_while_paused);
        assert_eq!(stored, 10_000);
    }
}
