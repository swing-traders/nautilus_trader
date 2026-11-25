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

//! Closing a Redis writer while Redis is paused. These tests have a binary of their own because
//! they install a global logger, which a node built by another test would then refuse.

#[cfg(test)]
#[cfg(feature = "redis")]
#[cfg(target_os = "linux")] // Databases only tested and supported on Linux
mod serial_tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use log::{Level, LevelFilter, Log, Metadata, Record};
    use nautilus_common::{
        cache::{CacheConfig, database::CacheDatabaseAdapter},
        enums::SerializationEncoding,
        msgbus::{BusMessage, BusPayloadType, MessageBusBacking, MessageBusConfig},
        testing::wait_until_async,
    };
    use nautilus_core::UUID4;
    use nautilus_infrastructure::redis::{
        cache::{RedisCacheConfig, RedisCacheDatabase, RedisCacheDatabaseAdapter},
        create_redis_connection, get_stream_key,
        msgbus::{RedisMessageBusBacking, RedisMessageBusConfig},
    };
    use nautilus_model::{
        events::{OrderDenied, OrderEventAny},
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
    };
    use redis::{AsyncCommands, aio::ConnectionManager};
    use rstest::rstest;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{
            TcpListener, TcpStream,
            tcp::{OwnedReadHalf, OwnedWriteHalf},
        },
        sync::watch,
    };
    use ustr::Ustr;

    const REDIS_ADDR: &str = "127.0.0.1:6379";
    /// The longest a close waits for a writer retrying because Redis isn't answering.
    const CLOSE_BOUND: Duration = Duration::from_secs(2);

    /// Every record logged at WARN or above, once `capture_logs` has installed it.
    struct CapturedLogs {
        records: Mutex<Vec<(Level, String)>>,
    }

    impl Log for CapturedLogs {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.level() <= Level::Warn
        }

        fn log(&self, record: &Record<'_>) {
            if self.enabled(record.metadata()) {
                self.records
                    .lock()
                    .unwrap()
                    .push((record.level(), record.args().to_string()));
            }
        }

        fn flush(&self) {}
    }

    static CAPTURED_LOGS: CapturedLogs = CapturedLogs {
        records: Mutex::new(Vec::new()),
    };

    /// Installs the capturing logger and clears it, holding the returned guard for the test so
    /// tests run one at a time and each sees only its own records.
    async fn capture_logs() -> tokio::sync::MutexGuard<'static, ()> {
        static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let guard = ONE_AT_A_TIME.lock().await;
        let _ = log::set_logger(&CAPTURED_LOGS);
        log::set_max_level(LevelFilter::Warn);
        CAPTURED_LOGS.records.lock().unwrap().clear();
        guard
    }

    fn logged(level: Level, message: &str) -> bool {
        CAPTURED_LOGS
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|(logged_level, logged)| *logged_level == level && logged == message)
    }

    const EXEC: &[u8] = b"EXEC\r\n";
    const XTRIM: &[u8] = b"XTRIM\r\n";

    /// A TCP proxy in front of the test Redis that holds every byte while paused, as a paused
    /// server does, drops transactions on demand, or holds the connections sending a request.
    struct FaultProxy {
        port: u16,
        controls: Controls,
    }

    #[derive(Clone)]
    struct Controls {
        paused: Arc<watch::Sender<bool>>,
        held_writes: Arc<AtomicUsize>,
        drop_transactions: Arc<AtomicBool>,
        dropped_transactions: Arc<AtomicUsize>,
        hold_sending: Arc<Mutex<Option<&'static [u8]>>>,
        released: Arc<watch::Sender<usize>>,
        drop_next_connection: Arc<AtomicBool>,
        drop_next_trim: Arc<AtomicBool>,
    }

    impl FaultProxy {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let controls = Controls {
                paused: Arc::new(watch::Sender::new(false)),
                held_writes: Arc::new(AtomicUsize::new(0)),
                drop_transactions: Arc::new(AtomicBool::new(false)),
                dropped_transactions: Arc::new(AtomicUsize::new(0)),
                hold_sending: Arc::new(Mutex::new(None)),
                released: Arc::new(watch::Sender::new(0)),
                drop_next_connection: Arc::new(AtomicBool::new(false)),
                drop_next_trim: Arc::new(AtomicBool::new(false)),
            };
            let proxy = Self {
                port,
                controls: controls.clone(),
            };

            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    if controls.drop_next_connection.swap(false, Ordering::SeqCst) {
                        drop(client);
                        continue;
                    }
                    let server = TcpStream::connect(REDIS_ADDR).await.unwrap();
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    tokio::spawn(forward_requests(
                        client_read,
                        server_write,
                        controls.clone(),
                    ));
                    tokio::spawn(forward_replies(
                        server_read,
                        client_write,
                        controls.paused.subscribe(),
                    ));
                }
            });

            proxy
        }

        fn pause(&self) {
            self.controls.paused.send_replace(true);
        }

        fn resume(&self) {
            self.controls.paused.send_replace(false);
        }

        /// The number of client writes that arrived while traffic was held.
        fn held_writes(&self) -> usize {
            self.controls.held_writes.load(Ordering::SeqCst)
        }

        /// While on, discards each request holding an `EXEC` and closes its connection, so
        /// Redis never runs a transaction; everything else passes.
        fn drop_transactions(&self, on: bool) {
            self.controls.drop_transactions.store(on, Ordering::SeqCst);
        }

        fn dropped_transactions(&self) -> usize {
            self.controls.dropped_transactions.load(Ordering::SeqCst)
        }

        /// Holds every request of a connection once it sends one containing `request`, until
        /// `release_held_connections`.
        fn hold_connections_sending(&self, request: &'static [u8]) {
            *self.controls.hold_sending.lock().unwrap() = Some(request);
        }

        /// Closes the next connection a client opens before relaying anything.
        fn drop_next_connection(&self) {
            self.controls
                .drop_next_connection
                .store(true, Ordering::SeqCst);
        }

        /// Discards the next request holding an `XTRIM` and closes its connection.
        fn drop_next_trim(&self) {
            self.controls.drop_next_trim.store(true, Ordering::SeqCst);
        }

        fn release_held_connections(&self) {
            *self.controls.hold_sending.lock().unwrap() = None;
            self.controls
                .released
                .send_modify(|generation| *generation += 1);
        }
    }

    async fn forward_requests(mut from: OwnedReadHalf, mut to: OwnedWriteHalf, controls: Controls) {
        let mut paused = controls.paused.subscribe();
        let mut released = controls.released.subscribe();
        let mut buf = vec![0u8; 64 * 1024];
        let mut held: Vec<u8> = Vec::new();
        let mut held_connection = false;
        loop {
            tokio::select! {
                read = from.read(&mut buf) => {
                    let n = match read {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    let request = &buf[..n];
                    let holds_exec = request.windows(EXEC.len()).any(|window| window == EXEC);
                    let holds_trim = request.windows(XTRIM.len()).any(|window| window == XTRIM);
                    if holds_trim && controls.drop_next_trim.swap(false, Ordering::SeqCst) {
                        return;
                    }
                    if holds_exec && controls.drop_transactions.load(Ordering::SeqCst) {
                        controls.dropped_transactions.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                    if let Some(sending) = *controls.hold_sending.lock().unwrap()
                        && request.windows(sending.len()).any(|window| window == sending)
                    {
                        held_connection = true;
                    }
                    if *paused.borrow() || held_connection || !held.is_empty() {
                        controls.held_writes.fetch_add(1, Ordering::SeqCst);
                        held.extend_from_slice(request);
                    } else if to.write_all(request).await.is_err() {
                        return;
                    }
                }
                Ok(()) = paused.changed(), if !held.is_empty() => {
                    if !*paused.borrow() && !held_connection {
                        if to.write_all(&held).await.is_err() {
                            return;
                        }
                        held.clear();
                    }
                }
                Ok(()) = released.changed(), if held_connection => {
                    held_connection = false;
                    if !*paused.borrow() {
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
        mut from: OwnedReadHalf,
        mut to: OwnedWriteHalf,
        mut paused: watch::Receiver<bool>,
    ) {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match from.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            let _ = paused.wait_for(|paused| !paused).await;
            if to.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    }

    async fn redis_connection() -> ConnectionManager {
        let config = RedisCacheConfig {
            connection_timeout: 1,
            number_of_retries: 0,
            ..Default::default()
        };
        create_redis_connection("test", &config)
            .await
            .expect("A running Redis service is required for this test")
    }

    async fn wait_for_key(con: &ConnectionManager, key: &str) {
        wait_until_async(
            || {
                let mut con = con.clone();
                let key = key.to_string();
                async move { con.exists::<_, bool>(&key).await.unwrap() }
            },
            Duration::from_secs(10),
        )
        .await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_cache_close_while_redis_is_paused_returns_and_logs_what_it_held() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;
        let config = CacheConfig {
            use_instance_id: true,
            ..Default::default()
        };
        let database = RedisCacheConfig {
            port: Some(proxy.port),
            connection_timeout: 1,
            response_timeout: 1,
            ..Default::default()
        };
        let mut cache =
            RedisCacheDatabase::new(TraderId::from("tester-001"), UUID4::new(), config, database)
                .await
                .unwrap();
        let ready = format!("{}:general:ready", cache.trader_key);
        let ledger = format!("{}:general:ledger", cache.trader_key);

        cache
            .insert(
                "general:ready".to_string(),
                Some(vec![Bytes::from_static(b"ready")]),
            )
            .unwrap();
        wait_for_key(&con, &ready).await;
        proxy.pause();
        cache
            .insert(
                "general:ledger".to_string(),
                Some(vec![Bytes::from_static(b"ledger")]),
            )
            .unwrap();
        // A second held write is the ledger's retry, sent after the first went unanswered
        wait_until_async(
            || async { proxy.held_writes() >= 2 },
            Duration::from_secs(10),
        )
        .await;
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let started = Instant::now();
            cache.close();
            let _ = closed_tx.send(started.elapsed());
        });
        let waited = closed_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the close returns while Redis is paused");
        proxy.resume();
        // The writer outlives its close, and delivers once Redis answers
        wait_for_key(&con, &ledger).await;
        let _: usize = con.del(&[&ready, &ledger]).await.unwrap();

        assert!(
            waited < CLOSE_BOUND + Duration::from_secs(1),
            "close waited {waited:?}"
        );
        assert!(logged(
            Level::Error,
            "Closed 'cache-process' while Redis is not answering: 1 queued writes not delivered"
        ));
    }

    fn numbered(seq: u32) -> BusMessage {
        BusMessage::with_str_topic(
            "test_topic",
            BusPayloadType::Custom(Ustr::default()),
            Bytes::from(seq.to_string()),
            SerializationEncoding::Json,
        )
    }

    async fn wait_for_stream_len(con: &ConnectionManager, key: &str, len: usize) {
        wait_until_async(
            || {
                let mut con = con.clone();
                let key = key.to_string();
                async move { con.xlen::<_, usize>(&key).await.unwrap() >= len }
            },
            Duration::from_secs(10),
        )
        .await;
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_bus_close_while_redis_is_paused_returns_and_logs_what_it_held() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            heartbeat_interval_secs: None,
            ..Default::default()
        };
        let backing = RedisMessageBusConfig {
            port: Some(proxy.port),
            connection_timeout: 1,
            response_timeout: 1,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);
        let mut bus = RedisMessageBusBacking::new(trader_id, instance_id, config, backing).unwrap();

        bus.publish(numbered(0));
        wait_for_stream_len(&con, &stream_key, 1).await;
        proxy.pause();
        for seq in 1..=3 {
            bus.publish(numbered(seq));
        }
        // A second held write is the retry, sent after the first went unanswered
        wait_until_async(
            || async { proxy.held_writes() >= 2 },
            Duration::from_secs(10),
        )
        .await;
        let started = Instant::now();
        bus.close();
        let waited = started.elapsed();
        proxy.resume();
        // The publisher outlives its close, and delivers once Redis answers
        wait_for_stream_len(&con, &stream_key, 4).await;
        let _: usize = con.del(&stream_key).await.unwrap();

        assert!(
            waited < CLOSE_BOUND + Duration::from_secs(1),
            "close waited {waited:?}"
        );
        assert!(logged(
            Level::Error,
            "Closed 'msgbus-publish' while Redis is not answering: 3 queued writes not delivered"
        ));
    }

    /// Closes `cache` on a thread of its own, returning how long the close took.
    fn close_on_thread(mut cache: RedisCacheDatabase) -> Duration {
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let started = Instant::now();
            cache.close();
            let _ = closed_tx.send(started.elapsed());
        });
        closed_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the close returns while Redis is not answering")
    }

    async fn cache_through(proxy: &FaultProxy, timeouts: u16) -> RedisCacheDatabase {
        let config = CacheConfig {
            use_instance_id: true,
            ..Default::default()
        };
        let database = RedisCacheConfig {
            port: Some(proxy.port),
            connection_timeout: timeouts,
            response_timeout: timeouts,
            ..Default::default()
        };
        RedisCacheDatabase::new(TraderId::from("tester-001"), UUID4::new(), config, database)
            .await
            .unwrap()
    }

    /// Writes a key through `cache` and waits for it, so its writer is delivering.
    async fn delivering(cache: &RedisCacheDatabase, con: &ConnectionManager) -> String {
        let ready = format!("{}:general:ready", cache.trader_key);
        cache
            .insert(
                "general:ready".to_string(),
                Some(vec![Bytes::from_static(b"ready")]),
            )
            .unwrap();
        wait_for_key(con, &ready).await;
        ready
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_cache_close_while_its_transaction_keeps_failing_returns_within_the_bound() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;
        let mut cache = cache_through(&proxy, 1).await;
        let ready = delivering(&cache, &con).await;
        let order = format!("{}:orders:O-1", cache.trader_key);
        let _: usize = con.rpush(&order, &b"initialized"[..]).await.unwrap();

        // Connections, fences and proof reads all succeed; only the transaction never runs
        proxy.drop_transactions(true);
        cache
            .update(
                "orders:O-1".to_string(),
                Some(vec![Bytes::from_static(b"filled")]),
            )
            .unwrap();
        wait_until_async(
            || async { proxy.dropped_transactions() >= 2 },
            Duration::from_secs(10),
        )
        .await;
        let waited = close_on_thread(cache);
        proxy.drop_transactions(false);
        // The writer outlives its close, and delivers once Redis runs its transaction
        wait_until_async(
            || {
                let mut con = con.clone();
                let order = order.clone();
                async move { con.llen::<_, usize>(&order).await.unwrap() == 2 }
            },
            Duration::from_secs(10),
        )
        .await;
        let _: usize = con.del(&[&ready, &order]).await.unwrap();

        assert!(
            waited < CLOSE_BOUND + Duration::from_secs(1),
            "close waited {waited:?}"
        );
        assert!(logged(
            Level::Error,
            "Closed 'cache-process' while Redis is not answering: 1 queued writes not delivered"
        ));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_cache_close_during_its_setup_counts_the_queued_writes() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;

        // The write connection's setup asks its `CLIENT ID`, which the read connection never does
        proxy.hold_connections_sending(b"CLIENT\r\n$2\r\nID\r\n");
        let cache = cache_through(&proxy, 20).await;
        let keys: Vec<String> = (1..=3)
            .map(|seq| format!("{}:general:key-{seq}", cache.trader_key))
            .collect();
        for seq in 1..=3 {
            cache
                .insert(
                    format!("general:key-{seq}"),
                    Some(vec![Bytes::from(seq.to_string())]),
                )
                .unwrap();
        }
        let waited = close_on_thread(cache);
        proxy.release_held_connections();
        for key in &keys {
            wait_for_key(&con, key).await;
        }
        let _: usize = con.del(&keys).await.unwrap();

        assert!(
            waited < CLOSE_BOUND + Duration::from_secs(1),
            "close waited {waited:?}"
        );
        assert!(logged(
            Level::Error,
            "Closed 'cache-process' while Redis is not answering: 3 queued writes not delivered"
        ));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_cache_close_during_a_retrying_order_update_counts_it() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;
        let cache = cache_through(&proxy, 1).await;
        let ready = delivering(&cache, &con).await;
        let adapter = RedisCacheDatabaseAdapter { database: cache };
        let denied = OrderDenied::new(
            TraderId::from("tester-001"),
            StrategyId::from("S-001"),
            InstrumentId::from("AUD/USD.SIM"),
            ClientOrderId::from("O-1"),
            Ustr::from("denied"),
            UUID4::new(),
            0.into(),
            0.into(),
        );

        proxy.pause();
        adapter
            .update_order(&OrderEventAny::Denied(denied))
            .unwrap();
        // A second held write is the order read's retry, sent after the first went unanswered
        wait_until_async(
            || async { proxy.held_writes() >= 2 },
            Duration::from_secs(10),
        )
        .await;
        let waited = close_on_thread(adapter.database);
        proxy.resume();
        let _: usize = con.del(&ready).await.unwrap();

        assert!(
            waited < CLOSE_BOUND + Duration::from_secs(1),
            "close waited {waited:?}"
        );
        assert!(logged(
            Level::Error,
            "Closed 'cache-process' while Redis is not answering: 1 queued writes not delivered"
        ));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_bus_close_during_its_setup_counts_the_queued_messages() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            heartbeat_interval_secs: None,
            ..Default::default()
        };
        let backing = RedisMessageBusConfig {
            port: Some(proxy.port),
            connection_timeout: 20,
            response_timeout: 20,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        proxy.pause();
        let mut bus = RedisMessageBusBacking::new(trader_id, instance_id, config, backing).unwrap();
        for seq in 1..=3 {
            bus.publish(numbered(seq));
        }
        let started = Instant::now();
        bus.close();
        let waited = started.elapsed();
        proxy.resume();
        wait_for_stream_len(&con, &stream_key, 3).await;
        let _: usize = con.del(&stream_key).await.unwrap();

        assert!(
            waited < CLOSE_BOUND + Duration::from_secs(1),
            "close waited {waited:?}"
        );
        assert!(logged(
            Level::Error,
            "Closed 'msgbus-publish' while Redis is not answering: 3 queued writes not delivered"
        ));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_bus_close_during_a_held_trim_after_a_failed_setup_counts_the_queued_messages() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            heartbeat_interval_secs: None,
            autotrim_mins: Some(1),
            ..Default::default()
        };
        let backing = RedisMessageBusConfig {
            port: Some(proxy.port),
            connection_timeout: 20,
            response_timeout: 20,
            number_of_retries: 0,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        // The first setup fails; its retry is held until the publications are queued behind it
        proxy.drop_next_connection();
        proxy.pause();
        let mut bus = RedisMessageBusBacking::new(trader_id, instance_id, config, backing).unwrap();
        wait_until_async(
            || async { proxy.held_writes() >= 1 },
            Duration::from_secs(10),
        )
        .await;
        for seq in 1..=3 {
            bus.publish(numbered(seq));
        }
        proxy.hold_connections_sending(b"XTRIM");
        proxy.resume();
        // A second held write is the first trim
        wait_until_async(
            || async { proxy.held_writes() >= 2 },
            Duration::from_secs(10),
        )
        .await;
        let started = Instant::now();
        bus.close();
        let waited = started.elapsed();
        proxy.release_held_connections();
        wait_for_stream_len(&con, &stream_key, 3).await;
        let _: usize = con.del(&stream_key).await.unwrap();

        assert!(
            waited < CLOSE_BOUND + Duration::from_secs(1),
            "close waited {waited:?}"
        );
        assert!(logged(
            Level::Error,
            "Closed 'msgbus-publish' while Redis is not answering: 3 queued writes not delivered"
        ));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_first_healthy_buffered_close_waits_for_the_writes_queued_during_setup() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;
        let config = CacheConfig {
            use_instance_id: true,
            buffer_interval_ms: Some(3_600_000),
            ..Default::default()
        };
        let database = RedisCacheConfig {
            port: Some(proxy.port),
            connection_timeout: 20,
            response_timeout: 20,
            ..Default::default()
        };

        // The write connection's setup is held while the writes queue behind it, then answered
        proxy.hold_connections_sending(b"CLIENT\r\n$2\r\nID\r\n");
        let mut cache =
            RedisCacheDatabase::new(TraderId::from("tester-001"), UUID4::new(), config, database)
                .await
                .unwrap();
        let keys: Vec<String> = (1..=3)
            .map(|seq| format!("{}:general:key-{seq}", cache.trader_key))
            .collect();
        for seq in 1..=3 {
            cache
                .insert(
                    format!("general:key-{seq}"),
                    Some(vec![Bytes::from(seq.to_string())]),
                )
                .unwrap();
        }
        proxy.release_held_connections();
        proxy.hold_connections_sending(EXEC);
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            cache.close();
            let _ = closed_tx.send(());
        });
        // The drain's answer comes later than a retrying close would wait, within the response
        // timeout: the writer is delivering, so its close outlasts that bound
        tokio::time::sleep(CLOSE_BOUND + Duration::from_millis(500)).await;
        let closed_before_answer = closed_rx.try_recv().is_ok();
        proxy.release_held_connections();
        let closed =
            closed_before_answer || closed_rx.recv_timeout(Duration::from_secs(20)).is_ok();
        let stored: usize = con.exists(&keys).await.unwrap();
        for key in &keys {
            wait_for_key(&con, key).await;
        }
        let _: usize = con.del(&keys).await.unwrap();

        assert!(
            !closed_before_answer,
            "the close returned before its drain was answered"
        );
        assert!(closed);
        assert_eq!(stored, 3);
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_a_bus_close_after_a_failed_trim_counts_the_queued_messages() {
        let _logs = capture_logs().await;
        let mut con = redis_connection().await;
        let proxy = FaultProxy::start().await;
        let trader_id = TraderId::from("tester-001");
        let instance_id = UUID4::new();
        let config = MessageBusConfig {
            use_instance_id: true,
            stream_per_topic: false,
            heartbeat_interval_secs: None,
            autotrim_mins: Some(1),
            ..Default::default()
        };
        let backing = RedisMessageBusConfig {
            port: Some(proxy.port),
            connection_timeout: 20,
            response_timeout: 20,
            ..Default::default()
        };
        let stream_key = get_stream_key(trader_id, instance_id, &config);

        // A healthy publisher whose first trim fails, then whose publication is held unanswered
        proxy.drop_next_trim();
        proxy.hold_connections_sending(EXEC);
        let mut bus = RedisMessageBusBacking::new(trader_id, instance_id, config, backing).unwrap();
        for seq in 1..=3 {
            bus.publish(numbered(seq));
        }
        wait_until_async(
            || async { proxy.held_writes() >= 1 },
            Duration::from_secs(10),
        )
        .await;
        let started = Instant::now();
        bus.close();
        let waited = started.elapsed();
        proxy.release_held_connections();
        wait_for_stream_len(&con, &stream_key, 3).await;
        let _: usize = con.del(&stream_key).await.unwrap();

        assert!(
            waited < CLOSE_BOUND + Duration::from_secs(1),
            "close waited {waited:?}"
        );
        assert!(logged(
            Level::Error,
            "Closed 'msgbus-publish' while Redis is not answering: 3 queued writes not delivered"
        ));
    }
}
