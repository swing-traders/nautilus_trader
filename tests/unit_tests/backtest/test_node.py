# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import multiprocessing
import tracemalloc
from unittest.mock import patch

import msgspec
import pytest

import nautilus_trader.backtest.node as node
from nautilus_trader.adapters.tardis.loaders import TardisCSVDataLoader
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.backtest.results import BacktestResult
from nautilus_trader.common.actor import Actor
from nautilus_trader.common.config import InvalidConfiguration
from nautilus_trader.config import BacktestDataConfig
from nautilus_trader.config import BacktestRunConfig
from nautilus_trader.config import BacktestVenueConfig
from nautilus_trader.config import DataCatalogConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
from nautilus_trader.test_kit.mocks.data import load_catalog_with_stub_quote_ticks_audusd
from nautilus_trader.test_kit.mocks.data import setup_catalog
from nautilus_trader.test_kit.providers import TestDataProvider
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.providers import get_test_data_large_path


class DummyStreamingSession:
    def __init__(self, chunk_size=None):
        self.chunk_size = chunk_size

    def to_query_result(self):
        return []


class DummyStreamingCatalog:
    def __init__(self, path: str, protocol: str | None):
        self.path = path
        self.fs_protocol = protocol
        self.calls = 0

    def get_file_list_from_data_cls(self, data_cls: type):
        self.calls += 1
        return [f"{self.path}/{data_cls.__name__}.parquet"]

    def filter_files(
        self,
        data_cls: type,
        file_paths: list[str],
        identifiers=None,
        start=None,
        end=None,
    ):
        return file_paths

    def backend_session(
        self,
        data_cls,
        identifiers,
        start,
        end,
        session,
        files,
        optimize_file_loading,
        **kwargs,
    ):
        return session


class DummyStreamingLogger:
    def __init__(self):
        self.warnings: list[str] = []

    def info(self, message, color=None):
        return None

    def warning(self, message, color=None):
        self.warnings.append(message)


class DummyStreamingEngine:
    def __init__(self):
        self.logger = DummyStreamingLogger()
        self.subscription_names: list[str] = []

    def add_data(self, data, validate=True, sort=True):
        return None

    def add_subscription_names(self, names):
        self.subscription_names.extend(names)

    def run(self, start=None, end=None, run_config_id=None, streaming=None):
        return None

    def clear_data(self):
        return None

    def end(self):
        return None


_AUDUSD_SIM = TestInstrumentProvider.default_fx_ccy("AUD/USD")
_BTCUSDT_HUOBI = TestInstrumentProvider.btcusdt_future_binance()  # Use as stand-in for Huobi

_STREAMING_BAR_TYPE = BarType.from_str("AUD/USD.SIM-1-MINUTE-BID-EXTERNAL")
_STREAMING_START_NS = 1_704_067_200_000_000_000  # 2024-01-01T00:00:00Z
_ONE_MINUTE_NS = 60_000_000_000


class StreamingRecorderActor(Actor):
    """
    Records the quotes and bars delivered to a subscriber, in delivery order.

    `bar_params` is passed to the bar subscription, so a test can reach the parameters
    a data request is made with.

    """

    def __init__(self, bar_params: dict[str, object] | None = None) -> None:
        super().__init__()
        self.received: list[tuple[str, int]] = []
        self.bar_params = bar_params

    def on_start(self) -> None:
        self.subscribe_bars(_STREAMING_BAR_TYPE, params=self.bar_params)
        self.subscribe_quote_ticks(_AUDUSD_SIM.id)

    def on_bar(self, bar: Bar) -> None:
        self.received.append(("bar", bar.ts_init))

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.received.append(("quote", tick.ts_init))


def load_catalog_with_quotes_and_bars(
    catalog: ParquetDataCatalog,
    bar_offset_ns: int,
) -> list[tuple[str, int]]:
    """
    Load six one-minute quotes and six bars for AUD/USD.SIM to the catalog.

    `bar_offset_ns` shifts every bar off its minute, so bar and quote timestamps either
    tie (0) or stay distinct (1).

    Returns every written record as a (kind, ts_init) pair in timestamp order, so a test
    can assert delivery against the data it wrote rather than against another run.

    """
    quotes = []
    bars = []

    for i in range(6):
        ts_event = _STREAMING_START_NS + i * _ONE_MINUTE_NS
        quotes.append(
            QuoteTick(
                instrument_id=_AUDUSD_SIM.id,
                bid_price=Price.from_str(f"0.{67000 + i}"),
                ask_price=Price.from_str(f"0.{67010 + i}"),
                bid_size=Quantity.from_int(1_000_000),
                ask_size=Quantity.from_int(1_000_000),
                ts_event=ts_event,
                ts_init=ts_event,
            ),
        )
        bars.append(
            Bar(
                bar_type=_STREAMING_BAR_TYPE,
                open=Price.from_str(f"0.{67000 + i}"),
                high=Price.from_str(f"0.{67020 + i}"),
                low=Price.from_str(f"0.{66990 + i}"),
                close=Price.from_str(f"0.{67005 + i}"),
                volume=Quantity.from_int(1_000_000),
                ts_event=ts_event + bar_offset_ns,
                ts_init=ts_event + bar_offset_ns,
            ),
        )

    catalog.write_data([_AUDUSD_SIM])
    catalog.write_data(quotes)
    catalog.write_data(bars)

    written = [("quote", quote.ts_init) for quote in quotes]
    written += [("bar", bar.ts_init) for bar in bars]
    written.sort(key=lambda record: record[1])

    return written


def load_catalog_with_a_boundary_quote_and_bars(catalog: ParquetDataCatalog) -> None:
    """
    Load three quotes and two bars for AUD/USD.SIM to the catalog.

    The second quote sits one nanosecond past the first, so a run reading one record at
    a time closes its first chunk exactly on the first quote, and a bar sits on each of
    those two timestamps.

    """
    timestamps = [
        _STREAMING_START_NS,
        _STREAMING_START_NS + 1,
        _STREAMING_START_NS + 2 * _ONE_MINUTE_NS,
    ]
    quotes = [
        QuoteTick(
            instrument_id=_AUDUSD_SIM.id,
            bid_price=Price.from_str("0.67000"),
            ask_price=Price.from_str("0.67010"),
            bid_size=Quantity.from_int(1_000_000),
            ask_size=Quantity.from_int(1_000_000),
            ts_event=ts_event,
            ts_init=ts_event,
        )
        for ts_event in timestamps
    ]
    bars = [
        Bar(
            bar_type=_STREAMING_BAR_TYPE,
            open=Price.from_str("0.67000"),
            high=Price.from_str("0.67020"),
            low=Price.from_str("0.66990"),
            close=Price.from_str("0.67005"),
            volume=Quantity.from_int(1_000_000),
            ts_event=ts_event,
            ts_init=ts_event,
        )
        for ts_event in timestamps[:2]
    ]

    catalog.write_data([_AUDUSD_SIM])
    catalog.write_data(quotes)
    catalog.write_data(bars)


def run_quotes_and_bars_backtest(
    catalog: ParquetDataCatalog,
    venue_config: BacktestVenueConfig,
    chunk_size: int | None,
    start: str | None = None,
    end: str | None = None,
    quote_time_window: tuple[int | None, int | None] | None = None,
    configure_bars: bool = True,
    bar_params: dict[str, object] | None = None,
) -> tuple[list[tuple[str, int]], BacktestResult]:
    """
    Run the quotes and bars catalog through a node, returning what was delivered.

    The engine is given the catalog so that a subscription which the engines own data
    does not cover is served from it. `quote_time_window` narrows the quote config to a
    (start, end) range, which a test can place away from the written quotes.
    `configure_bars` set to ``False`` leaves the bar series outside every data config,
    so its subscription is served by a data catalog request. `bar_params` is passed to
    that subscription.

    """
    quote_start, quote_end = quote_time_window or (None, None)
    data_configs = [
        BacktestDataConfig(
            catalog_path=catalog.path,
            catalog_fs_protocol=catalog.fs_protocol,
            data_cls=QuoteTick,
            instrument_id=_AUDUSD_SIM.id,
            start_time=quote_start,
            end_time=quote_end,
        ),
    ]

    if configure_bars:
        data_configs.append(
            BacktestDataConfig(
                catalog_path=catalog.path,
                catalog_fs_protocol=catalog.fs_protocol,
                data_cls=Bar,
                bar_types=[str(_STREAMING_BAR_TYPE)],
            ),
        )

    config = BacktestRunConfig(
        engine=BacktestEngineConfig(
            logging=LoggingConfig(bypass_logging=True),
            catalogs=[DataCatalogConfig(path=catalog.path, fs_protocol=catalog.fs_protocol)],
        ),
        venues=[venue_config],
        data=data_configs,
        chunk_size=chunk_size,
        start=start,
        end=end,
        raise_exception=True,
    )

    node_instance = BacktestNode(configs=[config])
    node_instance.build()

    actor = StreamingRecorderActor(bar_params)
    node_instance.get_engine(config.id).add_actor(actor)
    results = node_instance.run()

    return actor.received, results[0]


def run_streaming_with_stubs(
    monkeypatch,
    tmp_path,
    chunks: list[list[object]],
) -> DummyStreamingEngine:
    """
    Drive `_run_streaming` over the given chunks with a stubbed catalog, session,
    engine.

    The stub catalog reports one file for the data class, so the quote config counts as
    supplied by the stream. Returns the stub engine, whose logger recorded any warnings.

    """

    class StubStreamingSession:
        def __init__(self, chunk_size=None):
            self.chunk_size = chunk_size

        def to_query_result(self):
            return list(chunks)

    monkeypatch.setattr(node, "DataBackendSession", StubStreamingSession)
    monkeypatch.setattr(node, "pyo3_list_to_data_list", lambda chunk: chunk)
    monkeypatch.setattr(
        BacktestNode,
        "load_catalog",
        lambda _self, config: DummyStreamingCatalog(
            config.catalog_path,
            config.catalog_fs_protocol,
        ),
    )

    run_config = BacktestRunConfig(
        engine=BacktestEngineConfig(logging=LoggingConfig(bypass_logging=True)),
        venues=[
            BacktestVenueConfig(
                name="SIM",
                oms_type="HEDGING",
                account_type="MARGIN",
                base_currency="USD",
                starting_balances=["1000000 USD"],
            ),
        ],
        data=[
            BacktestDataConfig(
                catalog_path=(tmp_path / "stub_catalog").as_posix(),
                catalog_fs_protocol="file",
                data_cls=QuoteTick,
                instrument_id=_AUDUSD_SIM.id,
            ),
        ],
        chunk_size=1_000,
    )

    engine = DummyStreamingEngine()
    BacktestNode(configs=[run_config])._run_streaming(
        run_config_id=run_config.id,
        engine=engine,
        data_configs=run_config.data,
        chunk_size=run_config.chunk_size,
    )

    return engine


def load_catalog_with_quote_ticks(
    catalog: ParquetDataCatalog,
    count: int | None = None,
) -> tuple[int, int]:
    """
    Load quote ticks to catalog, optionally limiting count.

    Returns tuple of (start_time_ns, end_time_ns) for the loaded data.

    """
    wrangler = QuoteTickDataWrangler(_AUDUSD_SIM)
    ticks = wrangler.process(TestDataProvider().read_csv_ticks("truefx/audusd-ticks.csv"))
    ticks.sort(key=lambda x: x.ts_init)

    if count is not None:
        ticks = ticks[:count]

    catalog.write_data([_AUDUSD_SIM])
    catalog.write_data(ticks)
    return ticks[0].ts_init, ticks[-1].ts_init


def load_catalog_with_large_tardis_quotes(
    catalog: ParquetDataCatalog,
    limit: int = 500_000,
) -> tuple[int, int]:
    """
    Load large Tardis quote tick data to catalog for memory testing.

    Uses the gzipped Huobi BTC-USD quotes file (~1.15M records) from
    tests/test_data/large/. This provides realistic market data for testing streaming
    memory behavior.

    Returns tuple of (start_time_ns, end_time_ns) for the loaded data.

    """
    filepath = get_test_data_large_path() / "tardis_huobi-dm-swap_quotes_2020-05-01_BTC-USD.csv.gz"

    if not filepath.exists():
        pytest.skip(f"Large test data not found: {filepath}")

    loader = TardisCSVDataLoader(instrument_id=_BTCUSDT_HUOBI.id)
    ticks = loader.load_quotes(filepath, limit=limit)

    catalog.write_data([_BTCUSDT_HUOBI])
    catalog.write_data(ticks)
    return ticks[0].ts_init, ticks[-1].ts_init


def _run_backtest_measure_memory(config_json: bytes, result_queue: multiprocessing.Queue) -> None:
    """
    Run backtest in subprocess and measure peak memory.

    Must be at module level for multiprocessing to pickle it.

    """
    tracemalloc.start()
    config = BacktestRunConfig.parse(config_json)
    node = BacktestNode(configs=[config])
    node.run()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    result_queue.put(peak)


class TestBacktestNode:
    @pytest.fixture(autouse=True)
    def setup_method(self, tmp_path):
        self.catalog = setup_catalog(protocol="file", path=tmp_path / "catalog")
        self.venue_config = BacktestVenueConfig(
            name="SIM",
            oms_type="HEDGING",
            account_type="MARGIN",
            base_currency="USD",
            starting_balances=["1000000 USD"],
            # fill_model=fill_model,  # TODO: Implement
        )
        self.data_config = BacktestDataConfig(
            catalog_path=self.catalog.path,
            catalog_fs_protocol=self.catalog.fs_protocol,
            data_cls=QuoteTick,
            instrument_id=InstrumentId.from_str("AUD/USD.SIM"),
            start_time=1580398089820000000,
            end_time=1580504394501000000,
        )
        self.strategies = [
            ImportableStrategyConfig(
                strategy_path="nautilus_trader.examples.strategies.ema_cross:EMACross",
                config_path="nautilus_trader.examples.strategies.ema_cross:EMACrossConfig",
                config={
                    "instrument_id": "AUD/USD.SIM",
                    "bar_type": "AUD/USD.SIM-100-TICK-MID-INTERNAL",
                    "fast_ema_period": 10,
                    "slow_ema_period": 20,
                    "trade_size": "1_000_000",
                    "order_id_tag": "001",
                },
            ),
        ]
        self.backtest_configs = [
            BacktestRunConfig(
                engine=BacktestEngineConfig(
                    strategies=self.strategies,
                    logging=LoggingConfig(bypass_logging=True),
                ),
                venues=[self.venue_config],
                data=[self.data_config],
                chunk_size=5_000,
            ),
        ]
        load_catalog_with_stub_quote_ticks_audusd(self.catalog)  # Load sample data

    def test_init(self):
        # Arrange, Act
        node = BacktestNode(configs=self.backtest_configs)

        # Assert
        assert node

    @pytest.mark.parametrize(
        ("book_type"),
        [
            "L2_MBP",
            "L3_MBO",
        ],
    )
    def test_order_book_with_depth_data_config_validation(self, book_type: str) -> None:
        # Arrange
        venue_l3 = BacktestVenueConfig(
            name="SIM",
            oms_type="HEDGING",
            account_type="MARGIN",
            base_currency="USD",
            book_type=book_type,
            starting_balances=["1_000_000 USD"],
        )

        run_config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config, venue_l3],
            data=[self.data_config],
            chunk_size=None,  # No streaming
        )

        with pytest.raises(InvalidConfiguration) as exc_info:
            BacktestNode(configs=[run_config])

        assert (
            str(exc_info.value)
            == f"No order book data available for SIM with book type {book_type}"
        )

    def test_run(self):
        # Arrange
        node = BacktestNode(configs=self.backtest_configs)

        # Act
        results = node.run()

        # Assert
        assert len(results) == 1

    def test_backtest_run_batch_sync(self):
        # Arrange
        config = BacktestRunConfig(
            engine=BacktestEngineConfig(strategies=self.strategies),
            venues=[self.venue_config],
            data=[self.data_config],
            chunk_size=5_000,
        )

        node = BacktestNode(configs=[config])

        # Act
        results = node.run()

        # Assert
        assert len(results) == 1

    def test_backtest_run_results(self):
        # Arrange
        node = BacktestNode(configs=self.backtest_configs)

        # Act
        results = node.run()

        # Assert
        assert isinstance(results, list)
        assert len(results) == 1

    def test_node_config_from_raw(self):
        # Arrange
        raw = msgspec.json.encode(
            {
                "engine": {
                    "trader_id": "Test-111",
                    "logging": {
                        "log_level": "INFO",
                    },
                    "strategies": [
                        {
                            "strategy_path": "nautilus_trader.examples.strategies.ema_cross:EMACross",
                            "config_path": "nautilus_trader.examples.strategies.ema_cross:EMACrossConfig",
                            "config": {
                                "instrument_id": "AUD/USD.SIM",
                                "bar_type": "AUD/USD.SIM-100-TICK-MID-INTERNAL",
                                "fast_ema_period": 10,
                                "slow_ema_period": 20,
                                "trade_size": 1_000_000,
                                "order_id_tag": "001",
                            },
                        },
                    ],
                },
                "venues": [
                    {
                        "name": "SIM",
                        "oms_type": "HEDGING",
                        "account_type": "MARGIN",
                        "base_currency": "USD",
                        "starting_balances": ["1000000 USD"],
                    },
                ],
                "data": [
                    {
                        "catalog_path": "catalog",
                        "data_cls": "nautilus_trader.model.data:QuoteTick",
                        "instrument_id": "AUD/USD.SIM",
                        "start_time": 1580398089820000000,
                        "end_time": 1580504394501000000,
                    },
                ],
            },
        )

        # Act
        config = BacktestRunConfig.parse(raw)
        node = BacktestNode(configs=[config])

        # Assert
        node.run()

    def test_backtest_result_total_positions_matches_tearsheet_hedging(self):
        # Arrange
        node = BacktestNode(configs=self.backtest_configs)

        # Act
        results = node.run()
        result = results[0]
        engine = node.get_engines()[0]

        positions = list(engine.kernel.cache.positions())
        snapshots = list(engine.kernel.cache.position_snapshots())
        tearsheet_total = len(positions) + len(snapshots)

        # Assert
        assert result.total_positions == tearsheet_total
        assert result.total_positions == len(positions) + len(snapshots)

    def test_backtest_result_total_positions_matches_tearsheet_netting(self):
        # Arrange
        venue_config_netting = BacktestVenueConfig(
            name="SIM",
            oms_type="NETTING",
            account_type="MARGIN",
            base_currency="USD",
            starting_balances=["1000000 USD"],
        )
        config_netting = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[venue_config_netting],
            data=[self.data_config],
            chunk_size=5_000,
        )
        node = BacktestNode(configs=[config_netting])

        # Act
        results = node.run()
        result = results[0]
        engine = node.get_engines()[0]

        positions = list(engine.kernel.cache.positions())
        snapshots = list(engine.kernel.cache.position_snapshots())
        tearsheet_total = len(positions) + len(snapshots)

        # Assert
        assert result.total_positions == tearsheet_total
        assert result.total_positions == len(positions) + len(snapshots)


class TestBacktestNodeStreaming:
    """
    Tests for BacktestNode streaming mode memory efficiency.
    """

    @pytest.fixture(autouse=True)
    def setup_method(self, tmp_path):
        self.catalog = setup_catalog(protocol="file", path=tmp_path / "catalog")
        self.venue_config = BacktestVenueConfig(
            name="SIM",
            oms_type="HEDGING",
            account_type="MARGIN",
            base_currency="USD",
            starting_balances=["1000000 USD"],
        )
        self.strategies = [
            ImportableStrategyConfig(
                strategy_path="nautilus_trader.examples.strategies.ema_cross:EMACross",
                config_path="nautilus_trader.examples.strategies.ema_cross:EMACrossConfig",
                config={
                    "instrument_id": "AUD/USD.SIM",
                    "bar_type": "AUD/USD.SIM-100-TICK-MID-INTERNAL",
                    "fast_ema_period": 10,
                    "slow_ema_period": 20,
                    "trade_size": "1_000_000",
                    "order_id_tag": "001",
                },
            ),
        ]

    def test_streaming_processes_data_in_chunks(self):
        """
        Verify streaming mode processes multiple chunks, not all data at once.
        """
        # Arrange - load 10K ticks with chunk_size=1000, so should have ~10 chunks
        start_ns, end_ns = load_catalog_with_quote_ticks(self.catalog, count=10_000)

        data_config = BacktestDataConfig(
            catalog_path=self.catalog.path,
            catalog_fs_protocol=self.catalog.fs_protocol,
            data_cls=QuoteTick,
            instrument_id=InstrumentId.from_str("AUD/USD.SIM"),
            start_time=start_ns,
            end_time=end_ns,
        )
        config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config],
            data=[data_config],
            chunk_size=1_000,
        )

        chunk_count = 0
        original_run_streaming = BacktestNode._run_streaming

        def counting_run_streaming(self_node, *args, **kwargs):
            nonlocal chunk_count

            # Instrument the streaming loop by wrapping session.to_query_result
            from nautilus_trader.core.nautilus_pyo3 import DataBackendSession

            original_to_query_result = DataBackendSession.to_query_result

            def counting_to_query_result(session):
                nonlocal chunk_count
                for chunk in original_to_query_result(session):
                    chunk_count += 1
                    yield chunk

            DataBackendSession.to_query_result = counting_to_query_result
            try:
                return original_run_streaming(self_node, *args, **kwargs)
            finally:
                DataBackendSession.to_query_result = original_to_query_result

        # Act
        with patch.object(BacktestNode, "_run_streaming", counting_run_streaming):
            node = BacktestNode(configs=[config])
            node.run()

        # Assert - with 10K ticks and 1K chunk size, should have multiple chunks
        assert chunk_count > 1, f"Expected multiple chunks, was {chunk_count}"

    def test_streaming_clears_data_between_chunks(self):
        """
        Verify clear_data is called during streaming by checking _run_streaming is used.
        """
        # Arrange - load enough data to require multiple chunks
        start_ns, end_ns = load_catalog_with_quote_ticks(self.catalog, count=5_000)

        data_config = BacktestDataConfig(
            catalog_path=self.catalog.path,
            catalog_fs_protocol=self.catalog.fs_protocol,
            data_cls=QuoteTick,
            instrument_id=InstrumentId.from_str("AUD/USD.SIM"),
            start_time=start_ns,
            end_time=end_ns,
        )
        config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config],
            data=[data_config],
            chunk_size=1_000,
        )

        # Track that _run_streaming is called (which contains the clear_data calls)
        streaming_called = False
        original_run_streaming = BacktestNode._run_streaming

        def tracking_run_streaming(self_node, *args, **kwargs):
            nonlocal streaming_called
            streaming_called = True
            return original_run_streaming(self_node, *args, **kwargs)

        # Act
        with patch.object(BacktestNode, "_run_streaming", tracking_run_streaming):
            node = BacktestNode(configs=[config])
            node.run()

        # Assert - _run_streaming should be called when chunk_size is set
        assert streaming_called, "_run_streaming should be called when chunk_size is provided"

    @pytest.mark.slow
    def test_streaming_uses_less_memory_than_oneshot(self):
        """
        Verify streaming mode uses significantly less memory than one-shot mode.

        This test uses 500K real market ticks from the large Tardis dataset to create a
        realistic test scenario. Each mode runs in an isolated subprocess to ensure
        clean memory baselines without allocator arena contamination.

        """
        # Arrange
        start_ns, end_ns = load_catalog_with_large_tardis_quotes(self.catalog, limit=500_000)

        # Build configs as raw dicts for JSON serialization to subprocess
        base_config = {
            "engine": {
                "logging": {"bypass_logging": True},
                "strategies": [
                    {
                        "strategy_path": "nautilus_trader.examples.strategies.ema_cross:EMACross",
                        "config_path": "nautilus_trader.examples.strategies.ema_cross:EMACrossConfig",
                        "config": {
                            "instrument_id": str(_BTCUSDT_HUOBI.id),
                            "bar_type": f"{_BTCUSDT_HUOBI.id}-1000-TICK-MID-INTERNAL",
                            "fast_ema_period": 10,
                            "slow_ema_period": 20,
                            "trade_size": "0.01",
                            "order_id_tag": "001",
                        },
                    },
                ],
            },
            "venues": [
                {
                    "name": "BINANCE",
                    "oms_type": "NETTING",
                    "account_type": "MARGIN",
                    "base_currency": "USDT",
                    "starting_balances": ["1000000 USDT"],
                },
            ],
            "data": [
                {
                    "catalog_path": self.catalog.path,
                    "catalog_fs_protocol": self.catalog.fs_protocol,
                    "data_cls": "nautilus_trader.model.data:QuoteTick",
                    "instrument_id": str(_BTCUSDT_HUOBI.id),
                    "start_time": start_ns,
                    "end_time": end_ns,
                },
            ],
        }

        streaming_config = {**base_config, "chunk_size": 50_000}
        oneshot_config = {**base_config, "chunk_size": None}

        ctx = multiprocessing.get_context("spawn")

        # Streaming mode in subprocess
        streaming_queue: multiprocessing.Queue = ctx.Queue()
        streaming_proc = ctx.Process(
            target=_run_backtest_measure_memory,
            args=(msgspec.json.encode(streaming_config), streaming_queue),
        )
        streaming_proc.start()
        streaming_proc.join(timeout=120)
        assert streaming_proc.exitcode == 0, "Streaming subprocess failed"
        streaming_peak = streaming_queue.get()

        # One-shot mode in subprocess
        oneshot_queue: multiprocessing.Queue = ctx.Queue()
        oneshot_proc = ctx.Process(
            target=_run_backtest_measure_memory,
            args=(msgspec.json.encode(oneshot_config), oneshot_queue),
        )
        oneshot_proc.start()
        oneshot_proc.join(timeout=120)
        assert oneshot_proc.exitcode == 0, "One-shot subprocess failed"
        oneshot_peak = oneshot_queue.get()

        streaming_peak_mb = streaming_peak / 1024 / 1024
        oneshot_peak_mb = oneshot_peak / 1024 / 1024

        # Assert - streaming should use less peak memory than one-shot
        assert streaming_peak < oneshot_peak, (
            f"Streaming peak ({streaming_peak_mb:.1f}MB) should be less than "
            f"one-shot peak ({oneshot_peak_mb:.1f}MB). "
            "This indicates DataFusion may not be streaming data properly."
        )

    def test_streaming_produces_same_results_as_oneshot(self):
        """
        Verify streaming and one-shot modes produce equivalent results.
        """
        # Arrange - load 10K ticks
        start_ns, end_ns = load_catalog_with_quote_ticks(self.catalog, count=10_000)

        data_config = BacktestDataConfig(
            catalog_path=self.catalog.path,
            catalog_fs_protocol=self.catalog.fs_protocol,
            data_cls=QuoteTick,
            instrument_id=InstrumentId.from_str("AUD/USD.SIM"),
            start_time=start_ns,
            end_time=end_ns,
        )

        # Run with streaming (chunk_size=1000)
        streaming_config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config],
            data=[data_config],
            chunk_size=1_000,
        )
        streaming_node = BacktestNode(configs=[streaming_config])
        streaming_results = streaming_node.run()

        # Run with one-shot (chunk_size=None)
        oneshot_config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config],
            data=[data_config],
            chunk_size=None,
        )
        oneshot_node = BacktestNode(configs=[oneshot_config])
        oneshot_results = oneshot_node.run()

        # Assert - results should be equivalent
        streaming_result = streaming_results[0]
        oneshot_result = oneshot_results[0]

        assert streaming_result.total_orders == oneshot_result.total_orders, (
            f"Order count mismatch: streaming={streaming_result.total_orders}, "
            f"oneshot={oneshot_result.total_orders}"
        )
        assert streaming_result.total_positions == oneshot_result.total_positions, (
            f"Position count mismatch: streaming={streaming_result.total_positions}, "
            f"oneshot={oneshot_result.total_positions}"
        )

    def test_streaming_delivers_each_record_once_when_series_absent_from_first_chunk(self):
        """
        Verify a chunked run delivers every record exactly once when a subscribed series
        is absent from the first chunk.

        With `chunk_size=1` the first chunk holds a single quote, so the engine has not
        yet seen the bar series when the subscription is made. It must not open a
        catalog-backed stream for a series which the data configs for this run already
        supply, which would deliver those records a second time.

        Delivery order is not compared here, since the streaming backend orders records
        which share a `ts_init` differently to the one-shot path.

        """
        # Arrange - each bar ties with a quote, so the first chunk boundary shares a timestamp
        written = load_catalog_with_quotes_and_bars(self.catalog, bar_offset_ns=0)

        # Act
        streamed, streamed_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=1,
        )
        oneshot, oneshot_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=None,
        )

        # Assert
        assert len(streamed) == len(set(streamed)), f"Records delivered more than once: {streamed}"
        assert sorted(streamed) == sorted(written)
        assert sorted(streamed) == sorted(oneshot)
        assert streamed_result.iterations == oneshot_result.iterations

    def test_streaming_with_bound_end_matches_oneshot(self):
        """
        Verify a chunked run bound by `BacktestRunConfig.end` matches the one-shot run.

        A configured series is supplied by the stream the run reads its chunks from, so
        opening a catalog-backed stream for it as well would deliver every record of
        that series a second time.

        """
        # Arrange - bars sit one nanosecond off the quotes, so no timestamps are shared
        # and delivery order is directly comparable.
        written = load_catalog_with_quotes_and_bars(self.catalog, bar_offset_ns=1)
        end = "2024-01-01T00:05:00"
        end_ns = _STREAMING_START_NS + 5 * _ONE_MINUTE_NS

        # The run ends on the first record past `end`, so every earlier record is due
        expected = [record for record in written if record[1] <= end_ns]

        # Act
        streamed, streamed_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=1,
            end=end,
        )
        oneshot, oneshot_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=None,
            end=end,
        )

        # Assert
        assert len(streamed) == len(set(streamed)), f"Records delivered more than once: {streamed}"
        assert streamed == expected
        assert streamed == oneshot
        assert streamed_result.iterations == oneshot_result.iterations

    def test_streaming_interleaves_catalog_backed_subscription_with_later_chunks(self):
        """
        Verify a chunked run interleaves a catalog-backed subscription by timestamp.

        A series outside every data config is served by a request to a data catalog. A
        chunked run replaces the engines data iterator between chunks, so that stream
        has to be opened over the range of the chunk in hand and carried across the
        boundary, rather than delivered in full while the first chunk runs, which would
        show the strategy the future of that series.

        """
        # Arrange - bars sit one nanosecond past each quote and are outside every config
        written = load_catalog_with_quotes_and_bars(self.catalog, bar_offset_ns=1)
        end = "2024-01-01T00:05:00"
        end_ns = _STREAMING_START_NS + 5 * _ONE_MINUTE_NS

        # The run ends on the first record past `end`, so every earlier record is due
        expected = [record for record in written if record[1] <= end_ns]

        # Act
        streamed, streamed_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=1,
            end=end,
            configure_bars=False,
        )
        oneshot, oneshot_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=None,
            end=end,
            configure_bars=False,
        )

        # Assert
        assert [record for record in expected if record[0] == "bar"], "Test data proves nothing"
        assert oneshot == expected
        assert streamed == expected
        assert streamed_result.iterations == oneshot_result.iterations

    def test_streaming_delivers_catalog_backed_subscription_without_a_bound_end(self):
        """
        Verify an unbound run still delivers a catalog-backed subscription in full.

        Without `BacktestRunConfig.end` a run closes on the last record it reads, so the
        stream opened while the first chunk runs reaches no further than the range that
        chunk covers. Every later chunk has to reopen it, or the series is delivered
        once and then silently stops.

        """
        # Arrange - bars sit one nanosecond past each quote and are outside every config
        written = load_catalog_with_quotes_and_bars(self.catalog, bar_offset_ns=1)

        # The run ends with the last quote, since the bars are not part of its own data
        last_quote_ns = _STREAMING_START_NS + 5 * _ONE_MINUTE_NS
        expected = [record for record in written if record[1] <= last_quote_ns]

        # Act
        streamed, streamed_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=1,
            configure_bars=False,
        )
        oneshot, oneshot_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=None,
            configure_bars=False,
        )

        # Assert
        assert [record for record in expected if record[0] == "bar"], "Test data proves nothing"
        assert oneshot == expected
        assert streamed == expected
        assert streamed_result.iterations == oneshot_result.iterations

    def test_streaming_delivers_a_point_subscription_once_like_a_oneshot_run(self):
        """
        Verify a chunked run delivers a point subscription exactly once.

        A subscription made with `point_data` asks a data catalog for a single
        timestamp rather than for a range, and the run makes that request when the
        subscription is made. A chunked run reopens a subscription for each chunk to
        carry it across the boundary, which must leave a point request alone rather
        than repeat it once per chunk.

        """
        # Arrange - each bar ties with a quote, so the point requests can reach a bar
        load_catalog_with_quotes_and_bars(self.catalog, bar_offset_ns=0)

        # The request is made as the run opens, so the point it asks for is the first
        # quote, and the bar sharing that timestamp is the only one due.
        expected = [("quote", _STREAMING_START_NS), ("bar", _STREAMING_START_NS)]
        expected += [("quote", _STREAMING_START_NS + i * _ONE_MINUTE_NS) for i in range(1, 6)]

        # Act
        streamed, streamed_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=1,
            configure_bars=False,
            bar_params={"point_data": True},
        )
        oneshot, oneshot_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=None,
            configure_bars=False,
            bar_params={"point_data": True},
        )

        # Assert
        assert oneshot == expected
        assert streamed == expected
        assert streamed_result.iterations == oneshot_result.iterations

    def test_streaming_leaves_a_point_subscription_alone_on_a_chunk_boundary(self):
        """
        Verify a point subscription is left alone when its point closes a chunk.

        A chunk ends on the last timestamp before the chunk which follows it, so a point
        request made as the run opens can ask for exactly that timestamp. Reaching the
        end of a chunk is what tells a stream apart from one which is finished, and a
        point request which does so is still finished.

        """
        # Arrange - the second quote is one nanosecond past the first, so the first
        # chunk ends on the first quote, where the point request is made.
        load_catalog_with_a_boundary_quote_and_bars(self.catalog)

        # The request is made as the run opens, so the bar sharing the first quotes
        # timestamp is the only one due.
        expected = [
            ("quote", _STREAMING_START_NS),
            ("bar", _STREAMING_START_NS),
            ("quote", _STREAMING_START_NS + 1),
            ("quote", _STREAMING_START_NS + 2 * _ONE_MINUTE_NS),
        ]

        # Act
        streamed, streamed_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=1,
            configure_bars=False,
            bar_params={"point_data": True},
        )
        oneshot, oneshot_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=None,
            configure_bars=False,
            bar_params={"point_data": True},
        )

        # Assert
        assert oneshot == expected
        assert streamed == expected
        assert streamed_result.iterations == oneshot_result.iterations

    def test_streaming_empty_window_config_is_still_served_from_catalog(self):
        """
        Verify a config whose window holds no catalog files keeps its catalog stream.

        A one-shot run skips a data config which returns no rows, so that series is
        never registered and its subscription is served by a data catalog request
        instead. A chunked run must do the same, rather than treat the empty config as
        covering the series and drop it from the run.

        Only the set of records delivered is compared here, since what this test pins is
        the series being served at all. Delivery order is pinned by the tests covering
        the chunk boundary.

        """
        # Arrange - bars sit one nanosecond ahead of the quotes so the first chunk holds a
        # bar, and the quote config's window closes before the first quote was recorded.
        written = load_catalog_with_quotes_and_bars(self.catalog, bar_offset_ns=-1)
        quote_time_window = (
            _STREAMING_START_NS - 10 * _ONE_MINUTE_NS,
            _STREAMING_START_NS - 5 * _ONE_MINUTE_NS,
        )
        end = "2024-01-01T00:05:00"
        end_ns = _STREAMING_START_NS + 5 * _ONE_MINUTE_NS

        # The run ends on the first record past `end`, so every earlier record is due
        expected = [record for record in written if record[1] <= end_ns]

        # Act
        streamed, streamed_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=1,
            end=end,
            quote_time_window=quote_time_window,
        )
        oneshot, oneshot_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=None,
            end=end,
            quote_time_window=quote_time_window,
        )

        # Assert
        assert [record for record in expected if record[0] == "quote"], "Test data proves nothing"
        assert len(streamed) == len(set(streamed)), f"Records delivered more than once: {streamed}"
        assert sorted(streamed) == sorted(expected)
        assert sorted(oneshot) == sorted(expected)
        assert streamed_result.iterations == oneshot_result.iterations

    def test_streaming_inverted_window_config_is_still_served_from_catalog(self):
        """
        Verify a config whose effective window is inverted keeps its catalog stream.

        Narrowing a config with `end_time` while the run itself starts later leaves an
        inverted window which can select no rows at all. The file holding those rows may
        still span both bounds, so a config has to be judged on its window and not on
        the files alone, or the series is dropped from a chunked run.

        """
        # Arrange - the quote config closes at 00:02 while the run opens at 00:04
        written = load_catalog_with_quotes_and_bars(self.catalog, bar_offset_ns=1)
        start = "2024-01-01T00:04:00"
        end = "2024-01-01T00:05:00"
        start_ns = _STREAMING_START_NS + 4 * _ONE_MINUTE_NS
        end_ns = _STREAMING_START_NS + 5 * _ONE_MINUTE_NS
        quote_time_window = (None, _STREAMING_START_NS + 2 * _ONE_MINUTE_NS)

        # The run covers the records between `start` and `end` inclusive
        expected = [record for record in written if start_ns <= record[1] <= end_ns]

        # Act
        streamed, streamed_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=1,
            start=start,
            end=end,
            quote_time_window=quote_time_window,
        )
        oneshot, oneshot_result = run_quotes_and_bars_backtest(
            self.catalog,
            self.venue_config,
            chunk_size=None,
            start=start,
            end=end,
            quote_time_window=quote_time_window,
        )

        # Assert
        assert [record for record in expected if record[0] == "quote"], "Test data proves nothing"
        assert len(streamed) == len(set(streamed)), f"Records delivered more than once: {streamed}"
        assert sorted(streamed) == sorted(expected)
        assert sorted(oneshot) == sorted(expected)
        assert streamed_result.iterations == oneshot_result.iterations

    def test_streaming_warns_for_seeded_series_which_delivers_no_data(self, monkeypatch, tmp_path):
        """
        Verify one end-of-run warning names a configured series which delivered nothing.

        A config whose files intersect the run window is treated as supplied by the
        stream, so its subscription is not served from a data catalog. When those files
        hold no rows inside the window nothing arrives, and the run must say so rather
        than leave the series silently missing.

        """
        # Arrange - the session yields no chunks, so no configured series delivers data
        engine = run_streaming_with_stubs(monkeypatch, tmp_path, chunks=[])

        # Assert
        assert engine.subscription_names == [f"QuoteTick.{_AUDUSD_SIM.id}"]
        assert len(engine.logger.warnings) == 1, engine.logger.warnings
        assert f"QuoteTick.{_AUDUSD_SIM.id}" in engine.logger.warnings[0]

    def test_streaming_does_not_warn_when_every_seeded_series_delivers(
        self,
        monkeypatch,
        tmp_path,
    ):
        """
        Verify a run whose configured series all deliver data produces no such warning.
        """
        # Arrange - the single chunk carries the configured quote series
        quote = QuoteTick(
            instrument_id=_AUDUSD_SIM.id,
            bid_price=Price.from_str("0.67000"),
            ask_price=Price.from_str("0.67010"),
            bid_size=Quantity.from_int(1_000_000),
            ask_size=Quantity.from_int(1_000_000),
            ts_event=_STREAMING_START_NS,
            ts_init=_STREAMING_START_NS,
        )

        # Act
        engine = run_streaming_with_stubs(monkeypatch, tmp_path, chunks=[[quote]])

        # Assert
        assert engine.subscription_names == [f"QuoteTick.{_AUDUSD_SIM.id}"]
        assert engine.logger.warnings == []

    def test_run_streaming_caches_per_catalog(self, monkeypatch, tmp_path):
        monkeypatch.setattr(node, "DataBackendSession", DummyStreamingSession)

        catalog_instances: dict[str, DummyStreamingCatalog] = {}
        instrument_id = InstrumentId.from_str("AUD/USD.SIM")

        def fake_load_catalog(_self, config):
            catalog = DummyStreamingCatalog(config.catalog_path, config.catalog_fs_protocol)
            catalog_instances[config.catalog_path] = catalog
            return catalog

        monkeypatch.setattr(BacktestNode, "load_catalog", fake_load_catalog)

        data_config_a = BacktestDataConfig(
            catalog_path=(tmp_path / "catalog_a").as_posix(),
            catalog_fs_protocol="file",
            data_cls=QuoteTick,
            instrument_id=instrument_id,
            start_time=None,
            end_time=None,
        )

        data_config_b = BacktestDataConfig(
            catalog_path=(tmp_path / "catalog_b").as_posix(),
            catalog_fs_protocol="file",
            data_cls=QuoteTick,
            instrument_id=instrument_id,
            start_time=None,
            end_time=None,
        )

        run_config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config],
            data=[data_config_a, data_config_b],
            chunk_size=1_000,
        )

        node_instance = BacktestNode(configs=[run_config])

        class DummyEngine:
            def add_data(self, data, validate=True, sort=True):
                return None

            def run(self, start=None, end=None, run_config_id=None, streaming=None):
                return None

            def clear_data(self):
                return None

            def end(self):
                return None

        node_instance._run_streaming(
            run_config_id=run_config.id,
            engine=DummyStreamingEngine(),
            data_configs=run_config.data,
            chunk_size=run_config.chunk_size,
        )

        assert len(catalog_instances) == 2
        assert catalog_instances[data_config_a.catalog_path].calls == 1
        assert catalog_instances[data_config_b.catalog_path].calls == 1

    def test_streaming_mixed_builtin_and_custom_data_types(self, tmp_path):
        """
        Streaming a session containing both built-in Rust types and custom Python types
        must not raise ValueError on PyCapsule.

        Regression test for
        https://github.com/nautechsystems/nautilus_trader/issues/3853

        """
        from nautilus_trader.core.data import Data
        from nautilus_trader.core.nautilus_pyo3.model import register_custom_data_class
        from nautilus_trader.model.custom import customdataclass_pyo3
        from nautilus_trader.model.data import QuoteTick

        @customdataclass_pyo3()
        class NodeTestSignal(Data):
            value: float = 0.0

        register_custom_data_class(NodeTestSignal)

        catalog = setup_catalog(protocol="file", path=tmp_path / "mixed_catalog")
        start_ns, end_ns = load_catalog_with_quote_ticks(catalog, count=100)

        # Write custom data interleaved with the quote tick timestamps
        signals = [
            NodeTestSignal(
                ts_event=start_ns + i * 100_000_000,
                ts_init=start_ns + i * 100_000_000,
                value=float(i),
            )
            for i in range(1, 20)
        ]
        catalog.write_data(signals)

        instrument_id = InstrumentId.from_str("AUD/USD.SIM")

        quote_config = BacktestDataConfig(
            catalog_path=catalog.path,
            catalog_fs_protocol=catalog.fs_protocol,
            data_cls=QuoteTick,
            instrument_id=instrument_id,
            start_time=start_ns,
            end_time=end_ns,
        )
        signal_config = BacktestDataConfig(
            catalog_path=catalog.path,
            catalog_fs_protocol=catalog.fs_protocol,
            data_cls=NodeTestSignal,
            start_time=start_ns,
            end_time=end_ns,
        )

        config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config],
            data=[quote_config, signal_config],
            chunk_size=500,
            raise_exception=True,
        )

        node = BacktestNode(configs=[config])
        results = node.run()

        assert len(results) == 1

    def test_streaming_drops_capsule_chunks(self):
        """
        Each PyCapsule chunk yielded during streaming must be passed to
        drop_cvec_pycapsule, otherwise the underlying Vec<DataFFI> leaks.

        Regression guard for
        https://github.com/nautechsystems/nautilus_trader/issues/3889

        """
        from nautilus_trader.core.nautilus_pyo3 import DataBackendSession

        # Arrange - 10K ticks with chunk_size=1000 produces multiple capsule chunks
        start_ns, end_ns = load_catalog_with_quote_ticks(self.catalog, count=10_000)

        data_config = BacktestDataConfig(
            catalog_path=self.catalog.path,
            catalog_fs_protocol=self.catalog.fs_protocol,
            data_cls=QuoteTick,
            instrument_id=InstrumentId.from_str("AUD/USD.SIM"),
            start_time=start_ns,
            end_time=end_ns,
        )
        config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config],
            data=[data_config],
            chunk_size=1_000,
        )

        capsule_chunk_count = 0
        list_chunk_count = 0
        drop_call_count = 0

        original_drop = node.drop_cvec_pycapsule
        original_to_query_result = DataBackendSession.to_query_result

        def counting_drop(capsule):
            nonlocal drop_call_count
            drop_call_count += 1
            return original_drop(capsule)

        def counting_to_query_result(self_session):
            nonlocal capsule_chunk_count, list_chunk_count

            for chunk in original_to_query_result(self_session):
                if isinstance(chunk, list):
                    list_chunk_count += 1
                else:
                    capsule_chunk_count += 1
                yield chunk

        # Act
        with patch.object(node, "drop_cvec_pycapsule", counting_drop):
            DataBackendSession.to_query_result = counting_to_query_result
            try:
                BacktestNode(configs=[config]).run()
            finally:
                DataBackendSession.to_query_result = original_to_query_result

        # Assert - every capsule chunk was dropped exactly once
        assert capsule_chunk_count > 1, (
            f"Expected multiple capsule chunks, was {capsule_chunk_count}"
        )
        assert list_chunk_count == 0, (
            f"Built-in only stream should not yield list chunks, was {list_chunk_count}"
        )
        assert drop_call_count == capsule_chunk_count, (
            f"Expected {capsule_chunk_count} drops, was {drop_call_count}"
        )

    def test_streaming_does_not_drop_list_chunks(self, tmp_path):
        """
        Mixed streams yield list chunks for any chunk containing custom data; those must
        not be passed to drop_cvec_pycapsule (which would error or double-free).

        Regression guard for
        https://github.com/nautechsystems/nautilus_trader/issues/3889

        """
        from nautilus_trader.core.data import Data
        from nautilus_trader.core.nautilus_pyo3 import DataBackendSession
        from nautilus_trader.core.nautilus_pyo3.model import register_custom_data_class
        from nautilus_trader.model.custom import customdataclass_pyo3

        @customdataclass_pyo3()
        class DropTestSignal(Data):
            value: float = 0.0

        register_custom_data_class(DropTestSignal)

        catalog = setup_catalog(protocol="file", path=tmp_path / "drop_catalog")
        start_ns, end_ns = load_catalog_with_quote_ticks(catalog, count=200)

        signals = [
            DropTestSignal(
                ts_event=start_ns + i * 100_000_000,
                ts_init=start_ns + i * 100_000_000,
                value=float(i),
            )
            for i in range(1, 30)
        ]
        catalog.write_data(signals)

        instrument_id = InstrumentId.from_str("AUD/USD.SIM")
        quote_config = BacktestDataConfig(
            catalog_path=catalog.path,
            catalog_fs_protocol=catalog.fs_protocol,
            data_cls=QuoteTick,
            instrument_id=instrument_id,
            start_time=start_ns,
            end_time=end_ns,
        )
        signal_config = BacktestDataConfig(
            catalog_path=catalog.path,
            catalog_fs_protocol=catalog.fs_protocol,
            data_cls=DropTestSignal,
            start_time=start_ns,
            end_time=end_ns,
        )
        config = BacktestRunConfig(
            engine=BacktestEngineConfig(
                strategies=self.strategies,
                logging=LoggingConfig(bypass_logging=True),
            ),
            venues=[self.venue_config],
            data=[quote_config, signal_config],
            chunk_size=50,
            raise_exception=True,
        )

        capsule_chunk_count = 0
        list_chunk_count = 0
        drop_call_count = 0

        original_drop = node.drop_cvec_pycapsule
        original_to_query_result = DataBackendSession.to_query_result

        def counting_drop(capsule):
            nonlocal drop_call_count
            drop_call_count += 1
            return original_drop(capsule)

        def counting_to_query_result(self_session):
            nonlocal capsule_chunk_count, list_chunk_count

            for chunk in original_to_query_result(self_session):
                if isinstance(chunk, list):
                    list_chunk_count += 1
                else:
                    capsule_chunk_count += 1
                yield chunk

        # Act
        with patch.object(node, "drop_cvec_pycapsule", counting_drop):
            DataBackendSession.to_query_result = counting_to_query_result
            try:
                BacktestNode(configs=[config]).run()
            finally:
                DataBackendSession.to_query_result = original_to_query_result

        # Assert - the mixed stream produced both kinds; drops match capsule chunks only
        assert list_chunk_count > 0, (
            "Mixed stream should yield at least one list chunk for custom data"
        )
        assert drop_call_count == capsule_chunk_count, (
            f"drop_cvec_pycapsule called {drop_call_count} times for "
            f"{capsule_chunk_count} capsule chunks (and {list_chunk_count} list chunks); "
            "list chunks must not be dropped"
        )


class ShutdownAfterQuotesActor(Actor):
    """
    Actor that calls `shutdown_system` after receiving `shutdown_after` quotes.
    """

    def __init__(self, instrument_id: InstrumentId, shutdown_after: int) -> None:
        super().__init__()
        self._instrument_id = instrument_id
        self._shutdown_after = shutdown_after
        self._tick_count = 0
        self._shutdown_triggered = False

    def on_start(self) -> None:
        self.subscribe_quote_ticks(self._instrument_id)

    def on_quote_tick(self, tick) -> None:
        self._tick_count += 1
        if self._tick_count >= self._shutdown_after and not self._shutdown_triggered:
            self._shutdown_triggered = True
            self.shutdown_system("test shutdown")


def test_streaming_shutdown_stops_between_chunks(tmp_path):
    # Regression for #3920: shutdown_system() during a streaming BacktestNode
    # run must prevent later chunks from being loaded and processed.
    catalog = setup_catalog(protocol="file", path=tmp_path / "catalog")
    total_quotes = 2_000
    start_ns, end_ns = load_catalog_with_quote_ticks(catalog, count=total_quotes)

    instrument_id = InstrumentId.from_str("AUD/USD.SIM")
    shutdown_after = 10

    actor = ShutdownAfterQuotesActor(instrument_id, shutdown_after)

    venue_config = BacktestVenueConfig(
        name="SIM",
        oms_type="HEDGING",
        account_type="MARGIN",
        base_currency="USD",
        starting_balances=["1000000 USD"],
    )
    data_config = BacktestDataConfig(
        catalog_path=catalog.path,
        catalog_fs_protocol=catalog.fs_protocol,
        data_cls=QuoteTick,
        instrument_id=instrument_id,
        start_time=start_ns,
        end_time=end_ns,
    )
    run_config = BacktestRunConfig(
        engine=BacktestEngineConfig(logging=LoggingConfig(bypass_logging=True)),
        venues=[venue_config],
        data=[data_config],
        chunk_size=200,
    )

    bt_node = BacktestNode(configs=[run_config])
    bt_node.build()
    engine = bt_node.get_engine(run_config.id)
    engine.add_actor(actor)

    results = bt_node.run()

    assert len(results) == 1
    assert results[0].iterations < total_quotes, (
        f"Shutdown must stop streaming before all {total_quotes} quotes "
        f"are processed, was {results[0].iterations}"
    )
    # Actor should have triggered shutdown in the first chunk and received no
    # further ticks once the streaming loop bailed out
    assert actor._tick_count == shutdown_after, (
        f"Actor received more ticks after shutdown: expected {shutdown_after}, "
        f"was {actor._tick_count}"
    )
    # engine.end() must run on the streaming+shutdown path so the result is
    # finalized and the trader stops
    assert engine.run_finished is not None, "engine.end() must run on shutdown"
    assert engine.backtest_end is not None
    assert not engine.trader.is_running, "trader must stop after shutdown"
