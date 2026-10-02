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
"""
Tests for the execution engine binding a venue's own position ID to the cache position
its fills are booked under.
"""

import itertools
import subprocess
import sys
from enum import StrEnum
from pathlib import Path

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.config import ExecEngineConfig
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.engine import ExecutionEngine
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.risk.engine import RiskEngine
from nautilus_trader.test_kit.mocks.cache_database import MockCacheDatabase
from nautilus_trader.test_kit.mocks.exec_clients import MockExecutionClient
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.trading.strategy import Strategy


AUDUSD_SIM = TestInstrumentProvider.default_fx_ccy("AUD/USD")

LEG_7 = PositionId("EURUSD.MT5-LONG-7")
LEG_8 = PositionId("EURUSD.MT5-LONG-8")
TICKET = PositionId("8133477")
SECOND_TICKET = PositionId("8133480")


class RecordingCacheDatabase(MockCacheDatabase):
    """
    A mock cache database recording every venue position binding written through it.
    """

    def __init__(self) -> None:
        super().__init__()
        self.venue_position_writes: list[tuple[PositionId, PositionId]] = []

    def index_venue_position(self, venue_position_id: PositionId, position_id: PositionId) -> None:
        self.venue_position_writes.append((venue_position_id, position_id))
        super().index_venue_position(venue_position_id, position_id)


class HedgingHarness:
    """
    A HEDGING execution engine over a recording mock cache database.
    """

    def __init__(self) -> None:
        self.clock = TestClock()
        self.trader_id = TestIdStubs.trader_id()
        self.msgbus = MessageBus(trader_id=self.trader_id, clock=self.clock)
        self.database = RecordingCacheDatabase()
        self.cache = Cache(database=self.database)
        self.portfolio = Portfolio(msgbus=self.msgbus, cache=self.cache, clock=self.clock)
        self.exec_engine = ExecutionEngine(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            config=ExecEngineConfig(debug=True),
        )
        self.risk_engine = RiskEngine(
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        self.cache.add_instrument(AUDUSD_SIM)
        exec_client = MockExecutionClient(
            client_id=ClientId("SIM"),
            venue=Venue("SIM"),
            account_type=AccountType.MARGIN,
            base_currency=USD,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        self.portfolio.update_account(TestEventStubs.margin_account_state())
        self.exec_engine.register_client(exec_client)
        self.exec_engine.start()

        self.strategy = Strategy(StrategyConfig(oms_type="HEDGING"))
        self.strategy.register(
            trader_id=self.trader_id,
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        self.exec_engine.register_oms_type(self.strategy)

        self._venue_order_ids = itertools.count(1)
        self._trade_ids = itertools.count(1)

    def submit(self, side: OrderSide, quantity: int, position_id: PositionId) -> Order:
        order = self.strategy.order_factory.market(
            AUDUSD_SIM.id,
            side,
            Quantity.from_int(quantity),
        )
        self.risk_engine.execute(
            SubmitOrder(
                trader_id=self.trader_id,
                strategy_id=self.strategy.id,
                position_id=position_id,
                order=order,
                command_id=UUID4(),
                ts_init=self.clock.timestamp_ns(),
            ),
        )
        self._accept(order)
        return order

    def add_venue_order(self, side: OrderSide, quantity: int) -> Order:
        # An order the venue raised itself (a stop-loss on its position), which reaches the cache
        # through reconciliation under the external strategy and without a position ID
        order = TestExecStubs.market_order(
            instrument=AUDUSD_SIM,
            order_side=side,
            quantity=Quantity.from_int(quantity),
            strategy_id=StrategyId("EXTERNAL"),
            client_order_id=ClientOrderId(f"O-VENUE-{next(self._venue_order_ids)}"),
        )
        self.cache.add_order(order)
        self._accept(order)
        return order

    def fill(self, order: Order, position_id: PositionId | None, quantity: int) -> None:
        self.exec_engine.process(
            TestEventStubs.order_filled(
                order,
                AUDUSD_SIM,
                trade_id=TradeId(f"E-{next(self._trade_ids)}"),
                position_id=position_id,
                last_qty=Quantity.from_int(quantity),
            ),
        )

    def _accept(self, order: Order) -> None:
        self.exec_engine.process(TestEventStubs.order_submitted(order))
        self.exec_engine.process(
            TestEventStubs.order_accepted(
                order,
                venue_order_id=VenueOrderId(f"V-{next(self._venue_order_ids)}"),
            ),
        )


def run_bound_fill(harness: HedgingHarness) -> None:
    order = harness.submit(OrderSide.BUY, 100_000, LEG_7)
    harness.fill(order, TICKET, 100_000)


def run_conflicting_binding(harness: HedgingHarness) -> None:
    run_bound_fill(harness)
    order = harness.submit(OrderSide.BUY, 100_000, LEG_8)
    harness.fill(order, TICKET, 100_000)


def run_second_ticket_for_bound_position(harness: HedgingHarness) -> None:
    run_bound_fill(harness)
    order = harness.submit(OrderSide.BUY, 50_000, LEG_7)
    harness.fill(order, SECOND_TICKET, 50_000)


def run_repeated_second_ticket_for_bound_position(harness: HedgingHarness) -> None:
    run_bound_fill(harness)
    order = harness.submit(OrderSide.BUY, 50_000, LEG_7)
    harness.fill(order, SECOND_TICKET, 20_000)
    harness.fill(order, SECOND_TICKET, 30_000)


def run_cached_position_mismatch(harness: HedgingHarness) -> None:
    order = harness.submit(OrderSide.BUY, 100_000, LEG_7)
    harness.fill(order, None, 100_000)
    order = harness.submit(OrderSide.BUY, 100_000, LEG_8)
    harness.fill(order, LEG_7, 100_000)


class Scenario(StrEnum):
    BOUND_FILL = "bound_fill"
    CONFLICTING_BINDING = "conflicting_binding"
    SECOND_TICKET_FOR_BOUND_POSITION = "second_ticket_for_bound_position"
    REPEATED_SECOND_TICKET_FOR_BOUND_POSITION = "repeated_second_ticket_for_bound_position"
    CACHED_POSITION_MISMATCH = "cached_position_mismatch"


SCENARIOS = {
    Scenario.BOUND_FILL: run_bound_fill,
    Scenario.CONFLICTING_BINDING: run_conflicting_binding,
    Scenario.SECOND_TICKET_FOR_BOUND_POSITION: run_second_ticket_for_bound_position,
    Scenario.REPEATED_SECOND_TICKET_FOR_BOUND_POSITION: (
        run_repeated_second_ticket_for_bound_position
    ),
    Scenario.CACHED_POSITION_MISMATCH: run_cached_position_mismatch,
}


class TestVenuePositionBinding:
    def setup_method(self) -> None:
        self.harness = HedgingHarness()
        self.cache = self.harness.cache

    def test_fill_under_venue_position_id_books_to_cached_position_and_binds(self) -> None:
        # Arrange
        order = self.harness.submit(OrderSide.BUY, 100_000, LEG_7)

        # Act
        self.harness.fill(order, TICKET, 100_000)

        # Assert
        position = self.cache.position(LEG_7)
        assert position is not None
        assert position.quantity == Quantity.from_int(100_000)
        assert self.cache.position(TICKET) is None
        assert self.cache.position_id_for_venue(TICKET) == LEG_7
        assert self.cache.venue_position_ids(LEG_7) == frozenset({TICKET})
        assert self.harness.database.venue_position_writes == [(TICKET, LEG_7)]

    def test_repeated_fill_under_bound_venue_position_id_records_nothing_new(self) -> None:
        # Arrange
        order = self.harness.submit(OrderSide.BUY, 100_000, LEG_7)
        self.harness.fill(order, TICKET, 40_000)

        # Act
        self.harness.fill(order, TICKET, 60_000)

        # Assert
        assert self.cache.position(LEG_7).quantity == Quantity.from_int(100_000)
        assert self.cache.position_id_for_venue(TICKET) == LEG_7
        assert self.cache.venue_position_ids(LEG_7) == frozenset({TICKET})
        assert self.harness.database.venue_position_writes == [(TICKET, LEG_7)]

    def test_venue_position_id_bound_elsewhere_keeps_the_first_binding(self) -> None:
        # Arrange
        run_bound_fill(self.harness)
        order = self.harness.submit(OrderSide.BUY, 100_000, LEG_8)

        # Act
        self.harness.fill(order, TICKET, 100_000)

        # Assert
        assert self.cache.position(LEG_8).quantity == Quantity.from_int(100_000)
        assert self.cache.position_id_for_venue(TICKET) == LEG_7
        assert self.cache.venue_position_ids(LEG_7) == frozenset({TICKET})
        assert self.cache.venue_position_ids(LEG_8) == frozenset()
        assert self.harness.database.venue_position_writes == [(TICKET, LEG_7)]

    def test_second_venue_position_id_for_bound_position_binds_it_too(self) -> None:
        # Arrange, Act
        run_second_ticket_for_bound_position(self.harness)

        # Assert
        assert self.cache.position(LEG_7).quantity == Quantity.from_int(150_000)
        assert self.cache.position_id_for_venue(TICKET) == LEG_7
        assert self.cache.position_id_for_venue(SECOND_TICKET) == LEG_7
        assert self.cache.venue_position_ids(LEG_7) == frozenset({TICKET, SECOND_TICKET})
        assert self.harness.database.venue_position_writes == [
            (TICKET, LEG_7),
            (SECOND_TICKET, LEG_7),
        ]

    def test_fill_naming_a_cached_position_records_no_binding(self) -> None:
        # Arrange, Act
        run_cached_position_mismatch(self.harness)

        # Assert
        assert self.cache.position(LEG_7).quantity == Quantity.from_int(100_000)
        assert self.cache.position(LEG_8).quantity == Quantity.from_int(100_000)
        assert self.cache.position_id_for_venue(LEG_7) is None
        assert self.cache.venue_position_ids(LEG_8) == frozenset()
        assert self.harness.database.venue_position_writes == []

    def test_venue_fill_under_bound_venue_position_id_closes_the_bound_position(self) -> None:
        # Arrange
        run_bound_fill(self.harness)
        stop_loss = self.harness.add_venue_order(OrderSide.SELL, 100_000)

        # Act
        self.harness.fill(stop_loss, TICKET, 100_000)

        # Assert
        position = self.cache.position(LEG_7)
        assert position.is_closed
        assert position.side == PositionSide.FLAT
        assert self.cache.position(TICKET) is None
        assert [p.id for p in self.cache.positions()] == [LEG_7]

    def test_venue_fill_under_bound_venue_position_id_reduces_the_bound_position(self) -> None:
        # Arrange
        run_bound_fill(self.harness)
        partial_close = self.harness.add_venue_order(OrderSide.SELL, 40_000)

        # Act
        self.harness.fill(partial_close, TICKET, 40_000)

        # Assert
        position = self.cache.position(LEG_7)
        assert position.side == PositionSide.LONG
        assert position.quantity == Quantity.from_int(60_000)
        assert self.cache.position(TICKET) is None
        assert [p.id for p in self.cache.positions()] == [LEG_7]

    def test_venue_fill_under_unbound_venue_position_id_books_under_that_id(self) -> None:
        # Arrange
        run_bound_fill(self.harness)
        venue_order = self.harness.add_venue_order(OrderSide.SELL, 100_000)

        # Act
        self.harness.fill(venue_order, SECOND_TICKET, 100_000)

        # Assert
        assert self.cache.position(LEG_7).quantity == Quantity.from_int(100_000)
        position = self.cache.position(SECOND_TICKET)
        assert position is not None
        assert position.side == PositionSide.SHORT
        assert position.quantity == Quantity.from_int(100_000)


_LOG_CAPTURE_CHILD = """
import sys

from nautilus_trader.common.component import flush_logger
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

_guard = init_logging(level_stdout=LogLevel.INFO, colors=False, bypass=False)

from tests.unit_tests.execution.test_engine_venue_position_binding import SCENARIOS
from tests.unit_tests.execution.test_engine_venue_position_binding import HedgingHarness
from tests.unit_tests.execution.test_engine_venue_position_binding import Scenario

SCENARIOS[Scenario(sys.argv[1])](HedgingHarness())
flush_logger()
"""

# The tag each level carries in a written log line
_LEVEL_TAGS = {LogLevel.INFO: "[INFO]", LogLevel.WARNING: "[WARN]", LogLevel.ERROR: "[ERROR]"}


def _log_lines(scenario: Scenario) -> list[str]:
    # The suite initializes logging once with `bypass=True`, so a scenario's log is read from a
    # child process that initializes it to write out, errors going to stderr
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _LOG_CAPTURE_CHILD, scenario.value],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        cwd=Path(__file__).resolve().parents[3],
    )
    assert result.returncode == 0, (
        f"log capture child failed with exit code {result.returncode}\nstderr:\n{result.stderr}"
    )
    return result.stdout.splitlines() + result.stderr.splitlines()


def _lines_at(lines: list[str], level: LogLevel) -> list[str]:
    return [line for line in lines if _LEVEL_TAGS[level] in line]


def _cache_lines_at(lines: list[str], level: LogLevel) -> list[str]:
    return [line for line in _lines_at(lines, level) if ".Cache: " in line]


def test_binding_fill_logs_no_warning() -> None:
    # Arrange, Act
    lines = _log_lines(Scenario.BOUND_FILL)

    # Assert
    assert [line for line in _lines_at(lines, LogLevel.WARNING) if TICKET.value in line] == []
    assert _lines_at(lines, LogLevel.ERROR) == []


def test_conflicting_binding_logs_an_error_naming_both_positions() -> None:
    # Arrange, Act
    lines = _log_lines(Scenario.CONFLICTING_BINDING)

    # Assert
    errors = [line for line in _lines_at(lines, LogLevel.ERROR) if TICKET.value in line]
    assert len(errors) == 1
    assert LEG_7.value in errors[0]
    assert LEG_8.value in errors[0]


def test_second_venue_position_id_for_bound_position_logs_info_not_error() -> None:
    # Arrange, Act
    lines = _log_lines(Scenario.SECOND_TICKET_FOR_BOUND_POSITION)

    # Assert
    infos = [line for line in _cache_lines_at(lines, LogLevel.INFO) if SECOND_TICKET.value in line]
    assert len(infos) == 1
    assert LEG_7.value in infos[0]
    assert _lines_at(lines, LogLevel.ERROR) == []


def test_repeated_second_venue_position_id_logs_its_binding_once() -> None:
    # Arrange, Act
    lines = _log_lines(Scenario.REPEATED_SECOND_TICKET_FOR_BOUND_POSITION)

    # Assert
    infos = [line for line in _cache_lines_at(lines, LogLevel.INFO) if SECOND_TICKET.value in line]
    assert len(infos) == 1
    assert _lines_at(lines, LogLevel.ERROR) == []


def test_fill_naming_a_cached_position_still_warns() -> None:
    # Arrange, Act
    lines = _log_lines(Scenario.CACHED_POSITION_MISMATCH)

    # Assert
    warnings = [
        line for line in _lines_at(lines, LogLevel.WARNING) if "Incorrect position ID" in line
    ]
    assert len(warnings) == 1
    assert LEG_7.value in warnings[0]
    assert LEG_8.value in warnings[0]
