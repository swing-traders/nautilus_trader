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

import itertools
import json
from decimal import Decimal

import msgspec
import pytest

from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.config import LiveExecEngineConfig
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.live.execution_engine import PositionScopeSnapshot
from nautilus_trader.live.reconciliation import POSITION_REPAIR_OPEN
from nautilus_trader.live.reconciliation import POSITION_REPAIR_TRIM
from nautilus_trader.live.reconciliation import PositionRepairIntent
from nautilus_trader.live.reconciliation import diff_position_scope
from nautilus_trader.live.reconciliation import quantities_equal_at_size_precision
from nautilus_trader.model.currencies import BTC
from nautilus_trader.model.currencies import ETH
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.position import Position
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.mocks.exec_clients import MockLiveExecutionClient
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


SIM = Venue("SIM")
AUDUSD_SIM = TestInstrumentProvider.default_fx_ccy("AUD/USD")

# Fractional-size instrument so sub-unit quantities (0.001) are representable
BTCUSD_SIM = CurrencyPair(
    instrument_id=InstrumentId(symbol=Symbol("BTC/USD"), venue=SIM),
    raw_symbol=Symbol("BTC/USD"),
    base_currency=BTC,
    quote_currency=USD,
    price_precision=1,
    size_precision=3,
    price_increment=Price.from_str("0.1"),
    size_increment=Quantity.from_str("0.001"),
    lot_size=None,
    max_quantity=Quantity.from_str("1000.000"),
    min_quantity=Quantity.from_str("0.001"),
    max_price=None,
    min_price=None,
    max_notional=None,
    min_notional=None,
    margin_init=Decimal("0.01"),
    margin_maint=Decimal("0.01"),
    maker_fee=Decimal(0),
    taker_fee=Decimal(0),
    ts_event=0,
    ts_init=0,
)

# Size precision 2, the precision the cross-zero trim incident was measured at
ETHUSD_SIM = CurrencyPair(
    instrument_id=InstrumentId(symbol=Symbol("ETH/USD"), venue=SIM),
    raw_symbol=Symbol("ETH/USD"),
    base_currency=ETH,
    quote_currency=USD,
    price_precision=1,
    size_precision=2,
    price_increment=Price.from_str("0.1"),
    size_increment=Quantity.from_str("0.01"),
    lot_size=None,
    max_quantity=Quantity.from_str("1000.00"),
    min_quantity=Quantity.from_str("0.01"),
    max_price=None,
    min_price=None,
    max_notional=None,
    min_notional=None,
    margin_init=Decimal("0.01"),
    margin_maint=Decimal("0.01"),
    maker_fee=Decimal(0),
    taker_fee=Decimal(0),
    ts_event=0,
    ts_init=0,
)

RESIDUALS_KEY = "reconciliation:residuals"


class ResidualEntry(msgspec.Struct):
    """
    Mirrors the strict schema the downstream trading layer decodes residuals with.
    """

    reason: str
    ts: int


RESIDUAL_DECODER = msgspec.json.Decoder(dict[str, ResidualEntry])


class ConvergenceEnv:
    """
    Minimal live-engine harness for position convergence tests.
    """

    def __init__(
        self,
        loop,
        oms_type: OmsType = OmsType.HEDGING,
        load_instruments: bool = True,
    ) -> None:
        self.loop = loop
        self.oms_type = oms_type
        self.clock = LiveClock()
        self.trader_id = TestIdStubs.trader_id()
        self.account_id = AccountId(f"{SIM.value}-001")  # The mock client's own account
        self.msgbus = MessageBus(trader_id=self.trader_id, clock=self.clock)
        self.cache = TestComponentStubs.cache()
        self.portfolio = Portfolio(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        self.engine = LiveExecutionEngine(
            loop=loop,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            config=LiveExecEngineConfig(reconciliation=True),
        )
        self.client = MockLiveExecutionClient(
            loop=loop,
            client_id=ClientId(SIM.value),
            venue=SIM,
            account_type=AccountType.CASH,
            base_currency=USD,
            instrument_provider=InstrumentProvider(),
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            oms_type=oms_type,
        )
        self.portfolio.update_account(
            TestEventStubs.cash_account_state(account_id=self.account_id),
        )
        self.engine.register_client(self.client)

        # The node starts every execution client before the first convergence pass, and
        # a client's lifecycle state is what tells a real query failure from a shutdown.
        self.client.start()

        # A client can route nothing the cache still holds, so the harness starts
        # without loading an instrument at all.
        if load_instruments:
            self.cache.add_instrument(AUDUSD_SIM)
            self.cache.add_instrument(BTCUSD_SIM)

        # Isolate the watermark rail from the activity threshold deferral
        self.engine.position_check_threshold_ms = 0
        self.engine._position_check_threshold_ns = 0

        self._order_ids = itertools.count(1)

    def add_position(
        self,
        instrument,
        order_side: OrderSide,
        quantity: str,
        position_id: PositionId,
        ts_opened: int = 0,
        last_px: str = "1.0",
        strategy_id: StrategyId | None = None,
        account_id: AccountId | None = None,
    ) -> Position:
        client_order_id = ClientOrderId(f"O-SEED-{next(self._order_ids)}")
        order = TestExecStubs.limit_order(
            instrument=instrument,
            order_side=order_side,
            quantity=instrument.make_qty(Decimal(quantity)),
            price=instrument.make_price(Decimal(last_px)),
            client_order_id=client_order_id,
            strategy_id=strategy_id,
        )
        fill = TestEventStubs.order_filled(
            order,
            instrument=instrument,
            account_id=account_id or self.account_id,
            last_qty=instrument.make_qty(Decimal(quantity)),
            last_px=instrument.make_price(Decimal(last_px)),
            position_id=position_id,
            ts_event=ts_opened,
        )
        position = Position(instrument=instrument, fill=fill)
        self.cache.add_position(position, self.oms_type)
        return position

    def clear_reports(self) -> None:
        self.client._position_status_reports.clear()

    def add_report(
        self,
        instrument,
        position_side: PositionSide,
        quantity: str,
        venue_position_id: PositionId | None = None,
        avg_px_open: Decimal | None = None,
    ) -> PositionStatusReport:
        ts_now = self.clock.timestamp_ns()
        report = PositionStatusReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            venue_position_id=venue_position_id,
            position_side=position_side,
            quantity=instrument.make_qty(Decimal(quantity)),
            avg_px_open=avg_px_open,
            report_id=UUID4(),
            ts_last=ts_now,
            ts_init=ts_now,
        )
        self.client.add_position_status_report(report)
        return report

    def open_positions(self, instrument) -> list[Position]:
        return sorted(
            self.cache.positions_open(instrument_id=instrument.id),
            key=lambda p: p.id.value,
        )

    def residuals(self) -> dict:
        raw = self.cache.get(RESIDUALS_KEY)
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def reconciliation_orders(self) -> list:
        return sorted(
            (o for o in self.cache.orders() if not o.client_order_id.value.startswith("O-SEED-")),
            key=lambda o: o.client_order_id.value,
        )


@pytest.fixture
def env(event_loop):
    return ConvergenceEnv(event_loop)


# -- The measured production incident ----------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_pass_closes_every_cached_position_when_venue_flat(env):
    """
    Four symmetric virtual-id positions net to zero while the venue is flat; the startup
    convergence pass must close every one of them.
    """
    # Arrange
    targets = [
        env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.003", PositionId("P-VIRT-1")),
        env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.006", PositionId("P-VIRT-2")),
        env.add_position(BTCUSD_SIM, OrderSide.SELL, "0.003", PositionId("P-VIRT-3")),
        env.add_position(BTCUSD_SIM, OrderSide.SELL, "0.006", PositionId("P-VIRT-4")),
    ]
    assert len(env.open_positions(BTCUSD_SIM)) == 4
    assert sum(p.signed_decimal_qty() for p in targets) == Decimal(0)

    # Act
    await env.engine.reconcile_execution_state()

    # Assert
    assert env.open_positions(BTCUSD_SIM) == []

    repairs = env.reconciliation_orders()
    assert len(repairs) == 4
    assert all(o.is_reduce_only for o in repairs)

    repaired_targets = {env.cache.position_id(o.client_order_id) for o in repairs}
    assert repaired_targets == {t.id for t in targets}

    for target in targets:
        closed = env.cache.position(target.id)
        assert closed is not None
        assert closed.is_closed

    assert env.residuals() == {}


# -- G5: consistent hedge cache with both sides reported ----------------------------------------


@pytest.mark.asyncio
async def test_consistent_hedge_cache_converges_with_no_repairs(env):
    """
    Both hedge sides reported and matching cache must produce zero discrepancies across
    repeated passes.
    """
    # Arrange
    long_id = PositionId(f"{AUDUSD_SIM.id}-LONG")
    short_id = PositionId(f"{AUDUSD_SIM.id}-SHORT")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "9000", long_id)
    env.add_position(AUDUSD_SIM, OrderSide.SELL, "9000", short_id)
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "9000", venue_position_id=long_id)
    env.add_report(AUDUSD_SIM, PositionSide.SHORT, "9000", venue_position_id=short_id)

    # Act
    for _ in range(3):
        await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(AUDUSD_SIM)
    assert [p.id for p in positions] == [long_id, short_id]
    assert positions[0].quantity == Quantity.from_int(9000)
    assert positions[1].quantity == Quantity.from_int(9000)
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


# -- Order independence --------------------------------------------------------------------------


def _hedge_flip_state(env) -> tuple[PositionId, PositionId]:
    long_id = PositionId(f"{AUDUSD_SIM.id}-LONG")
    short_id = PositionId(f"{AUDUSD_SIM.id}-SHORT")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "3000", long_id)
    env.add_position(AUDUSD_SIM, OrderSide.SELL, "2000", short_id)
    return long_id, short_id


@pytest.mark.asyncio
async def test_report_order_does_not_change_outcome(event_loop):
    """
    The same venue state delivered in either report order must converge identically.
    """
    # Arrange
    outcomes = []

    for reverse in (False, True):
        env = ConvergenceEnv(event_loop)
        long_id, short_id = _hedge_flip_state(env)
        reports = [
            (PositionSide.LONG, "5000", long_id),
            (PositionSide.FLAT, "0", short_id),
        ]

        if reverse:
            reports.reverse()

        for side, qty, vid in reports:
            env.add_report(AUDUSD_SIM, side, qty, venue_position_id=vid)

        # Act
        await env.engine._check_positions_consistency()

        outcomes.append(
            (
                sorted(
                    (p.id.value, p.side, p.quantity.as_decimal())
                    for p in env.open_positions(AUDUSD_SIM)
                ),
                sorted(
                    (
                        str(env.cache.position_id(o.client_order_id)),
                        o.side,
                        o.quantity.as_decimal(),
                        o.is_reduce_only,
                    )
                    for o in env.reconciliation_orders()
                ),
            ),
        )

    # Assert
    assert outcomes[0] == outcomes[1]
    positions, repairs = outcomes[0]
    assert positions == [(f"{AUDUSD_SIM.id}-LONG", PositionSide.LONG, Decimal(5000))]
    assert repairs == [
        (f"{AUDUSD_SIM.id}-LONG", OrderSide.BUY, Decimal(2000), False),
        (f"{AUDUSD_SIM.id}-SHORT", OrderSide.BUY, Decimal(2000), True),
    ]


# -- Incomplete snapshot -------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_incomplete_snapshot_blocks_repair_and_preserves_retries(env):
    """
    A failed position query removes absence authority; nothing is repaired and no retry
    is consumed.
    """
    # Arrange
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-INCOMPLETE-1"))

    async def raise_error(command):
        raise RuntimeError("venue unavailable")

    env.client.generate_position_status_reports = raise_error

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(AUDUSD_SIM)
    assert len(positions) == 1
    assert positions[0].quantity == Quantity.from_int(1000)
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}

    residuals = env.residuals()
    assert sorted(residuals) == [
        f"{AUDUSD_SIM.id}|{env.account_id}",
        f"{BTCUSD_SIM.id}|{env.account_id}",
    ]
    entry = residuals[f"{AUDUSD_SIM.id}|{env.account_id}"]
    assert isinstance(entry["reason"], str)
    assert entry["reason"]
    assert isinstance(entry["ts"], int)


# -- Strict equality -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sub_unit_quantity_is_not_within_tolerance(env):
    """
    A cached position of one size increment against a flat venue must be repaired.
    """
    # Arrange
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.001", PositionId("P-TINY-1"))

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.open_positions(BTCUSD_SIM) == []
    assert env.residuals() == {}


# -- Watermark race ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_activity_during_query_discards_repair(env):
    """
    Local activity between query start and the first synthetic event discards that
    scope's repair without consuming a retry, and publishes no residual for it.
    """
    # Arrange
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-RACE-1"))

    key = (AUDUSD_SIM.id, env.account_id)
    original = env.client.generate_position_status_reports

    async def bump_then_report(command):
        reports = await original(command)
        env.engine._position_local_activity_ns[key] = env.clock.timestamp_ns()
        return reports

    env.client.generate_position_status_reports = bump_then_report

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(AUDUSD_SIM)
    assert len(positions) == 1
    assert positions[0].quantity == Quantity.from_int(1000)
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


# -- Net-granular trim ordering ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_net_granular_trim_takes_oldest_positions_first(env):
    """
    Net-granular excess trims the oldest entries first with exact quantities.
    """
    # Arrange
    oldest = env.add_position(
        BTCUSD_SIM,
        OrderSide.BUY,
        "0.005",
        PositionId("P-NET-A"),
        ts_opened=1_000,
    )
    middle = env.add_position(
        BTCUSD_SIM,
        OrderSide.BUY,
        "0.003",
        PositionId("P-NET-B"),
        ts_opened=2_000,
    )
    newest = env.add_position(
        BTCUSD_SIM,
        OrderSide.BUY,
        "0.002",
        PositionId("P-NET-C"),
        ts_opened=3_000,
    )
    env.add_report(BTCUSD_SIM, PositionSide.LONG, "0.004")

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.cache.position(oldest.id).is_closed
    assert env.cache.position(middle.id).quantity == BTCUSD_SIM.make_qty(Decimal("0.002"))
    assert env.cache.position(newest.id).quantity == BTCUSD_SIM.make_qty(Decimal("0.002"))
    assert sum(p.signed_decimal_qty() for p in env.open_positions(BTCUSD_SIM)) == Decimal("0.004")
    assert env.residuals() == {}


# -- Deficit repair ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deficit_opens_under_claiming_strategy(env):
    """
    A venue position absent from the cache is opened under the claiming strategy at the
    venue's quantity and venue position ID.
    """
    # Arrange
    claiming = StrategyId("S-CLAIM")
    env.engine._external_order_claims[BTCUSD_SIM.id] = claiming
    venue_position_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    env.add_report(
        BTCUSD_SIM,
        PositionSide.LONG,
        "1.000",
        venue_position_id=venue_position_id,
        avg_px_open=Decimal("100.0"),
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(BTCUSD_SIM)
    assert len(positions) == 1
    assert positions[0].id == venue_position_id
    assert positions[0].quantity == BTCUSD_SIM.make_qty(Decimal("1.000"))
    assert positions[0].strategy_id == claiming
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_deficit_opens_under_external_when_unclaimed(env):
    """
    An unclaimed instrument opens the missing venue position under EXTERNAL.
    """
    # Arrange
    venue_position_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    env.add_report(
        BTCUSD_SIM,
        PositionSide.LONG,
        "1.000",
        venue_position_id=venue_position_id,
        avg_px_open=Decimal("100.0"),
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(BTCUSD_SIM)
    assert len(positions) == 1
    assert positions[0].id == venue_position_id
    assert positions[0].quantity == BTCUSD_SIM.make_qty(Decimal("1.000"))
    assert positions[0].strategy_id == StrategyId("EXTERNAL")


# -- Per-entry net equality ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_net_equality_leaves_entries_untouched(env):
    """
    Entries that sum to the venue's net at net granularity are equal and untouched.
    """
    # Arrange
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "2.000", PositionId("P-EQ-A"), ts_opened=1_000)
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "1.000", PositionId("P-EQ-B"), ts_opened=2_000)
    env.add_position(BTCUSD_SIM, OrderSide.SELL, "1.000", PositionId("P-EQ-C"), ts_opened=3_000)
    env.add_report(BTCUSD_SIM, PositionSide.LONG, "2.000")

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(BTCUSD_SIM)
    assert [(p.id.value, p.quantity.as_decimal()) for p in positions] == [
        ("P-EQ-A", Decimal("2.000")),
        ("P-EQ-B", Decimal("1.000")),
        ("P-EQ-C", Decimal("1.000")),
    ]
    assert env.reconciliation_orders() == []
    assert env.residuals() == {}


# -- Residual document ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_residual_document_is_rewritten_whole_each_pass(env):
    """
    A non-converged pass writes one entry per scope; a converged pass rewrites an empty
    object.
    """
    # Arrange
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-RESID-1"))
    env.engine.generate_missing_orders = False

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    residuals = env.residuals()
    assert set(residuals) == {f"{AUDUSD_SIM.id}|{env.account_id}"}

    decoded = RESIDUAL_DECODER.decode(env.cache.get(RESIDUALS_KEY))
    entry = decoded[f"{AUDUSD_SIM.id}|{env.account_id}"]
    assert entry.reason
    assert entry.ts > 0

    # Arrange - venue now agrees with the cache
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "1000")

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.residuals() == {}
    assert RESIDUAL_DECODER.decode(env.cache.get(RESIDUALS_KEY)) == {}


# -- Trigger equivalence -------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_and_timer_triggers_produce_identical_repairs(event_loop):
    """
    Identical state converged through the startup trigger and the timer trigger must
    produce identical repairs.
    """
    # Arrange
    outcomes = []

    for use_startup in (True, False):
        env = ConvergenceEnv(event_loop)
        env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.500", PositionId("P-TRIG-A"), ts_opened=10)
        env.add_position(BTCUSD_SIM, OrderSide.SELL, "0.250", PositionId("P-TRIG-B"), ts_opened=20)

        # Act
        if use_startup:
            await env.engine.reconcile_execution_state()
        else:
            await env.engine._check_positions_consistency()

        outcomes.append(
            (
                sorted(
                    (p.id.value, p.quantity.as_decimal()) for p in env.open_positions(BTCUSD_SIM)
                ),
                sorted(
                    (
                        str(env.cache.position_id(o.client_order_id)),
                        o.side,
                        o.quantity.as_decimal(),
                        o.is_reduce_only,
                    )
                    for o in env.reconciliation_orders()
                ),
                env.residuals(),
            ),
        )

    # Assert
    assert outcomes[0] == outcomes[1]
    assert outcomes[0][0] == []
    assert outcomes[0][1] == [
        ("P-TRIG-A", OrderSide.SELL, Decimal("0.500"), True),
        ("P-TRIG-B", OrderSide.BUY, Decimal("0.250"), True),
    ]


# -- Target safety -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_excess_trim_never_flips_the_target(env):
    """
    An excess trim is capped at the target's current quantity, so the target reduces
    rather than flipping.
    """
    # Arrange
    target = env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.003", PositionId("P-CAP-1"))
    env.add_report(BTCUSD_SIM, PositionSide.LONG, "0.001")

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    repaired = env.cache.position(target.id)
    assert repaired.side == PositionSide.LONG
    assert repaired.quantity == BTCUSD_SIM.make_qty(Decimal("0.001"))

    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert repairs[0].is_reduce_only
    assert repairs[0].quantity == BTCUSD_SIM.make_qty(Decimal("0.002"))
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_retries_increment_only_on_failed_postcondition(env):
    """
    Retries are consumed by a complete snapshot's failed repair and by nothing else.
    """
    # Arrange
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-RETRY-1"))
    env.engine.position_check_retries = 2

    attempts = 0

    def failing_repair(report, trades, is_external=True):
        nonlocal attempts
        attempts += 1
        return True  # Reports success without applying anything

    env.engine._reconcile_order_report = failing_repair
    key = (AUDUSD_SIM.id, env.account_id)

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.engine._position_recon_retries[key] == 1
    assert attempts == 1

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.engine._position_recon_retries[key] == 2
    assert attempts == 2

    # Act - retries exhausted, no further repair attempts
    await env.engine._check_positions_consistency()

    # Assert
    assert env.engine._position_recon_retries[key] == 2
    assert attempts == 2
    assert list(env.residuals()) == [f"{AUDUSD_SIM.id}|{env.account_id}"]


@pytest.mark.asyncio
async def test_out_of_scope_instruments_are_not_repaired(env):
    """
    Rows outside the reconciliation instrument filter are out of scope.
    """
    # Arrange
    env.engine.reconciliation_instrument_ids = [AUDUSD_SIM.id]
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.003", PositionId("P-SCOPE-1"))

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert len(env.open_positions(BTCUSD_SIM)) == 1
    assert env.reconciliation_orders() == []
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_uncovered_scope_has_no_absence_authority(env):
    """
    A scope no registered client can report on is never closed on venue silence.
    """
    # Arrange - a cached position on a venue and account with no execution client
    other_venue = Venue("OTHER")
    other_account = AccountId("OTHER-001")
    other_instrument = TestInstrumentProvider.default_fx_ccy("GBP/USD", venue=other_venue)
    env.cache.add_instrument(other_instrument)
    env.add_position(
        other_instrument,
        OrderSide.BUY,
        "1000",
        PositionId("P-UNCOVERED-1"),
        account_id=other_account,
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert len(env.open_positions(other_instrument)) == 1
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert list(env.residuals()) == [f"{other_instrument.id}|{other_account}"]


@pytest.mark.asyncio
async def test_sub_increment_difference_compares_equal(env):
    """
    A difference finer than the instrument size increment is representational noise, so
    the scope compares equal with no repair, no residual and no retry consumed.
    """
    # Arrange - the venue reports a quantity finer than the instrument's increment
    target = env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.003", PositionId("P-SUBINC-1"))
    ts_now = env.clock.timestamp_ns()
    env.client.add_position_status_report(
        PositionStatusReport(
            account_id=env.account_id,
            instrument_id=BTCUSD_SIM.id,
            position_side=PositionSide.LONG,
            quantity=Quantity.from_str("0.0025"),
            report_id=UUID4(),
            ts_last=ts_now,
            ts_init=ts_now,
        ),
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.cache.position(target.id).quantity == BTCUSD_SIM.make_qty(Decimal("0.003"))
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_identical_duplicate_reports_collapse(env):
    """
    Reports keyed by venue position ID carry set semantics, so an identical duplicate
    row does not double the venue quantity.
    """
    # Arrange
    venue_position_id = PositionId(f"{AUDUSD_SIM.id}-LONG")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "9000", venue_position_id)
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "9000", venue_position_id=venue_position_id)
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "9000", venue_position_id=venue_position_id)

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(AUDUSD_SIM)
    assert len(positions) == 1
    assert positions[0].quantity == Quantity.from_int(9000)
    assert env.reconciliation_orders() == []
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_conflicting_duplicate_venue_ids_make_snapshot_incomplete(env):
    """
    A venue position ID reported twice with different quantities has no single truth, so
    the snapshot is incomplete.
    """
    # Arrange
    venue_position_id = PositionId(f"{AUDUSD_SIM.id}-LONG")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "9000", venue_position_id)
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "5000", venue_position_id=venue_position_id)
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "7000", venue_position_id=venue_position_id)

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(AUDUSD_SIM)
    assert len(positions) == 1
    assert positions[0].quantity == Quantity.from_int(9000)
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert list(env.residuals()) == [f"{AUDUSD_SIM.id}|{env.account_id}"]


@pytest.mark.asyncio
async def test_mixed_report_granularity_makes_snapshot_incomplete(env):
    """
    A scope mixing position-ID and net-granularity rows cannot be compared at either
    granularity, so the snapshot is incomplete.
    """
    # Arrange
    venue_position_id = PositionId(f"{AUDUSD_SIM.id}-LONG")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "9000", venue_position_id)
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "5000", venue_position_id=venue_position_id)
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "5000")

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(AUDUSD_SIM)
    assert len(positions) == 1
    assert positions[0].quantity == Quantity.from_int(9000)
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert list(env.residuals()) == [f"{AUDUSD_SIM.id}|{env.account_id}"]


@pytest.mark.asyncio
async def test_netting_trim_preserves_position_identity_and_owner(event_loop):
    """
    A NETTING scope repairs through the same target-safe machinery while keeping the
    `{instrument}-{strategy}` identity and the owning strategy.
    """
    # Arrange
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    strategy_id = StrategyId("S-001")
    position_id = PositionId(f"{AUDUSD_SIM.id}-{strategy_id}")
    env.add_position(
        AUDUSD_SIM,
        OrderSide.BUY,
        "1000",
        position_id,
        strategy_id=strategy_id,
    )
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "600", avg_px_open=Decimal("1.0"))

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(AUDUSD_SIM)
    assert len(positions) == 1
    assert positions[0].id == position_id
    assert positions[0].strategy_id == strategy_id
    assert positions[0].quantity == Quantity.from_int(600)

    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert repairs[0].strategy_id == strategy_id
    assert repairs[0].is_reduce_only
    assert repairs[0].side == OrderSide.SELL
    assert repairs[0].quantity == Quantity.from_int(400)
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_netting_venue_flat_closes_strategy_owned_position(event_loop):
    """
    Absence closes a strategy-owned NETTING position without tripping the reduce-only
    open denial.
    """
    # Arrange
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    strategy_id = StrategyId("S-001")
    position_id = PositionId(f"{AUDUSD_SIM.id}-{strategy_id}")
    env.add_position(
        AUDUSD_SIM,
        OrderSide.BUY,
        "1000",
        position_id,
        strategy_id=strategy_id,
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.open_positions(AUDUSD_SIM) == []
    assert env.cache.position(position_id).is_closed
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_absence_reported_by_another_account_does_not_close_positions(env):
    """
    A client reports only its own account, so its silence is not absence authority for a
    different account on the same venue.
    """
    # Arrange
    other_account = AccountId(f"{SIM.value}-002")
    env.cache.add_account(TestExecStubs.cash_account(other_account))
    env.add_position(
        AUDUSD_SIM,
        OrderSide.BUY,
        "1000",
        PositionId("P-OTHER-ACCOUNT-1"),
        account_id=other_account,
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(AUDUSD_SIM)
    assert len(positions) == 1
    assert positions[0].quantity == Quantity.from_int(1000)
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert list(env.residuals()) == [f"{AUDUSD_SIM.id}|{other_account}"]


@pytest.mark.asyncio
async def test_deficit_bound_to_a_foreign_position_id_is_refused(env):
    """
    Position IDs are keyed globally, so a deficit must not bind to an ID already held by
    another instrument or account.
    """
    # Arrange - the same venue position ID is already an open BTC/USD position
    shared_id = PositionId("P-SHARED")
    target = env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.003", shared_id)
    env.add_report(
        BTCUSD_SIM,
        PositionSide.LONG,
        "0.003",
        venue_position_id=shared_id,
        avg_px_open=Decimal("1.0"),
    )
    env.add_report(
        AUDUSD_SIM,
        PositionSide.LONG,
        "1000",
        venue_position_id=shared_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.cache.position(target.id).instrument_id == BTCUSD_SIM.id
    assert env.cache.position(target.id).quantity == BTCUSD_SIM.make_qty(Decimal("0.003"))
    assert env.open_positions(AUDUSD_SIM) == []
    assert env.reconciliation_orders() == []
    assert list(env.residuals()) == [f"{AUDUSD_SIM.id}|{env.account_id}"]


@pytest.mark.asyncio
async def test_failed_query_is_reported_even_when_the_cache_is_flat(event_loop):
    """
    A failed venue query leaves the scope unobserved, which must be reported even when
    the cache holds nothing there, with and without an instrument allow-list.
    """

    async def raise_error(command):
        raise RuntimeError("venue unavailable")

    # Arrange - an allow-list narrows which scopes the failed client is reported for
    allow_listed = ConvergenceEnv(event_loop)
    allow_listed.engine.reconciliation_instrument_ids = [AUDUSD_SIM.id]
    allow_listed.client.generate_position_status_reports = raise_error

    # Act
    converged = await allow_listed.engine._run_position_convergence_pass("test")

    # Assert
    assert not converged
    assert list(allow_listed.residuals()) == [f"{AUDUSD_SIM.id}|{allow_listed.account_id}"]
    assert dict(allow_listed.engine._position_recon_retries) == {}

    # Arrange - the default configuration has no allow-list to derive scopes from
    default = ConvergenceEnv(event_loop)
    default.client.generate_position_status_reports = raise_error

    # Act
    converged = await default.engine._run_position_convergence_pass("test")

    # Assert - every loaded instrument the failed client routes for is reported
    assert not converged
    decoded = RESIDUAL_DECODER.decode(default.cache.get(RESIDUALS_KEY))
    assert sorted(decoded) == [
        f"{AUDUSD_SIM.id}|{default.account_id}",
        f"{BTCUSD_SIM.id}|{default.account_id}",
    ]
    assert [entry.reason for entry in decoded.values()] == [
        f"position status query failed for {default.client.id}",
    ] * 2
    assert dict(default.engine._position_recon_retries) == {}


@pytest.mark.asyncio
async def test_deficit_finer_than_the_increment_opens_at_the_declared_precision(env):
    """
    A deficit the venue reports finer than the size increment opens at the declared
    precision rounded toward zero, so the repair never holds more than the venue does
    and the scope converges rather than gating on a quantity no order could carry.
    """
    # Arrange - the venue reports a quantity finer than the instrument's size increment
    ts_now = env.clock.timestamp_ns()
    env.client.add_position_status_report(
        PositionStatusReport(
            account_id=env.account_id,
            instrument_id=BTCUSD_SIM.id,
            position_side=PositionSide.LONG,
            quantity=Quantity.from_str("0.0026"),
            avg_px_open=Decimal("1.0"),
            report_id=UUID4(),
            ts_last=ts_now,
            ts_init=ts_now,
        ),
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert - opened at 0.002, never at 0.003 which the venue does not hold
    positions = env.open_positions(BTCUSD_SIM)
    assert len(positions) == 1
    assert positions[0].side == PositionSide.LONG
    assert positions[0].quantity == BTCUSD_SIM.make_qty(Decimal("0.002"))
    assert positions[0].strategy_id == StrategyId("EXTERNAL")

    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert repairs[0].side == OrderSide.BUY
    assert repairs[0].quantity == BTCUSD_SIM.make_qty(Decimal("0.002"))
    assert not repairs[0].is_reduce_only
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_residual_write_failure_fails_the_pass(env):
    """
    A pass which could not publish its residual document must not report success.
    """
    # Arrange
    env.engine._write_position_residuals = lambda residuals: False

    # Act
    converged = await env.engine._run_position_convergence_pass("test")

    # Assert
    assert not converged


class UnresolvedAccountExecutionClient(LiveExecutionClient):
    """
    A live client which has not resolved its account ID and whose position query fails.
    """

    def __init__(self, loop, client_id, venue, msgbus, cache, clock) -> None:
        super().__init__(
            loop=loop,
            client_id=client_id,
            venue=venue,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=USD,
            instrument_provider=InstrumentProvider(),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

    async def generate_position_status_reports(self, command):
        raise RuntimeError("venue unavailable")


@pytest.mark.asyncio
async def test_failed_client_without_account_identity_leaves_residuals_unwritten(event_loop):
    """
    A failed client whose account is still unresolved has no derivable scopes, so the
    pass leaves the previous residual document standing rather than publishing `{}`.
    """
    # Arrange - the venue's only client failed before establishing its account
    env = ConvergenceEnv(event_loop)
    env.engine.deregister_client(env.client)
    unresolved = UnresolvedAccountExecutionClient(
        loop=event_loop,
        client_id=ClientId(SIM.value),
        venue=SIM,
        msgbus=env.msgbus,
        cache=env.cache,
        clock=env.clock,
    )
    env.engine.register_client(unresolved)
    unresolved.start()
    assert unresolved.account_id is None
    assert len(env.cache.instruments()) == 2

    prior = {f"{AUDUSD_SIM.id}|{env.account_id}": {"reason": "prior pass", "ts": 1}}
    env.cache.add(RESIDUALS_KEY, json.dumps(prior).encode("utf-8"))

    reasons = []
    build_reason = env.engine._unpublishable_residuals_reason

    def spy(trigger, clients):
        message = build_reason(trigger, clients)
        reasons.append(message)
        return message

    env.engine._unpublishable_residuals_reason = spy

    # Act
    converged = await env.engine._run_position_convergence_pass("test")

    # Assert - nothing published, the pass fails, and one line names the failed client
    assert not converged
    assert env.residuals() == prior
    assert len(reasons) == 1
    assert unresolved.id.value in reasons[0]
    assert "account_id=None" in reasons[0]


@pytest.mark.asyncio
async def test_failed_query_reports_only_scopes_the_client_could_observe(env):
    """
    A failed query is reported for the scopes that client could have observed, not for
    every configured instrument.
    """
    # Arrange - one configured instrument sits on a venue the client does not serve
    other_instrument = TestInstrumentProvider.default_fx_ccy("GBP/USD", venue=Venue("OTHER"))
    env.cache.add_instrument(other_instrument)
    env.engine.reconciliation_instrument_ids = [AUDUSD_SIM.id, other_instrument.id]

    async def raise_error(command):
        raise RuntimeError("venue unavailable")

    env.client.generate_position_status_reports = raise_error

    # Act
    converged = await env.engine._run_position_convergence_pass("test")

    # Assert
    assert not converged
    assert list(env.residuals()) == [f"{AUDUSD_SIM.id}|{env.account_id}"]


@pytest.mark.asyncio
async def test_stale_trim_target_is_skipped_without_applying_anything(env):
    """
    A trim whose target is no longer an open position of the scope applies nothing, so
    it is a skip rather than a failed repair.
    """
    # Arrange
    scope = PositionScopeSnapshot(
        instrument_id=AUDUSD_SIM.id,
        account_id=env.account_id,
        reports=(),
        incomplete_reason=None,
    )
    intents = [
        PositionRepairIntent(
            action=POSITION_REPAIR_TRIM,
            order_side=OrderSide.SELL,
            quantity=Decimal(500),
            target_position_id=PositionId("P-GONE"),
            target_strategy_id=StrategyId("EXTERNAL"),
            avg_px=Decimal("1.0"),
        ),
    ]

    # Act
    attempted = env.engine._apply_position_repairs(scope, intents)

    # Assert
    assert attempted is False
    assert env.reconciliation_orders() == []


@pytest.mark.asyncio
async def test_deficit_bound_to_a_pending_order_position_id_is_refused(env):
    """
    A position ID already bound to an unfilled order is not free, because that order's
    fill will resolve onto it.
    """
    # Arrange - a pending BTC/USD order is pre-bound to the ID the venue reports for AUD/USD
    shared_id = PositionId("P-PENDING-SHARED")
    pending = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        client_order_id=ClientOrderId("O-PENDING-1"),
    )
    env.cache.add_order(pending, position_id=shared_id)
    env.add_report(
        AUDUSD_SIM,
        PositionSide.LONG,
        "1000",
        venue_position_id=shared_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.cache.position(shared_id) is None
    assert env.open_positions(AUDUSD_SIM) == []
    assert list(env.residuals()) == [f"{AUDUSD_SIM.id}|{env.account_id}"]


@pytest.mark.asyncio
async def test_absence_needs_the_routing_client_for_the_instrument_venue(env):
    """
    A client is absence authority only for the venues it is routed for, even when the
    account matches.
    """
    # Arrange - an instrument on a venue with no registered execution client, held under
    # the registered client's own account
    other_instrument = TestInstrumentProvider.default_fx_ccy("GBP/USD", venue=Venue("OTHER"))
    env.cache.add_instrument(other_instrument)
    env.add_position(other_instrument, OrderSide.BUY, "1000", PositionId("P-UNROUTED-1"))

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    positions = env.open_positions(other_instrument)
    assert len(positions) == 1
    assert positions[0].quantity == Quantity.from_int(1000)
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert list(env.residuals()) == [f"{other_instrument.id}|{env.account_id}"]


# -- B1: position-ID protection ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_position_id_protection_covers_idless_and_legacy_repairs(event_loop):
    """
    Every fabricated position ID is resolved before collision checks; unresolved order
    ownership blocks both convergence and legacy hedge fabrication.
    """
    # Arrange - another account already owns the old account-less fabricated ID
    env = ConvergenceEnv(event_loop)
    other_account = AccountId(f"{SIM.value}-002")
    env.cache.add_account(TestExecStubs.cash_account(other_account))
    accountless_id = PositionId(f"{AUDUSD_SIM.id}-EXTERNAL")
    foreign = env.add_position(
        AUDUSD_SIM,
        OrderSide.BUY,
        "1000",
        accountless_id,
        strategy_id=StrategyId("EXTERNAL"),
        account_id=other_account,
    )
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "1000", avg_px_open=Decimal("1.0"))

    # Act
    await env.engine._check_positions_consistency()

    # Assert - the local repair gets an account-scoped HEDGE ID and cannot grow `foreign`
    effective_id = PositionId(f"{AUDUSD_SIM.id}-{env.account_id}-EXTERNAL")
    opened = env.cache.position(effective_id)
    assert env.cache.position(foreign.id).quantity == Quantity.from_int(1000)
    assert opened is not None
    assert opened.account_id == env.account_id
    assert opened.strategy_id == StrategyId("EXTERNAL")
    assert opened.quantity == Quantity.from_int(1000)

    # Arrange - the same-instrument order bound to the effective ID has no proven account
    pending_env = ConvergenceEnv(event_loop)
    pending_id = PositionId(
        f"{AUDUSD_SIM.id}-{pending_env.account_id}-EXTERNAL",
    )
    pending = TestExecStubs.limit_order(
        instrument=AUDUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1000),
        client_order_id=ClientOrderId("O-PENDING-IDLESS"),
        strategy_id=StrategyId("EXTERNAL"),
    )
    pending_env.cache.add_order(pending, position_id=pending_id)
    pending_env.add_report(
        AUDUSD_SIM,
        PositionSide.LONG,
        "1000",
        avg_px_open=Decimal("1.0"),
    )

    # Act
    await pending_env.engine._check_positions_consistency()

    # Assert - unresolved account ownership is not treated as same-scope ownership
    assert pending.account_id is None
    assert pending_env.open_positions(AUDUSD_SIM) == []
    assert list(pending_env.residuals()) == [
        f"{AUDUSD_SIM.id}|{pending_env.account_id}",
    ]

    # Arrange - exercise the legacy missing-HEDGE path with the same unresolved binding
    legacy_env = ConvergenceEnv(event_loop)
    legacy_id = PositionId("P-LEGACY-PENDING")
    legacy_pending = TestExecStubs.limit_order(
        instrument=AUDUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1000),
        client_order_id=ClientOrderId("O-PENDING-LEGACY"),
        strategy_id=StrategyId("EXTERNAL"),
    )
    legacy_env.cache.add_order(legacy_pending, position_id=legacy_id)
    legacy_report = legacy_env.add_report(
        AUDUSD_SIM,
        PositionSide.LONG,
        "1000",
        venue_position_id=legacy_id,
        avg_px_open=Decimal("1.0"),
    )
    orders_before = len(legacy_env.cache.orders())

    # Act
    legacy_result = legacy_env.engine._reconcile_position_report_hedging(legacy_report)

    # Assert
    assert not legacy_result
    assert legacy_env.cache.position(legacy_id) is None
    assert len(legacy_env.cache.orders()) == orders_before


@pytest.mark.asyncio
async def test_repaired_scope_grows_under_the_same_fabricated_id(env):
    """
    A closed repair order is not ownership evidence, so venue growth after a repair
    converges under the fabricated ID the first repair already opened.
    """
    # Arrange - the venue holds LONG 1000 at net granularity and the cache is flat
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "1000", avg_px_open=Decimal("1.0"))

    # Act
    await env.engine._check_positions_consistency()

    # Assert - one fabricated open converges the scope
    fabricated_id = PositionId(f"{AUDUSD_SIM.id}-{env.account_id}-EXTERNAL")
    first_pass = env.open_positions(AUDUSD_SIM)
    assert [p.id for p in first_pass] == [fabricated_id]
    assert first_pass[0].quantity == Quantity.from_int(1000)
    assert env.residuals() == {}

    # Arrange - the venue grows to LONG 1500 after that repair
    env.clear_reports()
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "1500", avg_px_open=Decimal("1.0"))

    # Act
    await env.engine._check_positions_consistency()

    # Assert - the same ID grows to the venue quantity, leaving no residual
    second_pass = env.open_positions(AUDUSD_SIM)
    assert [p.id for p in second_pass] == [fabricated_id]
    assert second_pass[0].quantity == Quantity.from_int(1500)
    assert len(env.reconciliation_orders()) == 2
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_open_foreign_account_order_denies_the_fabricated_id(env):
    """
    An open order bound to the ID under another account is live ownership, so the repair
    is refused rather than binding a second account to that ID.
    """
    # Arrange - an accepted order of another account holds the fabricated ID
    other_account = AccountId(f"{SIM.value}-002")
    fabricated_id = PositionId(f"{AUDUSD_SIM.id}-{env.account_id}-EXTERNAL")
    foreign = TestExecStubs.limit_order(
        instrument=AUDUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1000),
        client_order_id=ClientOrderId("O-OPEN-FOREIGN"),
        strategy_id=StrategyId("EXTERNAL"),
    )
    foreign.apply(TestEventStubs.order_submitted(foreign, account_id=other_account))
    foreign.apply(TestEventStubs.order_accepted(foreign, account_id=other_account))
    env.cache.add_order(foreign, position_id=fabricated_id)
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "1000", avg_px_open=Decimal("1.0"))

    # Act
    await env.engine._check_positions_consistency()

    # Assert - no position is bound to the contested ID and the scope stays unconverged
    assert foreign.account_id == other_account
    assert not foreign.is_closed
    assert env.cache.position(fabricated_id) is None
    assert env.open_positions(AUDUSD_SIM) == []
    assert [o.client_order_id for o in env.cache.orders()] == [foreign.client_order_id]
    assert (
        env.residuals()[f"{AUDUSD_SIM.id}|{env.account_id}"]["reason"]
        == "no applicable repair for this discrepancy"
    )


# -- B2: received-but-unapplied fills ------------------------------------------------------------


@pytest.mark.asyncio
async def test_received_unapplied_fill_during_query_blocks_scope_repair(env):
    """
    A fill received during the venue query invalidates the snapshot before the event
    queue applies it, so the pass cannot synthesize a second close and reports nothing
    of a scope it never judged.
    """
    # Arrange
    target = env.add_position(
        AUDUSD_SIM,
        OrderSide.BUY,
        "1000",
        PositionId("P-RECEIVED-FILL"),
    )
    closing_order = TestExecStubs.limit_order(
        instrument=AUDUSD_SIM,
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(1000),
        client_order_id=ClientOrderId("O-SEED-RACE-CLOSE"),
        strategy_id=target.strategy_id,
    )
    env.cache.add_order(closing_order, position_id=target.id)
    closing_fill = TestEventStubs.order_filled(
        closing_order,
        instrument=AUDUSD_SIM,
        account_id=env.account_id,
        last_qty=Quantity.from_int(1000),
        last_px=Price.from_str("1.00000"),
        position_id=target.id,
    )
    original = env.client.generate_position_status_reports

    async def enqueue_fill_then_report(command):
        reports = await original(command)
        env.engine.process(closing_fill)
        return reports

    env.client.generate_position_status_reports = enqueue_fill_then_report

    # Act
    await env.engine._check_positions_consistency()

    # Assert - the real close is queued, while the cached target is untouched
    key = (AUDUSD_SIM.id, env.account_id)
    assert env.engine.evt_qsize() == 1
    assert env.engine._received_unapplied_fill_counts[key] == 1
    assert env.cache.position(target.id).side == PositionSide.LONG
    assert env.cache.position(target.id).quantity == Quantity.from_int(1000)
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


# -- B3: inbound HEDGE cross-zero two-step -------------------------------------------------------


def test_inbound_hedge_cross_zero_report_applies_the_two_step(env):
    """
    An ID-bearing report contradicting the cached side is venue truth: the cached
    position closes to exactly zero, then the reported side opens under the same ID and
    the same owner.
    """
    # Arrange
    strategy_id = StrategyId("S-OWNER")
    position_id = PositionId(f"{AUDUSD_SIM.id}-HEDGE-CROSS-ZERO")
    env.add_position(
        AUDUSD_SIM,
        OrderSide.BUY,
        "1000",
        position_id,
        strategy_id=strategy_id,
    )
    report = env.add_report(
        AUDUSD_SIM,
        PositionSide.SHORT,
        "1000",
        venue_position_id=position_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert - the venue side is held under the venue ID by the position's own owner
    assert result
    repaired = env.cache.position(position_id)
    assert repaired.side == PositionSide.SHORT
    assert repaired.quantity == Quantity.from_int(1000)
    assert repaired.strategy_id == strategy_id
    assert env.open_positions(AUDUSD_SIM) == [repaired]

    # The old LONG generation was closed to exactly zero before the open
    snapshots = env.cache.position_snapshots(position_id)
    assert len(snapshots) == 1
    assert snapshots[0].is_closed
    assert snapshots[0].entry == OrderSide.BUY

    assert sorted(
        (
            str(env.cache.position_id(order.client_order_id)),
            order.strategy_id.value,
            order.side,
            order.quantity.as_decimal(),
            order.is_reduce_only,
        )
        for order in env.reconciliation_orders()
    ) == [
        (position_id.value, "S-OWNER", OrderSide.SELL, Decimal(1000), False),
        (position_id.value, "S-OWNER", OrderSide.SELL, Decimal(1000), True),
    ]
    assert env.residuals() is None


def test_inbound_repair_raise_is_contained_as_a_scope_residual(env):
    """
    A repair raising mid-application must not escape the inbound report path: the scope
    records the exception as its residual and the report reconciles as failed.
    """
    # Arrange - a virtual ID under HEDGING, which the cache refuses to re-add once closed
    position_id = PositionId("P-VENUE-VIRTUAL")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", position_id)
    report = env.add_report(
        AUDUSD_SIM,
        PositionSide.SHORT,
        "1000",
        venue_position_id=position_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert - the applied close stands, the failed open is the scope's residual
    assert not result
    assert env.cache.position(position_id).is_closed
    assert env.open_positions(AUDUSD_SIM) == []
    assert (
        env.residuals()[f"{AUDUSD_SIM.id}|{env.account_id}"]["reason"]
        == "inbound repair raised KeyError"
    )


def test_id_bearing_report_under_netting_client_trims_the_bound_target(event_loop):
    """
    An ID-bearing runtime report under a NETTING client trims its own target with a
    bound reduce-only repair rather than fabricating opposing EXTERNAL exposure.
    """
    # Arrange - the supported Bybit combination: NETTING client, hedge-mode rows with IDs
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    strategy_id = StrategyId("S-OWNER")
    venue_position_id = PositionId(f"{AUDUSD_SIM.id}-LONG")
    env.add_position(
        AUDUSD_SIM,
        OrderSide.BUY,
        "100",
        venue_position_id,
        strategy_id=strategy_id,
    )
    report = env.add_report(
        AUDUSD_SIM,
        PositionSide.LONG,
        "60",
        venue_position_id=venue_position_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert
    assert result
    repaired = env.cache.position(venue_position_id)
    assert repaired.side == PositionSide.LONG
    assert repaired.quantity == Quantity.from_int(60)
    assert repaired.strategy_id == strategy_id
    assert env.cache.position(PositionId(f"{AUDUSD_SIM.id}-EXTERNAL")) is None
    assert env.open_positions(AUDUSD_SIM) == [repaired]

    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert repairs[0].strategy_id == strategy_id
    assert repairs[0].side == OrderSide.SELL
    assert repairs[0].quantity == Quantity.from_int(40)
    assert repairs[0].is_reduce_only
    assert env.cache.position_id(repairs[0].client_order_id) == venue_position_id
    assert env.residuals() is None


def test_id_bearing_sub_increment_difference_compares_equal(env):
    """
    A sub-increment difference on an ID-bearing report compares equal, so it is neither
    repaired nor reported even with `generate_missing_orders` disabled.
    """
    # Arrange
    env.engine.generate_missing_orders = False
    position_id = PositionId(f"{BTCUSD_SIM.id}-HEDGE-NOISE")
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.003", position_id)
    ts_now = env.clock.timestamp_ns()
    report = PositionStatusReport(
        account_id=env.account_id,
        instrument_id=BTCUSD_SIM.id,
        venue_position_id=position_id,
        position_side=PositionSide.LONG,
        quantity=Quantity.from_str("0.0035"),  # Half a size increment above the cache
        avg_px_open=Decimal("1.0"),
        report_id=UUID4(),
        ts_last=ts_now,
        ts_init=ts_now,
    )

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert
    assert result
    assert env.cache.position(position_id).quantity == BTCUSD_SIM.make_qty(Decimal("0.003"))
    assert env.reconciliation_orders() == []
    assert env.residuals() is None


def test_id_bearing_report_is_not_absence_authority_for_other_hedge_ids(event_loop):
    """
    A single ID-bearing report is positive truth for its own ID only, so an unreported
    hedge ID in the same scope is left untouched.
    """
    # Arrange
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    strategy_id = StrategyId("S-OWNER")
    target_id = PositionId(f"{AUDUSD_SIM.id}-LONG")
    unrelated_id = PositionId(f"{AUDUSD_SIM.id}-LONG-2")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "100", target_id, strategy_id=strategy_id)
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "40", unrelated_id, strategy_id=strategy_id)
    report = env.add_report(
        AUDUSD_SIM,
        PositionSide.LONG,
        "60",
        venue_position_id=target_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert - only the reported ID moves
    assert result
    assert env.cache.position(target_id).quantity == Quantity.from_int(60)
    unrelated = env.cache.position(unrelated_id)
    assert unrelated.side == PositionSide.LONG
    assert unrelated.quantity == Quantity.from_int(40)

    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert env.cache.position_id(repairs[0].client_order_id) == target_id
    assert env.residuals() is None


# -- B4: startup position authority ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_startup_mass_status_leaves_positions_to_the_convergence_pass(event_loop):
    """
    Startup mass status must not mutate positions, so a NETTING report reducing a
    strategy-owned position cannot fabricate unbound exposure ahead of the pass.
    """
    # Arrange - the venue nets short 50 against a strategy-owned long 100
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    strategy_id = StrategyId("S-OWNER")
    owned_id = PositionId(f"{AUDUSD_SIM.id}-{strategy_id}")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "100", owned_id, strategy_id=strategy_id)
    env.add_report(AUDUSD_SIM, PositionSide.SHORT, "50", avg_px_open=Decimal("1.0"))

    # Act
    await env.engine.reconcile_execution_state()

    # Assert - the owner is trimmed to flat and the venue residual opens exactly once
    external_id = PositionId(f"{AUDUSD_SIM.id}-EXTERNAL")
    assert env.cache.position(owned_id).is_closed
    assert env.cache.position(external_id).signed_decimal_qty() == Decimal(-50)
    assert sum(p.signed_decimal_qty() for p in env.open_positions(AUDUSD_SIM)) == Decimal(-50)
    assert sorted(
        (
            str(env.cache.position_id(order.client_order_id)),
            order.strategy_id.value,
            order.side,
            order.quantity.as_decimal(),
            order.is_reduce_only,
        )
        for order in env.reconciliation_orders()
    ) == [
        (external_id.value, "EXTERNAL", OrderSide.SELL, Decimal(50), False),
        (owned_id.value, "S-OWNER", OrderSide.SELL, Decimal(100), True),
    ]
    assert env.residuals() == {}


# -- B5: legacy NETTING repair routing -------------------------------------------------------------


def test_inbound_netting_cross_zero_report_applies_the_two_step(event_loop):
    """
    An inbound NETTING report contradicting the cached side splits into a bound close of
    the owner and an open of the reported side under the claimant.
    """
    # Arrange
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    strategy_id = StrategyId("S-OWNER")
    owned_id = PositionId(f"{AUDUSD_SIM.id}-{strategy_id}")
    external_id = PositionId(f"{AUDUSD_SIM.id}-EXTERNAL")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", owned_id, strategy_id=strategy_id)
    report = env.add_report(AUDUSD_SIM, PositionSide.SHORT, "1000", avg_px_open=Decimal("1.0"))

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert - the owner closes to exactly zero and the venue side opens once
    assert result
    assert env.cache.position(owned_id).is_closed
    opened = env.cache.position(external_id)
    assert opened.side == PositionSide.SHORT
    assert opened.quantity == Quantity.from_int(1000)
    assert env.open_positions(AUDUSD_SIM) == [opened]

    assert sorted(
        (
            str(env.cache.position_id(order.client_order_id)),
            order.strategy_id.value,
            order.side,
            order.quantity.as_decimal(),
            order.is_reduce_only,
        )
        for order in env.reconciliation_orders()
    ) == [
        (external_id.value, "EXTERNAL", OrderSide.SELL, Decimal(1000), False),
        (owned_id.value, "S-OWNER", OrderSide.SELL, Decimal(1000), True),
    ]
    assert env.residuals() is None


def test_inbound_netting_trim_binds_to_the_owning_position(event_loop):
    """
    An inbound NETTING report reduces its scope through a bound reduce-only trim of the
    owning position rather than fabricating opposing exposure.
    """
    # Arrange
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    strategy_id = StrategyId("S-OWNER")
    owned_id = PositionId(f"{AUDUSD_SIM.id}-{strategy_id}")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", owned_id, strategy_id=strategy_id)
    report = env.add_report(AUDUSD_SIM, PositionSide.LONG, "600", avg_px_open=Decimal("1.0"))

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert
    assert result
    repaired = env.cache.position(owned_id)
    assert repaired.quantity == Quantity.from_int(600)
    assert env.open_positions(AUDUSD_SIM) == [repaired]

    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert repairs[0].strategy_id == strategy_id
    assert repairs[0].side == OrderSide.SELL
    assert repairs[0].quantity == Quantity.from_int(400)
    assert repairs[0].is_reduce_only
    assert env.cache.position_id(repairs[0].client_order_id) == owned_id
    assert env.residuals() is None


# -- C1: declared-precision arithmetic at extreme quantities -------------------------------------


def test_max_size_quantity_compares_without_raising():
    """
    A legal maximum-size quantity at the maximum declared precision compares rather than
    escaping the default decimal context, and a one-increment difference at that
    magnitude stays a real discrepancy.
    """
    # Arrange, Act, Assert - the largest representable quantity against a flat cache
    assert not quantities_equal_at_size_precision(
        Decimal("34028236692093.00000000000000"),
        Decimal(0),
        16,
    )
    assert not quantities_equal_at_size_precision(
        Decimal("12345678901234.1234567890123400"),
        Decimal("12345678901234.1234567890123401"),
        16,
    )


def test_max_size_deficit_diffs_into_a_single_open():
    """
    The whole diff runs at a precision the operands fit, so a maximum-size venue
    position against a flat cache yields one open at exactly the reported quantity.
    """
    # Arrange
    report = PositionStatusReport(
        account_id=AccountId(f"{SIM.value}-001"),
        instrument_id=BTCUSD_SIM.id,
        position_side=PositionSide.LONG,
        quantity=Quantity.from_str("34028236692093.0000000000000000"),
        report_id=UUID4(),
        ts_last=0,
        ts_init=0,
    )

    # Act
    intents = diff_position_scope([report], [], 16)

    # Assert
    assert [(i.action, i.order_side, i.quantity) for i in intents] == [
        (POSITION_REPAIR_OPEN, OrderSide.BUY, Decimal("34028236692093.0000000000000000")),
    ]


def test_sub_increment_leftover_emits_no_open_intent(env):
    """
    A repair no order could carry is never emitted: what the trims leave behind finer
    than one size increment is noise the venue itself cannot hold.
    """
    # Arrange - the trimmable long is 0.0006 short of the venue net at a size precision of 3
    long = env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.003", PositionId("P-LEFTOVER-LONG"))
    short = env.add_position(BTCUSD_SIM, OrderSide.SELL, "0.001", PositionId("P-LEFTOVER-SHORT"))
    ts_now = env.clock.timestamp_ns()
    report = PositionStatusReport(
        account_id=env.account_id,
        instrument_id=BTCUSD_SIM.id,
        position_side=PositionSide.SHORT,
        quantity=Quantity.from_str("0.0016"),
        report_id=UUID4(),
        ts_last=ts_now,
        ts_init=ts_now,
    )

    # Act
    intents = diff_position_scope([report], [long, short], BTCUSD_SIM.size_precision)

    # Assert - the trim stands alone, the 0.0006 remainder is not an open
    assert [(i.action, i.order_side, i.quantity) for i in intents] == [
        (POSITION_REPAIR_TRIM, OrderSide.SELL, Decimal("0.003")),
    ]


# -- C2: duplicate reports compare at the declared precision --------------------------------------


@pytest.mark.asyncio
async def test_sub_increment_duplicate_reports_do_not_conflict(env):
    """
    Duplicate rows for one venue position ID differing by less than a size increment are
    the same position, so the snapshot stays complete and the scope converges.
    """
    # Arrange - two rows for one ID, 0.0004 apart at a declared size precision of 3
    venue_position_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "1.000", venue_position_id)
    ts_now = env.clock.timestamp_ns()

    for quantity in ("1.000", "1.0004"):
        env.client.add_position_status_report(
            PositionStatusReport(
                account_id=env.account_id,
                instrument_id=BTCUSD_SIM.id,
                venue_position_id=venue_position_id,
                position_side=PositionSide.LONG,
                quantity=Quantity.from_str(quantity),
                report_id=UUID4(),
                ts_last=ts_now,
                ts_init=ts_now,
            ),
        )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.cache.position(venue_position_id).quantity == BTCUSD_SIM.make_qty(Decimal("1.000"))
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


@pytest.mark.asyncio
async def test_increment_wide_duplicate_reports_still_conflict(env):
    """
    Duplicate rows for one venue position ID a full size increment apart have no single
    truth, so the snapshot stays incomplete.
    """
    # Arrange - two rows for one ID, 0.002 apart at a declared size precision of 3
    venue_position_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "1.000", venue_position_id)
    ts_now = env.clock.timestamp_ns()

    for quantity in ("1.000", "1.002"):
        env.client.add_position_status_report(
            PositionStatusReport(
                account_id=env.account_id,
                instrument_id=BTCUSD_SIM.id,
                venue_position_id=venue_position_id,
                position_side=PositionSide.LONG,
                quantity=Quantity.from_str(quantity),
                report_id=UUID4(),
                ts_last=ts_now,
                ts_init=ts_now,
            ),
        )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.cache.position(venue_position_id).quantity == BTCUSD_SIM.make_qty(Decimal("1.000"))
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert (
        env.residuals()[f"{BTCUSD_SIM.id}|{env.account_id}"]["reason"]
        == f"conflicting duplicate reports for venue position ID {venue_position_id}"
    )


@pytest.mark.asyncio
async def test_declared_equal_duplicates_retain_the_smallest_quantity(env):
    """
    Declared-equal duplicate rows for one venue position ID retain the numerically
    smallest quantity, so the repair never opens above a venue observation.
    """
    # Arrange - two rows for one ID, 0.0002 apart at a declared size precision of 3, and
    # ordered the other way as strings, since "10.0001" sorts before "9.9999".
    venue_position_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    ts_now = env.clock.timestamp_ns()

    for quantity in ("9.9999", "10.0001"):
        env.client.add_position_status_report(
            PositionStatusReport(
                account_id=env.account_id,
                instrument_id=BTCUSD_SIM.id,
                venue_position_id=venue_position_id,
                position_side=PositionSide.LONG,
                quantity=Quantity.from_str(quantity),
                avg_px_open=Decimal("100.0"),
                report_id=UUID4(),
                ts_last=ts_now,
                ts_init=ts_now,
            ),
        )

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert repairs[0].quantity.as_decimal() == Decimal("9.999")

    positions = env.open_positions(BTCUSD_SIM)
    assert len(positions) == 1
    assert positions[0].id == venue_position_id
    assert positions[0].quantity.as_decimal() == Decimal("9.999")
    assert env.residuals() == {}


def test_declared_equal_duplicates_retain_the_smallest_below_decimal_context(env):
    """
    Declared-equal duplicate rows whose difference vanishes at the ambient decimal
    context still retain the numerically smallest quantity, since the dedup sorts on the
    fixed-point raw rather than a rounded decimal.
    """
    # Arrange - two rows for one ID, two raw increments apart at a magnitude where
    # `as_decimal()` rounds both to the same 28-digit value, with the larger delivered
    # first. `Quantity.from_str` collapses the pair before it reaches the sort, so the
    # quantities are built from raw at the build's fixed precision of 16.
    venue_position_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    ts_now = env.clock.timestamp_ns()
    reports = [
        PositionStatusReport(
            account_id=env.account_id,
            instrument_id=BTCUSD_SIM.id,
            venue_position_id=venue_position_id,
            position_side=PositionSide.LONG,
            quantity=Quantity.from_raw(raw, 16),
            avg_px_open=Decimal("100.0"),
            report_id=UUID4(),
            ts_last=ts_now,
            ts_init=ts_now,
        )
        for raw in (123456789012341234567890123401, 123456789012341234567890123399)
    ]
    assert reports[0].quantity.as_decimal() == reports[1].quantity.as_decimal()

    # Act
    normalized, reason = env.engine._normalize_scope_reports(reports, 14)

    # Assert
    assert reason is None
    assert [r.quantity.raw for r in normalized] == [123456789012341234567890123399]


# -- C3: repair quantities are built from decimals, and the close leg leaves nothing --------------


def test_inbound_netting_cross_zero_closes_a_fractional_position_flat(event_loop):
    """
    A cross-zero close of a fractional position must reach exactly zero: a repair
    quantity floored through a double leaves a residue the open then trades against.
    """
    # Arrange - the measured incident: cached +0.58 at size precision 2, venue -1.00
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    env.cache.add_instrument(ETHUSD_SIM)
    strategy_id = StrategyId("S-OWNER")
    owned_id = PositionId(f"{ETHUSD_SIM.id}-{strategy_id}")
    external_id = PositionId(f"{ETHUSD_SIM.id}-EXTERNAL")
    env.add_position(ETHUSD_SIM, OrderSide.BUY, "0.58", owned_id, strategy_id=strategy_id)
    report = env.add_report(ETHUSD_SIM, PositionSide.SHORT, "1.00", avg_px_open=Decimal("1.0"))

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert - the owner closes to exactly zero, with no sub-increment residue left over
    assert result
    assert env.cache.position(owned_id).is_closed
    assert env.cache.position(owned_id).quantity == ETHUSD_SIM.make_qty(Decimal("0.00"))
    opened = env.cache.position(external_id)
    assert opened.side == PositionSide.SHORT
    assert opened.quantity == ETHUSD_SIM.make_qty(Decimal("1.00"))
    assert env.open_positions(ETHUSD_SIM) == [opened]

    assert sorted(
        (
            str(env.cache.position_id(order.client_order_id)),
            order.strategy_id.value,
            order.side,
            order.quantity.as_decimal(),
            order.is_reduce_only,
        )
        for order in env.reconciliation_orders()
    ) == [
        (external_id.value, "EXTERNAL", OrderSide.SELL, Decimal("1.00"), False),
        (owned_id.value, "S-OWNER", OrderSide.SELL, Decimal("0.58"), True),
    ]
    assert env.residuals() is None


def test_cross_zero_open_never_runs_while_a_close_target_holds_exposure(event_loop):
    """
    The open leg of a two-step repair runs only once the close left its target flat at
    the declared precision, so a close reporting success without moving the position
    residuals the scope instead of opening opposing exposure on top of it.
    """
    # Arrange - a close leg which reports success while its target keeps its quantity
    env = ConvergenceEnv(event_loop, oms_type=OmsType.NETTING)
    strategy_id = StrategyId("S-OWNER")
    owned_id = PositionId(f"{AUDUSD_SIM.id}-{strategy_id}")
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", owned_id, strategy_id=strategy_id)
    report = env.add_report(AUDUSD_SIM, PositionSide.SHORT, "1000", avg_px_open=Decimal("1.0"))
    env.engine._apply_position_trim = lambda scope, instrument, intent: True

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert - nothing opened, the untouched target stands and the scope is a residual
    assert not result
    target = env.cache.position(owned_id)
    assert target.is_open
    assert target.quantity == Quantity.from_int(1000)
    assert env.open_positions(AUDUSD_SIM) == [target]
    assert env.reconciliation_orders() == []
    assert (
        env.residuals()[f"{AUDUSD_SIM.id}|{env.account_id}"]["reason"]
        == "inbound cross-zero repair left the scope unconverged"
    )


# -- C4: a skipped fill is not an applied fill ----------------------------------------------------


@pytest.mark.asyncio
async def test_fill_skipped_for_a_conflicting_owner_does_not_defer_the_repair(env):
    """
    A venue fill skipped because its client order ID conflicts with the order its venue
    order ID resolves to was never applied, so the pass must reach its repair rather
    than re-query the same fill forever.
    """
    # Arrange - a cached order the conflicting fill resolves to by venue order ID
    venue_order_id = VenueOrderId("V-CONFLICT-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("1.000")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-VENUE-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)

    ts_now = env.clock.timestamp_ns()
    env.client.add_fill_reports(
        venue_order_id,
        [
            FillReport(
                account_id=env.account_id,
                instrument_id=BTCUSD_SIM.id,
                client_order_id=ClientOrderId("O-OTHER-OWNER"),
                venue_order_id=venue_order_id,
                trade_id=TradeId("T-CONFLICT-1"),
                order_side=OrderSide.BUY,
                last_qty=BTCUSD_SIM.make_qty(Decimal("1.000")),
                last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
                commission=Money(0, USD),
                liquidity_side=LiquiditySide.TAKER,
                report_id=UUID4(),
                ts_event=ts_now,
                ts_init=ts_now,
            ),
        ],
    )

    # Arrange - a scope discrepancy the pass must repair once the fill is recognized
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "1.000", PositionId("P-HEAL-1"))
    env.add_report(BTCUSD_SIM, PositionSide.LONG, "2.000", avg_px_open=Decimal("1.0"))

    # Act - two passes, since the skipped fill is missing again on every one of them
    await env.engine._check_positions_consistency()
    await env.engine._check_positions_consistency()

    # Assert - the deficit was repaired once, not re-queried pass after pass
    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert repairs[0].side == OrderSide.BUY
    assert repairs[0].quantity == BTCUSD_SIM.make_qty(Decimal("1.000"))
    assert sum(
        position.signed_decimal_qty() for position in env.open_positions(BTCUSD_SIM)
    ) == Decimal("2.000")
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


# -- C5: shutdown during fill healing ------------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_during_fill_healing_aborts_before_the_repair(env):
    """
    Shutdown beginning while the healing fill query is in flight leaves the fill
    unapplied, so the pass aborts before repairing rather than spending a retry.
    """
    # Arrange - a venue fill the healing query returns for the scope's deficit
    ts_now = env.clock.timestamp_ns()
    venue_order_id = VenueOrderId("V-SHUTDOWN-1")
    env.client.add_fill_reports(
        venue_order_id,
        [
            FillReport(
                account_id=env.account_id,
                instrument_id=BTCUSD_SIM.id,
                client_order_id=ClientOrderId("O-SHUTDOWN-1"),
                venue_order_id=venue_order_id,
                trade_id=TradeId("T-SHUTDOWN-1"),
                order_side=OrderSide.BUY,
                last_qty=BTCUSD_SIM.make_qty(Decimal("1.000")),
                last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
                commission=Money(0, USD),
                liquidity_side=LiquiditySide.TAKER,
                report_id=UUID4(),
                ts_event=ts_now,
                ts_init=ts_now,
            ),
        ],
    )
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "1.000", PositionId("P-SHUTDOWN-1"))
    env.add_report(BTCUSD_SIM, PositionSide.LONG, "2.000", avg_px_open=Decimal("1.0"))

    generate_fill_reports = env.client.generate_fill_reports

    async def stop_during_fill_query(command):
        env.engine._is_shutting_down = True
        return await generate_fill_reports(command)

    env.client.generate_fill_reports = stop_during_fill_query

    # Act
    await env.engine._check_positions_consistency()

    # Assert
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert env.residuals() == {}


# -- L1: a deferral is no verdict ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_deferred_scope_writes_no_residual_when_none_stands(env):
    """
    An actively trading scope defers on its watermark, which is no verdict: nothing is
    repaired, the pass does not fail, and the document holds no entry for the scope.
    """
    # Arrange - the venue snapshot matches the cache when the query starts
    standing = env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-ACTIVE-1"))
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "1000", avg_px_open=Decimal("1.0"))

    key = (AUDUSD_SIM.id, env.account_id)
    original = env.client.generate_position_status_reports

    async def fill_during_query(command):
        reports = await original(command)
        # The strategy's own fill lands while the venue query is in flight
        env.add_position(AUDUSD_SIM, OrderSide.BUY, "500", PositionId("P-ACTIVE-2"))
        env.engine._position_local_activity_ns[key] = env.clock.timestamp_ns()
        return reports

    env.client.generate_position_status_reports = fill_during_query

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert
    assert converged
    assert env.residuals() == {}
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert env.cache.position(standing.id).quantity == Quantity.from_int(1000)
    assert env.cache.position(PositionId("P-ACTIVE-2")).quantity == Quantity.from_int(500)


@pytest.mark.asyncio
async def test_deferred_scope_carries_forward_a_standing_residual(env):
    """
    A deferral erases nothing: the standing entry of the previous document survives the
    pass unchanged rather than being replaced by the deferral reason.
    """
    # Arrange - a first pass leaves a standing residual for the scope
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-STANDING-1"))
    env.engine.generate_missing_orders = False
    await env.engine._check_positions_consistency()

    scope_key = f"{AUDUSD_SIM.id}|{env.account_id}"
    entry = env.residuals()[scope_key]
    assert entry["reason"] == "`generate_missing_orders` disabled"

    key = (AUDUSD_SIM.id, env.account_id)
    original = env.client.generate_position_status_reports

    async def bump_then_report(command):
        reports = await original(command)
        env.engine._position_local_activity_ns[key] = env.clock.timestamp_ns()
        return reports

    env.client.generate_position_status_reports = bump_then_report

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert - the standing entry stands, reason and timestamp untouched
    assert converged
    assert env.residuals() == {scope_key: entry}
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}


# -- L2: a repair binds to a cache position, never to the venue-ID label -------------------------


def _mixed_id_space_scope(env) -> tuple[Position, Position, Position]:
    # The measured cache: LONG exposure held under virtual IDs only, nothing under the
    # canonical venue ID label
    external = StrategyId("EXTERNAL")
    oldest_long = env.add_position(
        BTCUSD_SIM,
        OrderSide.BUY,
        "0.006",
        PositionId("P-VIRT-1"),
        ts_opened=1_000,
        strategy_id=external,
    )
    newer_long = env.add_position(
        BTCUSD_SIM,
        OrderSide.BUY,
        "0.006",
        PositionId("P-VIRT-2"),
        ts_opened=2_000,
        strategy_id=external,
    )
    short = env.add_position(
        BTCUSD_SIM,
        OrderSide.SELL,
        "0.006",
        PositionId("P-VIRT-3"),
        ts_opened=3_000,
        strategy_id=external,
    )
    return oldest_long, newer_long, short


def test_inbound_hedge_report_trims_the_oldest_target_of_the_reported_side(env):
    """
    An ID-bearing report whose label holds nothing trims the reported side's oldest
    cache position rather than fabricating exposure under the label.
    """
    # Arrange
    oldest_long, newer_long, short = _mixed_id_space_scope(env)
    venue_long_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    report = env.add_report(
        BTCUSD_SIM,
        PositionSide.LONG,
        "0.006",
        venue_position_id=venue_long_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    result = env.engine._reconcile_position_report(report)

    # Assert - one bound reduce-only trim, and nothing under the label
    assert result
    assert env.cache.position(venue_long_id) is None

    repairs = env.reconciliation_orders()
    assert len(repairs) == 1
    assert repairs[0].side == OrderSide.SELL
    assert repairs[0].quantity == BTCUSD_SIM.make_qty(Decimal("0.006"))
    assert repairs[0].is_reduce_only
    assert env.cache.position_id(repairs[0].client_order_id) == oldest_long.id

    assert env.cache.position(oldest_long.id).is_closed
    assert env.cache.position(newer_long.id).quantity == BTCUSD_SIM.make_qty(Decimal("0.006"))
    assert env.cache.position(short.id).side == PositionSide.SHORT
    assert env.cache.position(short.id).quantity == BTCUSD_SIM.make_qty(Decimal("0.006"))
    assert env.residuals() is None


@pytest.mark.asyncio
async def test_pass_converges_a_mixed_id_space_scope_without_minting_the_label(env):
    """
    The complete snapshot converges the measured scope with bound trims only: no
    position is minted under a label holding nothing.
    """
    # Arrange
    oldest_long, newer_long, short = _mixed_id_space_scope(env)
    venue_long_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    venue_short_id = PositionId(f"{BTCUSD_SIM.id}-SHORT")
    env.add_report(
        BTCUSD_SIM,
        PositionSide.LONG,
        "0.006",
        venue_position_id=venue_long_id,
        avg_px_open=Decimal("1.0"),
    )
    env.add_report(
        BTCUSD_SIM,
        PositionSide.FLAT,
        "0",
        venue_position_id=venue_short_id,
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert - exactly one LONG 0.006 remains, held under its own cache ID
    assert [
        (p.id.value, p.side, p.quantity.as_decimal()) for p in env.open_positions(BTCUSD_SIM)
    ] == [(newer_long.id.value, PositionSide.LONG, Decimal("0.006"))]
    assert env.cache.position(venue_long_id) is None
    assert env.cache.position(venue_short_id) is None

    assert sorted(
        (
            str(env.cache.position_id(o.client_order_id)),
            o.side,
            o.quantity.as_decimal(),
            o.is_reduce_only,
        )
        for o in env.reconciliation_orders()
    ) == [
        (oldest_long.id.value, OrderSide.SELL, Decimal("0.006"), True),
        (short.id.value, OrderSide.BUY, Decimal("0.006"), True),
    ]
    assert env.residuals() == {}
    assert dict(env.engine._position_recon_retries) == {}


# -- F-B: reconciliation's own fills are never its own deferral evidence -------------------------


def _arm_activity_threshold(env, threshold_ms: int = 5_000) -> None:
    # The measured production threshold, which the harness otherwise disables
    env.engine.position_check_threshold_ms = threshold_ms
    env.engine._position_check_threshold_ns = millis_to_nanos(threshold_ms)


def _apply_fill_through_engine(
    env,
    instrument,
    order_side: OrderSide,
    quantity: str,
    position_id: PositionId,
    *,
    reconciliation: bool,
) -> OrderFilled:
    # Applies a fill on the engine's own event path so the position watermark is stamped
    # exactly as production stamps it, as reconciliation output or as a venue event.
    client_order_id = ClientOrderId(f"O-SEED-APPLIED-{next(env._order_ids)}")
    order = TestExecStubs.limit_order(
        instrument=instrument,
        order_side=order_side,
        quantity=instrument.make_qty(Decimal(quantity)),
        price=instrument.make_price(Decimal("1.0")),
        client_order_id=client_order_id,
    )
    env.cache.add_order(order, position_id=position_id)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(TestEventStubs.order_accepted(order, account_id=env.account_id))
    env.cache.update_order(order)

    ts_now = env.clock.timestamp_ns()
    fill = OrderFilled(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=instrument.id,
        client_order_id=client_order_id,
        venue_order_id=order.venue_order_id,
        account_id=env.account_id,
        trade_id=TradeId(client_order_id.value.replace("O-SEED-", "T-")),
        position_id=position_id,
        order_side=order_side,
        order_type=order.order_type,
        last_qty=instrument.make_qty(Decimal(quantity)),
        last_px=instrument.make_price(Decimal("1.0")),
        currency=instrument.quote_currency,
        commission=Money(0, instrument.quote_currency),
        liquidity_side=LiquiditySide.TAKER,
        event_id=UUID4(),
        ts_event=ts_now,
        ts_init=ts_now,
        reconciliation=reconciliation,
    )
    env.engine._handle_event_with_tracking(fill)
    return fill


@pytest.mark.asyncio
async def test_reconciliation_fill_does_not_defer_the_next_pass(env):
    """
    A fill generated by reconciliation is reconciliation's own output, so a pass inside
    the activity threshold judges the scope instead of deferring on it.
    """
    # Arrange - a reconciliation fill mints a position the venue does not hold
    _arm_activity_threshold(env)
    _apply_fill_through_engine(
        env,
        AUDUSD_SIM,
        OrderSide.BUY,
        "1000",
        PositionId("P-RECON-FILL-1"),
        reconciliation=True,
    )

    # Act - the pass runs well inside the threshold window
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert - judged, not deferred: the phantom is gone on this pass
    assert converged
    assert env.open_positions(AUDUSD_SIM) == []
    assert env.residuals() == {}
    assert (AUDUSD_SIM.id, env.account_id) not in env.engine._position_local_activity_ns


@pytest.mark.asyncio
async def test_venue_fill_still_defers_the_next_pass(env):
    """
    A fill which is not reconciliation's own output is local activity, so a pass inside
    the activity threshold still defers: nothing repaired, nothing published.
    """
    # Arrange - the same shape, with the fill arriving from the venue
    _arm_activity_threshold(env)
    _apply_fill_through_engine(
        env,
        AUDUSD_SIM,
        OrderSide.BUY,
        "1000",
        PositionId("P-VENUE-FILL-1"),
        reconciliation=False,
    )

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert - deferred: the position stands and no residual was published
    assert converged
    positions = env.open_positions(AUDUSD_SIM)
    assert [p.id for p in positions] == [PositionId("P-VENUE-FILL-1")]
    assert positions[0].quantity == Quantity.from_int(1000)
    assert env.reconciliation_orders() == []
    assert env.residuals() == {}
    assert dict(env.engine._position_recon_retries) == {}
    assert (AUDUSD_SIM.id, env.account_id) in env.engine._position_local_activity_ns


@pytest.mark.asyncio
async def test_startup_inferred_fill_does_not_defer_the_phantom_short(env):
    """
    The measured startup shape: an inferred reconciliation SELL opens a SHORT under the
    venue's LONG label while the venue holds LONG 0.006 there. The next pass inside the
    threshold must converge the scope rather than defer on reconciliation's own fill and
    leave the phantom standing.
    """
    # Arrange - the cache is flat until reconciliation mints the phantom
    _arm_activity_threshold(env)
    venue_long_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    venue_short_id = PositionId(f"{BTCUSD_SIM.id}-SHORT")
    assert env.open_positions(BTCUSD_SIM) == []

    _apply_fill_through_engine(
        env,
        BTCUSD_SIM,
        OrderSide.SELL,
        "0.006",
        venue_long_id,
        reconciliation=True,
    )
    assert env.cache.position(venue_long_id).side == PositionSide.SHORT

    env.add_report(
        BTCUSD_SIM,
        PositionSide.LONG,
        "0.006",
        venue_position_id=venue_long_id,
        avg_px_open=Decimal("1.0"),
    )
    env.add_report(BTCUSD_SIM, PositionSide.FLAT, "0", venue_position_id=venue_short_id)

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert - the cache holds the venue's LONG and nothing else
    assert converged
    assert [
        (p.id.value, p.side, p.quantity.as_decimal()) for p in env.open_positions(BTCUSD_SIM)
    ] == [(venue_long_id.value, PositionSide.LONG, Decimal("0.006"))]

    repairs = sorted(
        (
            str(env.cache.position_id(o.client_order_id)),
            o.side,
            o.quantity.as_decimal(),
            o.is_reduce_only,
        )
        for o in env.reconciliation_orders()
    )
    assert (venue_long_id.value, OrderSide.BUY, Decimal("0.006"), True) in repairs
    assert env.residuals() == {}
    assert dict(env.engine._position_recon_retries) == {}


@pytest.mark.asyncio
async def test_healed_fills_do_not_defer_the_re_query_pass(env):
    """
    Healing applies missing venue fills and holds the scope for a re-query, so the next
    pass inside the threshold must judge the scope rather than defer on the healing's
    own fills.
    """
    # Arrange - a cached order the venue fill belongs to
    _arm_activity_threshold(env)
    venue_order_id = VenueOrderId("V-HEAL-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("1.000")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-HEAL-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)

    ts_now = env.clock.timestamp_ns()
    env.client.add_fill_reports(
        venue_order_id,
        [
            FillReport(
                account_id=env.account_id,
                instrument_id=BTCUSD_SIM.id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                trade_id=TradeId("T-HEAL-1"),
                order_side=OrderSide.BUY,
                last_qty=BTCUSD_SIM.make_qty(Decimal("1.000")),
                last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
                commission=Money(0, USD),
                liquidity_side=LiquiditySide.TAKER,
                report_id=UUID4(),
                ts_event=ts_now,
                ts_init=ts_now,
            ),
        ],
    )

    # Arrange - the healed fill closes one unit of a two-unit deficit
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "1.000", PositionId("P-HEAL-1"))
    env.add_report(BTCUSD_SIM, PositionSide.LONG, "3.000", avg_px_open=Decimal("1.0"))

    # Act - the first pass heals and holds the scope, the second must repair the rest
    healed = await env.engine._run_position_convergence_pass("position check")
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert
    assert not healed
    assert converged
    assert sum(
        position.signed_decimal_qty() for position in env.open_positions(BTCUSD_SIM)
    ) == Decimal("3.000")
    assert env.residuals() == {}
    assert dict(env.engine._position_recon_retries) == {}


# -- F-A: a shutdown-raced pass has no verdict, and publishes none --------------------------------


async def _raise_query_error(command):
    raise RuntimeError("Request canceled: Adapter disconnecting or shutting down")


@pytest.mark.asyncio
async def test_failed_query_from_a_stopped_client_publishes_nothing(env):
    """
    A query failing because the client is stopping is that client's own lifecycle, not
    an observation of the venue: the pass has no verdict, so it publishes nothing and
    the previous document stands byte for byte.
    """
    # Arrange - a converged pass leaves an empty document standing
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-STOPPING-1"))
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "1000", avg_px_open=Decimal("1.0"))
    assert await env.engine._run_position_convergence_pass("position check")
    before = env.cache.get(RESIDUALS_KEY)
    assert json.loads(before.decode("utf-8")) == {}
    retries_before = dict(env.engine._position_recon_retries)

    # Arrange - teardown begins: the client stops, then the pass fires again
    env.client.stop()
    assert env.client.is_stopped
    env.client.generate_position_status_reports = _raise_query_error

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert
    assert converged
    assert env.cache.get(RESIDUALS_KEY) == before
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == retries_before
    assert env.cache.position(PositionId("P-STOPPING-1")).quantity == Quantity.from_int(1000)


@pytest.mark.asyncio
async def test_failed_query_from_a_stopped_client_carries_a_standing_residual(env):
    """
    The no-verdict pass erases nothing either: a standing entry survives the pass with
    its reason and timestamp untouched.
    """
    # Arrange - a first pass leaves a standing residual for the scope
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-STOPPING-2"))
    env.engine.generate_missing_orders = False
    assert not await env.engine._run_position_convergence_pass("position check")

    scope_key = f"{AUDUSD_SIM.id}|{env.account_id}"
    entry = env.residuals()[scope_key]
    assert entry["reason"] == "`generate_missing_orders` disabled"
    before = env.cache.get(RESIDUALS_KEY)

    # Arrange - repair is enabled again, so only a no-verdict pass leaves the entry alone
    env.engine.generate_missing_orders = True
    env.client.stop()
    env.client.generate_position_status_reports = _raise_query_error

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert
    assert converged
    assert env.cache.get(RESIDUALS_KEY) == before
    assert env.residuals() == {scope_key: entry}
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}


@pytest.mark.asyncio
async def test_failed_query_from_a_running_client_still_reports_the_scope(env):
    """
    A query failing while the client is still running is a real observability failure:
    the scope is incomplete, its residual is published and the pass reports failure.
    """
    # Arrange
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-RUNNING-1"))
    assert env.client.is_running
    env.client.generate_position_status_reports = _raise_query_error

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert
    assert not converged
    decoded = RESIDUAL_DECODER.decode(env.cache.get(RESIDUALS_KEY))
    assert sorted(decoded) == [
        f"{AUDUSD_SIM.id}|{env.account_id}",
        f"{BTCUSD_SIM.id}|{env.account_id}",
    ]
    assert [entry.reason for entry in decoded.values()] == [
        f"position status query failed for {env.client.id}",
    ] * 2
    assert env.reconciliation_orders() == []
    assert env.cache.position(PositionId("P-RUNNING-1")).quantity == Quantity.from_int(1000)
    assert dict(env.engine._position_recon_retries) == {}


@pytest.mark.asyncio
async def test_no_verdict_pass_keeps_a_residual_it_cannot_derive_a_scope_for(env):
    """
    A client stopping before it resolved its account has no derivable scopes at all, so
    the pass cannot say which scopes to leave alone: it publishes nothing and reports
    failure, leaving the standing entry exactly where it was.
    """
    # Arrange - the venue's only client stops before its account is known
    env.engine.deregister_client(env.client)
    unresolved = UnresolvedAccountExecutionClient(
        loop=env.loop,
        client_id=ClientId(SIM.value),
        venue=SIM,
        msgbus=env.msgbus,
        cache=env.cache,
        clock=env.clock,
    )
    env.engine.register_client(unresolved)
    unresolved.start()
    unresolved.stop()
    assert unresolved.account_id is None

    scope_key = f"{AUDUSD_SIM.id}|{env.account_id}"
    standing = {scope_key: {"reason": "prior pass", "ts": 1}}
    assert env.engine._write_position_residuals(standing)
    before = env.cache.get(RESIDUALS_KEY)

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert
    assert not converged
    assert env.cache.get(RESIDUALS_KEY) == before
    assert env.residuals() == standing


@pytest.mark.asyncio
async def test_no_verdict_pass_keeps_the_state_of_a_scope_it_cannot_load(env):
    """
    A scope the previous document still stands on is the stopping client's coverage even
    when this pass cannot derive it from a loaded instrument: both its entry and its
    retry counter survive the pass untouched.
    """
    # Arrange - a scope carried from an earlier session, its instrument no longer loaded
    unloaded = TestInstrumentProvider.default_fx_ccy("GBP/USD")
    assert unloaded.id not in {instrument.id for instrument in env.cache.instruments()}

    scope_key = f"{unloaded.id}|{env.account_id}"
    standing = {scope_key: {"reason": "prior pass", "ts": 1}}
    assert env.engine._write_position_residuals(standing)
    before = env.cache.get(RESIDUALS_KEY)
    env.engine._position_recon_retries[(unloaded.id, env.account_id)] = 1

    # Arrange - teardown begins
    env.client.stop()
    env.client.generate_position_status_reports = _raise_query_error

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert
    assert converged
    assert env.cache.get(RESIDUALS_KEY) == before
    assert dict(env.engine._position_recon_retries) == {(unloaded.id, env.account_id): 1}


@pytest.mark.asyncio
async def test_no_verdict_pass_keeps_the_state_of_a_client_with_nothing_loaded(event_loop):
    """
    A stopping client routes nothing the cache still holds, so its coverage is only
    derivable from the state this pass carries: the scope keeps its entry and its retry
    counter rather than being pruned as an observability failure.
    """
    # Arrange - the venue's client routes no loaded instrument at all
    env = ConvergenceEnv(event_loop, load_instruments=False)
    assert env.cache.instruments() == []

    unloaded = TestInstrumentProvider.default_fx_ccy("GBP/USD")
    scope_key = f"{unloaded.id}|{env.account_id}"
    standing = {scope_key: {"reason": "prior pass", "ts": 1}}
    assert env.engine._write_position_residuals(standing)
    before = env.cache.get(RESIDUALS_KEY)
    env.engine._position_recon_retries[(unloaded.id, env.account_id)] = 1

    # Arrange - teardown begins
    env.client.stop()
    env.client.generate_position_status_reports = _raise_query_error

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert
    assert converged
    assert env.cache.get(RESIDUALS_KEY) == before
    assert env.residuals() == standing
    assert dict(env.engine._position_recon_retries) == {(unloaded.id, env.account_id): 1}


@pytest.mark.asyncio
async def test_no_verdict_pass_keeps_a_residual_written_while_it_was_judging(env):
    """
    A residual written for another scope while this pass awaited a venue query is state
    the pass never judged, so publication must carry it rather than clobber it.
    """
    # Arrange - a second venue whose client stops, giving its scope no verdict
    other_venue = Venue("OTHER")
    other_instrument = TestInstrumentProvider.default_fx_ccy("GBP/USD", venue=other_venue)
    env.cache.add_instrument(other_instrument)
    other = MockLiveExecutionClient(
        loop=env.loop,
        client_id=ClientId(other_venue.value),
        venue=other_venue,
        account_type=AccountType.CASH,
        base_currency=USD,
        instrument_provider=InstrumentProvider(),
        msgbus=env.msgbus,
        cache=env.cache,
        clock=env.clock,
        oms_type=env.oms_type,
    )
    env.engine.register_client(other)
    other.start()
    other.stop()
    other.generate_position_status_reports = _raise_query_error

    # Arrange - a discrepancy on the running venue, so the pass awaits a fill query
    env.add_position(AUDUSD_SIM, OrderSide.BUY, "1000", PositionId("P-RACE-WRITE-1"))
    env.add_report(AUDUSD_SIM, PositionSide.LONG, "2000", avg_px_open=Decimal("1.0"))
    assert env.residuals() is None

    other_scope_key = f"{other_instrument.id}|{other.account_id}"
    generate_fill_reports = env.client.generate_fill_reports

    async def write_residual_then_report(command):
        # The inbound report path records a residual for the other venue mid-pass
        env.engine._write_position_scope_residual(
            other_instrument.id,
            other.account_id,
            "inbound repair raised KeyError",
        )
        return await generate_fill_reports(command)

    env.client.generate_fill_reports = write_residual_then_report

    # Act
    converged = await env.engine._run_position_convergence_pass("position check")

    # Assert - the mid-pass entry survived publication
    assert converged
    residuals = env.residuals()
    assert other_scope_key in residuals
    assert residuals[other_scope_key]["reason"] == "inbound repair raised KeyError"
    assert f"{AUDUSD_SIM.id}|{env.account_id}" not in residuals
    assert sum(
        position.signed_decimal_qty() for position in env.open_positions(AUDUSD_SIM)
    ) == Decimal(2000)


# -- F-C: an open position under the label is the label's authority -------------------------------


def _resting_exit_order(env, position_id: PositionId) -> object:
    # The leg's own reduce-only exit, which a hedge venue keeps bound to the side label
    exit_order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.SELL,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("2.0")),
        client_order_id=ClientOrderId("O-SEED-EXIT-LONG-1"),
        reduce_only=True,
    )
    env.cache.add_order(exit_order, position_id=position_id)
    exit_order.apply(TestEventStubs.order_submitted(exit_order, account_id=env.account_id))
    env.cache.update_order(exit_order)
    exit_order.apply(
        TestEventStubs.order_accepted(
            exit_order,
            account_id=env.account_id,
            venue_order_id=VenueOrderId("V-EXIT-LONG-1"),
        ),
    )
    env.cache.update_order(exit_order)
    return exit_order


@pytest.mark.asyncio
async def test_deficit_grows_the_open_position_the_label_already_holds(env):
    """
    A hedge label holding an open cache position is repaired toward the venue row even
    while the leg's own resting exit is bound to that ID, which is what a hedge venue
    reports on every pass.
    """
    # Arrange - the cache holds half of what the venue reports under the same label
    long_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    position = env.add_position(BTCUSD_SIM, OrderSide.BUY, "0.003", long_id)
    exit_order = _resting_exit_order(env, long_id)
    env.add_report(
        BTCUSD_SIM,
        PositionSide.LONG,
        "0.006",
        venue_position_id=long_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert - the repair grew the position the label already held, capped at the row
    assert not exit_order.is_closed
    repaired = env.cache.position(long_id)
    assert repaired is not None
    assert repaired.quantity.as_decimal() == Decimal("0.006")
    assert repaired.strategy_id == position.strategy_id

    repairs = env.reconciliation_orders()
    assert [(o.side, o.quantity.as_decimal(), o.is_reduce_only) for o in repairs] == [
        (OrderSide.BUY, Decimal("0.003"), False),
    ]
    assert env.cache.position_id(repairs[0].client_order_id) == long_id
    assert env.residuals() == {}
    assert dict(env.engine._position_recon_retries) == {}

    # Act - the same snapshot on the next pass must leave the converged scope alone
    await env.engine._check_positions_consistency()

    # Assert
    assert len(env.reconciliation_orders()) == 1
    assert env.cache.position(long_id).quantity.as_decimal() == Decimal("0.006")
    assert env.residuals() == {}
    assert dict(env.engine._position_recon_retries) == {}


@pytest.mark.asyncio
async def test_deficit_under_an_empty_label_with_a_live_order_is_still_refused(env):
    """
    A label holding no open position must not be minted under while a live order is
    bound to that ID, because that order's own fill would resolve onto it.
    """
    # Arrange - an accepted order holds the label the venue reports, with nothing open
    long_id = PositionId(f"{BTCUSD_SIM.id}-LONG")
    resting = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.006")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-RESTING-1"),
    )
    env.cache.add_order(resting, position_id=long_id)
    resting.apply(TestEventStubs.order_submitted(resting, account_id=env.account_id))
    env.cache.update_order(resting)
    resting.apply(
        TestEventStubs.order_accepted(
            resting,
            account_id=env.account_id,
            venue_order_id=VenueOrderId("V-RESTING-1"),
        ),
    )
    env.cache.update_order(resting)
    env.add_report(
        BTCUSD_SIM,
        PositionSide.LONG,
        "0.006",
        venue_position_id=long_id,
        avg_px_open=Decimal("1.0"),
    )

    # Act
    await env.engine._check_positions_consistency()

    # Assert - nothing was minted under the contested label
    assert not resting.is_closed
    assert env.cache.position(long_id) is None
    assert env.open_positions(BTCUSD_SIM) == []
    assert env.reconciliation_orders() == []
    assert dict(env.engine._position_recon_retries) == {}
    assert (
        env.residuals()[f"{BTCUSD_SIM.id}|{env.account_id}"]["reason"]
        == "no applicable repair for this discrepancy"
    )


# -- F-D: fill healing judges a fill once, not once per pass --------------------------------------


def _venue_fill_report(
    env,
    venue_order_id: VenueOrderId,
    trade_id: TradeId,
    quantity: str = "0.003",
) -> FillReport:
    ts_now = env.clock.timestamp_ns()
    return FillReport(
        account_id=env.account_id,
        instrument_id=BTCUSD_SIM.id,
        client_order_id=None,  # A foreign actor's order carries no local ID
        venue_order_id=venue_order_id,
        trade_id=trade_id,
        order_side=OrderSide.BUY,
        last_qty=BTCUSD_SIM.make_qty(Decimal(quantity)),
        last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
        commission=Money(0, USD),
        liquidity_side=LiquiditySide.TAKER,
        report_id=UUID4(),
        ts_event=ts_now,
        ts_init=ts_now,
    )


async def _healing_pass(env) -> tuple[list[FillReport], int]:
    missing, had_errors = await env.engine._query_and_find_missing_fills(
        BTCUSD_SIM.id,
        env.engine._clients.values(),
    )
    assert not had_errors
    applied = await env.engine._reconcile_missing_fills(missing, BTCUSD_SIM.id)
    return missing, applied


@pytest.mark.asyncio
async def test_unknown_order_fill_is_retried_and_applies_when_its_order_arrives(env):
    """
    A fill whose order is not cached is retried by every healing pass, and applies once
    the order reaches the cache, which is the delivery gap healing itself.
    """
    # Arrange
    venue_order_id = VenueOrderId("V-FOREIGN-1")
    trade_id = TradeId("T-FOREIGN-1")
    env.client.add_fill_reports(
        venue_order_id,
        [_venue_fill_report(env, venue_order_id, trade_id)],
    )

    attempts: list[TradeId] = []
    reconcile_single = env.engine._reconcile_fill_report_single

    def counting_single(report):
        attempts.append(report.trade_id)
        return reconcile_single(report)

    env.engine._reconcile_fill_report_single = counting_single

    # Act - the first pass defers the fill
    missing, applied = await _healing_pass(env)

    # Assert - the fill deferred, and nothing was consumed by the deferral
    assert [fill.trade_id for fill in missing] == [trade_id]
    assert applied == 0
    assert attempts == [trade_id]

    # Act - the second pass over the same reports
    missing, applied = await _healing_pass(env)

    # Assert - the fill is retried, still without consuming anything
    assert [fill.trade_id for fill in missing] == [trade_id]
    assert applied == 0
    assert attempts == [trade_id, trade_id]

    # Arrange - the order the fill belongs to reaches the cache
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-FOREIGN-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)

    # Act - the third pass
    missing, applied = await _healing_pass(env)

    # Assert - the retried fill applies to the order which arrived late
    assert [fill.trade_id for fill in missing] == [trade_id]
    assert applied == 1
    assert attempts == [trade_id, trade_id, trade_id]
    assert trade_id in env.engine._recent_fills_cache
    assert order.filled_qty.as_decimal() == Decimal("0.003")


@pytest.mark.asyncio
async def test_overfill_rejected_fill_is_not_judged_again_on_the_next_pass(env):
    """
    A venue fill an earlier fill already accounts for is rejected once as an overfill,
    and later passes skip it rather than re-fetching the same verdict forever.
    """
    # Arrange - an order already filled to its whole quantity under another trade ID
    venue_order_id = VenueOrderId("V-OVERFILL-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-OVERFILL-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_filled(
            order,
            instrument=BTCUSD_SIM,
            account_id=env.account_id,
            trade_id=TradeId("T-INFERRED-1"),
            position_id=PositionId("P-OVERFILL-1"),
            last_qty=BTCUSD_SIM.make_qty(Decimal("0.003")),
            last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
        ),
    )
    env.cache.update_order(order)

    trade_id = TradeId("T-VENUE-1")
    env.client.add_fill_reports(
        venue_order_id,
        [_venue_fill_report(env, venue_order_id, trade_id)],
    )

    judged: list[TradeId] = []
    reconcile_fill = env.engine._reconcile_fill_report

    def counting_fill(order_, report, instrument):
        judged.append(report.trade_id)
        return reconcile_fill(order_, report, instrument)

    env.engine._reconcile_fill_report = counting_fill

    # Act - the first pass judges the fill and rejects it as an overfill
    missing, applied = await _healing_pass(env)

    # Assert - the verdict is terminal, so nothing is applied
    assert [fill.trade_id for fill in missing] == [trade_id]
    assert applied == 0
    assert judged == [trade_id]
    assert order.filled_qty.as_decimal() == Decimal("0.003")

    # Act - the second pass over the same reports
    missing, applied = await _healing_pass(env)

    # Assert - the fill is no longer missing and is never judged a second time
    assert missing == []
    assert applied == 0
    assert judged == [trade_id]
    assert order.filled_qty.as_decimal() == Decimal("0.003")

    # Assert - the rejection is remembered against its order
    assert set(env.engine._overfill_rejected_trade_ids[order.client_order_id]) == {trade_id}

    # Act - clearing the order's retry bookkeeping is not a change to what the rejection
    # was judged against, so the verdict survives it.
    env.engine._clear_recon_tracking(order.client_order_id)

    # Assert
    assert set(env.engine._overfill_rejected_trade_ids[order.client_order_id]) == {trade_id}


@pytest.mark.asyncio
async def test_overfill_rejection_is_reconsidered_when_the_order_quantity_changes(env):
    """
    A rejection is terminal only while the order it was judged against stands, so an
    order quantity raised afterwards makes the same fill applicable again.
    """
    # Arrange - a partially filled order a 0.002 fill would overfill
    venue_order_id = VenueOrderId("V-UPDATED-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-UPDATED-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_filled(
            order,
            instrument=BTCUSD_SIM,
            account_id=env.account_id,
            trade_id=TradeId("T-PARTIAL-1"),
            position_id=PositionId("P-UPDATED-1"),
            last_qty=BTCUSD_SIM.make_qty(Decimal("0.002")),
            last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
        ),
    )
    env.cache.update_order(order)

    trade_id = TradeId("T-VENUE-UPDATED-1")
    env.client.add_fill_reports(
        venue_order_id,
        [_venue_fill_report(env, venue_order_id, trade_id, quantity="0.002")],
    )

    # Act - the first pass rejects the fill against the order's current quantity
    missing, applied = await _healing_pass(env)

    # Assert
    assert [fill.trade_id for fill in missing] == [trade_id]
    assert applied == 0
    assert order.filled_qty.as_decimal() == Decimal("0.002")

    # Act - the venue raises the order quantity, which the rejection was judged against
    env.engine._handle_event_with_tracking(
        TestEventStubs.order_updated(order, quantity=BTCUSD_SIM.make_qty(Decimal("0.004"))),
    )
    missing, applied = await _healing_pass(env)

    # Assert - the fill is missing again and applies within the raised quantity
    assert order.quantity.as_decimal() == Decimal("0.004")
    assert [fill.trade_id for fill in missing] == [trade_id]
    assert applied == 1
    assert order.filled_qty.as_decimal() == Decimal("0.004")
    assert env.engine._overfill_rejected_trade_ids == {}


@pytest.mark.asyncio
async def test_duplicate_fill_reports_in_one_response_are_judged_once(env):
    """
    A venue returning the same fill twice in one response must not be judged twice: the
    verdict recorded by the first is what the second is skipped on.
    """
    # Arrange - an order already filled to its whole quantity under another trade ID
    venue_order_id = VenueOrderId("V-DUPLICATE-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-DUPLICATE-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_filled(
            order,
            instrument=BTCUSD_SIM,
            account_id=env.account_id,
            trade_id=TradeId("T-INFERRED-DUPLICATE"),
            position_id=PositionId("P-DUPLICATE-1"),
            last_qty=BTCUSD_SIM.make_qty(Decimal("0.003")),
            last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
        ),
    )
    env.cache.update_order(order)

    trade_id = TradeId("T-VENUE-DUPLICATE-1")
    env.client.add_fill_reports(
        venue_order_id,
        [
            _venue_fill_report(env, venue_order_id, trade_id),
            _venue_fill_report(env, venue_order_id, trade_id),
        ],
    )

    judged: list[TradeId] = []
    reconcile_fill = env.engine._reconcile_fill_report

    def counting_fill(order_, report, instrument):
        judged.append(report.trade_id)
        return reconcile_fill(order_, report, instrument)

    env.engine._reconcile_fill_report = counting_fill

    # Act
    missing, applied = await _healing_pass(env)

    # Assert - both copies are missing, and only the first is judged
    assert [fill.trade_id for fill in missing] == [trade_id, trade_id]
    assert applied == 0
    assert judged == [trade_id]
    assert set(env.engine._overfill_rejected_trade_ids[order.client_order_id]) == {trade_id}


@pytest.mark.asyncio
async def test_healed_scope_is_summarized_apart_from_the_scopes_needing_action(env):
    """
    A scope held for re-observation after healing venue fills is expected operation, so
    the pass reports it apart from the scopes whose discrepancy still stands.
    """
    # Arrange - a cached order the venue fill belongs to, and a deficit it heals
    venue_order_id = VenueOrderId("V-SUMMARY-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("1.000")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-SUMMARY-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)

    ts_now = env.clock.timestamp_ns()
    env.client.add_fill_reports(
        venue_order_id,
        [
            FillReport(
                account_id=env.account_id,
                instrument_id=BTCUSD_SIM.id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                trade_id=TradeId("T-SUMMARY-1"),
                order_side=OrderSide.BUY,
                last_qty=BTCUSD_SIM.make_qty(Decimal("1.000")),
                last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
                commission=Money(0, USD),
                liquidity_side=LiquiditySide.TAKER,
                report_id=UUID4(),
                ts_event=ts_now,
                ts_init=ts_now,
            ),
        ],
    )
    env.add_position(BTCUSD_SIM, OrderSide.BUY, "1.000", PositionId("P-SUMMARY-1"))
    env.add_report(BTCUSD_SIM, PositionSide.LONG, "3.000", avg_px_open=Decimal("1.0"))

    # Act
    healed = await env.engine._run_position_convergence_pass("position check")

    # Assert - the scope is held for re-observation, on the healing reason
    scope_key = f"{BTCUSD_SIM.id}|{env.account_id}"
    assert not healed
    healed_reason = "applied missing venue fills, re-querying next pass"
    assert env.residuals()[scope_key]["reason"] == healed_reason

    # Assert - the summary keeps that scope apart from one which still needs action
    other_key = f"{AUDUSD_SIM.id}|{env.account_id}"
    residuals = {
        scope_key: env.residuals()[scope_key],
        other_key: {"reason": "no applicable repair for this discrepancy", "ts": 0},
    }
    healing, action_required = env.engine._unresolved_scope_summary(
        residuals,
        [(BTCUSD_SIM.id, env.account_id), (AUDUSD_SIM.id, env.account_id)],
    )
    assert healing == [scope_key]
    assert action_required == [other_key]


@pytest.mark.asyncio
async def test_overfill_rejections_do_not_outlive_the_fill_query_window(env):
    """
    A rejection is only remembered while the venue's fill query can still return that
    fill, so an entry older than the lookback window is pruned and judged afresh.
    """
    # Arrange - an order already filled to its whole quantity under another trade ID
    venue_order_id = VenueOrderId("V-PRUNE-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-PRUNE-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_filled(
            order,
            instrument=BTCUSD_SIM,
            account_id=env.account_id,
            trade_id=TradeId("T-INFERRED-PRUNE"),
            position_id=PositionId("P-PRUNE-1"),
            last_qty=BTCUSD_SIM.make_qty(Decimal("0.003")),
            last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
        ),
    )
    env.cache.update_order(order)

    trade_id = TradeId("T-VENUE-PRUNE-1")
    env.client.add_fill_reports(
        venue_order_id,
        [_venue_fill_report(env, venue_order_id, trade_id)],
    )

    judged: list[TradeId] = []
    reconcile_fill = env.engine._reconcile_fill_report

    def counting_fill(order_, report, instrument):
        judged.append(report.trade_id)
        return reconcile_fill(order_, report, instrument)

    env.engine._reconcile_fill_report = counting_fill

    # Act - the first pass records the rejection
    await _healing_pass(env)

    # Assert
    assert set(env.engine._overfill_rejected_trade_ids[order.client_order_id]) == {trade_id}

    # Act - the entry ages past the lookback window the fill query covers
    env.engine._overfill_rejected_trade_ids[order.client_order_id][trade_id] = 0
    env.engine._prune_overfill_rejected_fills()

    # Assert - nothing is retained, so the fill is judged afresh rather than suppressed
    assert env.engine._overfill_rejected_trade_ids == {}

    missing, applied = await _healing_pass(env)

    assert [fill.trade_id for fill in missing] == [trade_id]
    assert applied == 0
    assert judged == [trade_id, trade_id]
    assert set(env.engine._overfill_rejected_trade_ids[order.client_order_id]) == {trade_id}


def _fully_filled_order(env, venue_order_id: VenueOrderId, client_order_id: str, position_id: str):
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId(client_order_id),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_filled(
            order,
            instrument=BTCUSD_SIM,
            account_id=env.account_id,
            trade_id=TradeId(f"T-INFERRED-{client_order_id}"),
            position_id=PositionId(position_id),
            last_qty=BTCUSD_SIM.make_qty(Decimal("0.003")),
            last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
        ),
    )
    env.cache.update_order(order)
    return order


@pytest.mark.asyncio
async def test_recording_a_rejection_prunes_the_entries_past_the_query_window(env):
    """
    The periodic prune only runs while a reconciliation check is configured, so
    recording a verdict is what bounds the memory in every configuration.
    """
    # Arrange - one order whose venue fill is rejected as an overfill
    first_venue_order_id = VenueOrderId("V-BOUND-1")
    first = _fully_filled_order(env, first_venue_order_id, "O-SEED-BOUND-1", "P-BOUND-1")
    first_trade_id = TradeId("T-VENUE-BOUND-1")
    env.client.add_fill_reports(
        first_venue_order_id,
        [_venue_fill_report(env, first_venue_order_id, first_trade_id)],
    )

    # Act
    await _healing_pass(env)

    # Assert
    assert set(env.engine._overfill_rejected_trade_ids[first.client_order_id]) == {first_trade_id}

    # Arrange - that entry ages past the window while a second order's fill is rejected
    env.engine._overfill_rejected_trade_ids[first.client_order_id][first_trade_id] = 0
    second_venue_order_id = VenueOrderId("V-BOUND-2")
    second = _fully_filled_order(env, second_venue_order_id, "O-SEED-BOUND-2", "P-BOUND-2")
    second_trade_id = TradeId("T-VENUE-BOUND-2")
    env.client.add_fill_reports(
        second_venue_order_id,
        [_venue_fill_report(env, second_venue_order_id, second_trade_id)],
    )

    # Act
    await _healing_pass(env)

    # Assert - the aged entry is gone without the reconciliation task ever running
    assert env.engine._reconciliation_task is None
    assert set(env.engine._overfill_rejected_trade_ids) == {second.client_order_id}
    assert set(env.engine._overfill_rejected_trade_ids[second.client_order_id]) == {
        second_trade_id,
    }


@pytest.mark.asyncio
async def test_duplicate_applicable_fill_reports_count_one_application(env):
    """
    A venue returning the same applicable fill twice applies it once, so the pass counts
    one application while still judging the second copy against the applied one.
    """
    # Arrange - an open order the venue reports the same fill for twice
    venue_order_id = VenueOrderId("V-APPLICABLE-DUPLICATE-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-APPLICABLE-DUPLICATE-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)

    trade_id = TradeId("T-APPLICABLE-DUPLICATE-1")
    report = _venue_fill_report(env, venue_order_id, trade_id)
    env.client.add_fill_reports(venue_order_id, [report, report])

    judged: list[TradeId] = []
    reconcile_fill = env.engine._reconcile_fill_report

    def counting_fill(order_, fill_report, instrument):
        judged.append(fill_report.trade_id)
        return reconcile_fill(order_, fill_report, instrument)

    env.engine._reconcile_fill_report = counting_fill

    # Act
    missing, applied = await _healing_pass(env)

    # Assert - one application, and the duplicate still judged against the applied fill
    assert [fill.trade_id for fill in missing] == [trade_id, trade_id]
    assert applied == 1
    assert judged == [trade_id, trade_id]
    assert order.filled_qty.as_decimal() == Decimal("0.003")
    assert trade_id in env.engine._recent_fills_cache


@pytest.mark.asyncio
async def test_polling_an_unchanged_order_keeps_its_terminal_fill_verdict(env):
    """
    Querying an order's status clears its retry bookkeeping, which is not a change to
    what the rejection was judged against, so the terminal verdict survives it.
    """
    # Arrange - a partially filled open order a 0.002 fill would overfill
    venue_order_id = VenueOrderId("V-POLLED-1")
    order = TestExecStubs.limit_order(
        instrument=BTCUSD_SIM,
        order_side=OrderSide.BUY,
        quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
        price=BTCUSD_SIM.make_price(Decimal("1.0")),
        client_order_id=ClientOrderId("O-SEED-POLLED-1"),
    )
    env.cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order, account_id=env.account_id))
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_accepted(
            order,
            account_id=env.account_id,
            venue_order_id=venue_order_id,
        ),
    )
    env.cache.update_order(order)
    order.apply(
        TestEventStubs.order_filled(
            order,
            instrument=BTCUSD_SIM,
            account_id=env.account_id,
            trade_id=TradeId("T-PARTIAL-POLLED"),
            position_id=PositionId("P-POLLED-1"),
            last_qty=BTCUSD_SIM.make_qty(Decimal("0.002")),
            last_px=BTCUSD_SIM.make_price(Decimal("1.0")),
        ),
    )
    env.cache.update_order(order)

    trade_id = TradeId("T-VENUE-POLLED-1")
    env.client.add_fill_reports(
        venue_order_id,
        [_venue_fill_report(env, venue_order_id, trade_id, quantity="0.002")],
    )

    judged: list[TradeId] = []
    reconcile_fill = env.engine._reconcile_fill_report

    def counting_fill(order_, fill_report, instrument):
        judged.append(fill_report.trade_id)
        return reconcile_fill(order_, fill_report, instrument)

    env.engine._reconcile_fill_report = counting_fill

    # Act - the first pass rejects the fill and records the verdict
    await _healing_pass(env)

    # Assert
    assert judged == [trade_id]
    assert set(env.engine._overfill_rejected_trade_ids[order.client_order_id]) == {trade_id}

    # Act - the open order check polls the venue and finds the order unchanged
    ts_now = env.clock.timestamp_ns()
    env.engine._reconcile_order_reports(
        [
            OrderStatusReport(
                account_id=env.account_id,
                instrument_id=BTCUSD_SIM.id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                order_side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC,
                order_status=OrderStatus.PARTIALLY_FILLED,
                price=BTCUSD_SIM.make_price(Decimal("1.0")),
                quantity=BTCUSD_SIM.make_qty(Decimal("0.003")),
                filled_qty=BTCUSD_SIM.make_qty(Decimal("0.002")),
                report_id=UUID4(),
                ts_accepted=ts_now,
                ts_last=ts_now,
                ts_init=ts_now,
            ),
        ],
        {order.client_order_id},
    )

    # Assert - the verdict stands, so the next pass does not judge the same fill again
    assert set(env.engine._overfill_rejected_trade_ids[order.client_order_id]) == {trade_id}

    missing, applied = await _healing_pass(env)

    assert missing == []
    assert applied == 0
    assert judged == [trade_id]
    assert order.filled_qty.as_decimal() == Decimal("0.002")
