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
A run stopped by a simulation module at instant T ends at T.

The module-stop cases book a resting buy limit at 100.000 on the first print, and a print after T
asks 99.000, so a print dispatched past the stop would fill it. The AccountError case opens a
market buy on the first print and closes it at market when the trader stops.

"""

import pandas as pd
import pytest

from nautilus_trader.accounting.error import AccountError
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.modules import SimulationModule
from nautilus_trader.common.component import set_backtest_force_stop
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import SimulationModuleConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.trading.strategy import Strategy


USDJPY_SIM = TestInstrumentProvider.default_fx_ccy("USD/JPY")

SECOND_NS = 1_000_000_000
T = pd.Timestamp("2024-01-02 12:00:00", tz="UTC").value
START = T - 120 * SECOND_NS
END = T + 60 * SECOND_NS
NEVER = END + SECOND_NS


class StopsAtInstant(SimulationModule):
    def __init__(self, stop_ns: int):
        super().__init__(SimulationModuleConfig())
        self.stop_ns = stop_ns
        self.stopped = False
        self.processed_after_stop: list[int] = []

    def pre_process(self, data) -> None:
        pass

    def process(self, ts_now: int) -> None:
        if self.stopped:
            self.processed_after_stop.append(ts_now)
        elif ts_now >= self.stop_ns:
            self.stopped = True
            self.shutdown_system("liquidated")

    def log_diagnostics(self, logger) -> None:
        pass


class RaisesAccountErrorAtInstant(SimulationModule):
    def __init__(self, stop_ns: int):
        super().__init__(SimulationModuleConfig())
        self.stop_ns = stop_ns
        self.raised = False

    def pre_process(self, data) -> None:
        pass

    def process(self, ts_now: int) -> None:
        if ts_now >= self.stop_ns and not self.raised:
            self.raised = True
            raise AccountError("balance negative")

    def log_diagnostics(self, logger) -> None:
        pass


class RecordingStrategy(Strategy):
    def __init__(self, alert_times: list[int]):
        super().__init__()
        self.alert_times = alert_times
        self.alerts_fired: list[int] = []
        self.prints: list[int] = []
        self.event_times: list[int] = []
        self.order = None

    def on_start(self) -> None:
        self.subscribe_quote_ticks(USDJPY_SIM.id)
        for index, alert_time in enumerate(self.alert_times):
            self.clock.set_time_alert_ns(f"alert-{index}", alert_time, self._on_alert)

    def _on_alert(self, event) -> None:
        self.alerts_fired.append(event.ts_event)

    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.prints.append(tick.ts_event)
        if self.order is None:
            self.order = self.order_factory.limit(
                USDJPY_SIM.id,
                OrderSide.BUY,
                Quantity.from_int(100_000),
                Price.from_str("100.000"),
            )
            self.submit_order(self.order)

    def on_event(self, event) -> None:
        self.event_times.append(event.ts_event)


class ArmsStopAndSetsPastStartedTimer(RecordingStrategy):
    def __init__(self, alert_times: list[int]):
        super().__init__(alert_times)
        self.module: StopsAtInstant | None = None
        self.timer_fired: list[int] = []

    def _on_alert(self, event) -> None:
        super()._on_alert(event)
        self.module.stop_ns = T
        self.clock.set_timer_ns(
            "past-started",
            SECOND_NS,
            T - 2 * SECOND_NS,
            T + 2 * SECOND_NS,
            self._on_timer,
            allow_past=True,
        )

    def _on_timer(self, event) -> None:
        self.timer_fired.append(event.ts_event)


class BuysThenClosesOnStop(RecordingStrategy):
    def on_quote_tick(self, tick: QuoteTick) -> None:
        self.prints.append(tick.ts_event)
        if self.order is None:
            self.order = self.order_factory.market(
                USDJPY_SIM.id,
                OrderSide.BUY,
                Quantity.from_int(100_000),
            )
            self.submit_order(self.order)

    def on_stop(self) -> None:
        self.close_all_positions(USDJPY_SIM.id)


def _print_at(ts_ns: int) -> QuoteTick:
    return QuoteTick(
        instrument_id=USDJPY_SIM.id,
        bid_price=Price.from_str("110.000"),
        ask_price=Price.from_str("110.010"),
        bid_size=Quantity.from_int(1_000_000),
        ask_size=Quantity.from_int(1_000_000),
        ts_event=ts_ns,
        ts_init=ts_ns,
    )


def _crossing_print_at(ts_ns: int) -> QuoteTick:
    return QuoteTick(
        instrument_id=USDJPY_SIM.id,
        bid_price=Price.from_str("98.990"),
        ask_price=Price.from_str("99.000"),
        bid_size=Quantity.from_int(1_000_000),
        ask_size=Quantity.from_int(1_000_000),
        ts_event=ts_ns,
        ts_init=ts_ns,
    )


def _build(
    stop_ns: int,
    prints: list[QuoteTick],
    alert_times: list[int],
    strategy_class: type[RecordingStrategy] = RecordingStrategy,
    module_class: type[SimulationModule] = StopsAtInstant,
) -> tuple[BacktestEngine, RecordingStrategy, StopsAtInstant]:
    module = module_class(stop_ns)
    engine = BacktestEngine(BacktestEngineConfig(logging=LoggingConfig(bypass_logging=True)))
    engine.add_venue(
        venue=Venue("SIM"),
        oms_type=OmsType.HEDGING,
        account_type=AccountType.MARGIN,
        base_currency=USD,
        starting_balances=[Money(1_000_000, USD)],
        modules=[module],
    )
    engine.add_instrument(USDJPY_SIM)
    engine.add_data(prints)
    strategy = strategy_class(alert_times)
    engine.add_strategy(strategy)
    return engine, strategy, module


def _assert_ended_at_stop(
    engine: BacktestEngine,
    strategy: RecordingStrategy,
    module: StopsAtInstant,
) -> None:
    assert module.stopped
    assert engine.backtest_end.value == T
    assert engine.kernel.clock.timestamp_ns() == T
    assert strategy.clock.timestamp_ns() == T
    assert all(ts <= T for ts in strategy.prints)
    assert all(ts <= T for ts in strategy.event_times)
    assert engine.cache.quote_tick(USDJPY_SIM.id).ts_event <= T
    assert strategy.order.status == OrderStatus.ACCEPTED
    assert module.processed_after_stop == []
    assert engine.run_finished is not None
    assert not engine.trader.is_running


@pytest.fixture(autouse=True)
def clear_force_stop():
    yield
    set_backtest_force_stop(False)


def test_stop_at_a_print_leaves_the_later_print_and_alert_unrun():
    # The settle after the T print stops the run, so the T + 5 s print and T + 30 s alert never run.
    engine, strategy, module = _build(
        stop_ns=T,
        prints=[
            _print_at(T - 60 * SECOND_NS),
            _print_at(T),
            _crossing_print_at(T + 5 * SECOND_NS),
        ],
        alert_times=[T + 30 * SECOND_NS],
    )

    engine.run(start=START, end=END)

    _assert_ended_at_stop(engine, strategy, module)
    assert strategy.prints == [T - 60 * SECOND_NS, T]
    assert strategy.alerts_fired == []


def test_stop_with_no_data_after_it_leaves_the_later_alert_unfired():
    # Prints end before T: the T alert's settle stops the run and the T + 30 s alert never fires.
    engine, strategy, module = _build(
        stop_ns=T,
        prints=[_print_at(T - 60 * SECOND_NS)],
        alert_times=[T, T + 30 * SECOND_NS],
    )

    engine.run(start=START, end=END)

    _assert_ended_at_stop(engine, strategy, module)
    assert strategy.alerts_fired == [T]


def test_stop_at_a_due_alert_holds_the_clock_before_the_next_print():
    # The T alert is due when the clock advances to the T + 100 ms print. Its settle stops the run,
    # so the clock stays at T and that print is never dispatched.
    engine, strategy, module = _build(
        stop_ns=T,
        prints=[
            _print_at(T - 60 * SECOND_NS),
            _crossing_print_at(T + 100_000_000),
        ],
        alert_times=[T],
    )

    engine.run(start=START, end=END)

    _assert_ended_at_stop(engine, strategy, module)
    assert strategy.prints == [T - 60 * SECOND_NS]
    assert strategy.alerts_fired == [T]


def test_stop_inside_the_flush_of_end_leaves_the_later_alert_unfired():
    # A streaming run leaves both alerts to end(), whose flush settles T before the T + 30 s alert.
    engine, strategy, module = _build(
        stop_ns=T,
        prints=[_print_at(T - 60 * SECOND_NS)],
        alert_times=[T, T + 30 * SECOND_NS],
    )

    engine.run(start=START, end=END, streaming=True)
    assert not module.stopped

    engine.end()

    _assert_ended_at_stop(engine, strategy, module)
    assert strategy.alerts_fired == [T]


def test_stop_between_alerts_before_a_print_leaves_the_later_alert_unfired():
    # Both alerts fall before the T + 5 s print: the settle of T comes between them.
    engine, strategy, module = _build(
        stop_ns=T,
        prints=[
            _print_at(T - 60 * SECOND_NS),
            _crossing_print_at(T + 5 * SECOND_NS),
        ],
        alert_times=[T, T + SECOND_NS],
    )

    engine.run(start=START, end=END)

    _assert_ended_at_stop(engine, strategy, module)
    assert strategy.prints == [T - 60 * SECOND_NS]
    assert strategy.alerts_fired == [T]


def test_stop_between_alerts_in_the_final_flush_leaves_the_later_alert_unfired():
    # The print past the run's end leaves both alerts to the flush that closes the run.
    engine, strategy, module = _build(
        stop_ns=T,
        prints=[
            _print_at(T - 60 * SECOND_NS),
            _crossing_print_at(END + SECOND_NS),
        ],
        alert_times=[T, T + 30 * SECOND_NS],
    )

    engine.run(start=START, end=END)

    _assert_ended_at_stop(engine, strategy, module)
    assert strategy.alerts_fired == [T]


def test_stop_between_handlers_of_a_prints_own_instant_leaves_an_earlier_stamped_timer_unfired():
    # The T alert fires after the T print and sets a timer started at T - 2 s, whose first event is
    # stamped T - 1 s. Settling T ahead of that event stops the run, so it never fires and the clock
    # never moves back from T.
    engine, strategy, module = _build(
        stop_ns=NEVER,
        prints=[
            _print_at(T - 60 * SECOND_NS),
            _print_at(T),
            _crossing_print_at(T + 5 * SECOND_NS),
        ],
        alert_times=[T],
        strategy_class=ArmsStopAndSetsPastStartedTimer,
    )
    strategy.module = module

    engine.run(start=START, end=END)

    _assert_ended_at_stop(engine, strategy, module)
    assert strategy.prints == [T - 60 * SECOND_NS, T]
    assert strategy.alerts_fired == [T]
    assert strategy.timer_fired == []


def test_account_error_stop_fills_the_close_submitted_on_stop_at_the_stop_instant():
    # The T - 60 s print opens a buy of 100,000 at its ask. AccountError is raised in the settle of
    # the T print, which ends the loop with the clock at T and the T + 5 s print unrun. Stopping the
    # trader then submits the closing sell of 100,000, and the settle at T fills it against the T
    # print's bid, leaving no position.
    engine, strategy, _module = _build(
        stop_ns=T,
        prints=[
            _print_at(T - 60 * SECOND_NS),
            _print_at(T),
            _print_at(T + 5 * SECOND_NS),
        ],
        alert_times=[],
        strategy_class=BuysThenClosesOnStop,
        module_class=RaisesAccountErrorAtInstant,
    )

    engine.run(start=START, end=END)

    sells = [order for order in engine.cache.orders() if order.side == OrderSide.SELL]
    assert len(sells) == 1
    assert sells[0].status == OrderStatus.FILLED
    assert sells[0].filled_qty == Quantity.from_int(100_000)
    assert sells[0].ts_last == T
    assert engine.cache.positions_open() == []
    assert engine.backtest_end.value == T
    assert strategy.prints == [T - 60 * SECOND_NS, T]


def test_a_run_never_stopped_runs_every_print_and_alert_and_ends_at_its_last_event():
    # The clock rests on the last event, the T + 30 s alert: the flush to END moves no clock. The
    # T + 5 s print crosses the resting buy, which fills on that print.
    engine, strategy, module = _build(
        stop_ns=NEVER,
        prints=[
            _print_at(T - 60 * SECOND_NS),
            _print_at(T),
            _crossing_print_at(T + 5 * SECOND_NS),
        ],
        alert_times=[T, T + 30 * SECOND_NS],
    )

    engine.run(start=START, end=END)

    assert not module.stopped
    assert engine.backtest_end.value == T + 30 * SECOND_NS
    assert strategy.prints == [T - 60 * SECOND_NS, T, T + 5 * SECOND_NS]
    assert strategy.alerts_fired == [T, T + 30 * SECOND_NS]
    assert strategy.order.status == OrderStatus.FILLED
    assert strategy.order.ts_last == T + 5 * SECOND_NS
    assert engine.run_finished is not None
    assert not engine.trader.is_running
