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
The simulated exchange's admission of an order that reserved nothing while it rested, at its
first execution: a conditional order at its trigger, a zero-reserve limit when it first fills.

The venue's margin is valued by a test model declaring the rule a linear perpetual venue states:
a position holds its value at the mark / the leverage (maintenance 0.5 % of it), a limit holds
leaves * its price / the leverage, an opposite limit is absorbed by the position in the order it
would fill with only its excess charged, and a long and a short held together hold the larger
leg. Every expected figure is derived by hand in the test from the authored balances and prices.
"""

from decimal import ROUND_HALF_EVEN
from decimal import Decimal

import pytest

from nautilus_trader.accounting.accounts.margin import MarginAccount
from nautilus_trader.accounting.exposure import MarginBasis
from nautilus_trader.accounting.manager import AccountsManager
from nautilus_trader.accounting.margin_models import LeveragedMarginModel
from nautilus_trader.accounting.margin_models import MarginModel
from nautilus_trader.backtest.engine import SimulatedExchange
from nautilus_trader.backtest.execution_client import BacktestExecClient
from nautilus_trader.backtest.models import FeeModel
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.backtest.models import LatencyModel
from nautilus_trader.backtest.models import MakerTakerFeeModel
from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import ExecEngineConfig
from nautilus_trader.config import RiskEngineConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.execution.engine import ExecutionEngine
from nautilus_trader.model.currencies import BTC
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import BookAction
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionAdjustmentType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.events import OrderAccepted
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import OrderTriggered
from nautilus_trader.model.events import PositionAdjusted
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.portfolio.config import PortfolioConfig
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.risk.engine import RiskEngine
from nautilus_trader.test_kit.mocks.strategies import MockStrategy
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


BYBIT = Venue("BYBIT_TEST")
FROZEN = Venue("FROZEN_TEST")

_CONDITIONAL = (
    OrderType.STOP_MARKET,
    OrderType.STOP_LIMIT,
    OrderType.MARKET_IF_TOUCHED,
    OrderType.LIMIT_IF_TOUCHED,
    OrderType.TRAILING_STOP_MARKET,
    OrderType.TRAILING_STOP_LIMIT,
)


def _perp(symbol: str, taker_fee: str = "0", venue: Venue = BYBIT) -> CryptoPerpetual:
    return CryptoPerpetual(
        instrument_id=InstrumentId(Symbol(symbol), venue),
        raw_symbol=Symbol(symbol),
        base_currency=BTC,
        quote_currency=USDT,
        settlement_currency=USDT,
        is_inverse=False,
        price_precision=1,
        price_increment=Price.from_str("0.1"),
        size_precision=3,
        size_increment=Quantity.from_str("0.001"),
        max_quantity=Quantity.from_str("1000.000"),
        min_quantity=Quantity.from_str("0.001"),
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal(0),
        margin_maint=Decimal(0),
        maker_fee=Decimal(0),
        taker_fee=Decimal(taker_fee),
        ts_event=0,
        ts_init=0,
    )


BTCUSDT = _perp("BTCUSDT-LINEAR")
ETHUSDT = _perp("ETHUSDT-LINEAR")
BTCUSDT_FEE = _perp("BTCUSDT-LINEAR", taker_fee="0.00055")
# A venue of its own: NT registers calculated accounts per venue name for the process
BTCUSDT_FROZEN = _perp("BTCUSDT-LINEAR", venue=FROZEN)
ETHUSDT_FROZEN = _perp("ETHUSDT-LINEAR", venue=FROZEN)


def _money(amount: str) -> Money:
    return Money(Decimal(amount), USDT)


class _NotionalLeverageModel(MarginModel):
    """
    A declaring test model valuing an exposure at the mark / the leverage, with a 0.5 % floor.
    """

    margin_from_exposure = True
    checks_free_margin = True
    buy_price_type = PriceType.ASK
    sell_price_type = PriceType.BID

    def __init__(
        self,
        reserves_untriggered: bool = False,
        trigger_check: bool = True,
        pending_rate_zero: bool = False,
        taker_fee: Decimal | None = None,
    ) -> None:
        self._reserves_untriggered = reserves_untriggered
        self.trigger_check = trigger_check
        self.pending_rate_zero = pending_rate_zero
        self.taker_fee = taker_fee
        self.floor_rate = Decimal("0.005")

    def margin_currency(self, instrument):
        return USDT

    def reserves_untriggered(self, instrument):
        return self._reserves_untriggered

    def checks_at_execution(self, instrument, order_type, order_side):
        if not self.trigger_check:
            return False
        if order_type in _CONDITIONAL:
            return not self._reserves_untriggered
        if order_type == OrderType.LIMIT:
            return self.pending_rate_zero
        return False

    def calculate_exposure_margin(
        self,
        instrument,
        exposure,
        basis,
        leverage,
        to_currency,
        xrate_buy,
        xrate_sell,
        use_quote_for_inverse=False,
    ):
        total = self._figure(exposure, basis, Decimal(leverage))
        quantum = Decimal(1).scaleb(-to_currency.precision)
        return Money(total.quantize(quantum, rounding=ROUND_HALF_EVEN), to_currency)

    def _close_fee(self, quantity: Decimal, price: Decimal, is_long: bool, leverage) -> Decimal:
        # The fee to close at the bankruptcy price
        if self.taker_fee is None:
            return Decimal(0)
        if is_long:
            bankruptcy = price * (1 - 1 / leverage)
        else:
            bankruptcy = price * (1 + 1 / leverage)
        return quantity * bankruptcy * self.taker_fee

    def _order(self, entry, leverage, leaves: Decimal | None = None) -> Decimal:
        order_type, side, _quantity, entry_leaves, price = entry
        quantity = entry_leaves.as_decimal() if leaves is None else leaves
        if self.pending_rate_zero and order_type != OrderType.MARKET:
            return Decimal(0)
        value = quantity * price.as_decimal()
        figure = value / leverage
        if self.taker_fee is not None:
            figure += value * self.taker_fee
            figure += self._close_fee(quantity, price.as_decimal(), side == OrderSide.BUY, leverage)
        return figure

    def _position(self, quantity: Decimal, mark: Decimal, is_long: bool, leverage, floor: bool):
        value = quantity * mark
        if floor:
            figure = value * self.floor_rate
        else:
            figure = max(value / leverage, value * self.floor_rate)
        return figure + self._close_fee(quantity, mark, is_long, leverage)

    def _figure(self, exposure, basis, leverage) -> Decimal:
        mark = exposure.mark.as_decimal() if exposure.mark is not None else None
        long_qty = exposure.long_quantity.as_decimal() if exposure.long_quantity else Decimal(0)
        short_qty = exposure.short_quantity.as_decimal() if exposure.short_quantity else Decimal(0)

        if basis == MarginBasis.MAINTENANCE:
            if long_qty == short_qty:
                return Decimal(0)
            if long_qty > short_qty:
                return self._position(long_qty - short_qty, mark, True, leverage, floor=True)
            return self._position(short_qty - long_qty, mark, False, leverage, floor=True)

        if long_qty and short_qty:
            # Both legs held: the larger leg, each order on its own side
            legs = {
                OrderSide.BUY: self._position(long_qty, mark, True, leverage, floor=False),
                OrderSide.SELL: self._position(short_qty, mark, False, leverage, floor=False),
            }
            for entry in exposure.orders:
                legs[entry[1]] += self._order(entry, leverage)
            return max(legs.values())

        if long_qty:
            return self._netted(exposure, OrderSide.BUY, long_qty, mark, leverage)
        if short_qty:
            return self._netted(exposure, OrderSide.SELL, short_qty, mark, leverage)
        return sum((self._order(e, leverage) for e in exposure.orders), Decimal(0))

    def _netted(self, exposure, side, held_qty: Decimal, mark: Decimal, leverage) -> Decimal:
        figure = self._position(held_qty, mark, side == OrderSide.BUY, leverage, floor=False)
        figure += sum(
            (self._order(e, leverage) for e in exposure.orders if e[1] == side), Decimal(0)
        )

        # Opposite orders are absorbed in the order they would fill (a market order first, then
        # sells from the lowest price up and buys from the highest down); the excess is charged
        opposite = [e for e in exposure.orders if e[1] != side]
        if side == OrderSide.BUY:
            opposite.sort(key=lambda e: (e[0] != OrderType.MARKET, e[4].as_decimal()))
        else:
            opposite.sort(key=lambda e: (e[0] != OrderType.MARKET, -e[4].as_decimal()))
        remaining = held_qty
        for entry in opposite:
            leaves = entry[3].as_decimal()
            absorbed = min(remaining, leaves)
            remaining -= absorbed
            if leaves > absorbed:
                figure += self._order(entry, leverage, leaves - absorbed)
        return figure


class _PerLotModel(MarginModel):
    """
    A declaring test model charging fixed amounts per lot: 1,000 for a market order, 500 for a
    position held, nothing for a pending order of any kind.
    """

    margin_from_exposure = True
    checks_free_margin = True
    buy_price_type = PriceType.ASK
    sell_price_type = PriceType.BID

    def margin_currency(self, instrument):
        return USDT

    def reserves_untriggered(self, instrument):
        return False

    def checks_at_execution(self, instrument, order_type, order_side):
        return order_type != OrderType.MARKET

    def calculate_exposure_margin(
        self,
        instrument,
        exposure,
        basis,
        leverage,
        to_currency,
        xrate_buy,
        xrate_sell,
        use_quote_for_inverse=False,
    ):
        lots = exposure.net_quantity.as_decimal() if exposure.net_quantity else Decimal(0)
        figure = lots * 500
        if basis == MarginBasis.HELD:
            for order_type, _side, _quantity, leaves, _price in exposure.orders:
                if order_type == OrderType.MARKET:
                    figure += leaves.as_decimal() * 1000
        return Money(figure, to_currency)


class _ZeroFeeModel(FeeModel):
    def get_commission(self, order, fill_qty, fill_px, instrument):
        return Money(0, instrument.quote_currency)


class _Bench:
    """
    A simulated linear perpetual venue at leverage 10 wired to a mark-priced Portfolio, a
    strategy and the engines. A mark print with a position held re-writes that instrument's
    margin through the public writer, as a run's mark module does.
    """

    def __init__(
        self,
        *,
        balance: str,
        model: MarginModel,
        instruments: tuple = (BTCUSDT,),
        oms_type: OmsType = OmsType.NETTING,
        frozen: bool = False,
        fee_model: FeeModel | None = None,
        liquidity_consumption: bool = False,
        book_type: BookType = BookType.L1_MBP,
    ) -> None:
        self.clock = TestClock()
        self.trader_id = TestIdStubs.trader_id()
        self.msgbus = MessageBus(trader_id=self.trader_id, clock=self.clock)
        self.cache = TestComponentStubs.cache()
        self.portfolio = Portfolio(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            config=PortfolioConfig(use_mark_prices=True),
        )
        self.accounts = AccountsManager(self.cache, Logger("AccountsManager"), self.clock)
        self.data_engine = DataEngine(msgbus=self.msgbus, clock=self.clock, cache=self.cache)
        self.exec_engine = ExecutionEngine(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            config=ExecEngineConfig(),
        )
        self.risk_engine = RiskEngine(
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            config=RiskEngineConfig(),
        )
        self.venue = instruments[0].id.venue
        self.exchange = SimulatedExchange(
            venue=self.venue,
            oms_type=oms_type,
            account_type=AccountType.MARGIN,
            book_type=book_type,
            base_currency=USDT,
            starting_balances=[_money(balance)],
            default_leverage=Decimal(10),
            leverages={instrument.id: Decimal(10) for instrument in instruments},
            modules=[],
            fill_model=FillModel(),
            fee_model=fee_model or _ZeroFeeModel(),
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            latency_model=LatencyModel(0),
            margin_model=model,
            frozen_account=frozen,
            reject_stop_orders=False,
            liquidity_consumption=liquidity_consumption,
        )
        for instrument in instruments:
            self.exchange.add_instrument(instrument)
            self.cache.add_instrument(instrument)
        self.exec_client = BacktestExecClient(
            exchange=self.exchange,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            frozen_account=frozen,
        )
        self.exec_engine.register_client(self.exec_client)
        self.exchange.register_client(self.exec_client)

        self.strategy = MockStrategy(bar_type=TestDataStubs.bartype_usdjpy_1min_bid())
        self.strategy.register(
            trader_id=self.trader_id,
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        self.account_states: list[AccountState] = []
        self.msgbus.subscribe(topic="events.account.*", handler=self.account_states.append)
        self.exchange.reset()
        self.data_engine.start()
        self.exec_engine.start()
        self.strategy.start()
        self._trades = 0
        self._levels = 0

    @property
    def account(self) -> MarginAccount:
        return self.exchange.get_account()

    def quote(self, instrument, price: str, size: str = "1000.000") -> None:
        tick = QuoteTick(
            instrument_id=instrument.id,
            bid_price=Price.from_str(price),
            ask_price=Price.from_str(price),
            bid_size=Quantity.from_str(size),
            ask_size=Quantity.from_str(size),
            ts_event=0,
            ts_init=0,
        )
        self.exchange.process_quote_tick(tick)
        self.data_engine.process(tick)

    def level(self, instrument, side: OrderSide, price: str, size: str, action=BookAction.ADD):
        self._levels += 1
        self.exchange.process_order_book_delta(
            OrderBookDelta(
                instrument_id=instrument.id,
                action=action,
                order=BookOrder(side, Price.from_str(price), Quantity.from_str(size), self._levels),
                flags=0,
                sequence=self._levels,
                ts_event=0,
                ts_init=0,
            ),
        )

    def mark(self, instrument, price: str) -> None:
        self.data_engine.process(
            MarkPriceUpdate(
                instrument_id=instrument.id,
                value=Price.from_str(price),
                ts_event=0,
                ts_init=0,
            ),
        )
        account = self.account
        positions = self.cache.positions_open(instrument_id=instrument.id, account_id=account.id)
        if account.margin_from_exposure and positions:
            orders = self.cache.orders_open(instrument_id=instrument.id, account_id=account.id)
            self.accounts.update_positions(
                account=account,
                instrument=instrument,
                positions_open=positions,
                ts_event=0,
                orders_open=[o for o in orders if o.is_passive],
            )

    def trade(self, instrument, price: str, size: str = "1000.000", aggressor=None) -> None:
        self._trades += 1
        tick = TradeTick(
            instrument_id=instrument.id,
            price=Price.from_str(price),
            size=Quantity.from_str(size),
            aggressor_side=aggressor or AggressorSide.NO_AGGRESSOR,
            trade_id=TradeId(f"T-{self._trades}"),
            ts_event=0,
            ts_init=0,
        )
        self.exchange.process_trade_tick(tick)
        self.data_engine.process(tick)

    def submit(self, order, position_id=None):
        self.strategy.submit_order(order, position_id=position_id)
        self.exchange.process(0)
        return order

    def market(self, instrument, side: OrderSide, quantity: str, reduce_only: bool = False):
        return self.submit(
            self.strategy.order_factory.market(
                instrument.id,
                side,
                Quantity.from_str(quantity),
                reduce_only=reduce_only,
            ),
        )

    def limit(self, instrument, side: OrderSide, quantity: str, price: str):
        return self.submit(
            self.strategy.order_factory.limit(
                instrument.id,
                side,
                Quantity.from_str(quantity),
                Price.from_str(price),
            ),
        )

    def stop(self, instrument, side: OrderSide, quantity: str, trigger: str, reduce_only=False):
        return self.submit(
            self.strategy.order_factory.stop_market(
                instrument.id,
                side,
                Quantity.from_str(quantity),
                Price.from_str(trigger),
                reduce_only=reduce_only,
            ),
        )

    def stop_limit(self, instrument, side: OrderSide, quantity: str, trigger: str, price: str):
        return self.submit(
            self.strategy.order_factory.stop_limit(
                instrument.id,
                side,
                Quantity.from_str(quantity),
                Price.from_str(price),
                Price.from_str(trigger),
            ),
        )

    def open(self, instrument, side: OrderSide, quantity: str, price: str) -> None:
        # Opens a position at a price the mark also prints, so the balance stays the equity
        self.quote(instrument, price)
        self.mark(instrument, price)
        self.market(instrument, side, quantity)

    def margin(self, instrument) -> Money:
        init = self.account.margin_init(instrument.id)
        maint = self.account.margin_maint(instrument.id)
        if init is None:
            return _money("0")
        return Money(init.as_decimal() + maint.as_decimal(), USDT)

    def margins(self, instrument) -> tuple[Money, Money]:
        return self.account.margin_init(instrument.id), self.account.margin_maint(instrument.id)

    def total_margin(self) -> Money:
        return Money(
            self.account.total_margin_init(USDT).as_decimal()
            + self.account.total_margin_maint(USDT).as_decimal(),
            USDT,
        )

    def balance(self) -> Money:
        return self.account.balance_total(USDT)

    def position(self, instrument):
        positions = self.cache.positions_open(instrument_id=instrument.id)
        return [(p.side, p.quantity) for p in positions]

    def events(self, order, kind=None) -> list:
        found = [
            e
            for e in self.strategy.store
            if getattr(e, "client_order_id", None) == order.client_order_id
        ]
        if kind is None:
            return found
        return [e for e in found if isinstance(e, kind)]

    def rejections(self) -> list:
        return [e for e in self.strategy.store if isinstance(e, OrderRejected)]


def _two_longs(balance: str) -> _Bench:
    # 10 ETH long at 2,000 (held 20,000 / 10) and 10 BTC long at 50,000 (held 500,000 / 10)
    bench = _Bench(balance=balance, model=_NotionalLeverageModel(), instruments=(BTCUSDT, ETHUSDT))
    bench.open(ETHUSDT, OrderSide.BUY, "10.000", "2000.0")
    bench.open(BTCUSDT, OrderSide.BUY, "10.000", "50000.0")
    return bench


def _flip_case(balance: str) -> tuple[_Bench, object]:
    # Both longs with a stop sell of 20 BTC at 45,000 resting, then the mark printing 45,000
    bench = _two_longs(balance)
    stop = bench.stop(BTCUSDT, OrderSide.SELL, "20.000", "45000.0")
    assert stop.status == OrderStatus.ACCEPTED
    bench.mark(BTCUSDT, "45000.0")
    return bench, stop


def test_flip_stop_beyond_the_room_left_after_its_release_is_rejected_whole():
    # Wallet 55,000. On the 45,000 mark: BTC holds 10 * 45,000 / 10 = 45,000 (maintenance
    # 10 * 45,000 * 0.005 = 2,250, initial 42,750), ETH 2,000: total 47,000; equity
    # 55,000 + 10 * (45,000 - 50,000) = 5,000; free 5,000 - 47,000 = -42,000.
    # The stop as a market sell of 20 at 45,000: 10 absorbed, 10 * 45,000 / 10 = 45,000 beyond,
    # HELD 90,000 - the instrument's 45,000 = increment 45,000; the absorbed 10 release 45,000:
    # room -42,000 + 45,000 = 3,000 < 45,000 -> rejected whole.
    bench, stop = _flip_case("55000")
    assert bench.total_margin() == _money("47000")

    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

    assert stop.status == OrderStatus.REJECTED
    assert len(bench.events(stop, OrderRejected)) == 1
    assert bench.events(stop, OrderFilled) == []
    assert bench.position(BTCUSDT) == [(PositionSide.LONG, Quantity.from_str("10.000"))]
    assert bench.balance() == _money("55000")
    assert bench.margins(BTCUSDT) == (_money("42750"), _money("2250"))
    assert bench.total_margin() == _money("47000")


def test_funded_flip_stop_fills_whole():
    # Wallet 150,000: equity at the trigger 100,000, free 100,000 - 47,000 = 53,000, plus the
    # release 45,000: 98,000 >= 45,000 -> filled. The long closes with 10 (realised
    # 10 * (45,000 - 50,000) = -50,000, balance 100,000), a 10 BTC short at 45,000 holds
    # 45,000 (maintenance 2,250, initial 42,750) beside ETH 2,000.
    bench, stop = _flip_case("150000")

    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

    assert stop.status == OrderStatus.FILLED
    assert bench.rejections() == []
    assert bench.position(BTCUSDT) == [(PositionSide.SHORT, Quantity.from_str("10.000"))]
    assert bench.balance() == _money("100000")
    assert bench.margins(BTCUSDT) == (_money("42750"), _money("2250"))
    assert bench.total_margin() == _money("47000")


def test_reduce_only_stop_under_the_shortfall_fills_unchecked():
    # Wallet 55,000, a reduce-only stop sell of 10: never checked; the long closes
    # (realised -50,000, balance 5,000), BTC flat, ETH's 2,000 left.
    bench = _two_longs("55000")
    stop = bench.stop(BTCUSDT, OrderSide.SELL, "10.000", "45000.0", reduce_only=True)
    bench.mark(BTCUSDT, "45000.0")

    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

    assert stop.status == OrderStatus.FILLED
    assert bench.rejections() == []
    assert bench.position(BTCUSDT) == []
    assert bench.balance() == _money("5000")
    assert bench.margin(BTCUSDT) == _money("0")
    assert bench.total_margin() == _money("2000")


def _exit_and_flip(balance: str, exit_first: bool):
    bench = _two_longs(balance)
    if exit_first:
        bench.stop(BTCUSDT, OrderSide.SELL, "10.000", "45000.0", reduce_only=True)
        flip = bench.stop(BTCUSDT, OrderSide.SELL, "20.000", "45000.0")
    else:
        flip = bench.stop(BTCUSDT, OrderSide.SELL, "20.000", "45000.0")
        bench.stop(BTCUSDT, OrderSide.SELL, "10.000", "45000.0", reduce_only=True)
    assert flip.status == OrderStatus.ACCEPTED
    bench.mark(BTCUSDT, "45000.0")
    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)
    return bench, flip


def test_exit_and_flip_at_one_price_under_the_shortfall_end_flat_in_either_order():
    # Wallet 55,000. Exit first: flat at balance 5,000, then the flip is a 20 BTC opening,
    # 20 * 45,000 / 10 = 90,000 > free 5,000 - 2,000 = 3,000 -> rejected. Flip first: increment
    # 45,000 > room -42,000 + 45,000 = 3,000 -> rejected, then the exit closes the long.
    for exit_first in (True, False):
        bench, flip = _exit_and_flip("55000", exit_first)

        assert flip.status == OrderStatus.REJECTED
        assert bench.position(BTCUSDT) == []
        assert bench.balance() == _money("5000")
        assert bench.total_margin() == _money("2000")


def test_exit_and_flip_at_one_price_funded_end_short_twenty_in_either_order():
    # Wallet 150,000. Exit first: flat at 100,000, the flip opens 20: 90,000 <= free
    # 100,000 - 2,000 = 98,000. Flip first: it opens 10 (45,000 <= 53,000 + 45,000), then the
    # resting reduce-only exit, matched against the short, grows it to 20. Either way a 20 BTC
    # short at 45,000 holds 90,000 beside ETH 2,000: total 92,000, balance 100,000.
    for exit_first in (True, False):
        bench, flip = _exit_and_flip("150000", exit_first)

        assert flip.status == OrderStatus.FILLED
        assert bench.rejections() == []
        assert bench.position(BTCUSDT) == [(PositionSide.SHORT, Quantity.from_str("20.000"))]
        assert bench.balance() == _money("100000")
        assert bench.total_margin() == _money("92000")


def _stop_limit_bench(balance: str) -> tuple[_Bench, object]:
    # 1 BTC long at 50,000 (5,000), a buy stop-limit of 1 at trigger 51,000, limit 50,900, then
    # the mark printing 51,000
    bench = _Bench(balance=balance, model=_NotionalLeverageModel())
    bench.open(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")
    order = bench.stop_limit(BTCUSDT, OrderSide.BUY, "1.000", "51000.0", "50900.0")
    assert order.status == OrderStatus.ACCEPTED
    return bench, order


def test_stop_limit_is_admitted_at_its_trigger_and_then_charged_as_a_resting_limit():
    # Untriggered: margin 5,000 (initial 4,750, maintenance 250). On the 51,000 mark the long
    # holds 5,100 (initial 4,845, maintenance 255), equity 20,000 + 1,000 = 21,000. At the
    # trigger the order is a market buy of 1 at 51,000: 5,100 <= free 21,000 - 5,100 = 15,900
    # -> admitted, resting at 50,900 reserving 5,090: margin 10,190 (initial 9,935, maintenance
    # 255), written and published on the trigger event.
    bench, order = _stop_limit_bench("20000")
    assert bench.margins(BTCUSDT) == (_money("4750"), _money("250"))
    bench.mark(BTCUSDT, "51000.0")
    assert bench.margins(BTCUSDT) == (_money("4845"), _money("255"))
    published = len(bench.account_states)

    bench.trade(BTCUSDT, "51000.0", aggressor=AggressorSide.BUYER)

    assert order.status == OrderStatus.TRIGGERED
    assert len(bench.events(order, OrderTriggered)) == 1
    assert bench.margins(BTCUSDT) == (_money("9935"), _money("255"))
    assert len(bench.account_states) == published + 1
    state = bench.account_states[-1]
    assert next(m for m in state.margins if m.instrument_id == BTCUSDT.id).initial == _money("9935")

    # A 1 BTC limit buy at 50,000 needs 5,000: free 21,000 - 10,190 = 10,810 -> admitted
    later = bench.limit(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")
    assert later.status == OrderStatus.ACCEPTED


def test_limit_after_a_triggered_stop_limit_is_refused_against_its_reservation():
    # Wallet 14,000: equity on the 51,000 mark 15,000; the stop-limit is admitted (5,100 <=
    # 15,000 - 5,100) and reserves 5,090; a 1 BTC limit buy at 50,000 needs 5,000 > free
    # 15,000 - 10,190 = 4,810 -> refused.
    bench, _order = _stop_limit_bench("14000")
    bench.mark(BTCUSDT, "51000.0")
    bench.trade(BTCUSDT, "51000.0", aggressor=AggressorSide.BUYER)

    later = bench.limit(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")

    assert later.status == OrderStatus.REJECTED


def test_stop_limit_rejected_at_its_trigger_emits_no_trigger_event():
    # Wallet 5,500: on the 51,000 mark equity 6,500, the long holds 5,100, free 1,400; the stop
    # as a market buy of 1 at 51,000 needs 5,100 > 1,400 -> rejected whole, never triggered.
    bench, order = _stop_limit_bench("5500")
    bench.mark(BTCUSDT, "51000.0")

    bench.trade(BTCUSDT, "51000.0", aggressor=AggressorSide.BUYER)

    assert order.status == OrderStatus.REJECTED
    assert bench.events(order, OrderTriggered) == []
    assert [type(e) for e in bench.events(order)][-2:] == [OrderAccepted, OrderRejected]
    assert bench.margins(BTCUSDT) == (_money("4845"), _money("255"))


def test_stop_limit_triggered_at_submission_is_judged_before_it_is_accepted():
    # 1 BTC long at 50,000 (5,000), the market at 50,000, a buy stop-limit of 1 at trigger
    # 49,000, limit 48,900. Wallet 5,500 (free 500): judged as a market buy of 1 at 50,000,
    # 5,000 > 500 -> rejected, never accepted nor triggered, margin 5,000. Wallet 10,000 (free
    # 5,000): 5,000 <= 5,000 -> placed at 48,900 reserving 4,890: margin 9,890 (initial 9,640,
    # maintenance 250).
    bench = _Bench(balance="5500", model=_NotionalLeverageModel())
    bench.open(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")
    order = bench.stop_limit(BTCUSDT, OrderSide.BUY, "1.000", "49000.0", "48900.0")

    assert order.status == OrderStatus.REJECTED
    assert bench.events(order, OrderAccepted) == []
    assert bench.events(order, OrderTriggered) == []
    assert bench.margin(BTCUSDT) == _money("5000")

    bench = _Bench(balance="10000", model=_NotionalLeverageModel())
    bench.open(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")
    order = bench.stop_limit(BTCUSDT, OrderSide.BUY, "1.000", "49000.0", "48900.0")

    assert order.status == OrderStatus.TRIGGERED
    assert bench.margins(BTCUSDT) == (_money("9640"), _money("250"))

    # Wallet 9,950 (free 4,950): the 5,000 at the market exceeds it, though the limit's own 4,890
    # would fit -> rejected
    bench = _Bench(balance="9950", model=_NotionalLeverageModel())
    bench.open(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")
    order = bench.stop_limit(BTCUSDT, OrderSide.BUY, "1.000", "49000.0", "48900.0")

    assert order.status == OrderStatus.REJECTED
    assert bench.events(order, OrderAccepted) == []


def test_frozen_account_fills_the_flip_stop_unchecked():
    # The flip case with the account frozen: no margin is calculated, so nothing is checked;
    # the stop fills whole, closing the long and opening a 10 BTC short.
    bench = _Bench(
        balance="55000",
        model=_NotionalLeverageModel(),
        instruments=(BTCUSDT_FROZEN, ETHUSDT_FROZEN),
        frozen=True,
    )
    bench.open(ETHUSDT_FROZEN, OrderSide.BUY, "10.000", "2000.0")
    bench.open(BTCUSDT_FROZEN, OrderSide.BUY, "10.000", "50000.0")
    stop = bench.stop(BTCUSDT_FROZEN, OrderSide.SELL, "20.000", "45000.0")
    bench.mark(BTCUSDT_FROZEN, "45000.0")

    bench.trade(BTCUSDT_FROZEN, "45000.0", aggressor=AggressorSide.SELLER)

    assert not bench.account.calculate_account_state
    assert stop.status == OrderStatus.FILLED
    assert bench.rejections() == []
    assert bench.position(BTCUSDT_FROZEN) == [(PositionSide.SHORT, Quantity.from_str("10.000"))]


def _fee_case(balance: str) -> tuple[_Bench, object]:
    # Flat, taker fee 0.00055, a buy stop of 1 at 50,000 resting under a 49,900 market
    bench = _Bench(
        balance=balance,
        model=_NotionalLeverageModel(taker_fee=Decimal("0.00055")),
        instruments=(BTCUSDT_FEE,),
        fee_model=MakerTakerFeeModel(),
    )
    bench.mark(BTCUSDT_FEE, "50000.0")
    bench.quote(BTCUSDT_FEE, "49900.0")
    stop = bench.stop(BTCUSDT_FEE, OrderSide.BUY, "1.000", "50000.0")
    assert stop.status == OrderStatus.ACCEPTED
    bench.trade(BTCUSDT_FEE, "50000.0", aggressor=AggressorSide.BUYER)
    return bench, stop


def test_stop_requirement_carries_the_fees_to_open_and_close():
    # The order's cost: 50,000 / 10 = 5,000, the fee to open 50,000 * 0.00055 = 27.50, the
    # fee to close at the bankruptcy price 50,000 * 0.9 * 0.00055 = 24.75: 5,052.25. At balance
    # 5,052.24 -> rejected; at 5,052.25 -> filled, the commission 27.50 booked (balance
    # 5,024.75), the position holding 5,000 + 24.75 (maintenance 250 + 24.75 = 274.75, initial
    # 4,750).
    bench, stop = _fee_case("5052.24")
    assert stop.status == OrderStatus.REJECTED
    assert bench.position(BTCUSDT_FEE) == []

    bench, stop = _fee_case("5052.25")
    assert stop.status == OrderStatus.FILLED
    assert bench.balance() == _money("5024.75")
    assert bench.margins(BTCUSDT_FEE) == (_money("4750"), _money("274.75"))


def _carry_case(balance: str) -> tuple[_Bench, object]:
    # 10 ETH long at 2,000 (2,000 held) carrying an accrued swap debit of 100; BTC flat; a buy
    # stop of 1 BTC at 50,000 resting under a 49,900 market
    bench = _Bench(balance=balance, model=_NotionalLeverageModel(), instruments=(BTCUSDT, ETHUSDT))
    bench.open(ETHUSDT, OrderSide.BUY, "10.000", "2000.0")
    position = bench.cache.positions_open(instrument_id=ETHUSDT.id)[0]
    position.apply_adjustment(
        PositionAdjusted(
            trader_id=position.trader_id,
            strategy_id=position.strategy_id,
            instrument_id=position.instrument_id,
            position_id=position.id,
            account_id=position.account_id,
            adjustment_type=PositionAdjustmentType.SWAP_CHARGE,
            quantity_change=None,
            pnl_change=_money("-100"),
            reason=None,
            event_id=UUID4(),
            ts_event=0,
            ts_init=0,
            settled_change=_money("-100"),
        ),
    )
    bench.cache.update_position(position)
    bench.mark(BTCUSDT, "50000.0")
    bench.quote(BTCUSDT, "49900.0")
    stop = bench.stop(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")
    assert stop.status == OrderStatus.ACCEPTED
    bench.trade(BTCUSDT, "50000.0", aggressor=AggressorSide.BUYER)
    return bench, stop


def test_stop_requirement_is_compared_with_free_margin_net_of_accrued_carry():
    # The requirement 50,000 / 10 = 5,000; free = balance - the 100 accrued - ETH's 2,000:
    # at 7,099 -> 4,999 < 5,000 -> rejected; at 7,100 -> 5,000 -> filled, BTC 5,000 beside ETH
    # 2,000.
    bench, stop = _carry_case("7099")
    assert stop.status == OrderStatus.REJECTED

    bench, stop = _carry_case("7100")
    assert stop.status == OrderStatus.FILLED
    assert bench.margin(BTCUSDT) == _money("5000")
    assert bench.total_margin() == _money("7000")


def test_flip_stop_is_compared_with_the_account_wide_free_margin():
    # Wallet 96,000: equity at the trigger 46,000, free 46,000 - 47,000 = -1,000, release
    # 45,000: room 44,000 < 45,000 -> rejected (BTC alone, 46,000 - 45,000 + 45,000 = 46,000,
    # would admit). Wallet 97,000: free 0 + 45,000 = 45,000 >= 45,000 -> filled, the short
    # 45,000 beside ETH 2,000.
    bench, stop = _flip_case("96000")
    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)
    assert stop.status == OrderStatus.REJECTED
    assert bench.position(BTCUSDT) == [(PositionSide.LONG, Quantity.from_str("10.000"))]

    bench, stop = _flip_case("97000")
    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)
    assert stop.status == OrderStatus.FILLED
    assert bench.position(BTCUSDT) == [(PositionSide.SHORT, Quantity.from_str("10.000"))]
    assert bench.total_margin() == _money("47000")


def _per_lot(balance: str) -> _Bench:
    bench = _Bench(balance=balance, model=_PerLotModel())
    bench.mark(BTCUSDT, "100.0")
    bench.quote(BTCUSDT, "100.0")
    return bench


def test_zero_reserve_limit_is_judged_at_its_first_execution():
    # Pending kinds reserve 0, a market lot 1,000, a position lot 500. A flat 750 account: a
    # buy limit of 1 at 99 rests for 0; the trade at 99 executes it, judged as a market lot,
    # 1,000 > 750 -> rejected, no position. At 1,000 -> fills, the position holding 500.
    bench = _per_lot("750")
    order = bench.limit(BTCUSDT, OrderSide.BUY, "1.000", "99.0")
    assert order.status == OrderStatus.ACCEPTED
    assert bench.margin(BTCUSDT) == _money("0")

    bench.trade(BTCUSDT, "99.0", aggressor=AggressorSide.SELLER)

    assert order.status == OrderStatus.REJECTED
    assert bench.position(BTCUSDT) == []
    assert len(bench.events(order, OrderRejected)) == 1

    bench = _per_lot("1000")
    order = bench.limit(BTCUSDT, OrderSide.BUY, "1.000", "99.0")
    bench.trade(BTCUSDT, "99.0", aggressor=AggressorSide.SELLER)

    assert order.status == OrderStatus.FILLED
    assert bench.margin(BTCUSDT) == _money("500")


def test_marketable_zero_reserve_limit_is_judged_at_submission():
    # A flat 750 account, the market at 100: a buy limit of 1 at 101 executes on arrival,
    # judged as a market lot, 1,000 > 750 -> rejected whole, no position.
    bench = _per_lot("750")

    order = bench.limit(BTCUSDT, OrderSide.BUY, "1.000", "101.0")

    assert order.status == OrderStatus.REJECTED
    assert bench.position(BTCUSDT) == []


def test_rejected_limit_consumes_no_liquidity():
    # Pending kinds reserve 0, a market lot 1,000. A flat 750 account, one lot quoted at 99: the
    # buy limit of 1 at 99 is rejected (1,000 > 750) and takes nothing from the level, so a market
    # buy of 0.7 (700 <= 750) fills there whole; had the lot been consumed it would slip to 99.1.
    bench = _Bench(balance="750", model=_PerLotModel(), liquidity_consumption=True)
    bench.mark(BTCUSDT, "100.0")
    bench.quote(BTCUSDT, "100.0", size="1.000")
    limit = bench.limit(BTCUSDT, OrderSide.BUY, "1.000", "99.0")

    bench.quote(BTCUSDT, "99.0", size="1.000")
    assert limit.status == OrderStatus.REJECTED
    market = bench.market(BTCUSDT, OrderSide.BUY, "0.700")

    fills = [(e.last_px, e.last_qty) for e in bench.events(market, OrderFilled)]
    assert fills == [(Price.from_str("99.0"), Quantity.from_str("0.700"))]


def test_stop_adding_no_margin_is_never_refused_at_negative_free_margin():
    # Wallet 45,000, 10 BTC long at 50,000, the mark at 45,000: equity 45,000 - 50,000 = -5,000,
    # the long holds 45,000, free -50,000; a stop sell of 10 releases 45,000, so free plus its
    # release is -5,000. As a market sell of 10 it is wholly absorbed: HELD 45,000 less the
    # instrument's 45,000 adds nothing, and an order adding nothing is never refused -> filled.
    bench = _Bench(balance="50000", model=_NotionalLeverageModel())
    bench.open(BTCUSDT, OrderSide.BUY, "10.000", "50000.0")
    bench.exchange.adjust_account(_money("-5000"))
    stop = bench.stop(BTCUSDT, OrderSide.SELL, "10.000", "45000.0")
    bench.mark(BTCUSDT, "45000.0")

    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

    assert stop.status == OrderStatus.FILLED
    assert bench.rejections() == []
    assert bench.position(BTCUSDT) == []


def test_reserving_limit_fills_as_placed_without_an_admission():
    # Flat, wallet 5,050: a sell limit of 1 at 50,200 reserves 5,020 (free 30). On the 51,000
    # mark a trade at 51,000 gaps through it: it reserved, so it fills at 50,200 as placed; the
    # 1 BTC short valued at the mark holds 5,100 (initial 4,845, maintenance 255); balance
    # 5,050, unrealised 50,200 - 51,000 = -800, equity 4,250.
    bench = _Bench(balance="5050", model=_NotionalLeverageModel())
    bench.mark(BTCUSDT, "50000.0")
    bench.quote(BTCUSDT, "50000.0")
    order = bench.limit(BTCUSDT, OrderSide.SELL, "1.000", "50200.0")
    assert bench.margin(BTCUSDT) == _money("5020")
    bench.mark(BTCUSDT, "51000.0")

    bench.trade(BTCUSDT, "51000.0", aggressor=AggressorSide.BUYER)

    assert order.status == OrderStatus.FILLED
    assert bench.events(order, OrderFilled)[0].last_px == Price.from_str("50200.0")
    assert bench.margins(BTCUSDT) == (_money("4845"), _money("255"))
    assert bench.balance() == _money("5050")
    assert bench.portfolio.equity(BYBIT)[USDT] == _money("4250")


def test_zero_reserve_limit_rejection_publishes_the_unchanged_balances():
    # A flat 750 account: the buy limit at 99 reserved 0 and is rejected at the trade; the
    # account state on the rejection holds no margin, the balance 750 free.
    bench = _per_lot("750")
    order = bench.limit(BTCUSDT, OrderSide.BUY, "1.000", "99.0")
    published = len(bench.account_states)

    bench.trade(BTCUSDT, "99.0", aggressor=AggressorSide.SELLER)

    assert order.status == OrderStatus.REJECTED
    assert len(bench.account_states) == published + 1
    state = bench.account_states[-1]
    assert all(m.initial == _money("0") and m.maintenance == _money("0") for m in state.margins)
    assert state.balances[0].free == _money("750")


def test_untriggered_stop_reserves_nothing_while_it_rests():
    # 1 BTC long (5,000) on a 5,000 wallet: a stop sell of 2 at 45,000 is admitted at placement
    # with nothing to add, and the margin stays 5,000 (initial 4,750, maintenance 250).
    bench = _Bench(balance="5000", model=_NotionalLeverageModel())
    bench.open(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")

    stop = bench.stop(BTCUSDT, OrderSide.SELL, "2.000", "45000.0")

    assert stop.status == OrderStatus.ACCEPTED
    assert bench.margins(BTCUSDT) == (_money("4750"), _money("250"))
    state = bench.account_states[-1]
    assert next(m for m in state.margins if m.instrument_id == BTCUSDT.id).initial == _money("4750")


def test_untriggered_stop_is_charged_where_the_model_reserves_it():
    # A model that reserves resting stops: the stop sell of 2 at 45,000 beside the 1 BTC long
    # has 1 absorbed and 1 * 45,000 / 10 = 4,500 beyond: margin 9,500 while it rests
    # (initial 9,250, maintenance 250).
    bench = _Bench(balance="20000", model=_NotionalLeverageModel(reserves_untriggered=True))
    bench.open(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")

    stop = bench.stop(BTCUSDT, OrderSide.SELL, "2.000", "45000.0")

    assert stop.status == OrderStatus.ACCEPTED
    assert bench.margins(BTCUSDT) == (_money("9250"), _money("250"))


def test_rejected_stop_is_gone_from_the_matching_core():
    # After the flip case rejects the stop, a later trade through its trigger fills nothing
    bench, stop = _flip_case("55000")
    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

    bench.trade(BTCUSDT, "44000.0", aggressor=AggressorSide.SELLER)

    assert bench.exchange.get_open_orders(BTCUSDT.id) == []
    assert bench.events(stop, OrderFilled) == []
    assert bench.position(BTCUSDT) == [(PositionSide.LONG, Quantity.from_str("10.000"))]


def test_rejected_stop_is_never_judged_again():
    # The price returns above the trigger and falls through it again: one rejection only
    bench, stop = _flip_case("55000")
    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

    bench.trade(BTCUSDT, "46000.0", aggressor=AggressorSide.BUYER)
    bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

    assert len(bench.events(stop, OrderRejected)) == 1
    assert len(bench.rejections()) == 1


def test_stop_slipping_its_remainder_fills_whole_on_the_wallet_its_requirement_needs():
    # Flat, wallet 10,000: a buy stop of 2 at 50,000 needs 2 * 50,000 / 10 = 10,000 <= 10,000.
    # A trade of 0.5 at 50,000 fills 0.5 there and slips the 1.5 left to 50,000.1, unrefused.
    bench = _Bench(balance="10000", model=_NotionalLeverageModel())
    bench.mark(BTCUSDT, "50000.0")
    bench.quote(BTCUSDT, "49900.0")
    stop = bench.stop(BTCUSDT, OrderSide.BUY, "2.000", "50000.0")

    bench.trade(BTCUSDT, "50000.0", size="0.500", aggressor=AggressorSide.BUYER)

    fills = [(e.last_px, e.last_qty) for e in bench.events(stop, OrderFilled)]
    assert fills == [
        (Price.from_str("50000.0"), Quantity.from_str("0.500")),
        (Price.from_str("50000.1"), Quantity.from_str("1.500")),
    ]
    assert bench.rejections() == []


def test_stop_admitted_without_liquidity_fills_later_on_less_free_margin():
    # A depth book consuming liquidity; wallet 10,000. Two buy stops of 1 at 50,000 rest; an ask
    # of 1 at 50,000 arrives. The first is judged (5,000 <= free 10,000) and takes the whole
    # level, leaving the long 1 holding 5,000. The second is judged on free 10,000 - 5,000 =
    # 5,000: its 5,000 is admitted, with no liquidity left to fill. The wallet then falls to
    # 9,999 (free 4,999), and an ask of 2 at 50,000 fills the admitted stop, never judged again.
    bench = _Bench(
        balance="10000",
        model=_NotionalLeverageModel(),
        liquidity_consumption=True,
        book_type=BookType.L2_MBP,
    )
    bench.mark(BTCUSDT, "50000.0")
    bench.level(BTCUSDT, OrderSide.BUY, "49800.0", "10.000")
    bench.level(BTCUSDT, OrderSide.SELL, "49900.0", "10.000")
    first = bench.stop(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")
    second = bench.stop(BTCUSDT, OrderSide.BUY, "1.000", "50000.0")
    bench.level(BTCUSDT, OrderSide.SELL, "49900.0", "10.000", action=BookAction.DELETE)
    bench.level(BTCUSDT, OrderSide.SELL, "50000.0", "1.000")
    assert first.status == OrderStatus.FILLED
    assert second.status == OrderStatus.ACCEPTED
    assert second.filled_qty == Quantity.from_str("0.000")
    bench.exchange.adjust_account(_money("-1"))

    bench.level(BTCUSDT, OrderSide.SELL, "50000.0", "2.000", action=BookAction.UPDATE)

    assert second.status == OrderStatus.FILLED
    assert bench.rejections() == []
    assert bench.position(BTCUSDT) == [(PositionSide.LONG, Quantity.from_str("2.000"))]


def test_partly_filled_limit_completes_without_a_second_admission():
    # Pending kinds reserve 0, a market lot 1,000, a position lot 500. Wallet 2,000, a buy
    # limit of 2 at 1,000 (the market at 1,100): the first trade of 1 at 1,000 executes it,
    # judged whole, 2 * 1,000 = 2,000 <= 2,000, filling 1 (held 500).
    bench = _Bench(balance="2000", model=_PerLotModel())
    bench.mark(BTCUSDT, "1000.0")
    bench.quote(BTCUSDT, "1100.0")
    order = bench.limit(BTCUSDT, OrderSide.BUY, "2.000", "1000.0")
    bench.trade(BTCUSDT, "1000.0", size="1.000", aggressor=AggressorSide.SELLER)
    assert order.status == OrderStatus.PARTIALLY_FILLED
    # Equity 2,000 + 1 * (400 - 1,000) = 1,400, free 1,400 - 500 = 900 < the lot left's 1,000
    bench.mark(BTCUSDT, "400.0")

    bench.trade(BTCUSDT, "1000.0", size="1.000", aggressor=AggressorSide.SELLER)

    assert order.status == OrderStatus.FILLED
    assert bench.rejections() == []
    assert bench.position(BTCUSDT) == [(PositionSide.LONG, Quantity.from_str("2.000"))]


def test_limit_execution_is_valued_at_the_touch_not_its_fill_price():
    # A model whose resting limits reserve 0. Flat; a buy limit of 1 at 50,100 rests under a
    # 51,000 market; the market falls to 50,000 and the limit fills at its 50,100. Judged as a
    # market buy at the touch, 50,000 / 10 = 5,000: admitted on 5,000 (at the fill price it
    # would need 5,010), rejected on 4,999.99.
    for balance, status in (("5000", OrderStatus.FILLED), ("4999.99", OrderStatus.REJECTED)):
        bench = _Bench(balance=balance, model=_NotionalLeverageModel(pending_rate_zero=True))
        bench.mark(BTCUSDT, "50000.0")
        bench.quote(BTCUSDT, "51000.0")
        order = bench.limit(BTCUSDT, OrderSide.BUY, "1.000", "50100.0")

        bench.quote(BTCUSDT, "50000.0")

        assert order.status == status


def test_hedging_stop_is_compared_without_a_release():
    # HEDGING, 10 BTC long at 50,000, mark 45,000: the long holds 45,000, equity W - 50,000,
    # free W - 95,000. The stop sell of 20 opens its own short: the larger leg 20 * 45,000 / 10
    # = 90,000, increment 45,000, nothing absorbed. Rejected at W = 139,999.99, filled at
    # 140,000 with both legs open.
    for balance, status in (("139999.99", OrderStatus.REJECTED), ("140000", OrderStatus.FILLED)):
        bench = _Bench(balance=balance, model=_NotionalLeverageModel(), oms_type=OmsType.HEDGING)
        bench.open(BTCUSDT, OrderSide.BUY, "10.000", "50000.0")
        stop = bench.stop(BTCUSDT, OrderSide.SELL, "20.000", "45000.0")
        assert stop.status == OrderStatus.ACCEPTED
        bench.mark(BTCUSDT, "45000.0")

        bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

        assert stop.status == status

    assert sorted(bench.position(BTCUSDT)) == sorted(
        [
            (PositionSide.LONG, Quantity.from_str("10.000")),
            (PositionSide.SHORT, Quantity.from_str("20.000")),
        ],
    )


def test_bar_low_pass_triggers_the_same_admission():
    # The flip case on a 1-minute bar 45,500 / 45,600 / 44,900 / 45,100: the low pass
    # triggers the stop at 45,000, judged as on a trade (room 3,000 < 45,000) -> rejected.
    bench, stop = _flip_case("55000")
    bar_type = BarType(
        BTCUSDT.id,
        BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
        AggregationSource.EXTERNAL,
    )

    bench.exchange.process_bar(
        Bar(
            bar_type=bar_type,
            open=Price.from_str("45500.0"),
            high=Price.from_str("45600.0"),
            low=Price.from_str("44900.0"),
            close=Price.from_str("45100.0"),
            volume=Quantity.from_str("1000.000"),
            ts_event=0,
            ts_init=0,
        ),
    )

    assert stop.status == OrderStatus.REJECTED
    assert bench.position(BTCUSDT) == [(PositionSide.LONG, Quantity.from_str("10.000"))]


def test_nt_margin_models_reserve_resting_stops_and_check_nothing_at_execution():
    account = MarginAccount(
        TestEventStubs.margin_account_state(AccountId("SIM-001")),
        calculate_account_state=True,
    )
    account.set_margin_model(LeveragedMarginModel())

    assert account.reserves_untriggered(BTCUSDT)
    assert not account.checks_at_execution(BTCUSDT, OrderType.STOP_MARKET, OrderSide.SELL)
    assert not account.checks_at_execution(BTCUSDT, OrderType.LIMIT, OrderSide.BUY)


def _flat(balance: str, market: str) -> _Bench:
    bench = _Bench(balance=balance, model=_NotionalLeverageModel())
    bench.mark(BTCUSDT, "50000.0")
    bench.quote(BTCUSDT, market)
    return bench


@pytest.mark.parametrize(("balance", "admitted"), [("4999", False), ("5000", True)])
def test_limit_if_touched_touched_at_submission_is_judged_before_it_is_accepted(balance, admitted):
    # Flat, the market at 50,000: a buy limit-if-touched of 1, trigger 51,000, limit 48,900, is
    # touched on arrival and judged as a market buy at the ask, 50,000 / 10 = 5,000. On 4,999
    # it is rejected, never accepted nor triggered; on 5,000 it rests at 48,900 reserving 4,890.
    bench = _flat(balance, "50000.0")

    order = bench.submit(
        bench.strategy.order_factory.limit_if_touched(
            BTCUSDT.id,
            OrderSide.BUY,
            Quantity.from_str("1.000"),
            Price.from_str("48900.0"),
            Price.from_str("51000.0"),
        ),
    )

    if admitted:
        assert order.status == OrderStatus.TRIGGERED
        assert len(bench.events(order, OrderAccepted)) == 1
        assert len(bench.events(order, OrderTriggered)) == 1
        assert bench.margin(BTCUSDT) == _money("4890")
    else:
        assert order.status == OrderStatus.REJECTED
        assert bench.events(order, OrderAccepted) == []
        assert bench.events(order, OrderTriggered) == []
        assert bench.margin(BTCUSDT) == _money("0")


def _market_if_touched(bench: _Bench):
    # Rests under a 51,000 market, touched when the market falls to its 50,000
    return bench.strategy.order_factory.market_if_touched(
        BTCUSDT.id,
        OrderSide.BUY,
        Quantity.from_str("1.000"),
        Price.from_str("50000.0"),
    )


def _trailing_stop_market(bench: _Bench):
    # Rests over a 49,000 market trailing 1,000 behind it, triggered when it rises to 50,000
    return bench.strategy.order_factory.trailing_stop_market(
        BTCUSDT.id,
        OrderSide.BUY,
        Quantity.from_str("1.000"),
        trailing_offset=Decimal(1000),
        trigger_price=Price.from_str("50000.0"),
        trailing_offset_type=TrailingOffsetType.PRICE,
    )


@pytest.mark.parametrize(
    ("build", "market"),
    [(_market_if_touched, "51000.0"), (_trailing_stop_market, "49000.0")],
)
@pytest.mark.parametrize(("balance", "admitted"), [("4999", False), ("5000", True)])
def test_resting_conditional_market_order_is_judged_at_its_trigger(
    build, market, balance, admitted
):
    # Flat: a buy of 1 rests, then the market moves to 50,000 and triggers it, judged as a market
    # buy at its triggered 50,000, 50,000 / 10 = 5,000. On 4,999: rejected whole, no fill, flat,
    # the book's 1,000 ask intact, the balance 4,999 and no margin. On 5,000: filled, the long
    # holding 5,000.
    bench = _flat(balance, market)
    order = bench.submit(build(bench))
    assert order.status == OrderStatus.ACCEPTED

    bench.quote(BTCUSDT, "50000.0")

    if admitted:
        assert order.status == OrderStatus.FILLED
        assert bench.position(BTCUSDT) == [(PositionSide.LONG, Quantity.from_str("1.000"))]
        assert bench.margin(BTCUSDT) == _money("5000")
    else:
        assert order.status == OrderStatus.REJECTED
        assert bench.events(order, OrderFilled) == []
        assert bench.position(BTCUSDT) == []
        assert bench.exchange.get_open_orders(BTCUSDT.id) == []
        assert bench.exchange.get_book(BTCUSDT.id).best_ask_size() == Quantity.from_str("1000.000")
        assert bench.balance() == _money(balance)
        assert bench.margin(BTCUSDT) == _money("0")


def _limit_if_touched(bench: _Bench):
    # Rests under a 51,000 market, touched when the market falls to its 50,000
    return bench.strategy.order_factory.limit_if_touched(
        BTCUSDT.id,
        OrderSide.BUY,
        Quantity.from_str("1.000"),
        Price.from_str("49900.0"),
        Price.from_str("50000.0"),
    )


def _trailing_stop_limit(bench: _Bench):
    # Rests over a 49,000 market trailing 1,000 behind it with its limit 100 under the
    # trigger, triggered when the market rises to 50,000
    return bench.strategy.order_factory.trailing_stop_limit(
        BTCUSDT.id,
        OrderSide.BUY,
        Quantity.from_str("1.000"),
        limit_offset=Decimal(900),
        trailing_offset=Decimal(1000),
        price=Price.from_str("49900.0"),
        trigger_price=Price.from_str("50000.0"),
        trailing_offset_type=TrailingOffsetType.PRICE,
    )


@pytest.mark.parametrize(
    ("build", "market"),
    [(_limit_if_touched, "51000.0"), (_trailing_stop_limit, "49000.0")],
)
@pytest.mark.parametrize(("balance", "admitted"), [("4999", False), ("5000", True)])
def test_resting_conditional_limit_order_is_judged_at_its_trigger(build, market, balance, admitted):
    # Flat: a buy of 1 with limit 49,900 rests, then the market moves to 50,000 and triggers it,
    # judged as a market buy at its triggered 50,000: 5,000. On 4,999: rejected whole, never
    # triggered, no fill, the balance 4,999 and no margin. On 5,000: triggered, resting at
    # 49,900 (the ask 50,000 above it) reserving 4,990.
    bench = _flat(balance, market)
    order = bench.submit(build(bench))
    assert order.status == OrderStatus.ACCEPTED

    bench.quote(BTCUSDT, "50000.0")

    if admitted:
        assert order.status == OrderStatus.TRIGGERED
        assert bench.margin(BTCUSDT) == _money("4990")
    else:
        assert order.status == OrderStatus.REJECTED
        assert bench.events(order, OrderTriggered) == []
        assert bench.events(order, OrderFilled) == []
        assert bench.exchange.get_open_orders(BTCUSDT.id) == []
        assert bench.balance() == _money(balance)
        assert bench.margin(BTCUSDT) == _money("0")


def test_hedging_stop_naming_its_position_is_credited_that_position_s_release():
    # HEDGING, 10 BTC long at 50,000, mark 45,000: the long holds 45,000, equity W - 50,000,
    # free W - 95,000. A stop sell of 20 naming the long is a market sell of 20 against it:
    # 10 absorbed, 10 * 45,000 / 10 = 45,000 beyond, HELD 90,000 - 45,000 = increment 45,000;
    # the absorbed 10 release the long's 45,000, so room W - 95,000 + 45,000 = W - 50,000.
    # At W = 95,000 the room is 45,000 = 45,000 -> filled: the long closes (realised
    # 10 * (45,000 - 50,000) = -50,000, balance 45,000) and a 10 BTC short opens. At
    # W = 94,999.99 the room is 44,999.99 -> rejected, the long and the balance untouched.
    for balance, filled in (("95000", True), ("94999.99", False)):
        bench = _Bench(balance=balance, model=_NotionalLeverageModel(), oms_type=OmsType.HEDGING)
        bench.open(BTCUSDT, OrderSide.BUY, "10.000", "50000.0")
        long = bench.cache.positions_open(instrument_id=BTCUSDT.id)[0]
        stop = bench.submit(
            bench.strategy.order_factory.stop_market(
                BTCUSDT.id,
                OrderSide.SELL,
                Quantity.from_str("20.000"),
                Price.from_str("45000.0"),
            ),
            position_id=long.id,
        )
        assert stop.status == OrderStatus.ACCEPTED
        bench.mark(BTCUSDT, "45000.0")

        bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)

        if filled:
            assert stop.status == OrderStatus.FILLED
            assert bench.position(BTCUSDT) == [(PositionSide.SHORT, Quantity.from_str("10.000"))]
            assert bench.balance() == _money("45000")
        else:
            assert stop.status == OrderStatus.REJECTED
            assert bench.position(BTCUSDT) == [(PositionSide.LONG, Quantity.from_str("10.000"))]
            assert bench.balance() == _money("94999.99")


def test_bar_admission_reads_equity_at_the_last_mark():
    # Wallet 55,000, both longs, the BTC mark left at 50,000: BTC holds 50,000 and ETH 2,000,
    # total 52,000; equity 55,000, free 3,000. The stop sell of 20 at 45,000, triggered on the
    # bar's low pass, is a market sell of 20 at 45,000: 10 absorbed, 10 * 45,000 / 10 = 45,000
    # beyond, HELD 95,000 - 50,000 = increment 45,000; the absorbed 10 release 50,000: room
    # 53,000 >= 45,000 -> filled, the long closing at 45,000 (realised -50,000, balance 5,000)
    # and a 10 BTC short opening. Equity read at the 45,000 execution price would be 5,000,
    # leaving room below the 45,000.
    bench = _two_longs("55000")
    stop = bench.stop(BTCUSDT, OrderSide.SELL, "20.000", "45000.0")
    assert stop.status == OrderStatus.ACCEPTED
    bar_type = BarType(
        BTCUSDT.id,
        BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
        AggregationSource.EXTERNAL,
    )

    bench.exchange.process_bar(
        Bar(
            bar_type=bar_type,
            open=Price.from_str("45500.0"),
            high=Price.from_str("45600.0"),
            low=Price.from_str("44900.0"),
            close=Price.from_str("45100.0"),
            volume=Quantity.from_str("1000.000"),
            ts_event=0,
            ts_init=0,
        ),
    )

    assert stop.status == OrderStatus.FILLED
    assert bench.position(BTCUSDT) == [(PositionSide.SHORT, Quantity.from_str("10.000"))]
    assert bench.balance() == _money("5000")


def _against_the_long(bench: _Bench, oms_type: OmsType, order) -> object:
    # Under HEDGING the order names the long it closes, as the fill would apply it
    position_id = None
    if oms_type == OmsType.HEDGING:
        position_id = bench.cache.positions_open(instrument_id=BTCUSDT.id)[0].id
    return bench.submit(order, position_id=position_id)


@pytest.mark.parametrize("oms_type", [OmsType.NETTING, OmsType.HEDGING], ids=["netting", "hedging"])
def test_execution_admission_matches_a_market_order_plus_the_absorbed_release(oms_type):
    # 10 BTC long at 50,000, the mark at 45,000, on a wallet W: the long holds 45,000, equity
    # W - 50,000, free W - 95,000. A sell of 20 at 45,000 against the long: 10 absorbed,
    # 10 * 45,000 / 10 = 45,000 beyond, HELD 90,000 - the instrument's 45,000 = increment
    # 45,000, the absorbed 10 releasing the long's 45,000.
    # A market sell of 20 submitted at that moment, the market at 45,000, is refused on its
    # increment against the free margin alone: admitted where 45,000 <= W - 95,000, at
    # W = 140,000, refused at 139,999.99.
    # The stop sell of 20 triggered at 45,000 at that moment is judged on the same 45,000
    # against the free margin plus the release: admitted where 45,000 <= W - 95,000 + 45,000,
    # at W = 95,000, refused at 94,999.99 - the market order's two wallets less the release.
    def market_sell(balance: str):
        bench = _Bench(balance=balance, model=_NotionalLeverageModel(), oms_type=oms_type)
        bench.open(BTCUSDT, OrderSide.BUY, "10.000", "50000.0")
        bench.mark(BTCUSDT, "45000.0")
        bench.quote(BTCUSDT, "45000.0")
        order = bench.strategy.order_factory.market(
            BTCUSDT.id,
            OrderSide.SELL,
            Quantity.from_str("20.000"),
        )
        return _against_the_long(bench, oms_type, order).status

    def triggered_stop_sell(balance: str):
        bench = _Bench(balance=balance, model=_NotionalLeverageModel(), oms_type=oms_type)
        bench.open(BTCUSDT, OrderSide.BUY, "10.000", "50000.0")
        order = bench.strategy.order_factory.stop_market(
            BTCUSDT.id,
            OrderSide.SELL,
            Quantity.from_str("20.000"),
            Price.from_str("45000.0"),
        )
        stop = _against_the_long(bench, oms_type, order)
        assert stop.status == OrderStatus.ACCEPTED
        bench.mark(BTCUSDT, "45000.0")
        bench.trade(BTCUSDT, "45000.0", aggressor=AggressorSide.SELLER)
        return stop.status

    assert market_sell("140000") == OrderStatus.FILLED
    assert market_sell("139999.99") == OrderStatus.REJECTED
    assert triggered_stop_sell("95000") == OrderStatus.FILLED
    assert triggered_stop_sell("94999.99") == OrderStatus.REJECTED
