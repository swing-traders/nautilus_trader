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
Margin valued from an instrument's whole exposure: the account and model declarations, the
accounts manager's two paths, the Portfolio's revaluation and projected read, and the simulated
exchange's free-margin refusals.

Every expected figure is derived by hand in the test from the authored amounts, rates and quotes.
"""

import re
import subprocess
import sys
from decimal import ROUND_HALF_EVEN
from decimal import Decimal
from pathlib import Path

import pytest

from nautilus_trader.accounting.accounts.cash import CashAccount
from nautilus_trader.accounting.accounts.margin import MarginAccount
from nautilus_trader.accounting.exposure import Exposure
from nautilus_trader.accounting.exposure import MarginBasis
from nautilus_trader.accounting.manager import AccountsManager
from nautilus_trader.accounting.margin_models import LeveragedMarginModel
from nautilus_trader.accounting.margin_models import MarginModel
from nautilus_trader.accounting.margin_models import StandardMarginModel
from nautilus_trader.backtest.engine import SimulatedExchange
from nautilus_trader.backtest.execution_client import BacktestExecClient
from nautilus_trader.backtest.models import FeeModel
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.backtest.models import LatencyModel
from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import ExecEngineConfig
from nautilus_trader.config import RiskEngineConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.execution.engine import ExecutionEngine
from nautilus_trader.model.currencies import AUD
from nautilus_trader.model.currencies import BTC
from nautilus_trader.model.currencies import EUR
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.currencies import USDT
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.events import OrderModifyRejected
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import MarginBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import OrderList
from nautilus_trader.model.position import Position
from nautilus_trader.portfolio.config import PortfolioConfig
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.risk.engine import RiskEngine
from nautilus_trader.test_kit.mocks.strategies import MockStrategy
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


SIM = Venue("SIM")


def _pair(
    symbol: str,
    venue: Venue = SIM,
    margin_init: Decimal = Decimal(0),
    margin_maint: Decimal = Decimal(0),
    taker_fee: Decimal = Decimal(0),
) -> CurrencyPair:
    # A pair whose quantity counts lots, so a model's amounts read per lot
    base, quote = symbol.split("/")
    return CurrencyPair(
        instrument_id=InstrumentId(Symbol(symbol), venue),
        raw_symbol=Symbol(symbol),
        base_currency=_CURRENCIES[base],
        quote_currency=_CURRENCIES[quote],
        price_precision=5,
        size_precision=2,
        price_increment=Price.from_str("0.00001"),
        size_increment=Quantity.from_str("0.01"),
        lot_size=None,
        max_quantity=Quantity.from_str("10000000"),
        min_quantity=Quantity.from_str("0.01"),
        max_price=None,
        min_price=None,
        max_notional=None,
        min_notional=None,
        margin_init=margin_init,
        margin_maint=margin_maint,
        maker_fee=Decimal(0),
        taker_fee=taker_fee,
        ts_event=0,
        ts_init=0,
    )


def _qty(lots: str) -> Quantity:
    return Quantity.from_str(f"{Decimal(lots):.2f}")


_CURRENCIES = {"AUD": AUD, "BTC": BTC, "EUR": EUR, "USD": USD, "USDT": USDT}

EURUSD = _pair("EUR/USD")
EURAUD = _pair("EUR/AUD")
EURUSDT = _pair("EUR/USDT")
AUDUSD = _pair("AUD/USD")

_KINDS = {
    (OrderType.MARKET, OrderSide.BUY): "BUY",
    (OrderType.MARKET, OrderSide.SELL): "SELL",
    (OrderType.LIMIT, OrderSide.BUY): "BUY_LIMIT",
    (OrderType.LIMIT, OrderSide.SELL): "SELL_LIMIT",
    (OrderType.STOP_MARKET, OrderSide.BUY): "BUY_STOP",
    (OrderType.STOP_MARKET, OrderSide.SELL): "SELL_STOP",
}


def _rates(
    buy: tuple[str, str] = ("1", "1"),
    sell: tuple[str, str] = ("1", "1"),
    pending: str = "0",
    **overrides: str,
) -> dict[str, tuple[Decimal, Decimal]]:
    # (initial, maintenance) rate per order kind
    rates = {
        "BUY": (Decimal(buy[0]), Decimal(buy[1])),
        "SELL": (Decimal(sell[0]), Decimal(sell[1])),
    }
    for kind in ("BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"):
        rates[kind] = (Decimal(overrides.get(kind, pending)), Decimal(0))
    return rates


class _PerLotModel(MarginModel):
    """
    A declaring test model charging fixed amounts per lot by order kind, as MetaQuotes states
    its netting, hedging and larger-leg books; it records every call it is asked.
    """

    margin_from_exposure = True
    checks_free_margin = True
    buy_price_type = PriceType.ASK
    sell_price_type = PriceType.BID

    def __init__(
        self,
        currency,
        initial: str,
        rates: dict,
        maintenance: str = "0",
        hedged: str = "0",
        larger_leg: bool = False,
        hedging: bool = False,
    ) -> None:
        self.currency = currency
        self.initial = Decimal(initial)
        self.maintenance = Decimal(maintenance)
        self.hedged = Decimal(hedged)
        self.rates = rates
        self.larger_leg = larger_leg
        self.hedging = hedging
        self.calls: list[dict] = []

    def margin_currency(self, instrument):
        return self.currency

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
        self.calls.append(
            {
                "exposure": exposure,
                "basis": basis,
                "leverage": leverage,
                "to_currency": to_currency,
                "xrate_buy": xrate_buy,
                "xrate_sell": xrate_sell,
            },
        )
        if self.currency is None:
            return Money(0, to_currency)

        total = self._book(exposure, xrate_buy, xrate_sell)
        quantum = Decimal(1).scaleb(-to_currency.precision)
        return Money(total.quantize(quantum, rounding=ROUND_HALF_EVEN), to_currency)

    def _book(self, exposure, xrate_buy, xrate_sell) -> Decimal:
        held = self.maintenance if self.maintenance else self.initial
        xrate = {OrderSide.BUY: xrate_buy, OrderSide.SELL: xrate_sell}
        long_lots = exposure.long_quantity.as_decimal() if exposure.long_quantity else Decimal(0)
        short_lots = exposure.short_quantity.as_decimal() if exposure.short_quantity else Decimal(0)

        def order_part(entry) -> Decimal:
            order_type, side, _quantity, leaves, _price = entry
            rate = self.rates[_KINDS[(order_type, side)]][0]
            return leaves.as_decimal() * self.initial * rate * xrate[side]

        if self.larger_leg:
            legs = {
                OrderSide.BUY: long_lots * held * self.rates["BUY"][1] * xrate_buy,
                OrderSide.SELL: short_lots * held * self.rates["SELL"][1] * xrate_sell,
            }
            for entry in exposure.orders:
                legs[entry[1]] += order_part(entry)
            return max(legs.values())

        if self.hedging:
            # Hedging, basic method: uncovered at the larger leg, covered at the hedged amount
            covered = min(long_lots, short_lots)
            mean_rate = (self.rates["BUY"][1] + self.rates["SELL"][1]) / 2
            if long_lots > short_lots:
                uncovered = (long_lots - short_lots) * held * self.rates["BUY"][1] * xrate_buy
            else:
                uncovered = (short_lots - long_lots) * held * self.rates["SELL"][1] * xrate_sell
            book = uncovered + covered * self.hedged * mean_rate * (xrate_buy + xrate_sell) / 2
            return book + sum(
                (self._hedging_order(e, long_lots, short_lots, xrate) for e in exposure.orders),
                Decimal(0),
            )

        if exposure.net_quantity is None:
            return sum((order_part(e) for e in exposure.orders), Decimal(0))

        # Netting (or a single hedging leg): the position, its same-direction orders, and the
        # opposite orders only beyond the position's volume, the larger of the two held
        if exposure.net_side == PositionSide.LONG:
            side, opposite, rate = OrderSide.BUY, OrderSide.SELL, self.rates["BUY"][1]
        else:
            side, opposite, rate = OrderSide.SELL, OrderSide.BUY, self.rates["SELL"][1]
        position = exposure.net_quantity.as_decimal() * held * rate * xrate[side]
        same = sum((order_part(e) for e in exposure.orders if e[1] == side), Decimal(0))
        opposite_orders = [e for e in exposure.orders if e[1] == opposite]
        opposite_lots = sum((e[3].as_decimal() for e in opposite_orders), Decimal(0))
        if opposite_lots <= exposure.net_quantity.as_decimal():
            return position + same
        return max(position, sum((order_part(e) for e in opposite_orders), Decimal(0))) + same

    def _hedging_order(self, entry, long_lots, short_lots, xrate) -> Decimal:
        order_type, side, _quantity, leaves, _price = entry
        lots = leaves.as_decimal()
        if order_type != OrderType.MARKET:
            return lots * self.initial * self.rates[_KINDS[(order_type, side)]][0] * xrate[side]
        # A market order's lots covered by the opposite leg's uncovered volume cost the hedged
        # amount at the mean rate, the rest its kind's initial rate
        if side == OrderSide.BUY:
            open_opposite = max(short_lots - long_lots, Decimal(0))
        else:
            open_opposite = max(long_lots - short_lots, Decimal(0))
        covered = min(lots, open_opposite)
        mean_rate = (self.rates["BUY"][0] + self.rates["SELL"][0]) / 2
        mean_xrate = (xrate[OrderSide.BUY] + xrate[OrderSide.SELL]) / 2
        rate = self.rates[_KINDS[(order_type, side)]][0]
        return (
            covered * self.hedged * mean_rate * mean_xrate
            + (lots - covered) * self.initial * rate * xrate[side]
        )

    def last_held(self) -> Money:
        held = [c for c in self.calls if c["basis"] == MarginBasis.HELD]
        exposure = held[-1]["exposure"]
        return self.calculate_exposure_margin(
            None,
            exposure,
            MarginBasis.HELD,
            Decimal(1),
            held[-1]["to_currency"],
            held[-1]["xrate_buy"],
            held[-1]["xrate_sell"],
        )


class _RecordingLeveragedModel(LeveragedMarginModel):
    # NT's own model with a recorder on the exposure calculation it never declares
    def __init__(self) -> None:
        self.calls: list = []

    def calculate_exposure_margin(self, *args, **kwargs):
        self.calls.append(args)
        raise AssertionError("asked a non-declaring model")


class _ZeroFeeModel(FeeModel):
    def get_commission(self, order, fill_qty, fill_px, instrument):
        return Money(0, instrument.quote_currency)


class _Venue:
    """
    A simulated venue wired to a Portfolio, a strategy and the engines, with zero-spread prices
    so equity is the balance alone.
    """

    def __init__(
        self,
        *,
        balance: str,
        currency=USD,
        instrument: CurrencyPair = EURUSD,
        margin_model: MarginModel | None = None,
        oms_type: OmsType = OmsType.NETTING,
        account_type: AccountType = AccountType.MARGIN,
        default_leverage: Decimal = Decimal(1),
        leverages: dict | None = None,
        frozen: bool = False,
        use_mark_prices: bool = False,
        fee_model: FeeModel | None = None,
        conversion: tuple[str, str] | None = ("0.65000", "0.65020"),
        venue: Venue = SIM,
    ) -> None:
        self.instrument = instrument
        self.clock = TestClock()
        self.trader_id = TestIdStubs.trader_id()
        self.msgbus = MessageBus(trader_id=self.trader_id, clock=self.clock)
        self.cache = TestComponentStubs.cache()
        self.portfolio = Portfolio(
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            config=PortfolioConfig(use_mark_prices=use_mark_prices),
        )
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
        self.fee_model = fee_model or _ZeroFeeModel()
        self.exchange = SimulatedExchange(
            venue=venue,
            oms_type=oms_type,
            account_type=account_type,
            base_currency=currency,
            starting_balances=[Money(Decimal(balance), currency)],
            default_leverage=default_leverage,
            # The runner sets a leverage for every traded instrument of a margin venue
            leverages=(
                ({instrument.id: default_leverage} if leverages is None else leverages)
                if account_type == AccountType.MARGIN
                else {}
            ),
            modules=[],
            fill_model=FillModel(),
            fee_model=self.fee_model,
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            latency_model=LatencyModel(0),
            margin_model=margin_model,
            frozen_account=frozen,
        )
        self.exchange.add_instrument(instrument)
        self.exec_client = BacktestExecClient(
            exchange=self.exchange,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            frozen_account=frozen,
        )
        self.exec_engine.register_client(self.exec_client)
        self.exchange.register_client(self.exec_client)
        self.cache.add_instrument(instrument)
        if venue == SIM:
            self.cache.add_instrument(AUDUSD)

        self.strategy = MockStrategy(bar_type=TestDataStubs.bartype_usdjpy_1min_bid())
        self.strategy.register(
            trader_id=self.trader_id,
            portfolio=self.portfolio,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
        )
        self.exchange.reset()
        self.data_engine.start()
        self.exec_engine.start()
        self.strategy.start()

        if conversion is not None:
            self.quote(AUDUSD, conversion[0], conversion[1])
        self.price("1.10000")

    @property
    def account(self):
        return self.exchange.get_account()

    def quote(self, instrument: CurrencyPair, bid: str, ask: str) -> None:
        tick = QuoteTick(
            instrument_id=instrument.id,
            bid_price=Price.from_str(bid),
            ask_price=Price.from_str(ask),
            bid_size=Quantity.from_str("10000000.00"),
            ask_size=Quantity.from_str("10000000.00"),
            ts_event=0,
            ts_init=0,
        )
        if instrument.id in self.exchange.get_matching_engines():
            self.exchange.process_quote_tick(tick)
        self.data_engine.process(tick)

    def price(self, price: str) -> None:
        self.quote(self.instrument, price, price)

    def submit(self, order) -> None:
        self.strategy.submit_order(order)
        self.exchange.process(0)

    def market(self, side: OrderSide, lots: str, reduce_only: bool = False):
        order = self.strategy.order_factory.market(
            self.instrument.id,
            side,
            _qty(lots),
            reduce_only=reduce_only,
        )
        self.submit(order)
        return order

    def limit(self, side: OrderSide, lots: str, price: str, reduce_only: bool = False):
        order = self.strategy.order_factory.limit(
            self.instrument.id,
            side,
            _qty(lots),
            Price.from_str(price),
            reduce_only=reduce_only,
        )
        self.submit(order)
        return order

    def stop(self, side: OrderSide, lots: str, trigger: str):
        order = self.strategy.order_factory.stop_market(
            self.instrument.id,
            side,
            _qty(lots),
            Price.from_str(trigger),
        )
        self.submit(order)
        return order

    def modify(self, order, lots: str) -> None:
        self.strategy.modify_order(order, quantity=_qty(lots))
        self.exchange.process(0)

    def margin(self) -> Money:
        init = self.account.margin_init(self.instrument.id)
        maint = self.account.margin_maint(self.instrument.id)
        return Money(init.as_decimal() + maint.as_decimal(), init.currency)

    def rejections(self) -> list:
        return [
            e for e in self.strategy.store if isinstance(e, OrderRejected | OrderModifyRejected)
        ]


def _f1_model(**kwargs) -> _PerLotModel:
    # 100,000 AUD per lot, market kinds at 0.002, pending kinds at 0: 200 AUD per market lot
    rates = kwargs.pop("rates", _rates(buy=("0.002", "0.002"), sell=("0.002", "0.002")))
    return _PerLotModel(AUD, initial="100000", hedged="100000", rates=rates, **kwargs)


def _f7_model(currency=USD) -> _PerLotModel:
    # MetaQuotes' fixed-margin example: initial 1,000, maintenance 500, hedged 500, rates 1
    return _PerLotModel(currency, initial="1000", maintenance="500", hedged="500", rates=_rates())


def _margin_account(
    model: MarginModel | None = None, leverage: Decimal = Decimal(12)
) -> MarginAccount:
    account = MarginAccount(
        AccountState(
            account_id=AccountId("SIM-001"),
            account_type=AccountType.MARGIN,
            base_currency=USD,
            reported=True,
            balances=[AccountBalance(Money(1_000_000, USD), Money(0, USD), Money(1_000_000, USD))],
            margins=[],
            info={},
            event_id=UUID4(),
            ts_event=0,
            ts_init=0,
        ),
        calculate_account_state=True,
    )
    account.set_default_leverage(leverage)
    if model is not None:
        account.set_margin_model(model)
    return account


def _manager_with_rate() -> tuple[AccountsManager, object]:
    cache = TestComponentStubs.cache()
    cache.add_instrument(EURAUD)
    cache.add_instrument(AUDUSD)
    cache.add_quote_tick(
        QuoteTick(
            instrument_id=AUDUSD.id,
            bid_price=Price.from_str("0.65000"),
            ask_price=Price.from_str("0.65020"),
            bid_size=Quantity.from_str("10000000.00"),
            ask_size=Quantity.from_str("10000000.00"),
            ts_event=0,
            ts_init=0,
        ),
    )
    return AccountsManager(cache=cache, logger=Logger("AccountsManager"), clock=TestClock()), cache


def _partially_filled_buy_limit(instrument, lots: str, filled: str):
    order = _order_factory().limit(
        instrument.id,
        OrderSide.BUY,
        _qty(lots),
        Price.from_str("1.00000"),
    )
    order.apply(TestEventStubs.order_submitted(order))
    order.apply(TestEventStubs.order_accepted(order))
    order.apply(
        TestEventStubs.order_filled(
            order, instrument, last_qty=_qty(filled), last_px=Price.from_str("1.00000")
        )
    )
    return order


def _order_factory():
    from nautilus_trader.common.factories import OrderFactory

    return OrderFactory(
        trader_id=TestIdStubs.trader_id(), strategy_id=TestIdStubs.strategy_id(), clock=TestClock()
    )


def _position(instrument, side: OrderSide, lots: str, price: str, position_id: str) -> Position:
    order = _order_factory().market(instrument.id, side, _qty(lots))
    fill = TestEventStubs.order_filled(
        order,
        instrument,
        position_id=PositionId(position_id),
        last_px=Price.from_str(price),
    )
    return Position(instrument, fill)


# -- NT's own models keep today's figures ---------------------------------------------------------


@pytest.mark.parametrize("model", [StandardMarginModel(), LeveragedMarginModel()])
def test_nt_models_reserve_nothing_on_a_covered_pair(model):
    # One lot long and one short net to nothing, so today's maintenance pass reserves nothing
    manager, _ = _manager_with_rate()
    account = _margin_account(model)
    positions = [
        _position(EURAUD, OrderSide.BUY, "100000", "1.00000", "P-1"),
        _position(EURAUD, OrderSide.SELL, "100000", "1.00000", "P-2"),
    ]

    assert manager.update_positions(account, EURAUD, positions, 0)

    assert account.margin_maint(EURAUD.id) is None


@pytest.mark.parametrize(
    ("model", "expected"),
    [
        # 100,000 x 1.00000 x 0.03 = 3,000 AUD, at the BID 0.65 = 1,950.00 USD
        (StandardMarginModel(), Money("1950.00", USD)),
        # 100,000 x 1.00000 / 12 x 0.03 = 250 AUD, at the BID 0.65 = 162.50 USD
        (LeveragedMarginModel(), Money("162.50", USD)),
    ],
)
def test_nt_models_reserve_the_net_position(model, expected):
    manager, _ = _manager_with_rate()
    instrument = _pair("EUR/AUD", margin_init=Decimal("0.03"), margin_maint=Decimal("0.03"))
    account = _margin_account(model)
    positions = [_position(instrument, OrderSide.BUY, "100000", "1.00000", "P-1")]

    assert manager.update_positions(account, instrument, positions, 0)

    assert account.margin_maint(instrument.id) == expected


def test_nt_models_reserve_a_partially_filled_buy_on_its_total_quantity_at_the_bid():
    # 150,000 units, 50,000 filled: 150,000 x 1.00000 / 12 x 0.03 = 375 AUD, 250 per 100,000
    # on the total quantity, at the BID 0.65 = 243.75 USD
    manager, _ = _manager_with_rate()
    instrument = _pair("EUR/AUD", margin_init=Decimal("0.03"), margin_maint=Decimal("0.03"))
    account = _margin_account(LeveragedMarginModel())
    order = _partially_filled_buy_limit(instrument, "150000", "50000")

    assert manager.update_orders(account, instrument, [order], 0)

    assert account.margin_init(instrument.id) == Money("243.75", USD)


def test_nt_models_convert_a_sell_order_margin_at_the_ask():
    # 100,000 sell limit: 100,000 x 1.00000 / 12 x 0.03 = 250 AUD, at the ASK 0.6502 = 162.55 USD
    manager, _ = _manager_with_rate()
    instrument = _pair("EUR/AUD", margin_init=Decimal("0.03"), margin_maint=Decimal("0.03"))
    account = _margin_account(LeveragedMarginModel())
    order = _order_factory().limit(
        instrument.id, OrderSide.SELL, _qty("100000"), Price.from_str("1.00000")
    )
    order.apply(TestEventStubs.order_submitted(order))
    order.apply(TestEventStubs.order_accepted(order))

    assert manager.update_orders(account, instrument, [order], 0)

    assert account.margin_init(instrument.id) == Money("162.55", USD)


def test_nt_models_rewrite_nothing_on_a_conversion_quote():
    # 100,000 EUR/AUD long at 1.0 under NT's leveraged model at leverage 1: 100,000 x 1.0 x
    # 0.03 = 3,000 AUD at the BID 0.65 = 1,950.00 USD; an AUD/USD quote at 0.70 leaves it there
    instrument = _pair("EUR/AUD", margin_init=Decimal("0.03"), margin_maint=Decimal("0.03"))
    venue = _Venue(balance="1000000", instrument=instrument, margin_model=LeveragedMarginModel())
    venue.price("1.00000")
    venue.market(OrderSide.BUY, "100000")
    assert venue.account.margin_maint(instrument.id) == Money("1950.00", USD)

    venue.quote(AUDUSD, "0.70000", "0.70020")

    assert venue.account.margin_maint(instrument.id) == Money("1950.00", USD)


# -- The declaration selects the path -------------------------------------------------------------


def test_declaring_model_is_asked_for_exposure_margin():
    model = _f1_model()
    venue = _Venue(balance="10000", margin_model=model)

    venue.market(OrderSide.BUY, "1")

    assert {c["basis"] for c in model.calls} == {MarginBasis.HELD, MarginBasis.MAINTENANCE}


def test_non_declaring_model_is_never_asked_for_exposure_margin():
    model = _RecordingLeveragedModel()
    venue = _Venue(balance="1000000", margin_model=model)

    venue.limit(OrderSide.BUY, "1", "1.00000")
    venue.market(OrderSide.BUY, "1")

    assert model.calls == []
    assert venue.rejections() == []


# -- Conversion -----------------------------------------------------------------------------------


def test_buy_margin_converts_at_the_ask_and_sell_margin_at_the_bid():
    # 1 lot long holds 200 AUD at the ASK 0.6502 = 130.04 USD; 1 lot short at the BID 0.65 =
    # 130.00 USD
    long_venue = _Venue(balance="10000", margin_model=_f1_model())
    long_venue.market(OrderSide.BUY, "1")
    short_venue = _Venue(balance="10000", margin_model=_f1_model())
    short_venue.market(OrderSide.SELL, "1")

    assert long_venue.account.margin_maint(EURUSD.id) == Money("130.04", USD)
    assert short_venue.account.margin_maint(EURUSD.id) == Money("130.00", USD)


def test_market_buy_beyond_free_margin_by_one_cent_is_refused_then_admitted_at_the_figure():
    # Flat, a 1 lot market buy needs 200 AUD at the ASK 0.6502 = 130.04 USD
    refused = _Venue(balance="130.03", margin_model=_f1_model())
    refused.market(OrderSide.BUY, "1")
    admitted = _Venue(balance="130.04", margin_model=_f1_model())
    admitted.market(OrderSide.BUY, "1")

    assert [r.reason.split(":")[0] for r in refused.rejections()] == ["INSUFFICIENT_FREE_MARGIN"]
    assert admitted.rejections() == []
    assert admitted.margin() == Money("130.04", USD)


def test_larger_leg_is_chosen_after_conversion():
    # 1,000 lots short at 1,000 AUD per lot: 1,000,000 AUD at the BID 0.65 = 650,000.00 USD;
    # a 999.8 lot buy makes the long leg 999,800 AUD at the ASK 0.6502 = 650,069.96 USD, the
    # larger though smaller in AUD: increment 69.96
    def venue(free: str) -> _Venue:
        model = _PerLotModel(
            AUD, initial="1000", maintenance="1000", rates=_rates(), larger_leg=True
        )
        v = _Venue(
            balance=str(Decimal("650000.00") + Decimal(free)),
            margin_model=model,
            oms_type=OmsType.HEDGING,
        )
        v.market(OrderSide.SELL, "1000")
        assert v.margin() == Money("650000.00", USD)
        v.market(OrderSide.BUY, "999.8")
        return v

    refused = venue("69.95")
    admitted = venue("69.96")

    assert len(refused.rejections()) == 1
    assert admitted.rejections() == []
    assert admitted.margin() == Money("650069.96", USD)


def test_figure_lands_in_the_account_currency_rounded_once():
    # Two 0.01 lot buy limits at a 0.025 limit rate: 25 AUD each, 50 AUD at the ASK 0.6502 =
    # 32.51 USD (rounding each order first would give 16.26 + 16.26 = 32.52)
    model = _f1_model(
        rates=_rates(buy=("0.002", "0.002"), sell=("0.002", "0.002"), BUY_LIMIT="0.025")
    )
    venue = _Venue(balance="10000", margin_model=model)

    venue.limit(OrderSide.BUY, "0.01", "1.00000")
    venue.limit(OrderSide.BUY, "0.01", "1.00000")

    assert venue.account.margin_init(EURUSD.id) == Money("32.51", USD)


def test_model_receives_decimal_rates():
    model = _f1_model()
    venue = _Venue(balance="10000", margin_model=model)

    venue.market(OrderSide.BUY, "1")

    assert model.calls
    assert all(
        type(c["xrate_buy"]) is Decimal and type(c["xrate_sell"]) is Decimal for c in model.calls
    )
    assert (model.calls[-1]["xrate_buy"], model.calls[-1]["xrate_sell"]) == (
        Decimal("0.6502"),
        Decimal("0.65"),
    )


# -- What the model is handed ---------------------------------------------------------------------


def test_model_sees_both_sides_each_order_and_the_mark():
    model = _f1_model()
    venue = _Venue(balance="100000", margin_model=model, oms_type=OmsType.HEDGING)
    venue.market(OrderSide.BUY, "1")
    venue.market(OrderSide.SELL, "0.5")
    venue.cache.add_mark_price(MarkPriceUpdate(EURUSD.id, Price.from_str("1.10010"), 0, 0))

    venue.limit(OrderSide.BUY, "0.3", "1.00000")

    exposure = [c for c in model.calls if c["basis"] == MarginBasis.HELD][-1]["exposure"]
    assert (exposure.long_quantity, exposure.long_price) == (
        Quantity.from_str("1.00"),
        Decimal("1.1"),
    )
    assert (exposure.short_quantity, exposure.short_price) == (
        Quantity.from_str("0.50"),
        Decimal("1.1"),
    )
    assert exposure.orders == (
        (
            OrderType.LIMIT,
            OrderSide.BUY,
            Quantity.from_str("0.30"),
            Quantity.from_str("0.30"),
            Price.from_str("1.00000"),
        ),
    )
    assert exposure.mark == Price.from_str("1.10010")


class _OpenPriceModel(MarginModel):
    # A declaring model valuing each side's positions at their open price: lots x price x
    # 100,000 x 0.002 in USD
    margin_from_exposure = True
    checks_free_margin = True

    def margin_currency(self, instrument):
        return USD

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
        value = Decimal(0)
        if exposure.long_quantity is not None:
            value += exposure.long_quantity.as_decimal() * exposure.long_price
        if exposure.short_quantity is not None:
            value += exposure.short_quantity.as_decimal() * exposure.short_price
        return Money(
            (value * Decimal(100_000) * Decimal("0.002")).quantize(Decimal("0.01")), to_currency
        )


def test_same_side_positions_value_at_their_exact_open_prices():
    # 1 lot bought at 1.10000 and 2 at 1.10001 are valued (1 x 1.10000 + 2 x 1.10001) x 200 =
    # 660.004 = 660.00 USD, the per-position sum; their average 3.30002 / 3 rounded to the tick
    # (1.10001) would give 3 x 1.10001 x 200 = 660.006 = 660.01
    venue = _Venue(balance="100000", margin_model=_OpenPriceModel(), oms_type=OmsType.HEDGING)
    venue.market(OrderSide.BUY, "1")
    venue.price("1.10001")
    venue.market(OrderSide.BUY, "2")

    exposure = venue.exchange.exposure(EURUSD.id)

    assert exposure.long_price == Decimal("3.30002") / 3
    assert venue.margin() == Money("660.00", USD)


def test_netting_position_filled_twice_values_at_its_exact_average_open_price():
    # One position filled 1 lot at 1.10000 and 2 at 1.10001 opens at (1.1 + 2.20002) / 3, valued
    # 3.30002 x 200 = 660.004 = 660.00 USD; its tick-rounded average 1.10001 would give 660.01
    venue = _Venue(balance="100000", margin_model=_OpenPriceModel())
    venue.market(OrderSide.BUY, "1")
    venue.price("1.10001")
    venue.market(OrderSide.BUY, "2")

    exposure = venue.exchange.exposure(EURUSD.id)

    assert len(venue.cache.positions_open()) == 1
    assert exposure.long_price == Decimal("3.30002") / 3
    assert venue.margin() == Money("660.00", USD)


class _BaseCurrencyFeeModel(FeeModel):
    # Charges 0.01 EUR a fill, which the position takes off its quantity
    def get_commission(self, order, fill_qty, fill_px, instrument):
        return Money("0.01", EUR)


def test_quantity_adjusted_position_values_at_its_exact_average_open_price():
    # A 1 lot buy at 1.10000 charged 0.01 EUR holds 0.99 at 1.1: 0.99 x 1.1 x 200 = 217.80.
    # A 1 lot buy at 1.10001 averages in at (0.99 x 1.1 + 1.10001) / 1.99 = 2.18901 / 1.99 and
    # its fee leaves 1.98: 1.98 x 2.18901 / 1.99 x 200 = 435.60199 = 435.60. A 0.5 lot sell
    # keeps the average and with its fee leaves 1.47: 1.47 x 2.18901 / 1.99 x 200 = 323.40
    venue = _Venue(
        balance="100000", margin_model=_OpenPriceModel(), fee_model=_BaseCurrencyFeeModel()
    )
    venue.market(OrderSide.BUY, "1")
    exposure = venue.exchange.exposure(EURUSD.id)
    assert (exposure.long_quantity, exposure.long_price) == (_qty("0.99"), Decimal("1.1"))
    assert venue.margin() == Money("217.80", USD)

    venue.price("1.10001")
    venue.market(OrderSide.BUY, "1")
    exposure = venue.exchange.exposure(EURUSD.id)
    assert (exposure.long_quantity, exposure.long_price) == (
        _qty("1.98"),
        Decimal("2.18901") / Decimal("1.99"),
    )
    assert venue.margin() == Money("435.60", USD)

    venue.market(OrderSide.SELL, "0.5")
    exposure = venue.exchange.exposure(EURUSD.id)
    assert (exposure.long_quantity, exposure.long_price) == (
        _qty("1.47"),
        Decimal("2.18901") / Decimal("1.99"),
    )
    assert venue.margin() == Money("323.40", USD)


class _BtcFeeModel(FeeModel):
    # Charges a fixed BTC fee finer than the pair's 0.01 quantity step
    def __init__(self, fee: str) -> None:
        self.fee = Money(fee, BTC)

    def get_commission(self, order, fill_qty, fill_px, instrument):
        return self.fee


@pytest.mark.parametrize(
    ("lots", "fee", "held", "margin"),
    [
        # 1.0 - 0.005 is the double 0.99499999..., which rounds to 0.99 at two places (exact
        # half-even would give 1.00): 0.99 x 1.1 x 200 = 217.80
        ("1", "0.005", "0.99", "217.80"),
        # 2.0 - 0.015 is the double 1.98500000...098, which rounds to 1.99 (exact half-even
        # would give 1.98): 1.99 x 1.1 x 200 = 437.80
        ("2", "0.015", "1.99", "437.80"),
    ],
)
def test_fee_finer_than_the_quantity_step_leaves_the_quantity_the_position_holds(
    lots, fee, held, margin
):
    btc = _pair("BTC/USD")
    venue = _Venue(
        balance="100000",
        instrument=btc,
        margin_model=_OpenPriceModel(),
        fee_model=_BtcFeeModel(fee),
    )

    venue.market(OrderSide.BUY, lots)

    exposure = venue.exchange.exposure(btc.id)
    assert venue.cache.positions_open()[0].quantity == _qty(held)
    assert (exposure.long_quantity, exposure.long_price) == (_qty(held), Decimal("1.1"))
    assert venue.margin() == Money(margin, USD)


class _NotionalOrderModel(MarginModel):
    # A declaring model charging each order its working lots x price x 1,000 in USD, and
    # nothing for positions
    margin_from_exposure = True
    checks_free_margin = True

    def margin_currency(self, instrument):
        return USD

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
        value = sum(
            (
                leaves.as_decimal() * price.as_decimal() * 1000
                for _, _, _, leaves, price in exposure.orders
            ),
            Decimal(0),
        )
        return Money(value.quantize(Decimal("0.01")), to_currency)


def test_modify_raising_the_price_beyond_free_margin_is_refused():
    # A resting 1 lot buy limit at 1.00000 holds 1,000 USD of 1,050 (free 50); moved to 1.09000
    # it would hold 1,090, an increment of 90 > 50: refused, the order stays at 1.00000
    venue = _Venue(balance="1050", margin_model=_NotionalOrderModel())
    order = venue.limit(OrderSide.BUY, "1", "1.00000")
    assert venue.margin() == Money("1000.00", USD)

    venue.strategy.modify_order(order, price=Price.from_str("1.09000"))
    venue.exchange.process(0)

    assert [r.reason.split(":")[0] for r in venue.rejections()] == ["INSUFFICIENT_FREE_MARGIN"]
    assert isinstance(venue.rejections()[0], OrderModifyRejected)
    assert order.price == Price.from_str("1.00000")
    assert venue.margin() == Money("1000.00", USD)


def test_model_sees_the_order_under_admission():
    model = _f1_model()
    venue = _Venue(balance="100000", margin_model=model)
    order = venue.strategy.order_factory.market(EURUSD.id, OrderSide.BUY, _qty("2"))

    exposure = venue.exchange.exposure_after(EURUSD.id, order)

    assert exposure.orders == (
        (
            OrderType.MARKET,
            OrderSide.BUY,
            Quantity.from_str("2.00"),
            Quantity.from_str("2.00"),
            Price.from_str("1.10000"),
        ),
    )


def test_partially_filled_order_contributes_its_working_quantity():
    # AUD account, a 1.5 lot buy limit 0.5 filled at a 0.004 limit rate: the working 1 lot x
    # 100,000 x 0.004 = 400 AUD (the total would give 600)
    manager, _ = _manager_with_rate()
    model = _f1_model(
        rates=_rates(buy=("0.002", "0.002"), sell=("0.002", "0.002"), BUY_LIMIT="0.004")
    )
    account = MarginAccount(
        AccountState(
            account_id=AccountId("SIM-001"),
            account_type=AccountType.MARGIN,
            base_currency=AUD,
            reported=True,
            balances=[AccountBalance(Money(100_000, AUD), Money(0, AUD), Money(100_000, AUD))],
            margins=[],
            info={},
            event_id=UUID4(),
            ts_event=0,
            ts_init=0,
        ),
        calculate_account_state=True,
    )
    account.set_margin_model(model)
    account.set_leverage(EURAUD.id, Decimal(1))
    order = _partially_filled_buy_limit(EURAUD, "1.5", "0.5")

    assert manager.update_orders(account, EURAUD, [order], 0)

    assert account.margin_init(EURAUD.id) == Money("400.00", AUD)


# -- How the figure lands -------------------------------------------------------------------------


def test_instrument_balance_sums_to_held_after_order_and_position_events():
    # F-7 netting: Buy 1 holds 500; a resting 1 lot sell stop adds nothing (its kind's rate 0):
    # initial 0, maintenance 500
    model = _f7_model()
    venue = _Venue(balance="10000", margin_model=model)
    venue.market(OrderSide.BUY, "1")
    venue.account.update_balances([AccountBalance(Money(750, USD), Money(0, USD), Money(750, USD))])
    assert venue.margin() == model.last_held()

    venue.stop(OrderSide.SELL, "1", "1.05000")

    assert venue.account.margin_init(EURUSD.id) == Money("0.00", USD)
    assert venue.account.margin_maint(EURUSD.id) == Money("500.00", USD)
    assert venue.margin() == model.last_held()
    assert venue.rejections() == []


def test_portfolio_exposure_margin_equals_the_written_balance():
    venue = _Venue(balance="100000", margin_model=_f1_model(hedging=True), oms_type=OmsType.HEDGING)
    venue.market(OrderSide.BUY, "1")
    venue.market(OrderSide.SELL, "0.4")

    projected = venue.portfolio.exposure_margin(
        SIM, EURUSD.id, venue.exchange.exposure(EURUSD.id), MarginBasis.HELD
    )

    assert projected == venue.margin()


def test_position_event_reprices_a_resting_stop_left_beyond_a_reduced_position():
    # F-7 netting: Buy 2 holds 1,000 with a resting 2 lot sell stop; a 1.5 lot sell leaves
    # Buy 0.5: maintenance 0.5 x 500 = 250, initial max(250, 2 x 1,000 x 0) - 250 = 0
    venue = _Venue(balance="10000", margin_model=_f7_model())
    venue.market(OrderSide.BUY, "2")
    venue.stop(OrderSide.SELL, "2", "1.05000")
    assert venue.account.margin_maint(EURUSD.id) == Money("1000.00", USD)

    venue.market(OrderSide.SELL, "1.5")

    assert venue.account.margin_maint(EURUSD.id) == Money("250.00", USD)
    assert venue.account.margin_init(EURUSD.id) == Money("0.00", USD)


def test_conversion_quote_rewrites_cross_currency_margin_in_mark_price_mode():
    # 2,000 AUD per lot at AUD/USD 0.95 = 1,900 USD; the quote moving to 1.00 re-writes 2,000
    model = _PerLotModel(AUD, initial="2000", rates=_rates())
    venue = _Venue(
        balance="3000", margin_model=model, use_mark_prices=True, conversion=("0.95000", "0.95000")
    )
    venue.market(OrderSide.BUY, "1")
    assert venue.margin() == Money("1900.00", USD)

    venue.quote(AUDUSD, "1.00000", "1.00000")

    assert venue.margin() == Money("2000.00", USD)


def test_definition_without_margin_currency_writes_zero_without_a_rate():
    # No conversion quote exists at all, yet both balances are written as zero
    for currency, instrument, venue_id in (
        (USD, EURUSD, SIM),
        (USDT, _pair("EUR/USDT", Venue("ALT")), Venue("ALT")),
    ):
        venue = _Venue(
            balance="100",
            currency=currency,
            instrument=instrument,
            margin_model=_PerLotModel(None, initial="0", rates=_rates()),
            conversion=None,
            venue=venue_id,
        )
        venue.market(OrderSide.BUY, "1")

        assert venue.account.margin_init(instrument.id) == Money(0, currency)
        assert venue.account.margin_maint(instrument.id) == Money(0, currency)
        assert venue.rejections() == []


def test_fill_pnl_conversion_is_unchanged():
    # 100,000 EUR/AUD bought at 1.10000 and sold at 1.10100 realize 100,000 x 0.001 = 100 AUD,
    # booked at the BID 0.65 of the SELL fill: 65.00 USD
    instrument = _pair("EUR/AUD")
    model = _PerLotModel(None, initial="0", rates=_rates())
    venue = _Venue(balance="100000", instrument=instrument, margin_model=model)
    venue.market(OrderSide.BUY, "100000")
    venue.price("1.10100")

    venue.market(OrderSide.SELL, "100000")

    assert venue.account.balance_total(USD) == Money("100065.00", USD)


# -- Refusals -------------------------------------------------------------------------------------


def test_hedged_order_beyond_free_margin_is_rejected_with_insufficient_free_margin():
    # A 1 lot short held at 1,000; a 1 lot buy is covered at the hedged 2,000 per lot: increment
    # 2,000 > free 1,500 - 1,000 = 500
    model = _PerLotModel(
        USD, initial="1000", maintenance="1000", hedged="2000", rates=_rates(), hedging=True
    )
    venue = _Venue(balance="1500", margin_model=model, oms_type=OmsType.HEDGING)
    venue.market(OrderSide.SELL, "1")

    order = venue.market(OrderSide.BUY, "1")

    assert order.status == OrderStatus.REJECTED
    assert venue.rejections()[0].reason.split(":")[0] == "INSUFFICIENT_FREE_MARGIN"


def test_zero_margin_buy_limit_is_admitted_with_little_free_margin():
    # AUD account with 300 AUD free: a 1 lot buy limit's kind charges 0
    venue = _Venue(balance="300", currency=AUD, instrument=EURAUD, margin_model=_f1_model())

    order = venue.limit(OrderSide.BUY, "1", "1.00000")

    assert order.status == OrderStatus.ACCEPTED
    assert venue.rejections() == []


def test_amend_adding_no_margin_is_accepted():
    # A resting 1 lot buy limit amended to 3 lots with 50 AUD free: the limit kind charges 0
    venue = _Venue(balance="50", currency=AUD, instrument=EURAUD, margin_model=_f1_model())
    order = venue.limit(OrderSide.BUY, "1", "0.99000")

    venue.modify(order, "3")

    assert order.quantity == Quantity.from_str("3.00")
    assert venue.rejections() == []


def test_larger_leg_book_holds_the_larger_leg():
    # 1 long lot, 0.8 short lot and a 0.4 lot sell stop: max(1,000, 800 + 0) = 1,000
    model = _PerLotModel(
        USD, initial="1000", maintenance="1000", hedged="2000", rates=_rates(), larger_leg=True
    )
    venue = _Venue(balance="10000", margin_model=model, oms_type=OmsType.HEDGING)
    venue.market(OrderSide.BUY, "1")
    venue.market(OrderSide.SELL, "0.8")

    venue.stop(OrderSide.SELL, "0.4", "1.05000")

    assert venue.margin() == Money("1000.00", USD)


def test_order_charged_at_its_initial_rate_is_refused_then_admitted():
    # AUD account, BUY initial rate 0.004 and maintenance 0.002: a 1 lot market buy needs 400
    # AUD; refused with 300 free, admitted with 400 and held at 200 after
    rates = _rates(buy=("0.004", "0.002"), sell=("0.002", "0.002"))
    refused = _Venue(
        balance="300", currency=AUD, instrument=EURAUD, margin_model=_f1_model(rates=rates)
    )
    refused.market(OrderSide.BUY, "1")
    admitted = _Venue(
        balance="400", currency=AUD, instrument=EURAUD, margin_model=_f1_model(rates=rates)
    )
    admitted.market(OrderSide.BUY, "1")

    assert len(refused.rejections()) == 1
    assert admitted.rejections() == []
    assert admitted.margin() == Money("200.00", AUD)


def test_unflagged_netting_close_is_accepted_at_positive_free_margin():
    # 1 long lot holds 1,000 at equity 1,500 (free 500); the unflagged sell adds nothing
    model = _PerLotModel(USD, initial="1000", maintenance="1000", hedged="2000", rates=_rates())
    venue = _Venue(balance="1500", margin_model=model)
    venue.market(OrderSide.BUY, "1")

    venue.market(OrderSide.SELL, "1")

    assert venue.rejections() == []
    assert venue.cache.positions_open() == []
    assert venue.margin() == Money("0.00", USD)


def test_unflagged_netting_close_is_accepted_at_negative_free_margin_while_an_add_is_refused():
    # Equity 750 with 1,000 held (free -250): a 0.5 lot buy adds 500 > -250 and is refused;
    # the unflagged sell adds nothing and is accepted
    model = _PerLotModel(USD, initial="1000", maintenance="1000", hedged="2000", rates=_rates())
    venue = _Venue(balance="10000", margin_model=model)
    venue.market(OrderSide.BUY, "1")
    venue.account.update_balances([AccountBalance(Money(750, USD), Money(0, USD), Money(750, USD))])

    add = venue.market(OrderSide.BUY, "0.5")
    close = venue.market(OrderSide.SELL, "1")

    assert add.status == OrderStatus.REJECTED
    assert close.status == OrderStatus.FILLED
    assert len(venue.rejections()) == 1
    assert venue.margin() == Money("0.00", USD)


def test_reduce_only_order_passes_with_zero_free_margin():
    # 1 long lot holds exactly the balance of 1,000: free margin 0
    model = _PerLotModel(USD, initial="1000", maintenance="1000", rates=_rates())
    venue = _Venue(balance="1000", margin_model=model)
    venue.market(OrderSide.BUY, "1")

    order = venue.market(OrderSide.SELL, "1", reduce_only=True)

    assert order.status == OrderStatus.FILLED
    assert venue.rejections() == []


def test_each_order_of_a_list_is_admitted_against_the_margin_left():
    # 1,000 USD per lot on 1,500: the first 1 lot buy needs 1,000 and fills; the second then
    # needs 1,000 more against 500 free and is refused
    model = _PerLotModel(USD, initial="1000", maintenance="1000", rates=_rates())
    venue = _Venue(balance="1500", margin_model=model)
    factory = venue.strategy.order_factory
    first = factory.market(EURUSD.id, OrderSide.BUY, _qty("1"))
    second = factory.market(EURUSD.id, OrderSide.BUY, _qty("1"))

    venue.strategy.submit_order_list(OrderList(factory.generate_order_list_id(), [first, second]))
    venue.exchange.process(0)

    assert (first.status, second.status) == (OrderStatus.FILLED, OrderStatus.REJECTED)
    assert venue.margin() == Money("1000.00", USD)


def test_resting_reduce_only_exit_carries_no_margin():
    # A model charging every pending order 1,000 per lot in full: 1 long lot holds 1,000, and a
    # resting reduce-only 1 lot sell limit adds nothing
    model = _PerLotModel(
        USD, initial="1000", maintenance="1000", rates=_rates(pending="1"), hedging=True
    )
    venue = _Venue(balance="10000", margin_model=model)
    venue.market(OrderSide.BUY, "1")

    exit_order = venue.limit(OrderSide.SELL, "1", "1.20000", reduce_only=True)

    assert exit_order.status == OrderStatus.ACCEPTED
    assert venue.exchange.exposure(EURUSD.id).orders == ()
    assert venue.margin() == Money("1000.00", USD)


def test_public_position_updater_values_a_declaring_account_with_its_open_orders():
    # 1 long EUR/AUD lot holds 100,000 x 0.002 = 200 AUD and a resting 1 lot buy limit at its
    # 0.004 rate 400 AUD, at the ASK 0.6502: maintenance 130.04 USD, initial 260.08 USD
    manager, _ = _manager_with_rate()
    model = _f1_model(
        rates=_rates(buy=("0.002", "0.002"), sell=("0.002", "0.002"), BUY_LIMIT="0.004")
    )
    account = _margin_account(model)
    account.set_leverage(EURAUD.id, Decimal(1))
    position = _position(EURAUD, OrderSide.BUY, "1", "1.00000", "P-1")
    order = _order_factory().limit(EURAUD.id, OrderSide.BUY, _qty("1"), Price.from_str("0.90000"))
    order.apply(TestEventStubs.order_submitted(order))
    order.apply(TestEventStubs.order_accepted(order))

    assert manager.update_positions(account, EURAUD, [position], 0, orders_open=[order])

    assert account.margin_maint(EURAUD.id) == Money("130.04", USD)
    assert account.margin_init(EURAUD.id) == Money("260.08", USD)


FROZEN = Venue("FROZEN")


def _frozen_venue() -> _Venue:
    # A venue of its own: the account factory registers calculated accounts per venue name
    return _Venue(
        balance="10000",
        instrument=_pair("EUR/USD", FROZEN),
        margin_model=_f1_model(),
        frozen=True,
        conversion=None,
        venue=FROZEN,
    )


def test_frozen_account_keeps_zero_margin_and_admits_its_order():
    # 100 lots would need 100 x 200 AUD = 20,000 AUD on a 10,000 USD account
    venue = _frozen_venue()

    order = venue.market(OrderSide.BUY, "100")

    assert order.status == OrderStatus.FILLED
    assert venue.account.margins() == {}
    exposure = venue.exchange.exposure(venue.instrument.id)
    assert (
        venue.portfolio.exposure_margin(FROZEN, venue.instrument.id, exposure, MarginBasis.HELD)
        is None
    )


def test_frozen_account_on_declaring_model_declares_no_capability():
    venue = _frozen_venue()

    assert venue.account.checks_free_margin is False
    assert venue.account.margin_from_exposure is False


def test_cash_account_order_never_reaches_a_margin_model():
    model = _f1_model()
    venue = _Venue(balance="1000000", account_type=AccountType.CASH, margin_model=model)

    order = venue.market(OrderSide.BUY, "1")

    assert order.status == OrderStatus.FILLED
    assert model.calls == []
    assert venue.account.checks_free_margin is False
    assert venue.account.margin_from_exposure is False


def test_margin_path_reads_the_leverage_the_account_holds_for_the_instrument():
    # The declaring model is handed the instrument's 25, not the account default of 1; NT's
    # leveraged model at 25 holds 100,000 x 1.0 / 25 x 0.03 = 120 AUD, at the BID 0.65 = 78.00 USD
    model = _f1_model()
    venue = _Venue(balance="10000", margin_model=model, leverages={EURUSD.id: Decimal(25)})
    venue.market(OrderSide.BUY, "1")
    instrument = _pair("EUR/AUD", margin_init=Decimal("0.03"), margin_maint=Decimal("0.03"))
    nt_venue = _Venue(
        balance="1000000",
        instrument=instrument,
        margin_model=LeveragedMarginModel(),
        leverages={instrument.id: Decimal(25)},
    )
    nt_venue.price("1.00000")

    nt_venue.market(OrderSide.BUY, "100000")

    assert {c["leverage"] for c in model.calls} == {Decimal(25)}
    assert nt_venue.account.margin_maint(instrument.id) == Money("78.00", USD)


def test_margin_path_without_a_leverage_for_the_instrument_raises():
    manager, _ = _manager_with_rate()
    account = _margin_account(_f1_model())
    order = _order_factory().limit(EURAUD.id, OrderSide.BUY, _qty("1"), Price.from_str("1.00000"))
    order.apply(TestEventStubs.order_submitted(order))
    order.apply(TestEventStubs.order_accepted(order))

    with pytest.raises(ValueError, match=re.escape(str(EURAUD.id))):
        manager.update_orders(account, EURAUD, [order], 0)


# -- One WARN per refusal, the strategy's ---------------------------------------------------------

_WARN_CHILD = """
import sys
sys.path.insert(0, {root!r})
from nautilus_trader.common.component import flush_logger
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.model.enums import OrderSide

from tests.unit_tests.accounting import test_exposure_margin as t

_guard = init_logging(level_stdout=LogLevel.WARNING, colors=False, bypass=False)
scenario = {scenario!r}
if scenario == "submit":
    venue = t._Venue(balance="130.03", margin_model=t._f1_model())
    venue.market(OrderSide.BUY, "1")
else:
    rates = t._rates(buy=("0.002", "0.002"), sell=("0.002", "0.002"), BUY_LIMIT="0.004")
    venue = t._Venue(balance="500", margin_model=t._f1_model(rates=rates))
    order = venue.limit(OrderSide.BUY, "1", "1.00000")
    assert venue.margin() == t.Money("260.08", t.USD), venue.margin()
    venue.modify(order, "2")
flush_logger()
print("REJECTIONS", len(venue.rejections()), file=sys.stderr, flush=True)
"""


@pytest.mark.parametrize("scenario", ["submit", "modify"])
def test_refusal_logs_one_warning_the_strategys(scenario):
    # submit: a 1 lot market buy needing 130.04 USD with 130.03 free; modify: a resting 1 lot
    # buy limit at a 0.004 limit rate holds 400 AUD = 260.08 USD at the ASK (free 239.92), and
    # its amendment to 2 lots needs 520.16, an increment of 260.08 > 239.92
    root = str(Path(__file__).resolve().parents[3])
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", _WARN_CHILD.format(root=root, scenario=scenario)],
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
        cwd=root,
    )

    assert result.returncode == 0, result.stderr
    warnings = [line for line in result.stdout.splitlines() if "[WARN]" in line]
    assert "REJECTIONS 1" in result.stderr
    assert len(warnings) == 1
    assert "INSUFFICIENT_FREE_MARGIN" in warnings[0]
    assert "SimulatedExchange" not in warnings[0]


# -- The contract, not the type -------------------------------------------------------------------


class _ContractAccount(CashAccount):
    """
    A cash-typed account implementing the margin contract over its own books.
    """

    def __init__(self, event, model) -> None:
        super().__init__(event, calculate_account_state=True)
        self.model = model
        self.per_instrument: dict = {}
        self.account_wide: dict = {}

    checks_free_margin = True
    margin_from_exposure = True
    buy_price_type = PriceType.ASK
    sell_price_type = PriceType.BID

    def margin_currency(self, instrument):
        return self.model.margin_currency(instrument)

    def calculate_exposure_margin(
        self,
        instrument,
        exposure,
        basis,
        to_currency,
        xrate_buy,
        xrate_sell,
        use_quote_for_inverse=False,
    ):
        return self.model.calculate_exposure_margin(
            instrument, exposure, basis, Decimal(1), to_currency, xrate_buy, xrate_sell
        )

    def margins(self):
        return dict(self.per_instrument)

    def account_margins(self):
        return dict(self.account_wide)

    def margins_init(self):
        return {k: v.initial for k, v in self.per_instrument.items()}

    def margins_maint(self):
        return {k: v.maintenance for k, v in self.per_instrument.items()}

    def margin_init(self, instrument_id):
        margin = self.per_instrument.get(instrument_id)
        return None if margin is None else margin.initial

    def margin_maint(self, instrument_id):
        margin = self.per_instrument.get(instrument_id)
        return None if margin is None else margin.maintenance

    def total_margin_init(self, currency):
        books = [*self.per_instrument.values(), *self.account_wide.values()]
        return Money(
            sum((m.initial.as_decimal() for m in books if m.currency == currency), Decimal(0)),
            currency,
        )

    def total_margin_maint(self, currency):
        books = [*self.per_instrument.values(), *self.account_wide.values()]
        return Money(
            sum((m.maintenance.as_decimal() for m in books if m.currency == currency), Decimal(0)),
            currency,
        )

    def update_margin_init(self, instrument_id, margin_init):
        current = self.per_instrument.get(instrument_id)
        maint = current.maintenance if current else Money(0, margin_init.currency)
        self.per_instrument[instrument_id] = MarginBalance(margin_init, maint, instrument_id)

    def update_margin_maint(self, instrument_id, margin_maint):
        current = self.per_instrument.get(instrument_id)
        init = current.initial if current else Money(0, margin_maint.currency)
        self.per_instrument[instrument_id] = MarginBalance(init, margin_maint, instrument_id)

    def apply(self, event):
        CashAccount.apply(self, event)
        self.per_instrument = {
            m.instrument_id: m for m in event.margins if m.instrument_id is not None
        }
        self.account_wide = {m.currency: m for m in event.margins if m.instrument_id is None}


def _contract_portfolio():
    clock = TestClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = TestComponentStubs.cache()
    portfolio = Portfolio(msgbus=msgbus, cache=cache, clock=clock)
    cache.add_instrument(EURUSD)
    cache.add_instrument(AUDUSD)
    cache.add_quote_tick(
        QuoteTick(
            instrument_id=AUDUSD.id,
            bid_price=Price.from_str("0.65000"),
            ask_price=Price.from_str("0.65020"),
            bid_size=Quantity.from_str("10000000.00"),
            ask_size=Quantity.from_str("10000000.00"),
            ts_event=0,
            ts_init=0,
        ),
    )
    state = AccountState(
        account_id=AccountId("SIM-001"),
        account_type=AccountType.CASH,
        base_currency=USD,
        reported=True,
        balances=[AccountBalance(Money(10_000, USD), Money(0, USD), Money(10_000, USD))],
        margins=[MarginBalance(Money(0, USD), Money(130, USD), None)],
        info={},
        event_id=UUID4(),
        ts_event=0,
        ts_init=0,
    )
    account = _ContractAccount(state, _f1_model())
    account.apply(state)
    cache.add_account(account)
    return portfolio, cache, account


def _open_contract_position(portfolio, cache, account) -> None:
    # A 1 lot long held at 200 AUD = 130.04 USD at the ASK
    order = _order_factory().market(EURUSD.id, OrderSide.BUY, _qty("1"))
    fill = TestEventStubs.order_filled(
        order,
        EURUSD,
        account_id=account.id,
        position_id=PositionId("P-1"),
        last_px=Price.from_str("1.10000"),
    )
    position = Position(EURUSD, fill)
    cache.add_position(position, OmsType.NETTING)
    portfolio.update_position(TestEventStubs.position_opened(position))


def test_contract_account_without_margin_identity_is_computed_by_the_exposure_path():
    portfolio, cache, account = _contract_portfolio()
    order = _order_factory().limit(EURUSD.id, OrderSide.BUY, _qty("1"), Price.from_str("1.00000"))
    order.apply(TestEventStubs.order_submitted(order, account_id=account.id))
    cache.add_order(order)
    accepted = TestEventStubs.order_accepted(order, account_id=account.id)
    order.apply(accepted)
    cache.update_order(order)

    portfolio.update_order(accepted)
    assert account.margin_init(EURUSD.id) == Money("0.00", USD)

    _open_contract_position(portfolio, cache, account)
    assert account.margin_maint(EURUSD.id) == Money("130.04", USD)

    # AUD/USD moving to 0.7000/0.7002 re-writes 200 AUD at the ASK = 140.04 USD
    tick = QuoteTick(
        instrument_id=AUDUSD.id,
        bid_price=Price.from_str("0.70000"),
        ask_price=Price.from_str("0.70020"),
        bid_size=Quantity.from_str("10000000.00"),
        ask_size=Quantity.from_str("10000000.00"),
        ts_event=0,
        ts_init=0,
    )
    cache.add_quote_tick(tick)
    portfolio.revalue_margin(tick)
    assert account.margin_maint(EURUSD.id) == Money("140.04", USD)

    exposure = Exposure(EURUSD, cache.positions_open(), [], None)
    assert portfolio.exposure_margin(SIM, EURUSD.id, exposure, MarginBasis.HELD) == Money(
        "140.04", USD
    )


def test_contract_account_state_carries_and_restores_both_margin_collections():
    portfolio, cache, account = _contract_portfolio()
    _open_contract_position(portfolio, cache, account)
    manager = AccountsManager(cache=cache, logger=Logger("AccountsManager"), clock=TestClock())

    state = manager.generate_account_state(account, 0)
    restored = _ContractAccount(state, _f1_model())
    restored.apply(state)

    instrument_margin = MarginBalance(Money("0.00", USD), Money("130.04", USD), EURUSD.id)
    account_margin = MarginBalance(Money(0, USD), Money(130, USD), None)
    assert state.margins == [instrument_margin, account_margin]
    assert restored.margins() == {EURUSD.id: instrument_margin}
    assert restored.account_margins() == {USD: account_margin}
    assert restored.total_margin_maint(USD) == Money("260.04", USD)
    assert portfolio.margins_init(SIM) == {EURUSD.id: Money("0.00", USD)}
    assert portfolio.margins_maint(SIM) == {EURUSD.id: Money("130.04", USD)}


def test_margin_account_state_event_is_unchanged():
    account = _margin_account()
    account.update_margin(MarginBalance(Money(10, USD), Money(20, USD), EURUSD.id))
    account.update_margin(MarginBalance(Money(5, USD), Money(7, USD), None))
    manager, _ = _manager_with_rate()

    state = manager.generate_account_state(account, 0)

    assert state.account_type == AccountType.MARGIN
    assert state.is_reported is False
    assert state.balances == list(account.balances().values())
    assert state.margins == [
        MarginBalance(Money(10, USD), Money(20, USD), EURUSD.id),
        MarginBalance(Money(5, USD), Money(7, USD), None),
    ]


def test_declaring_model_scalar_calculations_raise():
    model = _f1_model()

    with pytest.raises(NotImplementedError):
        model.calculate_margin_init(
            EURUSD, Quantity.from_str("1"), Price.from_str("1.10000"), Decimal(1)
        )
    with pytest.raises(NotImplementedError):
        model.calculate_margin_maint(
            EURUSD, PositionSide.LONG, Quantity.from_str("1"), Price.from_str("1.10000"), Decimal(1)
        )
