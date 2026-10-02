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

from typing import Any

import pytest

from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.backtest.engine import OrderMatchingEngine
from nautilus_trader.backtest.engine import SimulatedExchange
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.backtest.models import MakerTakerFeeModel
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import BacktestDataConfig
from nautilus_trader.config import BacktestRunConfig
from nautilus_trader.config import BacktestVenueConfig
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.mocks.data import load_catalog_with_stub_quote_ticks_audusd
from nautilus_trader.test_kit.mocks.data import setup_catalog
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


_AUDUSD_SIM = TestInstrumentProvider.default_fx_ccy("AUD/USD")


class TestQuoteExecutionMatchingEngine:
    def setup_method(self) -> None:
        self.clock = TestClock()
        self.msgbus = MessageBus(
            trader_id=TestIdStubs.trader_id(),
            clock=self.clock,
        )
        self.cache = TestComponentStubs.cache()
        self.cache.add_instrument(_AUDUSD_SIM)
        self.account_id = TestIdStubs.account_id()
        self.events: list[Any] = []
        self.msgbus.register("ExecEngine.process", self.events.append)

    def _matching_engine(self, quote_execution: bool) -> OrderMatchingEngine:
        return OrderMatchingEngine(
            instrument=_AUDUSD_SIM,
            raw_id=0,
            fill_model=FillModel(),
            fee_model=MakerTakerFeeModel(),
            book_type=BookType.L1_MBP,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            msgbus=self.msgbus,
            cache=self.cache,
            clock=self.clock,
            quote_execution=quote_execution,
        )

    def _quote(self) -> QuoteTick:
        return TestDataStubs.quote_tick(
            instrument=_AUDUSD_SIM,
            bid_price=1.08500,
            ask_price=1.08512,
            bid_size=100,
            ask_size=100,
            ts_event=1,
            ts_init=1,
        )

    def _submit_market_buy(
        self,
        matching_engine: OrderMatchingEngine,
        client_order_id: str,
    ) -> None:
        order = TestExecStubs.market_order(
            instrument=_AUDUSD_SIM,
            order_side=OrderSide.BUY,
            client_order_id=ClientOrderId(client_order_id),
        )
        matching_engine.process_order(order, self.account_id)

    def test_quote_execution_off_quote_sets_no_market_and_trade_fills(self) -> None:
        matching_engine = self._matching_engine(quote_execution=False)

        matching_engine.process_quote_tick(self._quote())

        assert matching_engine.best_bid_price() is None
        assert matching_engine.best_ask_price() is None

        self._submit_market_buy(matching_engine, "O-1")

        rejected = [e for e in self.events if isinstance(e, OrderRejected)]
        assert [e.reason for e in rejected] == ["no market for AUD/USD.SIM"]
        assert not [e for e in self.events if isinstance(e, OrderFilled)]

        trade = TestDataStubs.trade_tick(
            instrument=_AUDUSD_SIM,
            price=1.08505,
            size=100,
            aggressor_side=AggressorSide.BUYER,
            ts_event=2,
            ts_init=2,
        )
        matching_engine.process_trade_tick(trade)
        self._submit_market_buy(matching_engine, "O-2")

        filled = [e for e in self.events if isinstance(e, OrderFilled)]
        assert [e.client_order_id for e in filled] == [ClientOrderId("O-2")]
        assert filled[0].last_px == Price.from_str("1.08505")

    def test_quote_execution_on_quote_fills_market_buy_at_ask(self) -> None:
        matching_engine = self._matching_engine(quote_execution=True)

        matching_engine.process_quote_tick(self._quote())

        assert matching_engine.best_bid_price() == Price.from_str("1.08500")
        assert matching_engine.best_ask_price() == Price.from_str("1.08512")

        self._submit_market_buy(matching_engine, "O-1")

        assert not [e for e in self.events if isinstance(e, OrderRejected)]
        filled = [e for e in self.events if isinstance(e, OrderFilled)]
        assert [e.client_order_id for e in filled] == [ClientOrderId("O-1")]
        assert filled[0].last_px == Price.from_str("1.08512")


def test_simulated_exchange_quote_execution_defaults_on() -> None:
    clock = TestClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = TestComponentStubs.cache()

    exchange = SimulatedExchange(
        venue=Venue("SIM"),
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        base_currency=USD,
        starting_balances=[Money(1_000_000, USD)],
        default_leverage=1,
        leverages={},
        modules=[],
        portfolio=Portfolio(msgbus=msgbus, cache=cache, clock=clock),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        fill_model=FillModel(),
        fee_model=MakerTakerFeeModel(),
    )

    assert exchange.quote_execution is True


@pytest.mark.parametrize(
    ("quote_execution", "expect_fills"),
    [
        (True, True),
        (False, False),
    ],
)
def test_node_venue_config_quote_execution_reaches_matching_engine(
    tmp_path,
    quote_execution: bool,
    expect_fills: bool,
) -> None:
    catalog = setup_catalog(protocol="file", path=tmp_path / "catalog")
    load_catalog_with_stub_quote_ticks_audusd(catalog)
    run_config = BacktestRunConfig(
        engine=BacktestEngineConfig(
            strategies=[
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
            ],
            logging=LoggingConfig(bypass_logging=True),
        ),
        venues=[
            BacktestVenueConfig(
                name="SIM",
                oms_type="HEDGING",
                account_type="MARGIN",
                base_currency="USD",
                starting_balances=["1_000_000 USD"],
                quote_execution=quote_execution,
            ),
        ],
        data=[
            BacktestDataConfig(
                catalog_path=catalog.path,
                catalog_fs_protocol=catalog.fs_protocol,
                data_cls=QuoteTick,
                instrument_id=InstrumentId.from_str("AUD/USD.SIM"),
                start_time=1580398089820000000,
                end_time=1580504394501000000,
            ),
        ],
        chunk_size=None,
        dispose_on_completion=False,
    )
    node = BacktestNode(configs=[run_config])

    node.run()

    orders = node.get_engine(run_config.id).kernel.cache.orders()
    assert orders
    if expect_fills:
        assert any(order.status == OrderStatus.FILLED for order in orders)
    else:
        assert all(order.status == OrderStatus.REJECTED for order in orders)
