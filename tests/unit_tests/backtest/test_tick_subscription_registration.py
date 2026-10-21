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

from itertools import batched

import pytest

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.common.actor import Actor
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import DataCatalogConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.data import TestDataStubs


class TickRecorder(Actor):
    def __init__(self, instrument_ids):
        super().__init__()
        self.instrument_ids = instrument_ids
        self.received = []

    def on_start(self):
        for instrument_id in self.instrument_ids:
            self.subscribe_quote_ticks(instrument_id)
            self.subscribe_trade_ticks(instrument_id)

    def on_quote_tick(self, tick):
        self.received.append(tick)

    def on_trade_tick(self, tick):
        self.received.append(tick)


@pytest.mark.parametrize("batch_size", [3, 20])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("paired_timestamps", [False, True])
@pytest.mark.parametrize(
    ("second_symbol", "second_venue"),
    [("GBP/USD", "SIM"), ("GBP/USD", "OTHER"), ("AUD/USD", "OTHER")],
)
def test_mixed_tick_streams_deliver_once_when_the_catalog_also_contains_them(
    tmp_path,
    batch_size,
    reverse,
    paired_timestamps,
    second_symbol,
    second_venue,
):
    instruments = [
        TestInstrumentProvider.default_fx_ccy("AUD/USD"),
        TestInstrumentProvider.default_fx_ccy(second_symbol, Venue(second_venue)),
    ]
    ticks = []
    for index in (0, 0, 1, 1, 0):
        ts_ns = (len(ticks) + 1) * 1_000_000_000
        ticks.append(
            TestDataStubs.quote_tick(instrument=instruments[index], ts_event=ts_ns, ts_init=ts_ns)
        )
        if not paired_timestamps:
            ts_ns += 1_000_000_000
        ticks.append(
            TestDataStubs.trade_tick(instrument=instruments[index], ts_event=ts_ns, ts_init=ts_ns)
        )

    catalog = ParquetDataCatalog(str(tmp_path))
    catalog.write_data(instruments)
    catalog.write_data(ticks)
    engine = BacktestEngine(
        BacktestEngineConfig(
            catalogs=[DataCatalogConfig(path=str(tmp_path))],
            logging=LoggingConfig(bypass_logging=True),
            run_analysis=False,
        ),
    )
    recorder = TickRecorder([instrument.id for instrument in instruments])
    try:
        for venue in dict.fromkeys(instrument.id.venue for instrument in instruments):
            engine.add_venue(
                venue=venue,
                oms_type=OmsType.HEDGING,
                account_type=AccountType.MARGIN,
                base_currency=USD,
                starting_balances=[Money(1_000_000, USD)],
            )
        for instrument in instruments:
            engine.add_instrument(instrument)
        supplied = list(reversed(ticks)) if reverse else ticks
        expected = sorted(supplied, key=lambda tick: tick.ts_init)
        for batch in batched(supplied, batch_size):
            engine.add_data(list(batch), validate=False)
        engine.add_actor(recorder)
        engine.run(start=ticks[0].ts_init, end=ticks[-1].ts_init)
        assert recorder.received == expected
    finally:
        engine.dispose()
