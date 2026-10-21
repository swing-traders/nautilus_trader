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

import os
import subprocess
import sys
from pathlib import Path

import pytest

from nautilus_trader.backtest import engine
from nautilus_trader.common import component


@pytest.mark.parametrize(
    ("stdout_level", "file_level", "expected"),
    [("INFO", None, 0), ("DEBUG", None, 1), ("INFO", "DEBUG", 1)],
)
def test_tick_repr_is_only_built_when_debug_can_be_emitted(
    tmp_path,
    stdout_level,
    file_level,
    expected,
):
    script = """
import sys
import nautilus_trader.backtest
import nautilus_trader.common
nautilus_trader.backtest.__path__.insert(0, sys.argv[1])
nautilus_trader.common.__path__.insert(0, sys.argv[2])
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.common.component import flush_logger
from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.data import TestDataStubs

class ObservedTick(TradeTick):
    representations = 0

    def __repr__(self):
        ObservedTick.representations += 1
        return super().__repr__()

instrument = TestInstrumentProvider.default_fx_ccy("AUD/USD")
source = TestDataStubs.trade_tick(instrument=instrument, ts_event=1, ts_init=1)
tick = ObservedTick(source.instrument_id, source.price, source.size, source.aggressor_side,
                    source.trade_id, source.ts_event, source.ts_init)
engine = BacktestEngine(BacktestEngineConfig(
    logging=LoggingConfig(log_level=sys.argv[3],
        log_level_file=None if sys.argv[4] == "None" else sys.argv[4],
        log_directory=sys.argv[5], log_file_name="matching"),
    run_analysis=False,
))
try:
    engine.add_venue(venue=Venue("SIM"), oms_type=OmsType.HEDGING,
                     account_type=AccountType.MARGIN, base_currency=USD,
                     starting_balances=[Money(1_000_000, USD)])
    engine.add_instrument(instrument)
    engine.add_data([tick])
    engine.run(start=1, end=1)
finally:
    engine.dispose()
    flush_logger()
print(f"REPR_COUNT={ObservedTick.representations}")
"""
    environment = dict(os.environ)
    environment.pop("NAUTILUS_LOG", None)
    result = subprocess.run(  # noqa: S603 - fixed test script and pytest parameters
        [
            sys.executable,
            "-c",
            script,
            str(Path(engine.__file__).parent),
            str(Path(component.__file__).parent),
            stdout_level,
            str(file_level),
            str(tmp_path),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    assert f"REPR_COUNT={expected}" in result.stdout
