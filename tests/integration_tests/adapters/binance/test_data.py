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

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.binance import data as binance_data
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceErrorCode
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.factories import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.futures.data import BinanceFuturesDataClient
from nautilus_trader.adapters.binance.http.error import BinanceClientError
from nautilus_trader.adapters.binance.http.error import BinanceServerError
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.test_kit.mocks.cache_database import MockCacheDatabase
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


@pytest.fixture
def refresh_client(event_loop, monkeypatch):
    clock = LiveClock()
    client = BinanceLiveDataClientFactory.create(
        loop=event_loop,
        name="BINANCE",
        config=BinanceDataClientConfig(
            api_key="SOME_BINANCE_API_KEY",
            api_secret="SOME_BINANCE_API_SECRET",
            account_type=BinanceAccountType.USDT_FUTURES,
        ),
        msgbus=MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock),
        cache=Cache(database=MockCacheDatabase()),
        clock=clock,
    )
    log = MagicMock()
    monkeypatch.setattr(BinanceFuturesDataClient, "_log", property(lambda _self: log))

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(binance_data.asyncio, "sleep", no_sleep)
    client._send_all_instruments_to_data_engine = MagicMock()
    return client, log


def _reloads(client, *outcomes):
    initialize = AsyncMock(side_effect=list(outcomes))
    client._instrument_provider = MagicMock(initialize=initialize)
    return initialize


@pytest.mark.asyncio
async def test_refresh_retryable_code_warns_with_count_and_success_logs_no_error(refresh_client):
    # Arrange
    client, log = refresh_client
    retryable = BinanceServerError(
        500,
        {"code": BinanceErrorCode.DISCONNECTED.value, "msg": "Disconnected"},
        {},
    )
    _reloads(client, retryable, None, asyncio.CancelledError())

    # Act
    await client._update_instruments(1)

    # Assert
    warnings = [call.args[0] for call in log.warning.call_args_list]
    assert len(warnings) == 1
    assert "1/3" in warnings[0]
    log.error.assert_not_called()
    client._send_all_instruments_to_data_engine.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [
        BinanceClientError(400, {"code": -99999, "msg": "Unknown"}, {}),
        RuntimeError("connection reset"),
    ],
    ids=["unknown-binance-code", "non-binance-exception"],
)
@pytest.mark.asyncio
async def test_refresh_unclassified_error_logs_one_error_and_runs_next_interval(
    refresh_client,
    error,
):
    # Arrange
    client, log = refresh_client
    initialize = _reloads(client, error, asyncio.CancelledError())

    # Act
    await client._update_instruments(1)

    # Assert
    assert log.error.call_count == 1
    log.warning.assert_not_called()
    assert initialize.await_count == 2


@pytest.mark.asyncio
async def test_refresh_cancelled_returns(refresh_client):
    # Arrange
    client, log = refresh_client
    initialize = _reloads(client, asyncio.CancelledError())

    # Act
    await client._update_instruments(1)

    # Assert
    assert initialize.await_count == 1
    log.error.assert_not_called()
