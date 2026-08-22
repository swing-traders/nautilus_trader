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
import gc
import signal
import weakref
from unittest.mock import Mock

import pytest

from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.test_kit.functions import eventually
from nautilus_trader.trading.strategy import Strategy


class StopFailure(Exception):
    """
    Raised from the `on_stop` handler of `RaisingStopStrategy`.
    """


class RaisingStopStrategy(Strategy):
    """
    Provides a strategy which fails its stop sequence.
    """

    def on_stop(self) -> None:
        raise StopFailure("on_stop failed")


class CancelingStopStrategy(Strategy):
    """
    Provides a strategy which cancels its stop sequence.
    """

    def on_stop(self) -> None:
        raise asyncio.CancelledError


class ExitingStopStrategy(Strategy):
    """
    Provides a strategy which exits the interpreter from its stop sequence.
    """

    def on_stop(self) -> None:
        raise SystemExit


def _node_config() -> TradingNodeConfig:
    return TradingNodeConfig(
        logging=LoggingConfig(bypass_logging=True),
        timeout_connection=1.0,
        timeout_reconciliation=1.0,
        timeout_portfolio=1.0,
        timeout_disconnection=1.0,  # Short timeout for testing
        timeout_post_stop=0.1,  # Short timeout for testing
    )


@pytest.mark.asyncio
async def test_run_async_reraises_error_from_stop():
    # Arrange
    loop = asyncio.get_running_loop()
    node = TradingNode(config=_node_config(), loop=loop)
    node.trader.add_strategy(RaisingStopStrategy())
    node.build()

    run_task = asyncio.ensure_future(node.run_async())
    await eventually(lambda: node.trader.is_running, timeout=5.0)

    # Act
    node.stop()
    await asyncio.wait({run_task}, timeout=5.0)

    # Assert
    assert run_task.done(), "`run_async` did not unwind after the stop raised"

    with pytest.raises(StopFailure):
        run_task.result()


@pytest.mark.asyncio
async def test_run_async_reraises_cancellation_from_stop():
    # Arrange
    loop = asyncio.get_running_loop()
    node = TradingNode(config=_node_config(), loop=loop)
    node.trader.add_strategy(CancelingStopStrategy())
    node.build()

    run_task = asyncio.ensure_future(node.run_async())
    await eventually(lambda: node.trader.is_running, timeout=5.0)

    # Act
    node.stop()
    await asyncio.wait({run_task}, timeout=5.0)

    # Assert
    assert run_task.done(), "`run_async` did not unwind after the stop was canceled"

    with pytest.raises(asyncio.CancelledError):
        run_task.result()


def test_run_reraises_system_exit_from_stop(event_loop):
    # Arrange
    node = TradingNode(config=_node_config(), loop=event_loop)
    node.trader.add_strategy(ExitingStopStrategy())
    node.build()

    def stop_when_running() -> None:
        if node.trader.is_running:
            node.stop()
        else:
            event_loop.call_later(0.01, stop_when_running)

    event_loop.call_later(0.01, stop_when_running)

    # Act
    with pytest.raises(SystemExit):
        node.run()

    # Assert
    assert node.kernel.stop_error is not None
    assert node.kernel.exec_engine.get_cmd_queue_task().done(), (
        "`run_async` did not unwind after the stop exited"
    )


@pytest.mark.asyncio
async def test_create_stop_task_retains_task_while_pending():
    # Arrange
    loop = asyncio.get_running_loop()
    node = TradingNode(config=_node_config(), loop=loop)
    node.build()

    async def suspended_stop() -> None:
        await loop.create_future()  # Never completes and is held by nothing else

    task_ref = weakref.ref(node.kernel.create_stop_task(suspended_stop))

    # Act
    await asyncio.sleep(0)
    gc.collect()
    await asyncio.sleep(0)

    # Assert
    assert task_ref() is not None, "the pending stop task was garbage collected"
    assert node.kernel.stop_error is None


@pytest.mark.asyncio
async def test_create_stop_task_when_already_pending():
    # Arrange
    loop = asyncio.get_running_loop()
    node = TradingNode(config=_node_config(), loop=loop)
    node.build()

    entered = 0

    async def suspended_stop() -> None:
        nonlocal entered
        entered += 1
        await loop.create_future()  # Never completes

    task = node.kernel.create_stop_task(suspended_stop)
    await asyncio.sleep(0)

    # Act
    task_again = node.kernel.create_stop_task(suspended_stop)
    await asyncio.sleep(0)

    # Assert
    assert task_again is task
    assert entered == 1


@pytest.mark.asyncio
async def test_create_stop_task_when_canceled_before_start():
    # Arrange
    loop = asyncio.get_running_loop()
    node = TradingNode(config=_node_config(), loop=loop)
    node.build()

    started = False

    async def counted_stop() -> None:
        nonlocal started
        started = True

    task = node.kernel.create_stop_task(counted_stop)

    # Act
    task.cancel()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # Assert
    assert task.cancelled()
    assert not started
    assert isinstance(node.kernel.stop_error, asyncio.CancelledError)


def test_loop_sig_handler_keeps_signal_handling_armed(event_loop):
    # Arrange
    node = TradingNode(config=_node_config(), loop=event_loop)
    received: list[signal.Signals] = []
    node.kernel._loop_sig_callback = received.append
    mock_loop = Mock()
    node.kernel._loop = mock_loop

    # Act
    node.kernel._loop_sig_handler(signal.SIGTERM)
    node.kernel._loop_sig_handler(signal.SIGINT)

    # Assert
    assert received == [signal.SIGTERM, signal.SIGINT]
    mock_loop.remove_signal_handler.assert_not_called()
    mock_loop.add_signal_handler.assert_not_called()
