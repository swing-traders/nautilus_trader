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
import json
import math
import os
from asyncio import Queue
from collections import Counter
from collections import deque
from collections.abc import Iterable
from decimal import ROUND_DOWN
from decimal import Decimal
from decimal import localcontext
from typing import Any
from typing import Final
from typing import NamedTuple
from typing import cast

import pandas as pd

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.fsm import InvalidStateTrigger
from nautilus_trader.core.message import Command
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.client import ExecutionClient
from nautilus_trader.execution.engine import ExecutionEngine
from nautilus_trader.execution.messages import GenerateExecutionMassStatus
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import QueryOrder
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import ExecutionReport
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.enqueue import ThrottledEnqueuer
from nautilus_trader.live.reconciliation import POSITION_REPAIR_OPEN
from nautilus_trader.live.reconciliation import POSITION_REPAIR_TRIM
from nautilus_trader.live.reconciliation import QUANTITY_CONTEXT_PRECISION
from nautilus_trader.live.reconciliation import VIRTUAL_POSITION_ID_PREFIX
from nautilus_trader.live.reconciliation import PositionRepairIntent
from nautilus_trader.live.reconciliation import adjust_fills_for_partial_window
from nautilus_trader.live.reconciliation import calculate_reconciliation_price
from nautilus_trader.live.reconciliation import create_inferred_order_filled_event
from nautilus_trader.live.reconciliation import create_order_accepted_event
from nautilus_trader.live.reconciliation import create_order_canceled_event
from nautilus_trader.live.reconciliation import create_order_expired_event
from nautilus_trader.live.reconciliation import create_order_filled_event
from nautilus_trader.live.reconciliation import create_order_rejected_event
from nautilus_trader.live.reconciliation import create_order_triggered_event
from nautilus_trader.live.reconciliation import create_order_updated_event
from nautilus_trader.live.reconciliation import diff_position_scope
from nautilus_trader.live.reconciliation import get_existing_fill_for_trade_id
from nautilus_trader.live.reconciliation import is_within_single_unit_tolerance
from nautilus_trader.live.reconciliation import quantities_equal_at_size_precision
from nautilus_trader.model.book import py_should_handle_own_book_order
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.enums import order_side_to_str
from nautilus_trader.model.enums import position_side_to_str
from nautilus_trader.model.enums import trailing_offset_type_to_str
from nautilus_trader.model.enums import trigger_type_to_str
from nautilus_trader.model.events import OrderEvent
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderInitialized
from nautilus_trader.model.events import OrderUpdated
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order
from nautilus_trader.model.orders import OrderUnpacker
from nautilus_trader.model.position import Position


InstrumentAccountKey = tuple[InstrumentId, AccountId]

POSITION_RESIDUALS_KEY: Final[str] = "reconciliation:residuals"

# A scope's verdict for one convergence pass. `DEFERRED` is the absence of a verdict:
# the scope was neither proven converged nor proven discrepant, so the pass neither
# repairs it, reports it, nor disturbs what the previous document already said of it
POSITION_SCOPE_CONVERGED: Final[str] = "CONVERGED"
POSITION_SCOPE_DEFERRED: Final[str] = "DEFERRED"
POSITION_SCOPE_UNCONVERGED: Final[str] = "UNCONVERGED"
POSITION_SCOPE_HEALED_REASON: Final[str] = "applied missing venue fills, re-querying next pass"


class PositionScopeSnapshot(NamedTuple):
    """
    Represents the venue-observed position state for one (instrument, account) scope.

    Parameters
    ----------
    instrument_id : InstrumentId
        The scope instrument ID.
    account_id : AccountId
        The scope account ID.
    reports : tuple[PositionStatusReport, ...]
        The venue reports for the scope. Empty means the venue holds no position there.
    incomplete_reason : str or ``None``
        Why the snapshot cannot authorize a repair, or ``None`` when it is complete.

    """

    instrument_id: InstrumentId
    account_id: AccountId
    reports: tuple[PositionStatusReport, ...]
    incomplete_reason: str | None


class LiveExecutionEngine(ExecutionEngine):
    """
    Provides a high-performance asynchronous live execution engine.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the engine.
    msgbus : MessageBus
        The message bus for the engine.
    cache : Cache
        The cache for the engine.
    clock : LiveClock
        The clock for the engine.
    config : LiveExecEngineConfig, optional
        The configuration for the instance.

    Raises
    ------
    TypeError
        If `config` is not of type `LiveExecEngineConfig`.

    """

    _sentinel: Final[None] = None

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        config: LiveExecEngineConfig | None = None,
    ) -> None:
        if config is None:
            config = LiveExecEngineConfig()
        PyCondition.type(config, LiveExecEngineConfig, "config")
        super().__init__(
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )

        self._loop: asyncio.AbstractEventLoop = loop
        self._cmd_queue: asyncio.Queue = Queue(maxsize=config.qsize)
        self._evt_queue: asyncio.Queue = Queue(maxsize=config.qsize)

        # Reconciliation
        self._recon_check_retries: Counter[ClientOrderId] = Counter()
        self._ts_last_query: dict[ClientOrderId, int] = {}
        self._missing_order_query_queue: deque[ClientOrderId] = deque()
        self._order_local_activity_ns: dict[ClientOrderId, int] = {}
        self._position_local_activity_ns: dict[InstrumentAccountKey, int] = {}
        self._received_unapplied_fill_counts: Counter[InstrumentAccountKey] = Counter()
        self._received_fill_event_counts: Counter[UUID4] = Counter()
        self._position_recon_retries: Counter[InstrumentAccountKey] = Counter()
        self._recent_fills_cache: dict[TradeId, int] = {}  # TradeId -> timestamp_ns (TTL cache)
        self._overfill_rejected_trade_ids: dict[ClientOrderId, dict[TradeId, int]] = {}
        self._inferred_fill_ts: dict[ClientOrderId, int] = {}
        self._fill_application_audit: dict[ClientOrderId, list[tuple[TradeId, str, int]]] = {}
        self._startup_reconciliation_event: asyncio.Event = asyncio.Event()
        self._filtered_external_orders_count: int = 0

        self._cmd_enqueuer: ThrottledEnqueuer[Command] = ThrottledEnqueuer(
            qname="cmd_queue",
            queue=self._cmd_queue,
            loop=self._loop,
            clock=self._clock,
            logger=self._log,
        )
        self._evt_enqueuer: ThrottledEnqueuer[OrderEvent] = ThrottledEnqueuer(
            qname="evt_queue",
            queue=self._evt_queue,
            loop=self._loop,
            clock=self._clock,
            logger=self._log,
        )

        # Async tasks
        self._cmd_queue_task: asyncio.Task | None = None
        self._evt_queue_task: asyncio.Task | None = None
        self._reconciliation_task: asyncio.Task | None = None
        self._own_books_audit_task: asyncio.Task | None = None
        self._is_shutting_down: bool = False
        self._kill: bool = False

        # Configuration
        self._reconciliation: bool = config.reconciliation
        self.reconciliation_lookback_mins: int = config.reconciliation_lookback_mins or 0
        self.reconciliation_instrument_ids: list[InstrumentId] = (
            config.reconciliation_instrument_ids or []
        )
        self.filter_unclaimed_external_orders: bool = config.filter_unclaimed_external_orders
        self.filter_position_reports: bool = config.filter_position_reports
        self.filtered_client_order_ids: list[ClientOrderId] = config.filtered_client_order_ids or []
        self.generate_missing_orders: bool = config.generate_missing_orders
        self.inflight_check_interval_ms: int = config.inflight_check_interval_ms
        self.inflight_check_threshold_ms: int = config.inflight_check_threshold_ms
        self.inflight_check_max_retries: int = config.inflight_check_retries
        self.own_books_audit_interval_secs: float | None = config.own_books_audit_interval_secs
        self.open_check_interval_secs: float | None = config.open_check_interval_secs
        self.open_check_open_only: bool = config.open_check_open_only
        self.open_check_lookback_mins: int = config.open_check_lookback_mins
        self.open_check_threshold_ms: int = config.open_check_threshold_ms
        self.open_check_missing_retries: int = config.open_check_missing_retries
        self.max_single_order_queries_per_cycle: int = config.max_single_order_queries_per_cycle
        self.single_order_query_delay_ms: int = config.single_order_query_delay_ms
        self.position_check_interval_secs: float | None = config.position_check_interval_secs
        self.position_check_lookback_mins: int = config.position_check_lookback_mins
        self.position_check_threshold_ms: int = config.position_check_threshold_ms
        self.position_check_retries: int = config.position_check_retries
        self.reconciliation_startup_delay_secs: float = config.reconciliation_startup_delay_secs
        self.graceful_shutdown_on_exception: bool = config.graceful_shutdown_on_exception

        self._log.info(f"{config.reconciliation=}", LogColor.BLUE)
        self._log.info(f"{config.reconciliation_lookback_mins=}", LogColor.BLUE)
        self._log.info(f"{config.reconciliation_instrument_ids=}", LogColor.BLUE)
        self._log.info(f"{config.filter_unclaimed_external_orders=}", LogColor.BLUE)
        self._log.info(f"{config.filter_position_reports=}", LogColor.BLUE)
        self._log.info(f"{config.filtered_client_order_ids=}", LogColor.BLUE)
        self._log.info(f"{config.inflight_check_interval_ms=}", LogColor.BLUE)
        self._log.info(f"{config.inflight_check_threshold_ms=}", LogColor.BLUE)
        self._log.info(f"{config.inflight_check_retries=}", LogColor.BLUE)
        self._log.info(f"{config.own_books_audit_interval_secs=}", LogColor.BLUE)
        self._log.info(f"{config.open_check_interval_secs=}", LogColor.BLUE)
        self._log.info(f"{config.open_check_open_only=}", LogColor.BLUE)
        self._log.info(f"{config.open_check_lookback_mins=}", LogColor.BLUE)
        self._log.info(f"{config.open_check_threshold_ms=}", LogColor.BLUE)
        self._log.info(f"{config.open_check_missing_retries=}", LogColor.BLUE)
        self._log.info(f"{config.max_single_order_queries_per_cycle=}", LogColor.BLUE)
        self._log.info(f"{config.single_order_query_delay_ms=}", LogColor.BLUE)
        self._log.info(f"{config.position_check_interval_secs=}", LogColor.BLUE)
        self._log.info(f"{config.position_check_lookback_mins=}", LogColor.BLUE)
        self._log.info(f"{config.position_check_threshold_ms=}", LogColor.BLUE)
        self._log.info(f"{config.position_check_retries=}", LogColor.BLUE)
        self._log.info(f"{config.reconciliation_startup_delay_secs=}", LogColor.BLUE)
        self._log.info(f"{config.purge_closed_orders_interval_mins=}", LogColor.BLUE)
        self._log.info(f"{config.purge_closed_orders_buffer_mins=}", LogColor.BLUE)
        self._log.info(f"{config.purge_closed_positions_interval_mins=}", LogColor.BLUE)
        self._log.info(f"{config.purge_closed_positions_buffer_mins=}", LogColor.BLUE)
        self._log.info(f"{config.purge_account_events_interval_mins=}", LogColor.BLUE)
        self._log.info(f"{config.purge_account_events_lookback_mins=}", LogColor.BLUE)
        self._log.info(f"{config.purge_from_database=}", LogColor.BLUE)
        self._log.info(f"{config.graceful_shutdown_on_exception=}", LogColor.BLUE)

        self._inflight_check_threshold_ns: int = millis_to_nanos(self.inflight_check_threshold_ms)
        self._open_check_threshold_ns: int = millis_to_nanos(self.open_check_threshold_ms)
        self._position_check_threshold_ns: int = millis_to_nanos(self.position_check_threshold_ms)

        # Register endpoints
        self._msgbus.register(
            endpoint="ExecEngine.reconcile_execution_report",
            handler=self.reconcile_execution_report,
        )
        self._msgbus.register(
            endpoint="ExecEngine.reconcile_execution_mass_status",
            handler=self.reconcile_execution_mass_status,
        )

    @property
    def reconciliation(self) -> bool:
        """
        Return whether the reconciliation process will be run on start.

        Returns
        -------
        bool

        """
        return self._reconciliation

    # -- LIFECYCLE ---------------------------------------------------------------------------------

    def connect(self) -> None:
        """
        Connect the engine by calling connect on all registered clients.
        """
        if self._clients:
            self._log.info("Connecting all clients...")
        elif self._external_clients:
            self._log.info(
                f"Configured for external clients: {self._external_clients}",
                LogColor.BLUE,
            )
        else:
            self._log.warning("No clients to connect")
            return

        for client in self._clients.values():
            client.connect()

    def disconnect(self) -> None:
        """
        Disconnect the engine by calling disconnect on all registered clients.
        """
        if self._clients:
            self._log.info("Disconnecting all clients...")
        else:
            self._log.warning("No clients to disconnect")
            return

        for client in self._clients.values():
            client.disconnect()

    def get_cmd_queue_task(self) -> asyncio.Task | None:
        """
        Return the internal command queue task for the engine.

        Returns
        -------
        asyncio.Task or ``None``

        """
        return self._cmd_queue_task

    def get_evt_queue_task(self) -> asyncio.Task | None:
        """
        Return the internal event queue task for the engine.

        Returns
        -------
        asyncio.Task or ``None``

        """
        return self._evt_queue_task

    def get_own_books_audit_task(self) -> asyncio.Task | None:
        """
        Return the own books audit task for the engine.

        Returns
        -------
        asyncio.Task or ``None``

        """
        return self._own_books_audit_task

    def get_reconciliation_task(self) -> asyncio.Task | None:
        """
        Return the continuous reconciliation task for the engine.

        Returns
        -------
        asyncio.Task or ``None``

        """
        return self._reconciliation_task

    def cmd_qsize(self) -> int:
        """
        Return the number of `Command` messages buffered on the internal queue.

        Returns
        -------
        int

        """
        return self._cmd_queue.qsize()

    def evt_qsize(self) -> int:
        """
        Return the number of `Event` messages buffered on the internal queue.

        Returns
        -------
        int

        """
        return self._evt_queue.qsize()

    def _on_start(self) -> None:
        if not self._loop.is_running():
            self._log.warning("Started when loop is not running")

        # Clear reconciliation event for fresh start cycle
        self._startup_reconciliation_event.clear()
        self._is_shutting_down = False

        self._cmd_queue_task = self._loop.create_task(self._run_cmd_queue(), name="cmd_queue")
        self._evt_queue_task = self._loop.create_task(self._run_evt_queue(), name="evt_queue")
        self._log.debug(f"Scheduled task '{self._cmd_queue_task.get_name()}'")
        self._log.debug(f"Scheduled task '{self._evt_queue_task.get_name()}'")

        # Start reconciliation task if any check is configured
        if (
            self.inflight_check_interval_ms
            or self.open_check_interval_secs
            or self.position_check_interval_secs
        ) and not self._reconciliation_task:
            self._reconciliation_task = self._loop.create_task(
                self._continuous_reconciliation_loop(),
                name="continuous_reconciliation",
            )
            self._log.debug(f"Scheduled task '{self._reconciliation_task.get_name()}'")
            self._log.info("Started reconciliation task", LogColor.BLUE)

        if self.own_books_audit_interval_secs and not self._own_books_audit_task:
            self._own_books_audit_task = self._loop.create_task(
                self._own_books_audit_loop(self.own_books_audit_interval_secs),
                name="own_books_audit",
            )

    def _on_stop(self) -> None:
        self._is_shutting_down = True

        if self._reconciliation_task:
            self._log.debug(f"Canceling task '{self._reconciliation_task.get_name()}'")
            self._reconciliation_task.cancel()
            self._reconciliation_task = None

        if self._own_books_audit_task:
            self._log.debug(f"Canceling task '{self._own_books_audit_task.get_name()}'")
            self._own_books_audit_task.cancel()
            self._own_books_audit_task = None

        if self._filtered_external_orders_count > 0:
            self._log.info(
                f"Filtered {self._filtered_external_orders_count:,} unclaimed EXTERNAL orders during run",
                LogColor.BLUE,
            )

        if self._kill:
            return  # Avoids enqueuing unnecessary sentinel messages when termination already signaled

        # This will stop queue processing as soon as they 'see' the sentinel message
        self._enqueue_sentinel()

    def _enqueue_sentinel(self) -> None:
        # Signal queue processing to stop
        self._loop.call_soon_threadsafe(self._cmd_queue.put_nowait, self._sentinel)
        self._loop.call_soon_threadsafe(self._evt_queue.put_nowait, self._sentinel)
        self._log.debug("Sentinel messages placed on queues")

    # -- COMMANDS ----------------------------------------------------------------------------------

    def kill(self) -> None:
        """
        Kill the engine by abruptly canceling the queue task and calling stop.
        """
        self._log.warning("Killing engine")
        self._kill = True
        self.stop()

        if self._cmd_queue_task:
            self._log.debug(f"Canceling task '{self._cmd_queue_task.get_name()}'")
            self._cmd_queue_task.cancel()
            self._cmd_queue_task = None

        if self._evt_queue_task:
            self._log.debug(f"Canceling task '{self._evt_queue_task.get_name()}'")
            self._evt_queue_task.cancel()
            self._evt_queue_task = None

    def execute(self, command: Command) -> None:
        """
        Execute the given command.

        If the internal queue is already full then will log a warning and block
        until queue size reduces.

        Parameters
        ----------
        command : Command
            The command to execute.

        """
        self._cmd_enqueuer.enqueue(command)

    def process(self, event: OrderEvent) -> None:
        """
        Process the given event message.

        If the internal queue is at or near capacity, it logs a warning (throttled)
        and schedules an asynchronous `put()` operation. This ensures all messages are
        eventually enqueued and processed without blocking the caller when the queue is full.

        Parameters
        ----------
        event : OrderEvent
            The event to process.

        """
        self._record_local_activity(event)

        if isinstance(event, OrderFilled):
            self._record_received_position_fill(event)

        self._evt_enqueuer.enqueue(event)

    # -- QUEUE PROCESSING --------------------------------------------------------------------------

    async def _run_cmd_queue(self) -> None:
        self._log.debug(
            f"Command message queue processing starting (qsize={self.cmd_qsize()})",
        )
        try:
            while True:
                try:
                    command: Command | None = await self._cmd_queue.get()
                    if command is self._sentinel:
                        break

                    self._execute_command(command)
                except asyncio.CancelledError:
                    self._log.warning("Canceled task 'run_cmd_queue'")
                    break
                except Exception as e:
                    self._handle_queue_exception(e, "command")
        finally:
            stopped_msg = "Command message queue stopped"

            if not self._cmd_queue.empty():
                self._log.warning(f"{stopped_msg} with {self.cmd_qsize()} message(s) on queue")
            else:
                self._log.debug(stopped_msg)

    async def _run_evt_queue(self) -> None:
        self._log.debug(
            f"Event message queue processing starting (qsize={self.evt_qsize()})",
        )
        try:
            while True:
                try:
                    event: OrderEvent | None = await self._evt_queue.get()
                    if event is self._sentinel:
                        break

                    self._handle_event_with_tracking(event)
                except asyncio.CancelledError:
                    self._log.warning("Canceled task 'run_evt_queue'")
                    break
                except Exception as e:
                    self._handle_queue_exception(e, "event")
        finally:
            stopped_msg = "Event message queue stopped"

            if not self._evt_queue.empty():
                self._log.warning(f"{stopped_msg} with {self.evt_qsize()} message(s) on queue")
            else:
                self._log.debug(stopped_msg)

    def _handle_queue_exception(self, e: Exception, queue_name: str) -> None:
        self._log.exception(
            f"Unexpected exception in {queue_name} queue processing: {e!r}",
            e,
        )

        if self.graceful_shutdown_on_exception:
            if not self._is_shutting_down:
                self._log.warning(
                    "Initiating graceful shutdown due to unexpected exception",
                )
                self.shutdown_system(
                    f"Unexpected exception in {queue_name} queue processing: {e!r}",
                )
                self._is_shutting_down = True
        else:
            self._log.error(
                "System will terminate immediately to prevent operation in degraded state",
            )
            os._exit(1)  # Immediate crash

    # -- CONTINUOUS MONITORING ---------------------------------------------------------------------

    async def _own_books_audit_loop(self, interval_secs: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval_secs)
                self._cache.audit_own_order_books()
        except asyncio.CancelledError:
            self._log.debug("Canceled task 'own_books_audit'")
        except Exception as e:
            self._log.exception("Error auditing own books", e)

    # ruff: noqa: C901
    async def _continuous_reconciliation_loop(self) -> None:
        try:
            # Convert intervals to nanoseconds (handle None values)
            inflight_check_interval_ns = (
                millis_to_nanos(self.inflight_check_interval_ms)
                if self.inflight_check_interval_ms > 0
                else 0
            )
            consistency_check_interval_ns = (
                secs_to_nanos(self.open_check_interval_secs) if self.open_check_interval_secs else 0
            )
            position_check_interval_ns = (
                secs_to_nanos(self.position_check_interval_secs)
                if self.position_check_interval_secs
                else 0
            )
            cache_prune_interval_ns = secs_to_nanos(60.0)

            # Determine minimum sleep interval (in seconds)
            intervals_secs: list[float] = []

            if self.inflight_check_interval_ms > 0:
                intervals_secs.append(self.inflight_check_interval_ms / 1000)

            if self.open_check_interval_secs:
                intervals_secs.append(self.open_check_interval_secs)

            if self.position_check_interval_secs:
                intervals_secs.append(self.position_check_interval_secs)

            min_interval_secs = min(intervals_secs) if intervals_secs else 1.0

            self._log.info(
                f"Starting continuous reconciliation with intervals: "
                f"inflight={self.inflight_check_interval_ms}ms, "
                f"consistency={self.open_check_interval_secs}s, "
                f"position={self.position_check_interval_secs}s",
                LogColor.BLUE,
            )

            # Only wait if reconciliation is enabled (otherwise event never set)
            if self.reconciliation:
                self._log.info(
                    "Awaiting startup reconciliation completion before starting continuous checks",
                    LogColor.BLUE,
                )
                await self._startup_reconciliation_event.wait()
                self._log.info("Startup reconciliation completed", LogColor.GREEN)

                # Apply additional startup delay AFTER reconciliation completes
                if self.reconciliation_startup_delay_secs > 0:
                    self._log.info(
                        f"Applying post-reconciliation startup delay "
                        f"({self.reconciliation_startup_delay_secs}s)",
                        LogColor.BLUE,
                    )
                    await asyncio.sleep(self.reconciliation_startup_delay_secs)
            else:
                self._log.info(
                    "Startup reconciliation disabled, proceeding with continuous checks",
                    LogColor.BLUE,
                )

            # Initialize timestamps to current time so first checks wait the full interval,
            # giving execution clients time to complete their connection initialization
            ts_now_init = self._clock.timestamp_ns()
            ts_last_inflight_check = ts_now_init
            ts_last_consistency_check = ts_now_init
            ts_last_position_check = ts_now_init
            ts_last_cache_prune = ts_now_init

            while True:
                if self._is_shutting_down:
                    self._log.debug("Reconciliation loop exiting due to stop signal")
                    break

                ts_now = self._clock.timestamp_ns()

                # Check in-flight orders
                if (
                    inflight_check_interval_ns > 0
                    and ts_now - ts_last_inflight_check >= inflight_check_interval_ns
                ):
                    # Check stop signal before starting check
                    if self._is_shutting_down:
                        break
                    try:
                        await self._check_inflight_orders()
                        ts_last_inflight_check = ts_now
                    except Exception as e:
                        self._log.exception("Failed in check_inflight_orders", e)

                # Check open orders consistency
                if (
                    consistency_check_interval_ns > 0
                    and ts_now - ts_last_consistency_check >= consistency_check_interval_ns
                ):
                    # Check stop signal before starting check
                    if self._is_shutting_down:
                        break
                    try:
                        await self._check_orders_consistency()
                        ts_last_consistency_check = ts_now
                    except Exception as e:
                        self._log.exception("Failed in check_orders_consistency", e)

                # Check positions consistency
                if (
                    position_check_interval_ns > 0
                    and ts_now - ts_last_position_check >= position_check_interval_ns
                ):
                    # Check stop signal before starting check
                    if self._is_shutting_down:
                        break
                    try:
                        await self._check_positions_consistency()
                        ts_last_position_check = ts_now
                    except Exception as e:
                        self._log.exception("Failed in check_positions_consistency", e)

                if ts_now - ts_last_cache_prune >= cache_prune_interval_ns:
                    try:
                        self._prune_recent_fills_cache()
                        self._prune_overfill_rejected_fills()
                        ts_last_cache_prune = ts_now
                    except Exception as e:
                        self._log.exception("Failed in prune_recent_fills_cache", e)

                await asyncio.sleep(min_interval_secs)
        except asyncio.CancelledError:
            self._log.debug("Canceled task 'continuous_reconciliation'")

    async def _check_inflight_orders(self) -> None:
        if self._is_shutting_down:
            self._log.debug("Skipping in-flight orders check due to stop signal")
            return

        self._log.debug("Checking in-flight orders status")

        delayed_orders: list[Order] = []
        inflight_orders: list[Order] = self._cache.orders_inflight()

        ts_now = self._clock.timestamp_ns()

        for order in inflight_orders:
            if ts_now > order.last_event.ts_event + self._inflight_check_threshold_ns:
                delayed_orders.append(order)

        if delayed_orders:
            self._log.debug(
                f"Detected {len(delayed_orders)} delayed in-flight "
                f"order{'' if len(delayed_orders) == 1 else 's'}",
            )

        # Query and potentially resolve each inconsistent order
        for order in delayed_orders:
            if not order.is_inflight:
                self._clear_recon_tracking(order.client_order_id, drop_last_query=False)
                continue

            last_query_ts = self._ts_last_query.get(order.client_order_id)
            if last_query_ts and ts_now - last_query_ts < self._inflight_check_threshold_ns:
                self._log.debug(
                    f"Skipping re-query for {order.client_order_id!r} - awaiting prior response",
                )
                continue

            retries = self._recon_check_retries[order.client_order_id]
            if retries >= self.inflight_check_max_retries:
                backlog = self.evt_qsize()
                if backlog > 0:
                    self._log.debug(
                        f"Deferring inflight resolution for {order.client_order_id!r} - event queue backlog {backlog}",
                    )
                    continue

                self._log.warning(
                    f"Order {order.client_order_id!r} exceeded max inflight retries ({retries}), "
                    f"resolving as failed",
                    LogColor.YELLOW,
                )
                self._resolve_inflight_order(order)
            else:
                self._log.debug(f"Querying {order} with venue...")
                query_ts = self._clock.timestamp_ns()
                query = QueryOrder(
                    trader_id=order.trader_id,
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                    command_id=UUID4(),
                    ts_init=query_ts,
                )
                self._execute_command(query)
                self._ts_last_query[order.client_order_id] = query_ts
                self._recon_check_retries[order.client_order_id] = retries + 1

    def _resolve_inflight_order(self, order: Order) -> None:
        if not order.is_inflight:
            self._log.debug(
                f"Skipping inflight resolution for {order.client_order_id!r} - current status {order.status_string()}",
            )
            self._clear_recon_tracking(order.client_order_id)
            self._order_local_activity_ns.pop(order.client_order_id, None)
            return

        ts_now = self._clock.timestamp_ns()

        if order.status == OrderStatus.SUBMITTED:
            rejected = create_order_rejected_event(
                order=order,
                ts_now=ts_now,
                reason="UNKNOWN",
            )
            self._log.debug(f"Generated {rejected}")
            self._handle_event_with_tracking(rejected)
        elif order.status in (OrderStatus.PENDING_UPDATE, OrderStatus.PENDING_CANCEL):
            canceled = create_order_canceled_event(
                order=order,
                ts_now=ts_now,
            )
            self._log.debug(f"Generated {canceled}")
            self._handle_event_with_tracking(canceled)
        else:
            raise RuntimeError(f"Invalid status for in-flight order, was '{order.status_string()}'")

        self._clear_recon_tracking(order.client_order_id)
        self._order_local_activity_ns.pop(order.client_order_id, None)

    async def _check_positions_consistency(self) -> None:
        await self._run_position_convergence_pass("position check")

    async def _run_position_convergence_pass(self, trigger: str) -> bool:
        # Single convergence pass shared by startup and the continuous check, building one
        # venue snapshot per scope, diffing it against the cache, and applying target-safe
        # repairs, triggered rather than driven by event type so both callers behave alike.
        if self._is_shutting_down:
            self._log.debug("Skipping position convergence pass due to stop signal")
            return True

        if self.filter_position_reports:
            self._log.debug(
                "Skipping position convergence pass: `filter_position_reports` enabled",
            )
            return True

        if not self._clients:
            self._log.debug("No execution clients for position convergence, early return")
            return True

        self._log.debug(f"Running position convergence pass ({trigger})")

        # Capture the per-scope activity watermarks before the query so a fill arriving
        # while the query is in flight can be detected before any repair is applied.
        watermarks = dict(self._position_local_activity_ns)

        client_reports, failed_clients = await self._query_position_status_reports()

        # Scopes are built for every failed client before the partition below, so what
        # this pass can derive of a client's coverage does not depend on which side of
        # that partition the client lands on. Every scope a no-verdict client covers is
        # spared below and never judged, so building it here only puts it on record.
        scopes = self._build_position_scopes(client_reports, failed_clients)

        # A query which failed while its client is no longer running failed on that
        # client's own deliberate lifecycle rather than on an observation of the venue,
        # so the pass holds no verdict for its coverage: those scopes are neither judged
        # nor published, and whatever the previous document says of them still stands.
        # Deriving that coverage is what makes the distinction usable, so a client with
        # no scope to spare stays an observability failure and reaches the guard which
        # publishes nothing rather than erasing state on an unmade observation. The
        # scopes spared below decide it, so a client whose only coverage is state this
        # pass carries is classified on that rather than on loaded instruments alone.
        no_verdict_clients: list[ExecutionClient] = []
        observability_failures: list[ExecutionClient] = []
        no_verdict_keys: set[InstrumentAccountKey] = set()

        for client in failed_clients:
            keys: set[InstrumentAccountKey] = (
                set() if client.is_running else self._no_verdict_scope_keys(client, scopes)
            )

            if keys:
                no_verdict_clients.append(client)
                no_verdict_keys.update(keys)
            else:
                observability_failures.append(client)

        failed_clients = observability_failures

        if no_verdict_clients:
            self._log.debug(
                f"Position convergence pass ({trigger}) holds no verdict for "
                f"{sorted(client.id.value for client in no_verdict_clients)}: "
                "client no longer running",
            )

        residuals: dict[str, dict[str, object]] = {}
        residual_keys: list[InstrumentAccountKey] = []
        deferred_scopes: list[str] = [
            f"{key[0]}|{key[1]}"
            for key in sorted(no_verdict_keys, key=lambda k: (k[0].value, k[1].value))
        ]
        ts_now = self._clock.timestamp_ns()

        for key in sorted(scopes, key=lambda k: (k[0].value, k[1].value)):
            if key in no_verdict_keys:
                continue

            scope_key = f"{key[0]}|{key[1]}"

            try:
                verdict, reason = await self._converge_position_scope(scopes[key], watermarks)
            except Exception as e:
                self._log.exception(f"Failed converging position scope {key}", e)
                verdict, reason = (
                    POSITION_SCOPE_UNCONVERGED,
                    f"convergence raised {type(e).__name__}",
                )

            if verdict == POSITION_SCOPE_DEFERRED:
                deferred_scopes.append(scope_key)
                continue

            if verdict == POSITION_SCOPE_UNCONVERGED:
                residual_keys.append(key)
                residuals[scope_key] = {
                    "reason": reason or "unresolved position discrepancy",
                    "ts": ts_now,
                }

        # A deferred scope produced no verdict, so what the previous document says of it
        # still stands: a standing residual is carried forward unchanged and an absent
        # entry stays absent, rather than the deferral itself being published as one.
        if deferred_scopes:
            previous = self._read_position_residuals()

            for scope_key in deferred_scopes:
                carried = previous.get(scope_key)

                if carried is not None:
                    residuals[scope_key] = carried

        # A failed client no residual scope represents leaves its coverage unaccounted
        # for, so writing here would publish convergence this pass never observed. The
        # previous document stands untouched instead and the pass reports failure.
        unrepresented = [
            client
            for client in failed_clients
            if not any(
                self._client_covers_position_scope(client, instrument_id, account_id)
                for instrument_id, account_id in residual_keys
            )
        ]

        if unrepresented:
            self._log.error(self._unpublishable_residuals_reason(trigger, unrepresented))
            published = False
        else:
            published = self._write_position_residuals(residuals)

        # Prune retry counters for scopes no longer observed, leaving the no-verdict ones
        # alone: this pass observed nothing of them, so it erases nothing of them either.
        stale = [
            key
            for key in self._position_recon_retries
            if key not in scopes and key not in no_verdict_keys
        ]

        for key in stale:
            self._position_recon_retries.pop(key, None)

        if residual_keys:
            healing, action_required = self._unresolved_scope_summary(residuals, residual_keys)

            if healing:
                self._log.info(
                    f"Position convergence pass ({trigger}) healed venue fills for "
                    f"{len(healing)} scope(s), re-querying next pass: {healing}",
                    LogColor.BLUE,
                )

            if action_required:
                self._log.warning(
                    f"Position convergence pass ({trigger}) unresolved for "
                    f"{len(action_required)} scope(s): {action_required}",
                    LogColor.YELLOW,
                )

            return False

        if failed_clients:
            self._log.error(
                f"Position convergence pass ({trigger}) could not observe "
                f"{sorted(client.id.value for client in failed_clients)}",
            )
            return False

        if not published:
            return False

        if deferred_scopes:
            self._log.debug(
                f"Position convergence pass ({trigger}) converged, "
                f"deferring {len(deferred_scopes)} scope(s): {sorted(deferred_scopes)}",
            )
        else:
            self._log.debug(f"Position convergence pass ({trigger}) converged")

        return True

    def _unresolved_scope_summary(
        self,
        residuals: dict[str, dict[str, object]],
        residual_keys: list[InstrumentAccountKey],
    ) -> tuple[list[str], list[str]]:
        # A scope healed from the venue's own fill history is held for re-observation
        # rather than left standing, so it is summarized apart from the scopes whose
        # discrepancy still needs acting on.
        healing: list[str] = []
        action_required: list[str] = []

        for key in residual_keys:
            scope_key = f"{key[0]}|{key[1]}"

            if residuals[scope_key]["reason"] == POSITION_SCOPE_HEALED_REASON:
                healing.append(scope_key)
            else:
                action_required.append(scope_key)

        return sorted(healing), sorted(action_required)

    def _unpublishable_residuals_reason(
        self,
        trigger: str,
        unrepresented: list[ExecutionClient],
    ) -> str:
        # Built separately so the single log line carries both the failed client and why
        # its unobserved scopes could not be derived (an unresolved account has none).
        clients = ", ".join(
            sorted(f"{client.id} (account_id={client.account_id})" for client in unrepresented),
        )

        return (
            f"Position convergence pass ({trigger}) is not writing reconciliation residuals: "
            f"no residual scope represents the failed client(s) {clients}, so their "
            "unobserved scopes could not be derived; the previous document stands"
        )

    async def _query_position_status_reports(
        self,
    ) -> tuple[list[tuple[ExecutionClient, list[PositionStatusReport]]], list[ExecutionClient]]:
        clients = list(self._clients.values())

        tasks = [
            c.generate_position_status_reports(
                GeneratePositionStatusReports(
                    instrument_id=None,  # Get all positions
                    start=None,  # No time filter - we want all open and closed positions
                    end=None,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                    log_receipt_level=LogLevel.DEBUG,
                ),
            )
            for c in clients
        ]

        try:
            position_reports_all = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            self._log.error(f"Failed to gather position status reports: {e}")
            return [], clients

        client_reports: list[tuple[ExecutionClient, list[PositionStatusReport]]] = []
        failed_clients: list[ExecutionClient] = []

        for client, reports_or_exception in zip(clients, position_reports_all, strict=True):
            if isinstance(reports_or_exception, BaseException):
                failed_clients.append(client)
                message = (
                    f"Failed to generate position status reports for venue {client.venue}: "
                    f"{reports_or_exception}"
                )

                # A client which is no longer running is stopping deliberately, so its
                # query failing is its own lifecycle rather than a venue we cannot reach.
                if client.is_running:
                    self._log.error(message)
                else:
                    self._log.debug(message)

                continue

            reports = cast("list[PositionStatusReport]", reports_or_exception)
            client_reports.append((client, list(reports)))

        return client_reports, failed_clients

    def _build_position_scopes(
        self,
        client_reports: list[tuple[ExecutionClient, list[PositionStatusReport]]],
        failed_clients: list[ExecutionClient],
    ) -> dict[InstrumentAccountKey, PositionScopeSnapshot]:
        reports_by_key: dict[InstrumentAccountKey, list[PositionStatusReport]] = {}

        for _client, reports in client_reports:
            for report in reports:
                if not self._consider_for_reconciliation(report.instrument_id):
                    self._log_skipping_reconciliation_on_instrument_id(report)
                    continue

                if self._cache.instrument(report.instrument_id) is None:
                    self._log.debug(
                        f"Skipping position report for {report.instrument_id}: "
                        "instrument not in cache",
                    )
                    continue

                key = (report.instrument_id, report.account_id)
                reports_by_key.setdefault(key, []).append(report)

        for position in self._cache.positions_open():
            if not self._consider_for_reconciliation(position.instrument_id):
                continue

            if self._cache.instrument(position.instrument_id) is None:
                self._log.debug(
                    f"Skipping cached position for {position.instrument_id}: "
                    "instrument not in cache",
                )
                continue

            reports_by_key.setdefault((position.instrument_id, position.account_id), [])

        for client in failed_clients:
            for key in self._failed_client_scope_keys(client):
                reports_by_key.setdefault(key, [])

        scopes: dict[InstrumentAccountKey, PositionScopeSnapshot] = {}

        for (instrument_id, account_id), scope_reports in reports_by_key.items():
            reason = self._position_snapshot_coverage_reason(
                instrument_id,
                account_id,
                scope_reports,
                failed_clients,
            )

            instrument = self._cache.instrument(instrument_id)

            if reason is None and instrument is not None:
                scope_reports, reason = self._normalize_scope_reports(
                    scope_reports,
                    instrument.size_precision,
                )

            scopes[(instrument_id, account_id)] = PositionScopeSnapshot(
                instrument_id=instrument_id,
                account_id=account_id,
                reports=tuple(scope_reports),
                incomplete_reason=reason,
            )

        return scopes

    def _no_verdict_scope_keys(
        self,
        client: ExecutionClient,
        scopes: dict[InstrumentAccountKey, PositionScopeSnapshot],
    ) -> set[InstrumentAccountKey]:
        # A no-verdict client's scopes are seeded from its loaded instruments exactly as a
        # failed client's are, then joined by every scope this pass holds state for: one
        # built this pass, or one holding a retry counter. A scope left out of that union
        # would be judged, or have its retries pruned, on an observation never made, and
        # a client the union leaves empty holds no coverage this pass could spare.
        candidates: set[InstrumentAccountKey] = set(scopes)
        candidates.update(self._position_recon_retries)

        keys: set[InstrumentAccountKey] = set(self._failed_client_scope_keys(client))
        keys.update(
            key for key in candidates if self._client_covers_position_scope(client, key[0], key[1])
        )

        return keys

    def _failed_client_scope_keys(
        self,
        client: ExecutionClient,
    ) -> list[InstrumentAccountKey]:
        # A client whose query failed leaves its scopes unobserved, so they are seeded
        # from every loaded instrument it routes for: cached position history is not a
        # complete scope list, and deriving from it alone reports a flat cache as
        # converged when nothing was actually observed.
        account_id = client.account_id

        if account_id is None:
            return []

        candidates = sorted(
            {
                instrument.id
                for instrument in self._cache.instruments()
                if self._consider_for_reconciliation(instrument.id)
            },
            key=lambda i: i.value,
        )

        return [
            (instrument_id, account_id)
            for instrument_id in candidates
            if self._client_covers_position_scope(client, instrument_id, account_id)
        ]

    def _position_snapshot_coverage_reason(
        self,
        instrument_id: InstrumentId,
        account_id: AccountId,
        reports: list[PositionStatusReport],
        failed_clients: list[ExecutionClient],
    ) -> str | None:
        for client in sorted(failed_clients, key=lambda c: c.id.value):
            if self._client_covers_position_scope(client, instrument_id, account_id):
                return f"position status query failed for {client.id}"

        # A report proves a client observed the scope; without one, silence is only
        # absence authority when some client actually reports on this account.
        if not reports and not any(
            self._client_covers_position_scope(client, instrument_id, account_id)
            for client in self._clients.values()
        ):
            return "no execution client covers this instrument and account"

        return None

    def _client_covers_position_scope(
        self,
        client: ExecutionClient,
        instrument_id: InstrumentId,
        account_id: AccountId,
    ) -> bool:
        # Cover follows the engine's own routing, so a scope is only observed by the client
        # which handles that venue, and only for that client's account. Before a client
        # connects its account is unknown, so routing alone establishes cover.
        if self._routing_map.get(instrument_id.venue, self._default_client) is not client:
            return False

        return client.account_id is None or client.account_id == account_id

    def _normalize_scope_reports(
        self,
        reports: list[PositionStatusReport],
        size_precision: int,
    ) -> tuple[list[PositionStatusReport], str | None]:
        # Reports keyed by venue position ID carry set semantics, so identical duplicates
        # collapse while a conflicting duplicate makes the snapshot incomplete. A scope
        # mixing both granularities cannot be compared at either, so it is incomplete too.
        # Pairing each ID-bearing report with its ID carries the partition's guarantee
        # into the type, so the dedup below never re-tests for `None`.
        id_bearing: list[tuple[PositionId, PositionStatusReport]] = [
            (r.venue_position_id, r) for r in reports if r.venue_position_id is not None
        ]
        id_less = [r for r in reports if r.venue_position_id is None]

        if id_bearing and id_less:
            return [], "venue mixed position-ID and net-granularity reports"

        if not id_bearing:
            return id_less, None

        deduplicated: dict[PositionId, PositionStatusReport] = {}

        # The first row sorted under an ID is the one retained, so quantity sorts
        # numerically to keep the smallest of a declared-equal pair: a repair moves the
        # cache toward venue truth and never above it, while a lexical order would retain
        # 10.0001 over 9.9999 and open a whole increment above the safe floor.
        for venue_position_id, report in sorted(
            id_bearing,
            key=lambda pair: (
                pair[0].value,
                position_side_to_str(pair[1].position_side),
                pair[1].quantity.raw,
                str(pair[1].avg_px_open),
            ),
        ):
            existing = deduplicated.get(venue_position_id)

            if existing is None:
                deduplicated[venue_position_id] = report
                continue

            quantities_equal = quantities_equal_at_size_precision(
                existing.quantity.as_decimal(),
                report.quantity.as_decimal(),
                size_precision,
            )

            if existing.position_side != report.position_side or not quantities_equal:
                return [], (
                    f"conflicting duplicate reports for venue position ID {venue_position_id}"
                )

        return list(deduplicated.values()), None

    async def _converge_position_scope(
        self,
        scope: PositionScopeSnapshot,
        watermarks: dict[InstrumentAccountKey, int],
    ) -> tuple[str, str | None]:
        instrument_id = scope.instrument_id
        account_id = scope.account_id
        key = (instrument_id, account_id)
        instrument = self._cache.instrument(instrument_id)

        if scope.incomplete_reason is not None or instrument is None:
            reason = scope.incomplete_reason or "instrument not loaded"
            self._log.warning(
                f"Position snapshot incomplete for {instrument_id} {account_id}: "
                f"{reason}; no repair and no absence authority this pass",
                LogColor.YELLOW,
            )
            return POSITION_SCOPE_UNCONVERGED, reason

        intents = self._position_repair_intents(scope, instrument)

        if not intents:
            self._position_recon_retries.pop(key, None)
            return POSITION_SCOPE_CONVERGED, None

        retries = self._position_recon_retries[key]

        if retries >= self.position_check_retries:
            self._log.error(
                f"Position discrepancy for {instrument_id} unresolved after "
                f"{self.position_check_retries} attempts; "
                "no further reconciliation attempts will be made",
            )
            return (
                POSITION_SCOPE_UNCONVERGED,
                f"unresolved after {self.position_check_retries} repair attempts",
            )

        deferral = self._position_repair_deferral(key, watermarks)

        if deferral is not None:
            self._log.info(
                f"Deferring position convergence for {instrument_id} {account_id}: {deferral}",
                LogColor.BLUE,
            )
            return POSITION_SCOPE_DEFERRED, deferral

        # A discrepancy the venue's fill history can close is a delivery gap healing
        # itself; what the healing leaves standing is reported where it is repaired or
        # published as unresolved.
        self._log.info(
            f"Position discrepancy detected for {instrument_id} {account_id}: "
            f"{len(intents)} repair(s) required; querying for missing fills...",
            LogColor.BLUE,
        )

        if await self._heal_position_scope_from_fills(instrument_id):
            # Applying venue fills invalidates the snapshot on the cache timestamp axis
            return POSITION_SCOPE_UNCONVERGED, POSITION_SCOPE_HEALED_REASON

        # Shutdown beginning during the awaited fill query leaves the healed fills
        # unapplied, so a repair here would act on a snapshot the healing never corrected.
        if self._is_shutting_down:
            self._log.debug("Skipping position repair due to stop signal")
            return POSITION_SCOPE_CONVERGED, None

        intents = self._position_repair_intents(scope, instrument)

        if not intents:
            self._position_recon_retries.pop(key, None)
            return POSITION_SCOPE_CONVERGED, None

        if not self.generate_missing_orders:
            self._log.warning(
                f"Position discrepancy for {instrument_id} unresolved when "
                "`generate_missing_orders` disabled, skipping repair",
                LogColor.YELLOW,
            )
            return POSITION_SCOPE_UNCONVERGED, "`generate_missing_orders` disabled"

        # Re-read the watermark immediately before the first synthetic event: local
        # activity during the awaited queries invalidates this snapshot.
        deferral = self._position_repair_deferral(key, watermarks)

        if deferral is not None:
            self._log.info(
                f"Discarding position repair for {instrument_id} {account_id}: {deferral}",
                LogColor.BLUE,
            )
            return POSITION_SCOPE_DEFERRED, deferral

        attempted = self._apply_position_repairs(scope, intents)

        if not self._position_repair_intents(scope, instrument):
            self._position_recon_retries.pop(key, None)
            return POSITION_SCOPE_CONVERGED, None

        if not attempted:
            return POSITION_SCOPE_UNCONVERGED, "no applicable repair for this discrepancy"

        self._position_recon_retries[key] = retries + 1
        self._log.error(
            f"Position repair for {instrument_id} {account_id} did not meet its "
            f"postcondition (attempt {retries + 1} of {self.position_check_retries})",
        )

        return POSITION_SCOPE_UNCONVERGED, "repair postcondition not met"

    def _position_repair_intents(
        self,
        scope: PositionScopeSnapshot,
        instrument: Instrument,
        positions_open: list[Position] | None = None,
    ) -> list[PositionRepairIntent]:
        # `positions_open` narrows the comparison when the caller holds positive truth
        # for part of the scope only; the whole scope is compared when it is omitted.
        if positions_open is None:
            positions_open = self._cache.positions_open(
                venue=None,  # Faster query filtering
                instrument_id=scope.instrument_id,
                account_id=scope.account_id,
            )

        return diff_position_scope(
            list(scope.reports),
            positions_open,
            instrument.size_precision,
        )

    def _position_repair_deferral(
        self,
        key: InstrumentAccountKey,
        watermarks: dict[InstrumentAccountKey, int],
    ) -> str | None:
        if self._received_unapplied_fill_counts[key] > 0:
            return "received fill awaiting local application"

        last_activity_ts = self._position_local_activity_ns.get(key)

        if last_activity_ts is None:
            return None

        if last_activity_ts != watermarks.get(key):
            return "local activity during venue position query"

        if self._position_check_threshold_ns > 0:
            ts_now = self._clock.timestamp_ns()

            if ts_now - last_activity_ts < self._position_check_threshold_ns:
                return (
                    f"recent local activity within threshold ({self.position_check_threshold_ms}ms)"
                )

        return None

    async def _heal_position_scope_from_fills(self, instrument_id: InstrumentId) -> bool:
        missing_fills, had_fill_query_errors = await self._query_and_find_missing_fills(
            instrument_id,
            self._clients.values(),
        )

        if had_fill_query_errors:
            self._log.warning(
                f"Fill report query failed for {instrument_id}; "
                "continuing on the venue position snapshot",
                LogColor.YELLOW,
            )

        applied = await self._reconcile_missing_fills(missing_fills, instrument_id)

        return applied > 0

    def _apply_position_repairs(
        self,
        scope: PositionScopeSnapshot,
        intents: list[PositionRepairIntent],
    ) -> bool:
        # Returns whether any synthetic repair was applied, which is what makes a failed
        # postcondition a consumed retry rather than a skipped intent.
        instrument = self._cache.instrument(scope.instrument_id)

        if instrument is None:
            self._log.debug(
                f"Cannot repair positions for {scope.instrument_id}: instrument not found",
            )
            return False

        attempted = False

        for intent in intents:
            outcome = self._apply_position_repair(scope, instrument, intent)

            if outcome is None:
                continue  # Skipped without applying anything

            attempted = True

            if not outcome:
                self._log.error(
                    f"Aborting remaining position repairs for {scope.instrument_id} "
                    f"{scope.account_id} after a failed {intent.action}",
                )
                break

        return attempted

    def _apply_position_repair(
        self,
        scope: PositionScopeSnapshot,
        instrument: Instrument,
        intent: PositionRepairIntent,
    ) -> bool | None:
        if intent.action == POSITION_REPAIR_TRIM:
            return self._apply_position_trim(scope, instrument, intent)

        return self._apply_position_open(scope, instrument, intent)

    def _apply_position_trim(
        self,
        scope: PositionScopeSnapshot,
        instrument: Instrument,
        intent: PositionRepairIntent,
    ) -> bool | None:
        # Revalidate the target at application: an over-close or flip must be impossible
        # by construction, so the quantity is re-capped at the target's current quantity.
        target = self._cache.position(intent.target_position_id)

        if (
            target is None
            or not target.is_open
            or target.instrument_id != scope.instrument_id
            or target.account_id != scope.account_id
        ):
            self._log.error(
                f"Cannot trim position {intent.target_position_id!r} for "
                f"{scope.instrument_id}: target is no longer an open position of this scope",
            )
            return None  # Nothing applied, the scope stays unconverged

        expected_side = OrderSide.SELL if target.side == PositionSide.LONG else OrderSide.BUY

        if expected_side != intent.order_side:
            self._log.error(
                f"Cannot trim position {target.id!r} for {scope.instrument_id}: "
                f"target side changed to {position_side_to_str(target.side)} since the snapshot",
            )
            return None  # Nothing applied, the scope stays unconverged

        quantity = self._make_repair_quantity(
            instrument,
            min(intent.quantity, target.quantity.as_decimal()),
        )

        if quantity is None:
            self._log.warning(
                f"Trim quantity {intent.quantity} is not representable at the size "
                f"precision of {instrument.id}, skipping repair of {target.id!r}",
                LogColor.YELLOW,
            )
            return None  # Nothing applied, the scope stays unconverged

        expected_qty = target.quantity.as_decimal() - quantity.as_decimal()

        self._log.warning(
            f"Repairing excess position {target.id!r} for {scope.instrument_id}: "
            f"reduce-only {order_side_to_str(intent.order_side)} {quantity} "
            f"({target.quantity} -> {expected_qty})",
            LogColor.YELLOW,
        )

        applied = self._apply_position_repair_order(
            scope=scope,
            instrument=instrument,
            order_side=intent.order_side,
            quantity=quantity,
            avg_px=intent.avg_px,
            position_id=target.id,
            strategy_id=target.strategy_id,
            reduce_only=True,
            tag=POSITION_REPAIR_TRIM,
        )

        if not applied:
            return False

        repaired = self._cache.position(target.id)

        if repaired is None or not quantities_equal_at_size_precision(
            repaired.quantity.as_decimal(),
            expected_qty,
            instrument.size_precision,
        ):
            self._log.error(
                f"Position trim postcondition failed for {target.id!r}: "
                f"expected {expected_qty}, "
                f"was {repaired.quantity if repaired else None}",
            )
            return False

        return True

    def _apply_position_open(
        self,
        scope: PositionScopeSnapshot,
        instrument: Instrument,
        intent: PositionRepairIntent,
    ) -> bool | None:
        # A deficit opens at the venue quantity rounded toward zero, so the repair can
        # never hold more exposure than the venue reported.
        quantity = self._make_repair_quantity(instrument, intent.quantity)

        if quantity is None or not quantities_equal_at_size_precision(
            quantity.as_decimal(),
            intent.quantity,
            instrument.size_precision,
        ):
            self._log.warning(
                f"Open quantity {intent.quantity} is not representable at the size "
                f"precision of {instrument.id}, skipping repair",
                LogColor.YELLOW,
            )
            return None  # Nothing applied, the scope stays unconverged

        strategy_id = self._position_open_strategy_id(scope, intent.target_position_id)
        position_id = self._resolve_position_repair_id(
            scope,
            intent.target_position_id,
            strategy_id,
        )

        if not self._position_id_is_free_for_scope(scope, position_id, strategy_id):
            return None  # Nothing applied, the scope stays unconverged

        self._log.warning(
            f"Repairing missing venue position for {scope.instrument_id} "
            f"{position_id!r}: {order_side_to_str(intent.order_side)} {quantity} "
            f"under {strategy_id}",
            LogColor.YELLOW,
        )

        applied = self._apply_position_repair_order(
            scope=scope,
            instrument=instrument,
            order_side=intent.order_side,
            quantity=quantity,
            avg_px=intent.avg_px,
            position_id=position_id,
            strategy_id=strategy_id,
            reduce_only=False,
            tag=POSITION_REPAIR_OPEN,
        )

        if not applied:
            return False

        opened = self._cache.position(position_id)

        if opened is None or not opened.is_open:
            self._log.error(
                f"Position open postcondition failed for {position_id!r}: "
                "no open position after applying the repair",
            )
            return False

        return True

    def _position_open_strategy_id(
        self,
        scope: PositionScopeSnapshot,
        position_id: PositionId | None,
    ) -> StrategyId:
        # An extension of an existing venue-ID position must preserve its owner
        existing = self._cache.position(position_id) if position_id is not None else None

        if existing is not None and (
            existing.instrument_id == scope.instrument_id
            and existing.account_id == scope.account_id
        ):
            return existing.strategy_id

        return self.get_external_order_claim(scope.instrument_id) or StrategyId("EXTERNAL")

    def _resolve_position_repair_id(
        self,
        scope: PositionScopeSnapshot,
        position_id: PositionId | None,
        strategy_id: StrategyId,
    ) -> PositionId:
        if position_id is not None:
            return position_id

        # Mirror `ExecutionEngine._resolve_oms_type`. It is a cdef method and is not
        # callable from this Python subclass, while its readonly inputs are exposed.
        oms_type = self._oms_overrides.get(strategy_id, OmsType.UNSPECIFIED)

        if oms_type == OmsType.UNSPECIFIED:
            client = self._routing_map.get(scope.instrument_id.venue, self._default_client)
            oms_type = client.oms_type if client is not None else OmsType.NETTING

        if oms_type == OmsType.NETTING:
            # This is the canonical NETTING identity and must not change
            return PositionId(f"{scope.instrument_id}-{strategy_id}")

        # HEDGING reports without a venue position ID need a fabricated ID which is
        # unique across both account and strategy scopes.
        return PositionId(f"{scope.instrument_id}-{scope.account_id}-{strategy_id}")

    def _position_id_is_free_for_scope(
        self,
        scope: PositionScopeSnapshot,
        position_id: PositionId,
        strategy_id: StrategyId,
    ) -> bool:
        # Position IDs are keyed globally in the cache, so an ID already belonging to
        # another instrument, account, or strategy must never be bound here.
        existing = self._cache.position(position_id)

        if existing is not None and (
            existing.instrument_id != scope.instrument_id
            or existing.account_id != scope.account_id
            or existing.strategy_id != strategy_id
        ):
            self._log.error(
                f"Cannot open position {position_id!r} for {scope.instrument_id} "
                f"{scope.account_id}: the ID already belongs to {existing.instrument_id} "
                f"{existing.account_id} under {existing.strategy_id}",
            )
            return False

        # An open position under this ID is the authority for it: the repair increases
        # that position toward the venue row rather than minting exposure under an empty
        # label, capped at what the row leaves uncovered, postcondition-checked, and
        # re-observed by the next pass. Hedge venues keep every exit bound to the side
        # label, so refusing here starves the side's deficit repair on every pass.
        if existing is not None and existing.is_open:
            return True

        # A live order bound to this ID is not free even when its identity matches: a
        # later fill could otherwise grow the just-repaired position beyond the venue
        # target. A closed order can never fill again, so it is not ownership evidence
        # and the position above stays the authority for an ID which already exists.
        for order in self._cache.orders_for_position(position_id):
            if order.is_closed:
                continue

            self._log.error(
                f"Cannot open position {position_id!r} for {scope.instrument_id} "
                f"{scope.account_id}: the ID is already bound to the open order "
                f"{order.client_order_id!r} for {order.instrument_id}, "
                f"account={order.account_id}, strategy={order.strategy_id}",
            )
            return False

        return True

    def _apply_position_repair_order(
        self,
        scope: PositionScopeSnapshot,
        instrument: Instrument,
        order_side: OrderSide,
        quantity: Quantity,
        avg_px: Decimal | None,
        position_id: PositionId | None,
        strategy_id: StrategyId,
        reduce_only: bool,
        tag: str,
    ) -> bool:
        # Always a fresh reconciliation order: reusing a historical cached order makes the
        # repair a silent no-op against a position it was never meant to move.
        client_order_id = ClientOrderId(UUID4().value)
        ts_now = self._clock.timestamp_ns()
        price = self._position_repair_price(instrument, order_side, avg_px)
        order_type = OrderType.LIMIT if price is not None else OrderType.MARKET

        if price is None:
            self._log.warning(
                f"Could not determine a repair price for {scope.instrument_id}, "
                "generating MARKET order for position repair",
            )

        report = OrderStatusReport(
            instrument_id=scope.instrument_id,
            account_id=scope.account_id,
            client_order_id=client_order_id,
            venue_order_id=self._create_synthetic_reconciliation_venue_order_id(
                account_id=scope.account_id,
                instrument_id=scope.instrument_id,
                order_side=order_side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                venue_position_id=position_id,
                ts_last=ts_now,
                tag=tag,
            ),
            venue_position_id=position_id,
            order_side=order_side,
            order_type=order_type,
            time_in_force=TimeInForce.GTC if price is not None else TimeInForce.IOC,
            order_status=OrderStatus.FILLED,
            price=price,
            quantity=quantity,
            filled_qty=quantity,
            avg_px=price.as_decimal() if price is not None else None,
            reduce_only=reduce_only,
            report_id=UUID4(),
            ts_accepted=ts_now,
            ts_last=ts_now,
            ts_init=ts_now,
        )

        # Bind the repair to its target before the fill resolves a position ID, so the
        # fill can only move the position the diff selected.
        order = self._generate_order(report, is_external=False, strategy_id=strategy_id)

        if order is None:
            self._log.error(
                f"Cannot repair position for {scope.instrument_id}: "
                f"reconciliation order {client_order_id!r} was not generated",
            )
            return False

        self._cache.add_order(order, position_id)

        return self._reconcile_order_report(report, trades=[], is_external=False)

    def _make_repair_quantity(
        self,
        instrument: Instrument,
        value: Decimal,
    ) -> Quantity | None:
        # `Instrument.make_qty` converts through a double, which floors an exact decimal a
        # whole increment low (0.58 at size precision 2 becomes 0.57), so the quantity is
        # built from the decimal itself, rounded toward zero at the declared precision.
        with localcontext(prec=QUANTITY_CONTEXT_PRECISION):
            quantized = value.quantize(
                Decimal(1).scaleb(-instrument.size_precision),
                rounding=ROUND_DOWN,
            )

            if quantized <= 0:
                return None

            try:
                return Quantity.from_str(str(quantized))
            except ValueError as e:
                self._log.error(
                    f"Cannot build a repair quantity of {quantized} for {instrument.id}: {e}",
                )
                return None

    def _position_repair_price(
        self,
        instrument: Instrument,
        order_side: OrderSide,
        avg_px: Decimal | None,
    ) -> Price | None:
        if avg_px is not None:
            return instrument.make_price(avg_px)

        quote = self._cache.quote_tick(instrument.id)

        if quote:
            return quote.ask_price if order_side == OrderSide.BUY else quote.bid_price

        return None

    def _read_position_residuals(self) -> dict[str, dict[str, object]]:
        raw = self._cache.get(POSITION_RESIDUALS_KEY)

        if raw is None:
            return {}

        try:
            decoded = json.loads(raw)
        except Exception as e:
            self._log.error(f"Discarding invalid reconciliation residuals JSON: {e}")
            return {}

        if not isinstance(decoded, dict):
            self._log.error("Discarding reconciliation residuals with a non-object root")
            return {}

        residuals: dict[str, dict[str, object]] = {}

        for key, entry in decoded.items():
            if (
                isinstance(key, str)
                and isinstance(entry, dict)
                and isinstance(entry.get("reason"), str)
                and type(entry.get("ts")) is int
            ):
                residuals[key] = {
                    "reason": entry["reason"],
                    "ts": entry["ts"],
                }

        return residuals

    def _write_position_scope_residual(
        self,
        instrument_id: InstrumentId,
        account_id: AccountId,
        reason: str,
    ) -> bool:
        residuals = self._read_position_residuals()
        residuals[f"{instrument_id}|{account_id}"] = {
            "reason": reason,
            "ts": self._clock.timestamp_ns(),
        }
        return self._write_position_residuals(residuals)

    def _write_position_residuals(self, residuals: dict[str, dict[str, object]]) -> bool:
        try:
            payload = json.dumps(residuals, sort_keys=True, separators=(",", ":"))
            self._cache.add(POSITION_RESIDUALS_KEY, payload.encode("utf-8"))
        except Exception as e:
            self._log.exception("Failed to write reconciliation residuals", e)
            return False

        return True

    async def _query_and_find_missing_fills(
        self,
        instrument_id: InstrumentId,
        clients: Iterable[ExecutionClient],
    ) -> tuple[list[FillReport], bool]:
        fill_lookback_start = self._clock.utc_now() - pd.Timedelta(
            minutes=self.position_check_lookback_mins,
        )

        fill_tasks = [
            c.generate_fill_reports(
                GenerateFillReports(
                    instrument_id=instrument_id,
                    venue_order_id=None,
                    start=fill_lookback_start,
                    end=None,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                ),
            )
            for c in clients
        ]

        fill_reports_all = await asyncio.gather(*fill_tasks, return_exceptions=True)

        venue_fills: list[FillReport] = []
        had_fill_query_errors = False

        for fills_or_exception in fill_reports_all:
            if isinstance(fills_or_exception, BaseException):
                had_fill_query_errors = True
                self._log.error(
                    f"Failed to generate fill reports for {instrument_id}: {fills_or_exception}",
                )
                continue

            fills = cast("list[FillReport]", fills_or_exception)
            venue_fills.extend(fills)

        cached_fill_trade_ids: set[TradeId] = set()

        for order in self._cache.orders(instrument_id=instrument_id):
            for event in order.events:
                if isinstance(event, OrderFilled):
                    cached_fill_trade_ids.add(event.trade_id)

        # Find missing fills (not in cache, not in recent fills cache, and not already
        # accounted for on their order by a verdict this pass would only repeat).
        missing_fills: list[FillReport] = []

        for fill in venue_fills:
            if fill.trade_id in cached_fill_trade_ids:
                continue

            if fill.trade_id in self._recent_fills_cache:
                continue

            if self._fill_is_already_accounted(fill):
                self._log.debug(
                    f"Skipping fill {fill.trade_id} for {instrument_id}: "
                    "already accounted for on its order, which rejected it as an overfill",
                )
                continue

            missing_fills.append(fill)

        return missing_fills, had_fill_query_errors

    def _fill_is_already_accounted(self, report: FillReport) -> bool:
        # An overfill rejection is terminal for that fill on that order: its economic
        # effect is already carried by the fills the order holds, so re-judging it every
        # pass only repeats the same rejection.
        client_order_id = self._cache.client_order_id(report.venue_order_id)

        if client_order_id is None:
            client_order_id = report.client_order_id

        if client_order_id is None:
            return False

        return report.trade_id in self._overfill_rejected_trade_ids.get(client_order_id, {})

    async def _reconcile_missing_fills(
        self,
        missing_fills: list[FillReport],
        instrument_id: InstrumentId,
    ) -> int:
        if not missing_fills:
            return 0

        self._log.info(
            f"Found {len(missing_fills)} missing fill(s) for {instrument_id}",
            LogColor.BLUE,
        )

        applied_trade_ids: set[TradeId] = set()
        deferred = 0
        skipped = 0
        errors = 0

        for fill_report in missing_fills:
            if self._fill_is_already_accounted(fill_report):
                # A duplicate of a fill an earlier entry of this same response judged
                skipped += 1
                self._log.debug(
                    f"Skipping fill {fill_report.trade_id} for {instrument_id}: "
                    "already accounted for on its order, which rejected it as an overfill",
                )
                continue

            try:
                result = self._reconcile_fill_report_single(fill_report)

                # A skip (filtered instrument, mismatched owner) also reports success, so
                # application is confirmed by the trade reaching the cache: counting a skip
                # as applied re-queries the same fill every pass and the scope, held on
                # "re-querying next pass", never reaches its repair.
                if fill_report.trade_id in self._recent_fills_cache:
                    # A trade applies once, however many copies of it this response
                    # carried, and the copies still judge against the applied fill.
                    if fill_report.trade_id in applied_trade_ids:
                        skipped += 1
                    else:
                        applied_trade_ids.add(fill_report.trade_id)
                elif result or self._fill_is_already_accounted(fill_report):
                    # A fill this pass judged terminal is accounted for on its order and
                    # is never retried, so it carries no promise of one.
                    skipped += 1
                    self._log.debug(
                        f"Skipped fill {fill_report.trade_id} for {instrument_id} was not "
                        f"applied to the cache, continuing on the venue position snapshot",
                    )
                else:
                    deferred += 1
                    self._log.debug(
                        f"Failed to reconcile fill {fill_report.trade_id} for {instrument_id}: "
                        f"order not yet cached or other prerequisite missing. "
                        f"Fill will be retried in next position check cycle.",
                    )
            except Exception as e:
                errors += 1
                self._log.error(
                    f"Exception reconciling missing fill {fill_report.trade_id} for {instrument_id}: {e}",
                )

        applied = len(applied_trade_ids)

        # One line per pass rather than one per fill: a deferred fill is re-judged on
        # every pass, and healing one is routine rather than an action-required state.
        self._log.info(
            f"Reconciled {len(missing_fills)} missing fill(s) for {instrument_id}: "
            f"applied={applied}, deferred={deferred}, skipped={skipped}, errors={errors}",
            LogColor.BLUE,
        )

        return applied

    def _prune_overfill_rejected_fills(self) -> None:
        # A rejection is only worth remembering while the venue's fill query can still
        # return that fill, which is the position check's own lookback window.
        ts_now = self._clock.timestamp_ns()
        ttl_ns = secs_to_nanos(self.position_check_lookback_mins * 60)

        for client_order_id in list(self._overfill_rejected_trade_ids):
            trade_ids = self._overfill_rejected_trade_ids[client_order_id]
            expired = [
                trade_id
                for trade_id, ts_rejected in trade_ids.items()
                if ts_now - ts_rejected > ttl_ns
            ]

            for trade_id in expired:
                trade_ids.pop(trade_id, None)

            if not trade_ids:
                self._overfill_rejected_trade_ids.pop(client_order_id, None)

    def _prune_recent_fills_cache(self, ttl_secs: float = 60.0) -> None:
        # Remove expired fills from cache (default TTL: 60 seconds)
        ts_now = self._clock.timestamp_ns()
        ttl_ns = secs_to_nanos(ttl_secs)
        expired_trade_ids = [
            trade_id
            for trade_id, ts_cached in self._recent_fills_cache.items()
            if ts_now - ts_cached > ttl_ns
        ]

        for trade_id in expired_trade_ids:
            self._recent_fills_cache.pop(trade_id, None)

    async def _check_orders_consistency(self) -> None:
        try:
            if self._is_shutting_down:
                self._log.debug("Skipping order consistency check due to stop signal")
                return

            self._log.debug("Checking order consistency between cached-state and venues")

            open_order_ids: set[ClientOrderId] = self._cache.client_order_ids_open()
            inflight_order_ids: set[ClientOrderId] = self._cache.client_order_ids_inflight()

            if self.reconciliation_instrument_ids:
                open_orders: list[Order] = self._cache.orders_open()
                open_orders = [
                    o for o in open_orders if o.instrument_id in self.reconciliation_instrument_ids
                ]
                open_order_ids = {o.client_order_id for o in open_orders}
                inflight_orders: list[Order] = self._cache.orders_inflight()
                inflight_orders = [
                    o
                    for o in inflight_orders
                    if o.instrument_id in self.reconciliation_instrument_ids
                ]
                inflight_order_ids = {o.client_order_id for o in inflight_orders}

            all_order_ids = open_order_ids | inflight_order_ids
            open_len = len(all_order_ids)
            self._log.debug(f"Found {open_len} order{'' if open_len == 1 else 's'} open in cache")

            if not self._clients:
                self._log.debug("No execution clients to check orders consistency, early return")
                return

            (
                all_order_reports,
                venue_reported_ids,
                failed_clients,
            ) = await self._query_order_status_reports()

            self._reconcile_order_reports(all_order_reports, open_order_ids)

            await self._handle_missing_orders_at_venue(
                all_order_ids,
                venue_reported_ids,
                failed_clients,
            )

            if not self.open_check_open_only:
                # Cache-wide fill audit is not run for open-only responses
                self._validate_open_orders_consistency()
        except Exception as e:
            self._log.exception("Error in check_order_consistency", e)

    def _validate_open_orders_consistency(self) -> None:
        for order in self._cache.orders_open():
            computed_filled = sum(e.last_qty for e in order.events if isinstance(e, OrderFilled))
            if computed_filled != order.filled_qty:
                self._log.error(
                    f"INCONSISTENCY: {order.client_order_id} "
                    f"computed={computed_filled} vs cached={order.filled_qty}",
                )

    async def _handle_missing_orders_at_venue(
        self,
        open_order_ids: set[ClientOrderId],
        venue_reported_ids: set[ClientOrderId],
        failed_order_report_clients: set[ClientId] | None = None,
    ) -> None:
        missing_at_venue: set[ClientOrderId] = open_order_ids - venue_reported_ids
        ts_now = self._clock.timestamp_ns()

        # A FIFO queue rotates the per-cycle cap: an attempted order is removed before
        # its query and re-appended at the tail while it stays open, so neither a
        # persistently failing order nor a conclusively open one can starve the rest
        self._missing_order_query_queue = deque(
            cid for cid in self._missing_order_query_queue if cid in missing_at_venue
        )
        queued_order_ids = set(self._missing_order_query_queue)
        self._missing_order_query_queue.extend(
            sorted(missing_at_venue - queued_order_ids, key=lambda cid: cid.value)
        )
        query_order: list[ClientOrderId] = list(self._missing_order_query_queue)

        targeted_queries_count = 0
        logged_limit_warning = False

        for client_order_id in query_order:
            order = self._cache.order(client_order_id)
            if order is None:
                self._log.error(f"{client_order_id!r} missing at venue and not found in cache")
                continue

            if self._client_for_order(order) is None:
                self._log.debug(
                    f"Skipping missing-order reconciliation for {client_order_id!r} - "
                    f"no registered execution client queries this order",
                )
                continue

            if self._did_order_status_query_fail(order, failed_order_report_clients):
                self._log.warning(
                    f"Skipping missing-order reconciliation for {client_order_id!r}: "
                    f"failed to query order status from its execution client",
                    LogColor.YELLOW,
                )
                continue

            # Check if order is too recent to reconcile (avoid race conditions)
            ts_last = order.ts_last
            if (ts_now - ts_last) < self._open_check_threshold_ns:
                self._log.debug(
                    f"Skipping reconciliation for {client_order_id!r} - order too recent "
                    f"(age={(ts_now - ts_last) / 1_000_000}ms < threshold={self.open_check_threshold_ms}ms)",
                )
                continue

            local_activity = self._order_local_activity_ns.get(client_order_id)
            if local_activity and (ts_now - local_activity) < self._open_check_threshold_ns:
                self._log.debug(
                    f"Skipping reconciliation for {client_order_id!r}; "
                    f"pending local activity ({(ts_now - local_activity) / 1_000_000}ms < threshold={self.open_check_threshold_ms}ms)",
                )
                continue

            retries = self._recon_check_retries.get(client_order_id, 0)
            if retries >= self.open_check_missing_retries:
                if targeted_queries_count >= self.max_single_order_queries_per_cycle:
                    self._recon_check_retries[client_order_id] = retries + 1

                    if not logged_limit_warning:
                        # Count how many orders at threshold are being deferred
                        orders_at_threshold_remaining = (
                            sum(
                                1
                                for cid in missing_at_venue
                                if self._recon_check_retries.get(cid, 0)
                                >= self.open_check_missing_retries
                            )
                            - targeted_queries_count
                        )
                        self._log.warning(
                            f"Reached max single-order queries ({self.max_single_order_queries_per_cycle}) "
                            f"this cycle, deferring {orders_at_threshold_remaining} order(s) at threshold to next cycle",
                            LogColor.YELLOW,
                        )
                        logged_limit_warning = True

                    continue  # Skip query but continue processing other orders

                self._log.warning(
                    f"Order {client_order_id!r} not found at venue after {retries} retries, performing single-order query",
                    LogColor.YELLOW,
                )
                self._clear_recon_tracking(client_order_id, drop_last_query=False)
                conclusive = await self._resolve_order_not_found_at_venue(order)
                targeted_queries_count += 1

                if not conclusive:
                    # Hold at the threshold so the next cycle queries again, rather than
                    # spending another `open_check_missing_retries` cycles on an order
                    # the venue has already been absent for.
                    self._recon_check_retries[client_order_id] = retries

                if order.is_open:
                    self._missing_order_query_queue.append(client_order_id)

                # Add delay between single-order queries (skip after final query)
                if (
                    targeted_queries_count < self.max_single_order_queries_per_cycle
                    and self.single_order_query_delay_ms > 0
                ):
                    await asyncio.sleep(self.single_order_query_delay_ms / 1000.0)
            else:
                self._recon_check_retries[client_order_id] = retries + 1
                self._log.debug(
                    f"Order {client_order_id!r} not found at venue, retry {retries + 1}/{self.open_check_missing_retries}",
                )

    def _client_for_order(self, order: Order) -> ExecutionClient | None:
        # Resolve the execution client which received the given order, mirroring the
        # routing priority of `_find_client_for_command`: explicit client ID, account ID
        # issuer, instrument venue, then the default client. An external client is never
        # queried by this engine, so such an order has no resolvable owner and must not
        # be attributed to another client.
        client_id = self._cache.client_id(order.client_order_id)

        if client_id is not None:
            client = self._clients.get(client_id)

            if client is not None:
                return client

            if client_id in self._external_clients:
                return None

        if order.account_id is not None:
            issuer = order.account_id.get_issuer()

            client = self._clients.get(ClientId(issuer))
            if client is not None:
                return client

            client = self._routing_map.get(Venue(issuer))
            if client is not None:
                return client

        return self._routing_map.get(order.instrument_id.venue, self._default_client)

    def _did_order_status_query_fail(
        self,
        order: Order,
        failed_order_report_clients: set[ClientId] | None,
    ) -> bool:
        if not failed_order_report_clients:
            return False

        client = self._client_for_order(order)

        return client is not None and client.id in failed_order_report_clients

    async def _resolve_order_not_found_at_venue(self, order: Order) -> bool:
        # Returns whether the venue answered for this order, so the caller can hold the
        # order at its retry threshold when nothing could be concluded.
        ts_now = self._clock.timestamp_ns()

        self._log.debug(
            f"Performing single-order query for {order.client_order_id!r} before resolving",
            LogColor.BLUE,
        )

        client = self._client_for_order(order)
        if client is None:
            self._log.warning(
                f"No execution client for {order.client_order_id!r}, "
                f"cannot resolve order missing at venue",
            )
            return False  # Cannot conclude, the order keeps its cached state

        try:
            query_ts = self._clock.timestamp_ns()
            command = GenerateOrderStatusReport(
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=order.venue_order_id,
                command_id=UUID4(),
                ts_init=query_ts,
            )

            self._ts_last_query[order.client_order_id] = query_ts
            report = await client.generate_order_status_report(command)
        except Exception as e:
            self._log.warning(
                f"Targeted query for {order.client_order_id!r} failed ({e}), "
                f"cannot resolve order missing at venue",
            )
            return False  # Cannot conclude, the order keeps its cached state

        if report is not None:
            self._log.info(
                f"Found {order.client_order_id!r} via targeted query: {report.order_status}",
                LogColor.BLUE,
            )
            self._reconcile_order_report(report, trades=[])
            return True  # The venue's individual answer is the authority

        # The venue answered and does not know the order: resolve from cached state
        if not order.is_open:
            self._log.debug(
                f"Skipping reconciliation for {order.client_order_id!r} - already {order.status_string()}",
            )
            self._clear_recon_tracking(order.client_order_id)
            self._order_local_activity_ns.pop(order.client_order_id, None)
            return True

        if order.status == OrderStatus.ACCEPTED:
            self._log.warning(
                f"Reconciling {order.client_order_id!r}: ACCEPTED order not found at venue, marking as REJECTED",
                LogColor.YELLOW,
            )
            rejected = create_order_rejected_event(
                order=order,
                ts_now=ts_now,
                reason="ORDER_NOT_FOUND_AT_VENUE",
            )
            self._handle_event_with_tracking(rejected)
            self._clear_recon_tracking(order.client_order_id)
            self._order_local_activity_ns.pop(order.client_order_id, None)
            return True

        if order.status == OrderStatus.PARTIALLY_FILLED:
            self._log.warning(
                f"Reconciling {order.client_order_id!r}: PARTIALLY_FILLED "
                f"order not found at venue, marking as CANCELED (preserving {order.filled_qty} filled quantity)",
                LogColor.YELLOW,
            )
            canceled = create_order_canceled_event(
                order=order,
                ts_now=ts_now,
            )
            self._handle_event_with_tracking(canceled)
            self._clear_recon_tracking(order.client_order_id)
            self._order_local_activity_ns.pop(order.client_order_id, None)
            return True

        if order.status == OrderStatus.SUBMITTED:
            self._log.warning(
                f"Reconciling {order.client_order_id!r}: SUBMITTED order not found at venue, marking as REJECTED",
                LogColor.YELLOW,
            )
            rejected = create_order_rejected_event(
                order=order,
                ts_now=ts_now,
                reason="ORDER_NOT_FOUND_AT_VENUE",
            )
            self._handle_event_with_tracking(rejected)
            self._clear_recon_tracking(order.client_order_id)
            self._order_local_activity_ns.pop(order.client_order_id, None)
            return True

        if order.is_inflight:
            self._log.debug(
                f"Deferring resolution for {order.client_order_id!r} - still inflight state {order.status_string()}",
            )
            self._clear_recon_tracking(order.client_order_id, drop_last_query=False)
            self._ts_last_query[order.client_order_id] = ts_now
            return True

        if order.is_closed:
            if order.status == OrderStatus.FILLED:
                self._log.debug(
                    f"{order.client_order_id!r} is FILLED and not found at venue (expected behavior)",
                )
            else:
                self._log.warning(
                    f"Order {order.client_order_id!r} is already closed as {order.status_string()}, "
                    "skipping missing-order resolution",
                )
            self._clear_recon_tracking(order.client_order_id)
            self._order_local_activity_ns.pop(order.client_order_id, None)
            return True

        self._log.warning(
            f"Unexpected order status {order.status_string()} "
            f"for order not found at venue: {order.client_order_id!r}",
        )
        self._clear_recon_tracking(order.client_order_id)
        self._order_local_activity_ns.pop(order.client_order_id, None)

        return True

    async def _query_order_status_reports(
        self,
    ) -> tuple[list[OrderStatusReport], set[ClientOrderId], set[ClientId]]:
        order_status_start = self._clock.utc_now() - pd.Timedelta(
            minutes=self.open_check_lookback_mins,
        )

        clients = list(self._clients.values())

        tasks = [
            c.generate_order_status_reports(
                GenerateOrderStatusReports(
                    instrument_id=None,
                    start=order_status_start,
                    end=None,
                    open_only=self.open_check_open_only,
                    command_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                    log_receipt_level=LogLevel.DEBUG,
                ),
            )
            for c in clients
        ]

        order_reports_all = await asyncio.gather(*tasks, return_exceptions=True)
        all_order_reports: list[OrderStatusReport] = []
        failed_clients: set[ClientId] = set()

        for client, reports_or_exception in zip(clients, order_reports_all, strict=True):
            if isinstance(reports_or_exception, BaseException):
                failed_clients.add(client.id)
                message = (
                    f"Failed to generate order status reports for client {client.id}: "
                    f"{reports_or_exception}"
                )

                # A client which is no longer running is stopping deliberately, so its
                # query failing is its own lifecycle rather than a venue we cannot reach.
                if client.is_running:
                    self._log.error(message)
                else:
                    self._log.debug(message)

                continue

            reports = cast(list[OrderStatusReport], reports_or_exception)
            all_order_reports.extend(reports)

        venue_reported_ids: set[ClientOrderId] = set()

        for report in all_order_reports:
            client_order_id = report.client_order_id

            if client_order_id is None and report.venue_order_id is not None:
                # A venue-only report still identifies a cached order, which is not absent
                client_order_id = self._cache.client_order_id(report.venue_order_id)

            if client_order_id is not None:
                venue_reported_ids.add(client_order_id)

        return all_order_reports, venue_reported_ids, failed_clients

    def _reconcile_order_reports(
        self,
        all_order_reports: list[OrderStatusReport],
        open_order_ids: set[ClientOrderId],
    ) -> None:
        ts_now = self._clock.timestamp_ns()

        for report in all_order_reports:
            is_in_open_ids = report.client_order_id in open_order_ids

            # Clear any retry counts for successfully queried orders
            if report.client_order_id:
                self._clear_recon_tracking(report.client_order_id)
            elif report.venue_order_id:
                # Try to map venue-only ID to client order ID and clear that retry counter
                mapped_client_id = self._cache.client_order_id(report.venue_order_id)
                if mapped_client_id:
                    self._clear_recon_tracking(mapped_client_id)

            # Check if we should reconcile this order
            should_reconcile = False
            reconcile_reason = ""

            if report.is_open != is_in_open_ids:
                should_reconcile = True
                reconcile_reason = f"venue_open={report.is_open}, cache_open={is_in_open_ids}"
            elif report.client_order_id:
                order = self._cache.order(report.client_order_id)
                if order:
                    # Check filled_qty mismatch, treating None as zero
                    report_filled = (
                        report.filled_qty
                        if report.filled_qty is not None
                        else Quantity.zero(order.quantity.precision)
                    )

                    if order.filled_qty != report_filled:
                        should_reconcile = True
                        reconcile_reason = (
                            f"filled_qty mismatch: venue={report_filled}, cache={order.filled_qty}"
                        )

            if should_reconcile:
                # Apply include filter before reconciling
                if not self._consider_for_reconciliation(report.instrument_id):
                    self._log.debug(
                        f"Skipping reconciliation for {report.client_order_id!r}: "
                        f"instrument {report.instrument_id} not in include list",
                    )
                    continue

                # Check for recent local activity to avoid race conditions with in-flight fills
                local_activity = self._order_local_activity_ns.get(report.client_order_id)
                if local_activity and (ts_now - local_activity) < self._open_check_threshold_ns:
                    self._log.debug(
                        f"Deferring reconciliation for {report.client_order_id!r}: "
                        f"recent local activity ({(ts_now - local_activity) / 1_000_000:.0f}ms < "
                        f"threshold={self.open_check_threshold_ms}ms), "
                        f"reason was: {reconcile_reason}",
                    )
                    continue

                self._log.debug(
                    f"Reconciling {report.client_order_id!r}: {reconcile_reason}",
                    LogColor.BLUE,
                )
                self._reconcile_order_report(report, trades=[])

    # -- REQUEST HANDLERS --------------------------------------------------------------------------

    def generate_execution_mass_status(self, command: GenerateExecutionMassStatus) -> None:
        """
        Handle request to generate execution mass status, triggering startup
        reconciliation.
        """
        self._log.info(f"Received {command!r}", LogColor.BLUE)
        self._loop.create_task(self.reconcile_execution_state())

    async def reconcile_execution_state(
        self,
        timeout_secs: float = 10.0,
    ) -> bool:
        """
        Reconcile execution state as main entry point for startup reconciliation,
        coordinating reconciliation across all execution clients.

        Parameters
        ----------
        timeout_secs : float, default 10.0
            The timeout (seconds) for reconciliation to complete.

        Returns
        -------
        bool
            True if execution state reconciled within the timeout, else False.

        Raises
        ------
        ValueError
            If `timeout_secs` is not positive (> 0).

        """
        PyCondition.positive(timeout_secs, "timeout_secs")

        try:
            return await asyncio.wait_for(
                self._reconcile_execution_state(),
                timeout=timeout_secs,
            )
        except TimeoutError:
            self._log.error(f"Timed out ({timeout_secs}s) reconciling execution state")
            return False

    async def _reconcile_execution_state(self) -> bool:
        try:
            for client_id in self._external_clients:
                command = GenerateExecutionMassStatus(
                    trader_id=self.trader_id,
                    client_id=client_id,
                    command_id=UUID4(),
                    venue=None,
                    ts_init=self._clock.timestamp_ns(),
                )
                self._log.info(
                    f"Requesting execution mass status from {client_id}",
                    LogColor.BLUE,
                )
                self._msgbus.publish(
                    topic=f"commands.trading.{client_id}",
                    msg=command,
                )

            if not self._clients:
                self._log.debug("No execution clients for reconciliation")
                # Signal completion even with no clients
                return True

            results: list[bool] = []

            # Request execution mass status report from clients
            reconciliation_lookback_mins: int | None = (
                self.reconciliation_lookback_mins if self.reconciliation_lookback_mins > 0 else None
            )
            mass_status_coros = [
                c.generate_mass_status(reconciliation_lookback_mins) for c in self._clients.values()
            ]
            mass_status_all = await asyncio.gather(*mass_status_coros, return_exceptions=True)

            # Reconcile each mass status with the execution engine
            for mass_status_or_exception in mass_status_all:
                if isinstance(mass_status_or_exception, BaseException):
                    self._log.error(f"Failed to generate mass status: {mass_status_or_exception}")
                    results.append(False)
                    continue

                if mass_status_or_exception is None:
                    self._log.warning(
                        "No execution mass status available for reconciliation "
                        "(likely due to an adapter client error when generating reports)",
                    )
                    results.append(False)
                    continue

                mass_status = cast("ExecutionMassStatus", mass_status_or_exception)
                client_id = mass_status.client_id
                # venue = mass_status.venue
                result = self._reconcile_execution_mass_status(mass_status)

                if not result and self.filter_position_reports:
                    self._log_reconciliation_result(client_id, result)
                    results.append(result)
                    self._log.warning(
                        "`filter_position_reports` enabled, skipping further reconciliation",
                    )
                    continue

                self._log_reconciliation_result(client_id, result)
                results.append(result)

                self._msgbus.publish(
                    topic=f"reports.execution.{mass_status.venue}",
                    msg=mass_status,
                )

            # Converge cached positions onto the venue snapshot as the final startup step,
            # where non-convergence publishes residuals and is deliberately not fatal.
            try:
                await self._run_position_convergence_pass("startup")
            except Exception as e:
                self._log.exception("Failed running startup position convergence pass", e)

            return all(results)
        finally:
            # Always signal completion to prevent continuous loop signal await hang
            self._startup_reconciliation_event.set()

    def _log_reconciliation_result(self, value: ClientId | InstrumentId, result: bool) -> None:
        if result:
            self._log.info(f"Reconciliation for {value} succeeded", LogColor.GREEN)
        else:
            self._log.warning(f"Reconciliation for {value} failed")

    def reconcile_execution_report(self, report: ExecutionReport) -> bool:
        """
        Reconcile a single execution report received at runtime, routing to appropriate
        reconciliation method based on report type.
        """
        self._log.debug(f"<--[RPT] {report}")
        self.report_count += 1

        if not self._consider_for_reconciliation(report.instrument_id):
            self._log_skipping_reconciliation_on_instrument_id(report)
            return True  # Filtered

        self._log.debug(f"Reconciling {report}", color=LogColor.BLUE)

        if isinstance(report, OrderStatusReport):
            result = self._reconcile_order_report(report, [])  # No trades to reconcile
        elif isinstance(report, FillReport):
            result = self._reconcile_fill_report_single(report)
        elif isinstance(report, PositionStatusReport):
            result = self._reconcile_position_report(report)
        else:
            self._log.error(  # pragma: no cover (design-time error)
                f"Cannot handle unrecognized report: {report}",  # pragma: no cover (design-time error)
            )
            return False

        self._msgbus.publish(
            topic=f"reports.execution.{report.instrument_id.venue}.{report.instrument_id.symbol}",
            msg=report,
        )

        return result

    # -- RECONCILIATION ----------------------------------------------------------------------------

    def reconcile_execution_mass_status(self, report: ExecutionMassStatus) -> None:
        """
        Entry point for mass status reconciliation.
        """
        self._reconcile_execution_mass_status(report)

    def _reconcile_execution_mass_status(
        self,
        mass_status: ExecutionMassStatus,
    ) -> bool:
        self._log.debug(f"<--[RPT] {mass_status}")
        self.report_count += 1

        self._log.info(
            f"Reconciling ExecutionMassStatus for {mass_status.venue}",
            color=LogColor.BLUE,
        )

        # Adjust fills for instruments with incomplete first lifecycles
        self._adjust_mass_status_fills(mass_status)

        # Deduplicate orders in mass status
        self._deduplicate_mass_status_orders(mass_status)

        results: list[bool] = []
        reconciled_orders: set[ClientOrderId] = set()
        reconciled_trades: set[TradeId] = set()

        # Reconcile all reported orders
        for venue_order_id, order_report in mass_status.order_reports.items():
            trades = mass_status.fill_reports.get(venue_order_id, [])

            if not self._consider_for_reconciliation(order_report.instrument_id):
                self._log_skipping_reconciliation_on_instrument_id(order_report)
                continue

            client_order_id = order_report.client_order_id

            if client_order_id is not None and client_order_id in self.filtered_client_order_ids:
                self._log.debug(
                    f"Skipping {type(order_report).__name__} reconciliation for {order_report.client_order_id!r}: "
                    f"in `filtered_client_order_ids` list",
                    LogColor.MAGENTA,
                )
                continue

            # Check for duplicate trade IDs
            for fill_report in trades:
                if fill_report.trade_id in reconciled_trades:
                    self._log.warning(
                        f"Duplicate {fill_report.trade_id!r} detected: {fill_report}",
                    )

                reconciled_trades.add(fill_report.trade_id)

            try:
                # Apply all fills - let position cycle naturally through all lifecycles
                result = self._reconcile_order_report(order_report, trades)
            except InvalidStateTrigger as e:
                self._log.error(str(e))
                result = False

            results.append(result)

            if order_report.client_order_id is not None:
                # Only track orders where instrument was loaded (others are filtered)
                instrument = self._cache.instrument(order_report.instrument_id)
                if instrument is not None:
                    reconciled_orders.add(order_report.client_order_id)

                    if result and order_report.venue_order_id is not None:
                        self._ensure_venue_order_id_indexed(
                            client_order_id=order_report.client_order_id,
                            venue_order_id=order_report.venue_order_id,
                        )

        # Position reports carried by a mass status are deliberately not reconciled here:
        # the convergence pass is the sole position mutator, and it diffs against its own
        # freshly queried snapshot rather than one already mutated from underneath it.

        # Publish mass status
        self._msgbus.publish(
            topic=f"reports.execution.{mass_status.venue}",
            msg=mass_status,
        )

        # Validate reconciliation state for consistency
        self._validate_reconciliation_state(mass_status, reconciled_orders)

        return all(results)

    def _adjust_mass_status_fills(self, mass_status: ExecutionMassStatus) -> None:
        # Adjust fills for instruments with incomplete first lifecycles
        # Start with original orders and fills
        final_orders = dict(mass_status._order_reports)
        final_fills = dict(mass_status._fill_reports)

        reconciliation_instruments: list[Instrument] = []

        for instrument_id, position_reports in mass_status.position_reports.items():
            # Skip hedge mode instruments (have venue_position_id) as partial-window
            # adjustment assumes a single net position per instrument
            is_hedge_mode = any(r.venue_position_id is not None for r in position_reports)
            if is_hedge_mode:
                self._log.debug(
                    f"Skipping fill adjustment for {instrument_id}: "
                    f"hedge mode (has venue_position_id)",
                )
                continue

            # Respect reconciliation_instrument_ids filter
            if not self._consider_for_reconciliation(instrument_id):
                self._log.debug(
                    f"Skipping fill adjustment for {instrument_id}: "
                    f"not in `reconciliation_instrument_ids` include list",
                )
                continue

            instrument = self._cache.instrument(instrument_id)
            if not instrument:
                self._log.debug(
                    f"Skipping fill adjustment for {instrument_id}: instrument not found in cache",
                )
                continue

            reconciliation_instruments.append(instrument)

        self._log.info(
            f"Attempting to adjust fills for {len(reconciliation_instruments)} instruments",
            LogColor.BLUE,
        )
        adjusted_results = adjust_fills_for_partial_window(
            mass_status,
            reconciliation_instruments,
            self._log,
        )
        self._log.info(
            f"Updating adjusted fills for {len(reconciliation_instruments)} instruments",
            LogColor.BLUE,
        )

        for instrument_id, (
            adjusted_orders_for_instrument,
            adjusted_fills_for_instrument,
        ) in adjusted_results.items():
            # Remove old orders and fills for this instrument
            for venue_order_id in list(final_orders.keys()):
                order = final_orders[venue_order_id]
                if order.instrument_id == instrument_id:
                    del final_orders[venue_order_id]

            for venue_order_id in list(final_fills.keys()):
                fills = final_fills[venue_order_id]
                if fills and fills[0].instrument_id == instrument_id:
                    del final_fills[venue_order_id]

            # Add adjusted orders and fills for this instrument
            final_orders.update(adjusted_orders_for_instrument)
            final_fills.update(adjusted_fills_for_instrument)

        # Apply all adjustments at once
        mass_status._order_reports = final_orders
        mass_status._fill_reports = final_fills
        self._log.info(
            f"Final order_reports contains {len(final_orders)} orders, fill_reports contains {len(final_fills)} fills across all instruments",
            LogColor.BLUE,
        )

    def _deduplicate_mass_status_orders(self, mass_status: ExecutionMassStatus) -> None:
        # Remove duplicate orders within mass status report
        seen_client_order_ids: dict[ClientOrderId, VenueOrderId] = {}
        duplicate_venue_order_ids: list[VenueOrderId] = []
        orders_to_skip: list[VenueOrderId] = []

        # First pass: deduplicate within the current report
        for venue_order_id, order_report in mass_status._order_reports.items():
            if order_report.client_order_id is not None:
                if order_report.client_order_id in seen_client_order_ids:
                    # Duplicate found in current report - mark for removal
                    duplicate_venue_order_ids.append(venue_order_id)
                    self._log.warning(
                        f"Deduplicating order: {order_report.client_order_id} "
                        f"(venue_order_id={venue_order_id}, "
                        f"keeping first occurrence {seen_client_order_ids[order_report.client_order_id]})",
                    )
                else:
                    # First occurrence - track it
                    seen_client_order_ids[order_report.client_order_id] = venue_order_id

        # Second pass: check against cached orders to prevent duplicates
        # Only skip if order is an exact match (same status, filled_qty, etc.)
        # This prevents duplicate creation while still allowing reconciliation of mismatches
        for venue_order_id, order_report in mass_status._order_reports.items():
            if venue_order_id in duplicate_venue_order_ids:
                continue  # Already marked as duplicate

            # Check if this order already exists in cache by client_order_id
            if order_report.client_order_id is not None:
                cached_order = self._cache.order(order_report.client_order_id)
                if cached_order is not None:
                    # Skip closed reconciliation orders to prevent duplicate inferred fills on restart
                    if (
                        cached_order.is_closed
                        and cached_order.tags is not None
                        and "RECONCILIATION" in cached_order.tags
                    ):
                        orders_to_skip.append(venue_order_id)
                        self._log.debug(
                            f"Skipping closed reconciliation order {order_report.client_order_id}: "
                            f"synthetic position adjustment from previous session",
                        )
                        continue

                    # Order exists in cache - check if it's an exact duplicate
                    # Only skip if it's an exact match (prevents duplicate creation)
                    # But still reconcile if there are any discrepancies
                    report_filled = (
                        order_report.filled_qty
                        if order_report.filled_qty is not None
                        else Quantity.zero(cached_order.quantity.precision)
                    )

                    # Check for exact match - same status, filled_qty, and instrument
                    is_exact_match = (
                        cached_order.status == order_report.order_status
                        and cached_order.filled_qty == report_filled
                        and cached_order.instrument_id == order_report.instrument_id
                        and cached_order.side == order_report.order_side
                    )

                    if is_exact_match:
                        # Exact duplicate - skip to prevent duplicate creation
                        orders_to_skip.append(venue_order_id)
                        self._log.debug(
                            f"Skipping exact duplicate order {order_report.client_order_id}: "
                            f"order already exists in cache with identical state",
                        )
                        continue
                    # If not exact match, continue with reconciliation to fix discrepancies

            # Also check by venue_order_id if client_order_id lookup failed or wasn't provided
            if order_report.venue_order_id is not None and order_report.client_order_id is None:
                cached_client_id = self._cache.client_order_id(order_report.venue_order_id)
                if cached_client_id is not None:
                    cached_order = self._cache.order(cached_client_id)
                    if cached_order is not None:
                        # Update the report to use the cached client_order_id for consistency
                        order_report.client_order_id = cached_client_id
                        self._log.debug(
                            f"Found cached order {cached_client_id} by venue_order_id {order_report.venue_order_id}, "
                            f"updating report to use cached client_order_id",
                        )
                        # Don't skip - still need to reconcile in case there are discrepancies

        # Remove duplicates and orders to skip
        orders_to_remove = set(duplicate_venue_order_ids) | set(orders_to_skip)
        for venue_order_id in orders_to_remove:
            del mass_status._order_reports[venue_order_id]

            # Also remove associated fills
            if venue_order_id in mass_status._fill_reports:
                del mass_status._fill_reports[venue_order_id]

        if orders_to_remove:
            self._log.debug(
                f"Removed {len(orders_to_remove)} duplicate/skipped order(s) from reconciliation "
                f"({len(duplicate_venue_order_ids)} duplicates, {len(orders_to_skip)} already in cache)",
                LogColor.YELLOW,
            )

    def _validate_reconciliation_state(
        self,
        mass_status: ExecutionMassStatus,
        reconciled_orders: set[ClientOrderId],
    ) -> None:
        venue_order_ids_seen: set[VenueOrderId] = set()
        issues: list[str] = []

        for order_report in mass_status._order_reports.values():
            if order_report.venue_order_id is None:
                continue

            # Skip orders that were filtered (e.g., instrument not loaded)
            if order_report.client_order_id not in reconciled_orders:
                self._log.debug(
                    f"Skipping validation for {order_report.client_order_id} "
                    f"(venue_order_id={order_report.venue_order_id}) - not in reconciled_orders",
                )
                continue

            if order_report.venue_order_id in venue_order_ids_seen:
                issues.append(
                    f"Duplicate venue_order_id {order_report.venue_order_id} in mass status",
                )

            venue_order_ids_seen.add(order_report.venue_order_id)

            # Check if venue_order_id is properly indexed
            if order_report.client_order_id:
                cached_client_id = self._cache.client_order_id(order_report.venue_order_id)
                if cached_client_id is None:
                    issues.append(
                        f"Venue order ID {order_report.venue_order_id} not indexed in cache "
                        f"for client_order_id {order_report.client_order_id}",
                    )
                elif cached_client_id != order_report.client_order_id:
                    issues.append(
                        f"Venue order ID {order_report.venue_order_id} indexing mismatch: "
                        f"expected {order_report.client_order_id}, found {cached_client_id}",
                    )

        if issues:
            self._log.warning(
                f"Reconciliation state validation found {len(issues)} issue(s):\n"
                + "\n".join(f"  - {issue}" for issue in issues),
            )
        else:
            self._log.debug(
                f"Reconciliation state validation passed for {len(mass_status._order_reports)} order(s)",
            )

    # -- FILL RECONCILIATION -----------------------------------------------------------------------

    def _reconcile_fill_report_single(self, report: FillReport) -> bool:
        if self._is_shutting_down:
            return True  # Skip reconciliation during shutdown

        if not self._consider_for_reconciliation(report.instrument_id):
            self._log_skipping_reconciliation_on_instrument_id(report)
            return True  # Filtered

        client_order_id: ClientOrderId | None = self._cache.client_order_id(
            report.venue_order_id,
        )

        if client_order_id is None:
            # Expected operation: the venue's fill history heals the delivery gap, and
            # what stays unhealed is reported by the convergence pass rather than here.
            self._log.debug(
                f"FillReport received before OrderStatusReport for {report.venue_order_id!r}, "
                "deferring reconciliation - this may require a synthetic order",
            )
            return False  # Failed

        order: Order | None = self._cache.order(client_order_id)

        if order is None:
            # Try to find order by venue_order_id if client_order_id lookup failed
            # This handles cases where external orders might not be fully indexed yet
            if report.venue_order_id is not None:
                order = self._find_order_by_venue_order_id(
                    venue_order_id=report.venue_order_id,
                    instrument_id=report.instrument_id,
                    order_side=None,  # Don't filter by side to find any matching order
                )

                if order is not None:
                    self._log.debug(
                        f"Found order {order.client_order_id} by venue_order_id "
                        f"{report.venue_order_id} for fill report",
                    )
                    # Ensure mapping is indexed
                    self._ensure_venue_order_id_indexed(
                        client_order_id=order.client_order_id,
                        venue_order_id=report.venue_order_id,
                        log_context="for fill report",
                    )

            if order is None:
                self._log.debug(
                    f"FillReport received before order cached for {client_order_id!r} "
                    f"(venue_order_id={report.venue_order_id!r}), deferring reconciliation",
                )
                return False  # Failed

        if report.client_order_id is not None and report.client_order_id != order.client_order_id:
            self._log.warning(
                f"Skipping fill reconciliation for {report.trade_id!r}: "
                f"report.client_order_id={report.client_order_id!r} does not match "
                f"order.client_order_id={order.client_order_id!r} resolved from "
                f"venue_order_id={report.venue_order_id!r}",
            )
            return True  # Mismatched owner; skip without retry storm

        # Log external order processing for better visibility
        if order.strategy_id.value == "EXTERNAL":
            self._log.debug(
                f"Processing fill for external order {order.client_order_id} "
                f"(venue_order_id={order.venue_order_id})",
            )

        instrument: Instrument | None = self._cache.instrument(order.instrument_id)
        if instrument is None:
            self._log.debug(
                f"Cannot reconcile order for {order.client_order_id!r}: "
                f"instrument {order.instrument_id} not found",
            )
            return True  # Filtered instrument not loaded

        return self._reconcile_fill_report(order, report, instrument)

    def _fill_reports_equal(self, cached_fill: OrderFilled, report: FillReport) -> bool:
        # Commission can be missing on reports from some venues/paths; compare safely
        if cached_fill.commission is None and report.commission is None:
            commissions_equal = True
        elif cached_fill.commission is None or report.commission is None:
            commissions_equal = False
        else:
            commissions_equal = (
                cached_fill.commission.currency == report.commission.currency
                and cached_fill.commission == report.commission
            )

        return (
            cached_fill.last_qty == report.last_qty
            and cached_fill.last_px == report.last_px
            and commissions_equal
            and cached_fill.liquidity_side == report.liquidity_side
            and cached_fill.ts_event == report.ts_event
        )

    def _rollback_fill_audit_entry(
        self,
        client_order_id: ClientOrderId,
        audit_entry: tuple[TradeId, str, int],
    ) -> None:
        # Remove audit entry when fill application fails
        if audit_entry in self._fill_application_audit.get(client_order_id, []):
            self._fill_application_audit[client_order_id].remove(audit_entry)

    # -- POSITION RECONCILIATION -------------------------------------------------------------------

    def _reconcile_position_report(self, report: PositionStatusReport) -> bool:
        if self._is_shutting_down:
            return True  # Skip reconciliation during shutdown

        if not self._consider_for_reconciliation(report.instrument_id):
            self._log_skipping_reconciliation_on_instrument_id(report)
            return True  # Filtered

        if self._is_position_report_stale(report):
            return True  # Snapshot is stale on the cache's timestamp axis

        # Mirror the convergence pass's per-scope wrap at the inbound boundary, so a
        # repair raising mid-application leaves the scope a written residual instead of
        # escaping into the report handler with nothing recording the failure.
        try:
            if report.venue_position_id is not None:
                return self._reconcile_position_report_hedging(report)
            else:
                return self._reconcile_position_report_netting(report)
        except Exception as e:
            self._log.exception(f"Failed applying inbound position repair for {report}", e)
            self._write_position_scope_residual(
                instrument_id=report.instrument_id,
                account_id=report.account_id,
                reason=f"inbound repair raised {type(e).__name__}",
            )
            return False

    def _is_position_report_stale(self, report: PositionStatusReport) -> bool:
        # A report timestamp earlier than the cached position's last applied event is
        # stale on that timestamp axis, so the snapshot is discarded rather than
        # reconciled against newer cached state.
        ts_last_applied: int | None

        if report.venue_position_id is not None:
            position = self._cache.position(report.venue_position_id)
            ts_last_applied = position.ts_last if position is not None else None
        else:
            positions_open = self._cache.positions_open(
                venue=None,  # Faster query filtering
                instrument_id=report.instrument_id,
                account_id=report.account_id,
            )
            ts_last_applied = (
                max(position.ts_last for position in positions_open) if positions_open else None
            )

        if ts_last_applied is None or report.ts_last >= ts_last_applied:
            return False

        self._log.info(
            f"Discarding stale position status report for {report.instrument_id}: "
            f"report timestamp={report.ts_last} predates "
            f"cached position timestamp={ts_last_applied}",
            LogColor.BLUE,
        )

        return True

    def _consider_for_reconciliation(self, instrument_id: InstrumentId) -> bool:
        if self.reconciliation_instrument_ids:
            return instrument_id in self.reconciliation_instrument_ids

        return True

    def _log_skipping_reconciliation_on_instrument_id(self, report: ExecutionReport) -> None:
        self._log.debug(
            f"Skipping {type(report).__name__} reconciliation for {report.instrument_id}: "
            f"not in `reconciliation_instrument_ids` include list",
            LogColor.MAGENTA,
        )

    def _reconcile_position_report_hedging(self, report: PositionStatusReport) -> bool:
        self._log.info(
            f"Reconciling HEDGE position for {report.instrument_id}, venue_position_id={report.venue_position_id}",
            LogColor.BLUE,
        )

        position: Position | None = self._cache.position(report.venue_position_id)

        if position is not None and (
            position.instrument_id != report.instrument_id
            or position.account_id != report.account_id
        ):
            self._log.error(
                f"Cannot reconcile {report.instrument_id} {report.venue_position_id!r}: "
                f"the position ID belongs to {position.instrument_id} {position.account_id}",
            )
            return True  # Not this scope's position, leave it untouched

        instrument = self._cache.instrument(report.instrument_id)

        if instrument is None:
            self._log.debug(
                f"Cannot reconcile position for {report.instrument_id}: instrument not found",
            )
            return True  # Filtered instrument not loaded

        if position is None:
            if quantities_equal_at_size_precision(
                report.signed_decimal_qty,
                Decimal(0),
                instrument.size_precision,
            ):
                return True  # Both flat, no issue

            if not self.generate_missing_orders:
                self._log.error(
                    f"Cannot reconcile position: {report.venue_position_id!r} not found "
                    "and `generate_missing_orders` is disabled",
                )
                return False

            strategy_id = self.get_external_order_claim(report.instrument_id) or StrategyId(
                "EXTERNAL",
            )
            scope = PositionScopeSnapshot(
                instrument_id=report.instrument_id,
                account_id=report.account_id,
                reports=(report,),
                incomplete_reason=None,
            )
            position_id = self._resolve_position_repair_id(
                scope,
                report.venue_position_id,
                strategy_id,
            )

            if not self._position_id_is_free_for_scope(scope, position_id, strategy_id):
                return False

            return self._reconcile_missing_hedge_position(report, instrument)

        position_signed_decimal_qty: Decimal = position.signed_decimal_qty()

        if not quantities_equal_at_size_precision(
            position_signed_decimal_qty,
            report.signed_decimal_qty,
            instrument.size_precision,
        ):
            if not self.generate_missing_orders:
                self._log.error(
                    f"Cannot reconcile {report.instrument_id} {report.venue_position_id!r}: "
                    f"position net qty {position_signed_decimal_qty} != reported net qty "
                    f"{report.signed_decimal_qty} and `generate_missing_orders` is disabled",
                )
                return False

            return self._reconcile_hedge_position_discrepancy(
                report=report,
                position=position,
                position_signed_decimal_qty=position_signed_decimal_qty,
                instrument=instrument,
            )

        return True  # Reconciled

    def _reconcile_hedge_position_discrepancy(
        self,
        report: PositionStatusReport,
        position: Position,
        position_signed_decimal_qty: Decimal,
        instrument: Instrument,
    ) -> bool:
        self._log.warning(
            f"Hedge position discrepancy for {report.instrument_id} "
            f"{report.venue_position_id!r}: cached={position_signed_decimal_qty}, "
            f"venue={report.signed_decimal_qty}, generating reconciliation order",
            LogColor.YELLOW,
        )

        return self._repair_reported_hedge_position(report, instrument)

    def _reconcile_missing_hedge_position(
        self,
        report: PositionStatusReport,
        instrument: Instrument,
    ) -> bool:
        self._log.warning(
            f"Missing hedge position for {report.instrument_id} "
            f"{report.venue_position_id!r}: venue reports {report.signed_decimal_qty}, "
            f"generating reconciliation order",
            LogColor.YELLOW,
        )

        return self._repair_reported_hedge_position(report, instrument)

    def _repair_reported_hedge_position(
        self,
        report: PositionStatusReport,
        instrument: Instrument,
    ) -> bool:
        # An ID-bearing report is positive truth for its own venue position ID and never
        # absence authority for the scope's other hedge IDs, so only that ID is compared,
        # and it is repaired through the same bound, capped, reduce-only machinery the
        # convergence pass uses rather than an unbound fabricated fill.
        scope = PositionScopeSnapshot(
            instrument_id=report.instrument_id,
            account_id=report.account_id,
            reports=(report,),
            incomplete_reason=None,
        )
        venue_position_id = report.venue_position_id
        intents = self._position_repair_intents(
            scope,
            instrument,
            self._reported_hedge_targets(scope, report),
        )

        if not intents:
            return True  # Reconciled

        self._apply_position_repairs(scope, intents)

        remaining = self._position_repair_intents(
            scope,
            instrument,
            self._reported_hedge_targets(scope, report),
        )

        if remaining:
            reason = "inbound hedge repair left the reported position unconverged"
            self._log.error(
                f"{reason} for {report.instrument_id} {venue_position_id!r}; "
                "the convergence pass re-observes it from a complete snapshot",
            )
            self._write_position_scope_residual(
                instrument_id=report.instrument_id,
                account_id=report.account_id,
                reason=reason,
            )
            return False

        return True  # Reconciled

    def _reported_hedge_targets(
        self,
        scope: PositionScopeSnapshot,
        report: PositionStatusReport,
    ) -> list[Position]:
        # The reported side's exposure is what the report is truth for, so the comparison
        # takes the position under the reported ID plus the side's exposure held under
        # virtual IDs, which carry no venue claim. A position under another venue ID is
        # that ID's own business: a single report is never absence authority for it.
        targets: list[Position] = []
        labelled = self._cache.position(report.venue_position_id)

        if (
            labelled is not None
            and labelled.is_open
            and labelled.instrument_id == scope.instrument_id
            and labelled.account_id == scope.account_id
        ):
            targets.append(labelled)

        if report.position_side not in (PositionSide.LONG, PositionSide.SHORT):
            return targets

        for position in self._cache.positions_open(
            venue=None,  # Faster query filtering
            instrument_id=scope.instrument_id,
            account_id=scope.account_id,
        ):
            if (
                position.side == report.position_side
                and position.id.value.startswith(VIRTUAL_POSITION_ID_PREFIX)
                and position.id != report.venue_position_id
            ):
                targets.append(position)

        return targets

    def _reconcile_position_report_netting(
        self,
        report: PositionStatusReport,
    ) -> bool:
        self._log.info(f"Reconciling NET position for {report.instrument_id}", LogColor.BLUE)

        instrument = self._cache.instrument(report.instrument_id)
        if instrument is None:
            self._log.debug(
                f"Cannot reconcile position for {report.instrument_id}: instrument not found",
            )
            return True  # Filtered instrument not loaded

        positions_open: list[Position] = self._cache.positions_open(
            venue=None,  # Faster query filtering
            instrument_id=report.instrument_id,
            account_id=report.account_id,
        )
        split_ownership_message = self._netting_split_position_ownership_message(
            report,
            positions_open,
        )

        if split_ownership_message is not None:
            self._log.warning(split_ownership_message, LogColor.YELLOW)

        position_signed_decimal_qty: Decimal = Decimal()

        for position in positions_open:
            position_signed_decimal_qty += position.signed_decimal_qty()

        self._log.info(f"{report.signed_decimal_qty=}", LogColor.BLUE)
        self._log.info(f"{position_signed_decimal_qty=}", LogColor.BLUE)

        # A single report is positive evidence for its own scope and never absence
        # authority beyond it, so it is diffed at net granularity and repaired through
        # the same target-safe machinery the convergence pass uses.
        scope = PositionScopeSnapshot(
            instrument_id=report.instrument_id,
            account_id=report.account_id,
            reports=(report,),
            incomplete_reason=None,
        )
        intents = self._position_repair_intents(scope, instrument)

        if not intents:
            self._verify_netting_avg_px(report, positions_open)
            return True  # Reconciled

        if not self.generate_missing_orders:
            self._log.warning(
                f"Discrepancy for {report.instrument_id} position "
                "when `generate_missing_orders` disabled, skipping further reconciliation",
            )
            return True

        # A diff carrying both a close and an open is the venue contradicting the cached
        # side: real orders flipped the position at the exchange, which is two events
        # there and must be two here.
        if any(intent.action == POSITION_REPAIR_TRIM for intent in intents) and any(
            intent.action == POSITION_REPAIR_OPEN for intent in intents
        ):
            return self._reconcile_cross_zero_position(
                report=report,
                instrument=instrument,
                scope=scope,
                intents=intents,
                position_signed_decimal_qty=position_signed_decimal_qty,
            )

        self._apply_position_repairs(scope, intents)

        if self._position_repair_intents(scope, instrument):
            reason = "inbound netting repair left the scope unconverged"
            self._log.error(
                f"{reason} for {report.instrument_id} {report.account_id}; "
                "the convergence pass re-observes it from a complete snapshot",
            )
            self._write_position_scope_residual(
                instrument_id=report.instrument_id,
                account_id=report.account_id,
                reason=reason,
            )
            return False

        return True  # Reconciled

    def _reconcile_cross_zero_position(
        self,
        report: PositionStatusReport,
        instrument: Instrument,
        scope: PositionScopeSnapshot,
        intents: list[PositionRepairIntent],
        position_signed_decimal_qty: Decimal,
    ) -> bool:
        self._log.info(
            f"Position crosses through zero for {report.instrument_id}: "
            f"current={position_signed_decimal_qty}, target={report.signed_decimal_qty}. "
            "Splitting reconciliation into two fills: close existing position, then open "
            "new position",
            LogColor.BLUE,
        )

        # First fill: close the existing position (bring it to zero) with reduce-only
        # repairs each capped at their own target, so no leg can over-close or flip it
        closes = [intent for intent in intents if intent.action == POSITION_REPAIR_TRIM]
        close_result = self._apply_cross_zero_leg(scope, instrument, closes)
        unflat = self._first_unflat_close_target(instrument, closes) if close_result else None

        if unflat is not None:
            self._log.error(
                f"Close leg for {report.instrument_id} left {unflat.id!r} holding "
                f"{unflat.quantity}; not opening the reported side",
            )

        # Second fill: open the reported position, never before the close left it flat
        opens = [intent for intent in intents if intent.action == POSITION_REPAIR_OPEN]
        open_result = (
            close_result and unflat is None and self._apply_cross_zero_leg(scope, instrument, opens)
        )

        if not (close_result and open_result) or self._position_repair_intents(scope, instrument):
            self._log.error(
                f"Failed to reconcile cross-zero position for {report.instrument_id}: "
                f"close={close_result}, open={open_result}",
            )
            self._write_position_scope_residual(
                instrument_id=report.instrument_id,
                account_id=report.account_id,
                reason="inbound cross-zero repair left the scope unconverged",
            )
            return False

        return True  # Reconciliation complete via split fills

    def _apply_cross_zero_leg(
        self,
        scope: PositionScopeSnapshot,
        instrument: Instrument,
        intents: list[PositionRepairIntent],
    ) -> bool:
        # Every repair of a leg must apply: a partially applied close would leave the
        # open crossing zero on whatever exposure remains.
        if not intents:
            return False

        return all(self._apply_position_repair(scope, instrument, intent) for intent in intents)

    def _first_unflat_close_target(
        self,
        instrument: Instrument,
        closes: list[PositionRepairIntent],
    ) -> Position | None:
        # The open leg must never trade against exposure the close was meant to remove, so
        # each close target is re-read and tested flat at the declared size precision.
        for intent in closes:
            position = self._cache.position(intent.target_position_id)

            if position is None:
                continue

            if not quantities_equal_at_size_precision(
                position.quantity.as_decimal(),
                Decimal(0),
                instrument.size_precision,
            ):
                return position

        return None

    def _verify_netting_avg_px(
        self,
        report: PositionStatusReport,
        positions_open: list[Position],
    ) -> None:
        # Quantities agree here, so a disagreeing average price is the remaining signal
        # that the venue's fill history was not fully reconciled.
        if report.avg_px_open is None:
            return

        total_value = Decimal(0)
        total_qty = Decimal(0)

        for position in positions_open:
            qty = abs(position.signed_decimal_qty())

            if position.avg_px_open and qty > 0:
                total_value += Decimal(str(position.avg_px_open)) * qty
                total_qty += qty

        if total_qty == 0:
            return

        current_avg_px = total_value / total_qty
        avg_px_diff = abs(current_avg_px - report.avg_px_open)
        relative_diff = avg_px_diff / report.avg_px_open if report.avg_px_open != 0 else 0

        if relative_diff > Decimal("0.0001"):  # 0.01% tolerance
            self._log.warning(
                f"Position avg_px mismatch for {report.instrument_id} after reconciliation: "
                f"internal={current_avg_px}, venue={report.avg_px_open}, "
                f"diff={avg_px_diff} ({relative_diff * 100:.4f}%). "
                f"This indicates incomplete reconciliation data from the venue.",
                LogColor.YELLOW,
            )
        else:
            self._log.info(
                f"Position avg_px verified for {report.instrument_id}: "
                f"internal={current_avg_px}, venue={report.avg_px_open}",
                LogColor.BLUE,
            )

    def _netting_split_position_ownership_message(
        self,
        report: PositionStatusReport,
        positions_open: list[Position],
    ) -> str | None:
        strategy_ids = sorted({position.strategy_id.value for position in positions_open})
        if len(strategy_ids) <= 1:
            return None

        position_details = ", ".join(
            f"{position.id}:strategy_id={position.strategy_id},"
            f"signed_qty={position.signed_decimal_qty()}"
            for position in sorted(positions_open, key=lambda pos: pos.id.value)
        )
        return (
            f"NETTING position ownership is split for account_id={report.account_id}, "
            f"instrument_id={report.instrument_id}: strategy_ids={strategy_ids}, "
            f"positions=[{position_details}]. This is legal in Nautilus but dangerous on venues "
            "with account-level net positions; use `external_order_claims` when one strategy "
            "should claim reconciled exposure."
        )

    def _create_position_reconciliation_report(
        self,
        report: PositionStatusReport,
        instrument: Instrument,
        position_signed_decimal_qty: Decimal,
        diff_quantity: Quantity,
        current_avg_px: Decimal | None,
    ) -> OrderStatusReport | None:
        order_side = (
            OrderSide.BUY
            if report.signed_decimal_qty > position_signed_decimal_qty
            else OrderSide.SELL
        )

        # Calculate reconciliation price
        reconciliation_price = calculate_reconciliation_price(
            current_position_qty=position_signed_decimal_qty,
            current_position_avg_px=current_avg_px,
            target_position_qty=report.signed_decimal_qty,
            target_position_avg_px=report.avg_px_open,
            instrument=instrument,
        )

        # If we couldn't calculate a price, use a reasonable fallback
        if reconciliation_price is None:
            # If avg_px_open is None, we cannot compute an exact reconciliation price
            # and will fall back to a market price.
            self._log.warning(
                f"Cannot calculate exact reconciliation price for {report.instrument_id}: "
                f"position report lacks average price information, using last quote fallback",
            )

            quote = self._cache.quote_tick(report.instrument_id)

            if quote:
                if order_side == OrderSide.BUY:
                    reconciliation_price = quote.ask_price
                else:  # OrderSide.SELL
                    reconciliation_price = quote.bid_price
            else:
                # If no market data, use current average price of positions as fallback
                if current_avg_px is not None:
                    reconciliation_price = instrument.make_price(current_avg_px)

        now = self._clock.timestamp_ns()

        if reconciliation_price:
            # Generate a LIMIT order with the calculated reconciliation price
            avg_px = reconciliation_price.as_decimal()

            # Always a fresh reconciliation order: a reused historical order makes the
            # repair a silent no-op against a position it was never meant to move.
            return OrderStatusReport(
                instrument_id=report.instrument_id,
                account_id=report.account_id,
                venue_order_id=self._create_synthetic_reconciliation_venue_order_id(
                    account_id=report.account_id,
                    instrument_id=report.instrument_id,
                    order_side=order_side,
                    order_type=OrderType.LIMIT,
                    quantity=diff_quantity,
                    price=reconciliation_price,
                    venue_position_id=report.venue_position_id,
                    ts_last=report.ts_last,
                ),
                venue_position_id=report.venue_position_id,
                order_side=order_side,
                order_type=OrderType.LIMIT,
                time_in_force=TimeInForce.GTC,
                order_status=OrderStatus.FILLED,
                price=reconciliation_price,
                quantity=diff_quantity,
                filled_qty=diff_quantity,
                avg_px=avg_px,
                report_id=UUID4(),
                ts_accepted=now,
                ts_last=now,
                ts_init=now,
            )
        else:
            # No price information, fall back to generated MARKET order
            avg_px = None
            self._log.warning(
                f"Could not determine reconciliation price for {report.instrument_id}, "
                f"generating MARKET order for position reconciliation "
                f"(current: {position_signed_decimal_qty}, target: {report.signed_decimal_qty})",
            )

            # Always a fresh reconciliation order: a reused historical order makes the
            # repair a silent no-op against a position it was never meant to move.
            return OrderStatusReport(
                instrument_id=report.instrument_id,
                account_id=report.account_id,
                venue_order_id=self._create_synthetic_reconciliation_venue_order_id(
                    account_id=report.account_id,
                    instrument_id=report.instrument_id,
                    order_side=order_side,
                    order_type=OrderType.MARKET,
                    quantity=diff_quantity,
                    price=None,
                    venue_position_id=report.venue_position_id,
                    ts_last=report.ts_last,
                ),
                venue_position_id=report.venue_position_id,
                order_side=order_side,
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.IOC,
                order_status=OrderStatus.FILLED,
                quantity=diff_quantity,
                filled_qty=diff_quantity,
                avg_px=avg_px,
                report_id=UUID4(),
                ts_accepted=now,
                ts_last=now,
                ts_init=now,
            )

    def _create_synthetic_reconciliation_venue_order_id(
        self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        order_side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        price: Price | None,
        venue_position_id: PositionId | None,
        ts_last: int,
        tag: str | None = None,
    ) -> VenueOrderId:
        pyo3_venue_order_id = nautilus_pyo3.create_position_reconciliation_venue_order_id(
            nautilus_pyo3.AccountId(account_id.value),
            nautilus_pyo3.InstrumentId.from_str(instrument_id.value),
            nautilus_pyo3.OrderSide(order_side.name),
            nautilus_pyo3.OrderType(order_type.name),
            nautilus_pyo3.Quantity.from_str(str(quantity)),
            nautilus_pyo3.Price.from_str(str(price)) if price else None,
            nautilus_pyo3.PositionId(venue_position_id.value) if venue_position_id else None,
            ts_last,
            tag,
        )
        return VenueOrderId(pyo3_venue_order_id.value)

    def _reconcile_order_report(
        self,
        report: OrderStatusReport,
        trades: list[FillReport],
        is_external: bool = True,
    ) -> bool:
        if self._is_shutting_down:
            return True  # Skip reconciliation during shutdown

        client_order_id = self._resolve_client_order_id(report)

        # Reset retry count
        self._clear_recon_tracking(client_order_id)

        self._log.debug(f"Reconciling order for {client_order_id!r}", LogColor.MAGENTA)
        order: Order = self._cache.order(client_order_id)

        if order is None:
            instrument = self._cache.instrument(report.instrument_id)
            if instrument is None:
                self._log.debug(
                    f"Cannot reconcile order for {client_order_id!r}: "
                    f"instrument {report.instrument_id} not found",
                )
                return True  # Filtered instrument not loaded

            order = self._generate_order(report, is_external)

            if order is None:
                # External order dropped
                return True  # No further reconciliation

            # Add to cache without determining any position ID initially
            self._cache.add_order(order)

            # Explicitly index venue_order_id for external orders to ensure they can be found
            # by venue_order_id in subsequent reconciliation passes
            if order.venue_order_id is not None:
                self._ensure_venue_order_id_indexed(
                    client_order_id=order.client_order_id,
                    venue_order_id=order.venue_order_id,
                )

            if self.manage_own_order_books and py_should_handle_own_book_order(order):
                self._add_own_book_order(order)

        else:
            # Order already exists, check instrument
            instrument = self._cache.instrument(order.instrument_id)
            if instrument is None:
                self._log.debug(
                    f"Cannot reconcile order for {order.client_order_id!r}: "
                    f"instrument {order.instrument_id} not found",
                )
                return True  # Filtered instrument not loaded

        # Handle order status transitions
        status_result = self._handle_order_status_transitions(order, report, trades, instrument)
        if status_result is not None:
            return status_result

        # Reconcile all trades
        for trade in trades:
            self._reconcile_fill_report(order, trade, instrument)

        if report.avg_px is None:
            self._log.warning("report.avg_px was `None` when a value was expected")

        # Handle fill quantity mismatches
        return self._handle_fill_quantity_mismatch(order, report, instrument, client_order_id)

    def _resolve_client_order_id(self, report: OrderStatusReport) -> ClientOrderId:
        client_order_id: ClientOrderId | None = report.client_order_id
        if client_order_id is None:
            client_order_id = self._cache.client_order_id(report.venue_order_id)
            if client_order_id is None and report.venue_order_id is not None:
                # Check if an external order with this venue_order_id already exists
                # by searching cached orders (handles cases where index might not be built yet)
                cached_order = self._find_order_by_venue_order_id(
                    venue_order_id=report.venue_order_id,
                    instrument_id=report.instrument_id,
                    order_side=report.order_side,
                )

                if cached_order is not None:
                    client_order_id = cached_order.client_order_id
                    self._log.debug(
                        f"Found existing external order {client_order_id} by venue_order_id "
                        f"{report.venue_order_id}, reusing",
                    )
                    # Ensure mapping is indexed
                    self._ensure_venue_order_id_indexed(
                        client_order_id=client_order_id,
                        venue_order_id=report.venue_order_id,
                    )

            if client_order_id is None:
                # Generate external client order ID
                client_order_id = ClientOrderId(UUID4().value)

            # Assign to report
            report.client_order_id = client_order_id

        return client_order_id

    def _ensure_venue_order_id_indexed(
        self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        log_context: str = "",
    ) -> None:
        # Index venue_order_id in cache for lookups
        try:
            self._cache.add_venue_order_id(
                client_order_id,
                venue_order_id,
                overwrite=False,
            )
        except ValueError:
            # Mapping already exists or conflicts - this is expected if order was
            # previously indexed or if there's a conflict (which should be rare)
            self._log.debug(
                f"Venue order ID {venue_order_id} already indexed for "
                f"{client_order_id}{' ' + log_context if log_context else ''}, skipping",
            )

    def _handle_fill_quantity_mismatch(
        self,
        order: Order,
        report: OrderStatusReport,
        instrument: Instrument,
        client_order_id: ClientOrderId,
    ) -> bool:
        if report.filled_qty < order.filled_qty:
            # Gather diagnostic information
            fill_history = [
                (event.trade_id, event.last_qty, event.ts_event)
                for event in order.events
                if isinstance(event, OrderFilled)
            ]

            self._log.error(
                f"report.filled_qty {report.filled_qty} < order.filled_qty {order.filled_qty}, "
                f"this could potentially be caused by duplicate fills or corrupted cached state; "
                f"order_id={order.client_order_id}, venue_order_id={order.venue_order_id}, "
                f"total_fills_applied={len(fill_history)}, "
                f"fill_trade_ids={order.trade_ids}, "
                f"inferred_fill={'yes' if client_order_id in self._inferred_fill_ts else 'no'}, "
                f"order_status={order.status}, report_status={report.order_status}",
            )

            # Log each fill for forensics
            for trade_id, qty, ts in fill_history:
                self._log.error(f"  Fill: {trade_id}, qty={qty}, ts={ts}")

            return False  # Failed

        if report.filled_qty > order.filled_qty:
            # Check if order is already closed to avoid duplicate inferred fills
            if order.is_closed:
                # Use the higher precision for tolerance check
                precision = max(report.filled_qty.precision, order.filled_qty.precision)

                if is_within_single_unit_tolerance(
                    report.filled_qty.as_decimal(),
                    order.filled_qty.as_decimal(),
                    precision,
                ):
                    return True

                self._log.debug(  # TODO: Reduce level to debug after initial development phase
                    f"{order.instrument_id} {order.client_order_id!r} already {order.status_string()} but "
                    f"reported difference in filled_qty: "
                    f"report={report.filled_qty}, cached={order.filled_qty}, "
                    f"skipping inferred fill generation for closed order",
                )
                return True  # Consider it reconciled to avoid infinite loops

            # This is due to missing fill report(s), there may now be some
            # information loss if multiple fills occurred to reach the reported
            # state, or if commissions differed from the default.
            try:
                fill: OrderFilled = self._generate_inferred_fill(order, report, instrument)
                self._handle_event_with_tracking(fill)
            except ValueError as e:
                self._log.error(
                    f"Cannot generate inferred fill for {order.client_order_id}: {e}. "
                    f"Reconciliation for this order failed.",
                )
                return False  # Failed

            if (
                report.avg_px is not None
                and order.avg_px is not None
                and not math.isclose(float(report.avg_px), float(order.avg_px))
            ):
                self._log.warning(
                    f"report.avg_px {report.avg_px} != order.avg_px {order.avg_px}, "
                    "this could potentially be caused by information loss due to inferred fills",
                )

        return True  # Reconciled

    def _handle_order_status_transitions(
        self,
        order: Order,
        report: OrderStatusReport,
        trades: list[FillReport],
        instrument: Instrument,
    ) -> bool | None:
        if report.order_status == OrderStatus.REJECTED:
            if order.status != OrderStatus.REJECTED:
                self._generate_order_rejected(order, report)

            return True  # Reconciled

        if report.order_status == OrderStatus.ACCEPTED:
            if order.status != OrderStatus.ACCEPTED:
                self._generate_order_accepted(order, report)

            # Detect deltas even when already accepted (e.g. venue-side reduce-only
            # quantity reduction or priceMatch adjustment that we missed).
            if self._should_update(order, report):
                self._generate_order_updated(order, report)

            return True  # Reconciled

        # Order must have been accepted from this point
        if order.status in (OrderStatus.INITIALIZED, OrderStatus.SUBMITTED):
            self._generate_order_accepted(order, report)

        # Update order quantity and price differences
        if self._should_update(order, report):
            self._generate_order_updated(order, report)

        if report.order_status == OrderStatus.TRIGGERED:
            if order.status != OrderStatus.TRIGGERED:
                self._generate_order_triggered(order, report)

            return True  # Reconciled

        if report.order_status == OrderStatus.CANCELED:
            if order.status != OrderStatus.CANCELED and order.is_open:
                if report.ts_triggered > 0:
                    self._generate_order_triggered(order, report)

                # Reconcile all trades
                for trade in trades:
                    self._reconcile_fill_report(order, trade, instrument)

                self._generate_order_canceled(order, report)

            return True  # Reconciled

        if report.order_status == OrderStatus.EXPIRED:
            if order.status != OrderStatus.EXPIRED and order.is_open:
                if report.ts_triggered > 0:
                    self._generate_order_triggered(order, report)

                # Reconcile all trades before expired event (same as canceled)
                for trade in trades:
                    self._reconcile_fill_report(order, trade, instrument)

                self._generate_order_expired(order, report)

            return True  # Reconciled

        return None  # Continue with fill reconciliation

    def _should_update(self, order: Order, report: OrderStatusReport) -> bool:
        if report.quantity != order.quantity and report.quantity >= order.filled_qty:
            return True  # Valid quantity update

        match order.order_type:
            case OrderType.LIMIT:
                return report.price != order.price
            case OrderType.STOP_MARKET | OrderType.TRAILING_STOP_MARKET:
                return report.trigger_price != order.trigger_price
            case OrderType.STOP_LIMIT | OrderType.TRAILING_STOP_LIMIT:
                return report.trigger_price != order.trigger_price or report.price != order.price
            case _:
                return False

    def _reconcile_fill_report(
        self,
        order: Order,
        report: FillReport,
        instrument: Instrument,
    ) -> bool:
        # Check if this fill should be skipped (predates inferred fill or is duplicate)
        skip_result = self._check_and_skip_duplicate_fill(order, report)
        if skip_result is not None:
            return skip_result

        # Check if fill would cause overfill
        potential_filled_qty = order.filled_qty + report.last_qty
        if potential_filled_qty > order.quantity:
            if not self.allow_overfills:
                # Remembered against its order so later passes skip it rather than
                # re-fetching the same rejection. Recording prunes first, since the
                # periodic prune runs only while a reconciliation check is configured.
                self._prune_overfill_rejected_fills()
                self._overfill_rejected_trade_ids.setdefault(order.client_order_id, {})[
                    report.trade_id
                ] = self._clock.timestamp_ns()
                self._log.warning(
                    f"Rejecting fill that would cause overfill for {order.client_order_id!r}: "
                    f"order.quantity={order.quantity}, order.filled_qty={order.filled_qty}, "
                    f"fill.last_qty={report.last_qty}, would result in filled_qty={potential_filled_qty}",
                )
                return False  # Reject fill to prevent overfill
            # allow_overfills=True: log warning but allow the fill through
            self._log.warning(
                f"Allowing overfill during reconciliation for {order.client_order_id!r}: "
                f"order.quantity={order.quantity}, order.filled_qty={order.filled_qty}, "
                f"fill.last_qty={report.last_qty}, will result in filled_qty={potential_filled_qty}",
            )

        # Verify total fills consistency BEFORE applying
        current_total = sum(
            event.last_qty for event in order.events if isinstance(event, OrderFilled)
        )

        if current_total != order.filled_qty:
            self._log.error(
                f"INCONSISTENCY DETECTED before applying fill: "
                f"sum(fills)={current_total} != order.filled_qty={order.filled_qty} "
                f"for {order.client_order_id}",
            )

        # Final check: ensure trade_id doesn't already exist before generating fill
        # This prevents KeyError from being raised in _apply_event_to_order
        existing_fill = get_existing_fill_for_trade_id(order, report.trade_id)
        if report.trade_id in order.trade_ids or existing_fill is not None:
            self._log.debug(
                f"Fill with trade_id {report.trade_id} already exists for order {order.client_order_id}, skipping duplicate",
            )
            return True  # Fill already exists, treat as successful

        # Track fill application in audit trail BEFORE generating the fill
        # This ensures cleanup on close remains effective if this fill closes the order
        if order.client_order_id not in self._fill_application_audit:
            self._fill_application_audit[order.client_order_id] = []

        audit_entry = (report.trade_id, "reconciliation", self._clock.timestamp_ns())
        self._fill_application_audit[order.client_order_id].append(audit_entry)

        try:
            self._generate_order_filled(order, report, instrument)
        except InvalidStateTrigger as e:
            self._rollback_fill_audit_entry(order.client_order_id, audit_entry)
            self._log.error(str(e))
            return False
        except ValueError as e:
            self._rollback_fill_audit_entry(order.client_order_id, audit_entry)
            # Handle the negative leaves_qty error
            self._log.exception(
                f"ValueError when applying fill to {order.client_order_id!r}: {e}",
                e,
            )
            return False

        # Check correct ordering of fills
        if report.ts_event < order.ts_last:
            self._log.warning(
                f"OrderFilled applied out of chronological order from {report}",
            )
        return True

    def _check_and_skip_duplicate_fill(
        self,
        order: Order,
        report: FillReport,
    ) -> bool | None:
        # Check if this fill predates an inferred reconciliation fill
        # This prevents historical fills from being applied on top of inferred fills
        client_order_id = order.client_order_id
        if client_order_id in self._inferred_fill_ts:
            earliest_inferred_ts = self._inferred_fill_ts[client_order_id]
            if report.ts_event < earliest_inferred_ts:
                self._log.debug(
                    f"Skipping historical fill {report.trade_id} (ts_event={report.ts_event}) "
                    f"for {client_order_id!r} as it predates inferred reconciliation fill "
                    f"(ts={earliest_inferred_ts}); this fill is already accounted for in the inferred fill",
                )
                return True  # Skip this fill, it's already covered by inferred fill

        # Check for duplicate fill by trade_id - check both trade_ids collection and events
        # This handles cases where order is loaded from cache and trade_ids might not be fully populated
        existing_fill = get_existing_fill_for_trade_id(order, report.trade_id)
        if report.trade_id in order.trade_ids or existing_fill is not None:
            # Fill already applied; check if data is consistent.
            # An existing fill may be sourced from the cache on start,
            # or may exist in-memory when a reconciliation is triggered.

            # Log detailed info about when it was first applied
            if order.client_order_id in self._fill_application_audit:
                audit = self._fill_application_audit[order.client_order_id]
                previous = [a for a in audit if a[0] == report.trade_id]
                if previous:
                    self._log.debug(
                        f"Duplicate fill detected; {report.trade_id} was already applied "
                        f"at ts={previous[0][2]}, source={previous[0][1]}",
                    )

            if existing_fill and not self._fill_reports_equal(existing_fill, report):
                differences: list[str] = []

                # Last quantity
                if existing_fill.last_qty != report.last_qty:
                    differences.append(f"qty: {existing_fill.last_qty} vs {report.last_qty}")

                # Last price
                if existing_fill.last_px != report.last_px:
                    differences.append(f"px: {existing_fill.last_px} vs {report.last_px}")

                # Commission
                if existing_fill.commission is None and report.commission is not None:
                    differences.append(f"commission: None vs {report.commission}")
                elif existing_fill.commission is not None and report.commission is None:
                    differences.append(f"commission: {existing_fill.commission} vs None")
                elif existing_fill.commission is not None and report.commission is not None:
                    if existing_fill.commission.currency != report.commission.currency:
                        differences.append(
                            f"commission currency: {existing_fill.commission.currency} vs {report.commission.currency}",
                        )
                    elif existing_fill.commission != report.commission:
                        differences.append(
                            f"commission: {existing_fill.commission} vs {report.commission}",
                        )

                # Liquidity side
                if existing_fill.liquidity_side != report.liquidity_side:
                    differences.append(
                        f"liquidity: {existing_fill.liquidity_side} vs {report.liquidity_side}",
                    )

                # Timestamp
                if existing_fill.ts_event != report.ts_event:
                    differences.append(
                        f"ts_event: {existing_fill.ts_event} vs {report.ts_event}",
                    )

                self._log.warning(
                    f"Fill report data differs from existing data for trade_id {report.trade_id}, "
                    f"differences: {', '.join(differences)}; retaining cached data for consistency",
                )

            # If trade_id is in order.trade_ids or we found an existing fill, skip this fill
            # This prevents duplicate fills from being applied
            return True  # Fill already applied, continue with existing data

        return None  # Not a duplicate, proceed with fill

    def _generate_inferred_fill(
        self,
        order: Order,
        report: OrderStatusReport,
        instrument: Instrument,
    ) -> OrderFilled:
        client = None
        client_id = self._cache.client_id(order.client_order_id)
        if client_id is not None:
            client = self._clients.get(client_id)

        if client is None:
            client = self._routing_map.get(instrument.id.venue, self._default_client)

        filled = create_inferred_order_filled_event(
            order=order,
            ts_now=self._clock.timestamp_ns(),
            report=report,
            instrument=instrument,
            client=client,
        )
        self._log.info(f"Generated inferred {filled}", LogColor.BLUE)

        return filled

    # -- ORDER AND EVENTS GENERATION ---------------------------------------------------------------

    def _generate_order(
        self,
        report: OrderStatusReport,
        is_external: bool = True,
        strategy_id: StrategyId | None = None,
    ) -> Order | None:
        self._log.debug(f"Generating order {report.client_order_id!r}", color=LogColor.MAGENTA)

        options: dict[str, Any] = {}

        if report.price is not None:
            options["price"] = str(report.price)

        if report.trigger_price is not None:
            options["trigger_price"] = str(report.trigger_price)

        if report.trigger_type is not None:
            options["trigger_type"] = trigger_type_to_str(report.trigger_type)

        if report.limit_offset is not None:
            options["limit_offset"] = str(report.limit_offset)
            options["trailing_offset_type"] = trailing_offset_type_to_str(
                report.trailing_offset_type,
            )

        if report.trailing_offset is not None:
            options["trailing_offset"] = str(report.trailing_offset)
            options["trailing_offset_type"] = trailing_offset_type_to_str(
                report.trailing_offset_type,
            )

        if report.display_qty is not None:
            options["display_qty"] = str(report.display_qty)

        options["expire_time_ns"] = (
            0 if report.expire_time is None else dt_to_unix_nanos(report.expire_time)
        )

        # A caller-supplied strategy ID binds a position repair to the strategy owning the
        # target position, so the repair fill reaches that strategy's position.
        tags: list[str] | None = ["RECONCILIATION"] if strategy_id is not None else None

        if strategy_id is None:
            # Check if any strategy has claimed external orders for this instrument,
            # which allows strategies to resume managing existing orders on restart.
            strategy_id = self.get_external_order_claim(report.instrument_id)

            if strategy_id is None:
                # All unclaimed reconciliation uses the EXTERNAL strategy ID,
                # with tags distinguishing the source for filtering purposes.
                strategy_id = StrategyId("EXTERNAL")

                if is_external:
                    # Actual external order found on venue
                    tags = ["VENUE"]
                else:
                    # Internal position diff alignment (synthetic fill)
                    tags = ["RECONCILIATION"]
            else:
                # External order claimed by a strategy via `external_order_claims` config,
                # so this order will be managed by the claiming strategy.
                self._log.info(
                    f"External order {report.client_order_id} for {report.instrument_id} "
                    f"claimed by strategy {strategy_id}",
                    LogColor.BLUE,
                )

        # Filter unclaimed external orders (but not reconciliation fills)
        if self.filter_unclaimed_external_orders and tags and "VENUE" in tags:
            self._filtered_external_orders_count += 1

            if self._filtered_external_orders_count == 1:
                self._log.warning("Filtering unclaimed EXTERNAL orders", LogColor.BLUE)

            return None  # No further reconciliation

        initialized = OrderInitialized(
            trader_id=self.trader_id,
            strategy_id=strategy_id,
            instrument_id=report.instrument_id,
            client_order_id=report.client_order_id,
            order_side=report.order_side,
            order_type=report.order_type,
            quantity=report.quantity,
            time_in_force=report.time_in_force,
            post_only=report.post_only,
            reduce_only=report.reduce_only,
            quote_quantity=False,
            options=options,
            emulation_trigger=TriggerType.NO_TRIGGER,
            trigger_instrument_id=None,
            contingency_type=report.contingency_type,
            order_list_id=report.order_list_id,
            linked_order_ids=report.linked_order_ids,
            parent_order_id=report.parent_order_id,
            exec_algorithm_id=None,
            exec_algorithm_params=None,
            exec_spawn_id=None,
            tags=tags,
            event_id=UUID4(),
            ts_init=self._clock.timestamp_ns(),
            reconciliation=True,
        )

        order: Order = OrderUnpacker.from_init(initialized)
        self._log.debug(f"Generated {initialized}")

        return order

    def _generate_order_rejected(self, order: Order, report: OrderStatusReport) -> None:
        rejected = create_order_rejected_event(
            order=order,
            ts_now=self._clock.timestamp_ns(),
            report=report,
        )
        self._log.debug(f"Generated {rejected}")
        self._handle_event_with_tracking(rejected)

    def _generate_order_accepted(self, order: Order, report: OrderStatusReport) -> None:
        # Clear any retry counts when order transitions to ACCEPTED
        self._clear_recon_tracking(order.client_order_id)

        # Also try to clear by venue order ID mapping
        if report.venue_order_id:
            mapped_client_id = self._cache.client_order_id(report.venue_order_id)
            if mapped_client_id:
                self._clear_recon_tracking(mapped_client_id)

        accepted = create_order_accepted_event(
            trader_id=self.trader_id,
            order=order,
            ts_now=self._clock.timestamp_ns(),
            report=report,
        )
        self._log.debug(f"Generated {accepted}")
        self._handle_event_with_tracking(accepted)

    def _generate_order_triggered(self, order: Order, report: OrderStatusReport) -> None:
        if order.order_type not in (
            OrderType.STOP_LIMIT,
            OrderType.TRAILING_STOP_LIMIT,
            OrderType.LIMIT_IF_TOUCHED,
        ):
            self._log.debug(
                f"Skipping OrderTriggered for {order.type_string()} order "
                f"{order.client_order_id!r}: market-style stops have no TRIGGERED state",
            )
            return

        triggered = create_order_triggered_event(
            trader_id=self.trader_id,
            order=order,
            ts_now=self._clock.timestamp_ns(),
            report=report,
        )
        self._log.debug(f"Generated {triggered}")
        self._handle_event_with_tracking(triggered)

    def _generate_order_updated(self, order: Order, report: OrderStatusReport) -> None:
        updated = create_order_updated_event(
            trader_id=self.trader_id,
            order=order,
            ts_now=self._clock.timestamp_ns(),
            report=report,
        )
        self._log.debug(f"Generated {updated}")
        self._handle_event_with_tracking(updated)

    def _generate_order_canceled(self, order: Order, report: OrderStatusReport) -> None:
        canceled = create_order_canceled_event(
            order=order,
            ts_now=self._clock.timestamp_ns(),
            report=report,
        )
        self._log.debug(f"Generated {canceled}")
        self._handle_event_with_tracking(canceled)

    def _generate_order_expired(self, order: Order, report: OrderStatusReport) -> None:
        expired = create_order_expired_event(
            order=order,
            ts_now=self._clock.timestamp_ns(),
            report=report,
        )
        self._log.debug(f"Generated {expired}")
        self._handle_event_with_tracking(expired)

    def _generate_order_filled(
        self,
        order: Order,
        report: FillReport,
        instrument: Instrument,
    ) -> None:
        filled = create_order_filled_event(
            order=order,
            ts_now=self._clock.timestamp_ns(),
            report=report,
            instrument=instrument,
        )
        self._log.debug(f"Generated {filled}")
        self._handle_event_with_tracking(filled)

    # -- INTERNAL ----------------------------------------------------------------------------------

    def _clear_recon_tracking(
        self,
        client_order_id: ClientOrderId,
        *,
        drop_last_query: bool = True,
    ) -> None:
        self._recon_check_retries.pop(client_order_id, None)

        if client_order_id in self._missing_order_query_queue:
            self._missing_order_query_queue.remove(client_order_id)

        if drop_last_query:
            self._ts_last_query.pop(client_order_id, None)

    def _handle_event_with_tracking(self, event: OrderEvent) -> None:
        # Handle an order event with activity tracking, recording fills in cache and
        # cleaning up tracking data for closed orders.
        self._record_local_activity(event)
        received_fill = False

        if isinstance(event, OrderFilled):
            received_fill = self._received_fill_event_counts[event.id] > 0
            ts_now = self._clock.timestamp_ns()
            self._recent_fills_cache[event.trade_id] = ts_now

            if event.reconciliation:
                # Track inferred fill timestamps to prevent duplicate historical fills
                client_order_id = event.client_order_id
                if client_order_id not in self._inferred_fill_ts:
                    self._inferred_fill_ts[client_order_id] = event.ts_event
            else:
                # Stamp application time: a venue ts_event ahead of this clock would make
                # the grace delta negative and suppress the position check. Reconciliation's
                # own fills never stamp it, since a pass is a diff of venue state against
                # the cache and its own output is not evidence which defers the next one.
                self._position_local_activity_ns[(event.instrument_id, event.account_id)] = ts_now
        elif isinstance(event, OrderUpdated):
            # An overfill rejection was judged against the order's quantity, so a change
            # to it makes the same fill applicable again and the verdict is discarded.
            order = self._cache.order(event.client_order_id)

            if order is None or order.quantity != event.quantity:
                self._overfill_rejected_trade_ids.pop(event.client_order_id, None)

        try:
            self._handle_event(event)
        finally:
            if received_fill:
                self._clear_received_position_fill(event)

        if event.client_order_id is None:
            return

        order = self._cache.order(event.client_order_id)
        if order and order.is_closed:
            self._clear_recon_tracking(order.client_order_id)
            self._order_local_activity_ns.pop(order.client_order_id, None)
            self._inferred_fill_ts.pop(order.client_order_id, None)
            self._fill_application_audit.pop(order.client_order_id, None)

    def _record_received_position_fill(self, event: OrderFilled) -> None:
        # Record before enqueueing: the venue query and queue consumer are independent,
        # so receive time is the earliest point at which the snapshot is known stale.
        key = (event.instrument_id, event.account_id)
        self._position_local_activity_ns[key] = self._clock.timestamp_ns()
        self._received_unapplied_fill_counts[key] += 1
        self._received_fill_event_counts[event.id] += 1

    def _clear_received_position_fill(self, event: OrderFilled) -> None:
        event_count = self._received_fill_event_counts[event.id]

        if event_count <= 1:
            self._received_fill_event_counts.pop(event.id, None)
        else:
            self._received_fill_event_counts[event.id] = event_count - 1

        key = (event.instrument_id, event.account_id)
        scope_count = self._received_unapplied_fill_counts[key]

        if scope_count <= 1:
            self._received_unapplied_fill_counts.pop(key, None)
        else:
            self._received_unapplied_fill_counts[key] = scope_count - 1

    def _record_local_activity(self, event: OrderEvent | None) -> None:
        if event is None:
            return

        client_order_id = event.client_order_id
        if client_order_id is None:
            return

        # Use receipt time (current clock time) instead of venue time (ts_event)
        # to accurately track when we last processed activity for this order.
        # This avoids race conditions where network/queue latency makes events
        # appear "old" even though they just arrived.
        self._order_local_activity_ns[client_order_id] = self._clock.timestamp_ns()

    def _find_order_by_venue_order_id(
        self,
        venue_order_id: VenueOrderId,
        instrument_id: InstrumentId,
        order_side: OrderSide | None = None,
    ) -> Order | None:
        # Fallback search when venue_order_id index not built
        cached_orders = self._cache.orders(
            venue=instrument_id.venue,
            instrument_id=instrument_id,
            side=order_side,
        )

        for cached_order in cached_orders:
            if cached_order.venue_order_id == venue_order_id:
                return cached_order

        return None
