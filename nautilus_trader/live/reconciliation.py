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
Reconciliation functions for live trading.
"""

from decimal import ROUND_DOWN
from decimal import Decimal
from decimal import localcontext
from typing import Final
from typing import NamedTuple

from nautilus_trader.cache.transformers import transform_instrument_to_pyo3
from nautilus_trader.common.component import Logger
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.client import ExecutionClient
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.model.currencies import register_currency
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.events import OrderAccepted
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import OrderTriggered
from nautilus_trader.model.events import OrderUpdated
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import Order
from nautilus_trader.model.position import Position


def is_within_single_unit_tolerance(
    value1: Decimal,
    value2: Decimal,
    precision: int,
) -> bool:
    """
    Check if two decimal values are within single unit tolerance based on precision.

    Handles rounding discrepancies from venues (e.g., OKX fillSz vs accFillSz).

    Parameters
    ----------
    value1 : Decimal
        The first value to compare.
    value2 : Decimal
        The second value to compare.
    precision : int
        The decimal precision for tolerance calculation.

    Returns
    -------
    bool

    """
    # Only apply tolerance for fractional quantities (precision > 0)
    if precision == 0:
        return value1 == value2  # Integer quantities require exact match

    tolerance = Decimal(10) ** -precision

    return abs(value1 - value2) <= tolerance


def get_existing_fill_for_trade_id(
    order: Order,
    trade_id: TradeId,
) -> OrderFilled | None:
    """
    Find an existing fill event for a trade ID in the order's event history.

    Parameters
    ----------
    order : Order
        The order to search.
    trade_id : TradeId
        The trade ID to find.

    Returns
    -------
    OrderFilled or ``None``

    """
    for event in order.events:
        if isinstance(event, OrderFilled) and event.trade_id == trade_id:
            return event

    return None


def create_order_rejected_event(
    order: Order,
    ts_now: int,
    report: OrderStatusReport | None = None,
    reason: str | None = None,
) -> OrderRejected:
    """
    Create an OrderRejected event for reconciliation.

    This function unifies the creation of OrderRejected events across different
    reconciliation paths (startup with report, continuous without report).

    Parameters
    ----------
    order : Order
        The order to create the rejection event for.
    ts_now : int
        The current timestamp in nanoseconds.
    report : OrderStatusReport, optional
        The order status report from the venue (if available).
    reason : str, optional
        The rejection reason (used when no report is available).

    Returns
    -------
    OrderRejected

    """
    if report:
        # Use report data when available (startup reconciliation)
        return OrderRejected(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            account_id=report.account_id,
            reason=report.cancel_reason or reason or "UNKNOWN",
            event_id=UUID4(),
            ts_event=report.ts_last,
            ts_init=ts_now,
            reconciliation=True,
        )
    else:
        # Use current timestamp and provided reason (continuous reconciliation)
        return OrderRejected(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            account_id=order.account_id,
            reason=reason or "UNKNOWN",
            event_id=UUID4(),
            ts_event=ts_now,
            ts_init=ts_now,
            reconciliation=True,
        )


def create_order_canceled_event(
    order: Order,
    ts_now: int,
    report: OrderStatusReport | None = None,
) -> OrderCanceled:
    """
    Create an OrderCanceled event for reconciliation.

    This function unifies the creation of OrderCanceled events across different
    reconciliation paths (startup with report, continuous without report).

    Parameters
    ----------
    order : Order
        The order to create the cancellation event for.
    ts_now : int
        The current timestamp in nanoseconds.
    report : OrderStatusReport, optional
        The order status report from the venue (if available).

    Returns
    -------
    OrderCanceled

    """
    if report:
        # Use report data when available (startup reconciliation)
        return OrderCanceled(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            instrument_id=report.instrument_id,
            client_order_id=report.client_order_id,
            venue_order_id=report.venue_order_id,
            account_id=report.account_id,
            event_id=UUID4(),
            ts_event=report.ts_last,
            ts_init=ts_now,
            reconciliation=True,
        )
    else:
        # Use current timestamp (continuous reconciliation)
        return OrderCanceled(
            trader_id=order.trader_id,
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id,
            account_id=order.account_id,
            event_id=UUID4(),
            ts_event=ts_now,
            ts_init=ts_now,
            reconciliation=True,
        )


def create_order_expired_event(
    order: Order,
    ts_now: int,
    report: OrderStatusReport,
) -> OrderExpired:
    """
    Create an OrderExpired event for reconciliation.

    Parameters
    ----------
    order : Order
        The order to create the expiration event for.
    ts_now : int
        The current timestamp in nanoseconds.
    report : OrderStatusReport
        The order status report from the venue.

    Returns
    -------
    OrderExpired

    """
    return OrderExpired(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=report.instrument_id,
        client_order_id=report.client_order_id,
        venue_order_id=report.venue_order_id,
        account_id=report.account_id,
        event_id=UUID4(),
        ts_event=report.ts_last,
        ts_init=ts_now,
        reconciliation=True,
    )


def create_order_accepted_event(
    trader_id: TraderId,
    order: Order,
    ts_now: int,
    report: OrderStatusReport,
) -> OrderAccepted:
    """
    Create an OrderAccepted event for reconciliation.

    Parameters
    ----------
    trader_id : TraderId
        The trader ID for the order.
    order : Order
        The order to create the acceptance event for.
    ts_now : int
        The current timestamp in nanoseconds.
    report : OrderStatusReport
        The order status report from the venue.

    Returns
    -------
    OrderAccepted

    """
    return OrderAccepted(
        trader_id=trader_id,
        strategy_id=order.strategy_id,
        instrument_id=report.instrument_id,
        client_order_id=report.client_order_id,
        venue_order_id=report.venue_order_id,
        account_id=report.account_id,
        event_id=UUID4(),
        ts_event=report.ts_accepted,
        ts_init=ts_now,
        reconciliation=True,
    )


def create_order_triggered_event(
    trader_id: TraderId,
    order: Order,
    ts_now: int,
    report: OrderStatusReport,
) -> OrderTriggered:
    """
    Create an OrderTriggered event for reconciliation.

    Parameters
    ----------
    trader_id : TraderId
        The trader ID for the order.
    order : Order
        The order to create the trigger event for.
    ts_now : int
        The current timestamp in nanoseconds.
    report : OrderStatusReport
        The order status report from the venue.

    Returns
    -------
    OrderTriggered

    """
    return OrderTriggered(
        trader_id=trader_id,
        strategy_id=order.strategy_id,
        instrument_id=report.instrument_id,
        client_order_id=report.client_order_id,
        venue_order_id=report.venue_order_id,
        account_id=report.account_id,
        event_id=UUID4(),
        ts_event=report.ts_triggered,
        ts_init=ts_now,
        reconciliation=True,
    )


def create_order_updated_event(
    trader_id: TraderId,
    order: Order,
    ts_now: int,
    report: OrderStatusReport,
) -> OrderUpdated:
    """
    Create an OrderUpdated event for reconciliation.

    Parameters
    ----------
    trader_id : TraderId
        The trader ID for the order.
    order : Order
        The order to create the update event for.
    ts_now : int
        The current timestamp in nanoseconds.
    report : OrderStatusReport
        The order status report from the venue.

    Returns
    -------
    OrderUpdated

    """
    return OrderUpdated(
        trader_id=trader_id,
        strategy_id=order.strategy_id,
        instrument_id=report.instrument_id,
        client_order_id=report.client_order_id,
        venue_order_id=report.venue_order_id,
        account_id=report.account_id,
        quantity=report.quantity,
        price=report.price,
        trigger_price=report.trigger_price,
        event_id=UUID4(),
        ts_event=report.ts_last,
        ts_init=ts_now,
        reconciliation=True,
        is_quote_quantity=order.is_quote_quantity,
    )


def create_order_filled_event(
    order: Order,
    ts_now: int,
    report: FillReport,
    instrument: Instrument,
) -> OrderFilled:
    """
    Create an OrderFilled event for reconciliation.

    Parameters
    ----------
    order : Order
        The order to create the fill event for.
    ts_now : int
        The current timestamp in nanoseconds.
    report : FillReport
        The fill report from the venue.
    instrument : Instrument
        The instrument for the order.

    Returns
    -------
    OrderFilled

    """
    info = None
    if report.avg_px is not None:
        info = {"avg_px": instrument.make_price(report.avg_px)}

    return OrderFilled(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=report.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=report.venue_order_id,
        account_id=report.account_id,
        trade_id=report.trade_id,
        position_id=report.venue_position_id,
        order_side=order.side,
        order_type=order.order_type,
        last_qty=report.last_qty,
        last_px=report.last_px,
        currency=instrument.quote_currency,
        commission=report.commission,
        liquidity_side=report.liquidity_side,
        event_id=UUID4(),
        ts_event=report.ts_event,
        ts_init=ts_now,
        reconciliation=True,
        info=info,
    )


def create_inferred_order_filled_event(
    order: Order,
    ts_now: int,
    report: OrderStatusReport,
    instrument: Instrument,
    client: ExecutionClient | None = None,
) -> OrderFilled:
    """
    Create an inferred OrderFilled event for reconciliation.

    This function is used when fill details are missing but can be inferred
    from order status reports showing filled quantities.

    Parameters
    ----------
    order : Order
        The order to create the inferred fill for.
    ts_now : int
        The current timestamp in nanoseconds.
    report : OrderStatusReport
        The order status report showing filled quantity.
    instrument : Instrument
        The instrument for the order.
    client : ExecutionClient, optional
        The execution client for venue-specific commission calculation.
        When provided, calls ``client.calculate_commission`` to obtain the
        commission. Falls back to zero when the client returns ``None`` or
        no client is provided.

    Returns
    -------
    OrderFilled

    """
    # Infer liquidity side
    liquidity_side: LiquiditySide = LiquiditySide.NO_LIQUIDITY_SIDE

    if order.order_type in (
        OrderType.MARKET,
        OrderType.STOP_MARKET,
        OrderType.TRAILING_STOP_MARKET,
    ):
        liquidity_side = LiquiditySide.TAKER
    elif report.post_only:
        liquidity_side = LiquiditySide.MAKER

    # Calculate last qty
    last_qty: Quantity = instrument.make_qty(report.filled_qty - order.filled_qty)

    # Calculate last px
    if order.avg_px is None:
        # For the first fill, use the report's average price
        if report.avg_px:
            last_px: Price = instrument.make_price(report.avg_px)
        elif report.price is not None:
            # If no avg_px but we have a price (e.g., from LIMIT order), use that
            last_px = report.price
        else:
            # Retain original fallback for now
            last_px = instrument.make_price(0.0)
    else:
        report_cost: float = float(report.avg_px or 0.0) * float(report.filled_qty)
        filled_cost = float(order.avg_px) * float(order.filled_qty)
        incremental_cost = report_cost - filled_cost

        if float(last_qty) > 0:
            last_px = instrument.make_price(incremental_cost / float(last_qty))
        else:
            last_px = instrument.make_price(report.avg_px)

    commission: Money | None = None
    if client is not None:
        commission = client.calculate_commission(instrument, last_qty, last_px, liquidity_side)
    if commission is None:
        commission = Money(0, instrument.quote_currency)

    position_id = report.venue_position_id or PositionId(f"{instrument.id}-EXTERNAL")
    pyo3_trade_id = nautilus_pyo3.create_inferred_reconciliation_trade_id(
        nautilus_pyo3.AccountId(report.account_id.value),
        nautilus_pyo3.InstrumentId.from_str(report.instrument_id.value),
        nautilus_pyo3.ClientOrderId(order.client_order_id.value),
        nautilus_pyo3.VenueOrderId(report.venue_order_id.value) if report.venue_order_id else None,
        nautilus_pyo3.OrderSide(order.side.name),
        nautilus_pyo3.OrderType(order.order_type.name),
        nautilus_pyo3.Quantity.from_str(str(report.filled_qty)),
        nautilus_pyo3.Quantity.from_str(str(last_qty)),
        nautilus_pyo3.Price.from_str(str(last_px)),
        nautilus_pyo3.PositionId(position_id.value),
        report.ts_last,
    )
    trade_id = TradeId(pyo3_trade_id.value)

    return OrderFilled(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=report.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=report.venue_order_id,
        account_id=report.account_id,
        position_id=position_id,
        trade_id=trade_id,
        order_side=order.side,
        order_type=order.order_type,
        last_qty=last_qty,
        last_px=last_px,
        currency=instrument.quote_currency,
        commission=commission,
        liquidity_side=liquidity_side,
        event_id=UUID4(),
        ts_event=report.ts_last,
        ts_init=ts_now,
        reconciliation=True,
    )


def calculate_reconciliation_price(
    current_position_qty: Decimal,
    current_position_avg_px: Decimal | None,
    target_position_qty: Decimal,
    target_position_avg_px: Decimal | None,
    instrument: Instrument,
) -> Price | None:
    """
    Calculate the price needed for a reconciliation order to achieve target position.

    This is a pure function that calculates what price a fill would need to have
    to move from the current position state to the target position state with the
    correct average price, accounting for the netting simulation logic.

    Parameters
    ----------
    current_position_qty : Decimal
        The current signed position quantity (positive for long, negative for short).
    current_position_avg_px : Decimal, optional
        The current position average price (can be None for flat position).
    target_position_qty : Decimal
        The target signed position quantity.
    target_position_avg_px : Decimal, optional
        The target position average price.
    instrument : Instrument
        The instrument for price precision.

    Returns
    -------
    Price or ``None``

    Notes
    -----
    The function handles three scenarios:
    1. Flat to position: reconciliation_px = target_avg_px
    2. Position flip (sign change): reconciliation_px = target_avg_px (due to value reset in simulation)
    3. Accumulation/reduction: weighted average formula

    """
    result = nautilus_pyo3.calculate_reconciliation_price(
        current_position_qty,
        current_position_avg_px,
        target_position_qty,
        target_position_avg_px,
    )

    if result is None:
        return None

    return instrument.make_price(result)


def adjust_fills_for_partial_window_single(
    mass_status: ExecutionMassStatus,
    instrument: Instrument,
    logger: Logger | None = None,
) -> tuple[dict[VenueOrderId, OrderStatusReport], dict[VenueOrderId, list[FillReport]]]:
    """
    Adjust fills to account for incomplete position lifecycle at window start.
    """
    return adjust_fills_for_partial_window(mass_status, [instrument], logger)[instrument.id]


def adjust_fills_for_partial_window(
    mass_status: ExecutionMassStatus,
    instruments: list[Instrument],
    logger: Logger | None = None,
) -> dict[
    InstrumentId,
    tuple[dict[VenueOrderId, OrderStatusReport], dict[VenueOrderId, list[FillReport]]],
]:
    """
    Adjust fills to account for incomplete position lifecycle at window start.

    This function analyzes fill reports from a lookback window and adjusts them
    to ensure the simulated position matches the venue's reported position, accounting
    for scenarios where:
    - The position lifecycle started before the lookback window
    - Multiple position lifecycles occurred (with zero-crossings)
    - Fill reports from old lifecycles should be excluded

    Parameters
    ----------
    mass_status : ExecutionMassStatus
        The execution mass status containing order, fill, and position reports.
    instruments : list[Instrument]
        The instruments to adjust fills for (all instruments in the mass status).
    logger : Logger, optional
        The logger for diagnostic output.

    Returns
    -------
    tuple[dict[VenueOrderId, OrderStatusReport], dict[VenueOrderId, list[FillReport]]]
        Tuple of (adjusted order reports, adjusted fill reports) matching venue position.

    """
    # Register all required commission currencies
    seen_currencies: set[Currency] = set()

    for fill_list in mass_status.fill_reports.values():
        for fill in fill_list:
            currency = fill.commission.currency
            if currency not in seen_currencies:
                register_currency(currency)

                if logger:
                    logger.debug(f"Registered currency: {currency}")
                seen_currencies.add(currency)

    pyo3_mass_status = mass_status.to_pyo3()

    pyo3_instruments = [transform_instrument_to_pyo3(instrument) for instrument in instruments]
    results: dict[
        InstrumentId,
        tuple[dict[VenueOrderId, OrderStatusReport], dict[VenueOrderId, list[FillReport]]],
    ] = {}

    for instrument, pyo3_instrument in zip(instruments, pyo3_instruments, strict=False):
        assert instrument.id.value == pyo3_instrument.id.value
        pyo3_orders, pyo3_fills = nautilus_pyo3.process_mass_status_for_reconciliation(
            pyo3_mass_status,
            pyo3_instrument,
        )
        orders: dict[VenueOrderId, OrderStatusReport] = {}

        for venue_order_id_str, pyo3_order in pyo3_orders.items():
            venue_order_id = VenueOrderId(venue_order_id_str)
            order = OrderStatusReport.from_pyo3(pyo3_order)
            orders[venue_order_id] = order

        fills: dict[VenueOrderId, list[FillReport]] = {}

        for venue_order_id_str, pyo3_reports in pyo3_fills.items():
            venue_order_id = VenueOrderId(venue_order_id_str)
            reports = []

            for pyo3_report in pyo3_reports:
                report = FillReport.from_pyo3(pyo3_report)
                reports.append(report)

            fills[venue_order_id] = reports

            if logger:
                logger.debug(
                    f"Adjusted fills for {instrument.id}: {len(orders)} orders, {len(fills)} fills",
                )

        results[instrument.id] = (orders, fills)

    return results


POSITION_REPAIR_TRIM: Final[str] = "TRIM"
POSITION_REPAIR_OPEN: Final[str] = "OPEN"

# Locally generated position IDs carry this prefix (see `PositionId.is_virtual_c`), so
# they hold no venue claim and the venue's own rows are authority over what they hold
VIRTUAL_POSITION_ID_PREFIX: Final[str] = "P-"


class PositionRepairIntent(NamedTuple):
    """
    Represents a single target-safe position repair action.

    Parameters
    ----------
    action : str
        The repair action, either ``TRIM`` (reduce a cached position toward the venue)
        or ``OPEN`` (open exposure the venue holds and the cache does not).
    order_side : OrderSide
        The side of the reconciliation order which applies the repair.
    quantity : Decimal
        The repair quantity at the instrument's declared size precision, never exceeding
        the target's observed quantity for a trim.
    target_position_id : PositionId or ``None``
        The position ID the repair is bound to. Always set for a trim; set for an open
        when the venue reports a position ID at that granularity.
    target_strategy_id : StrategyId or ``None``
        The strategy owning the target position (trims only).
    avg_px : Decimal or ``None``
        The average price to apply the repair at, when known.

    """

    action: str
    order_side: OrderSide
    quantity: Decimal
    target_position_id: PositionId | None
    target_strategy_id: StrategyId | None
    avg_px: Decimal | None


# Quantity arithmetic runs at the operand digits plus the declared size precision with
# headroom: the default 28-digit context rounds a large aggregate below one size
# increment, and makes `quantize` raise at the maximum representable quantity.
QUANTITY_CONTEXT_PRECISION: Final[int] = 60


def quantities_equal_at_size_precision(
    value1: Decimal,
    value2: Decimal,
    size_precision: int,
) -> bool:
    """
    Check whether two position quantities are equal at a declared size precision.

    A difference finer than one size increment is representational noise the venue
    cannot hold and no order could repair, so it compares equal. A difference of one
    increment or more stays a real discrepancy, which is why the quantized magnitude is
    tested against zero rather than the raw difference against a tolerance one increment
    wide.

    Parameters
    ----------
    value1 : Decimal
        The first quantity to compare.
    value2 : Decimal
        The second quantity to compare.
    size_precision : int
        The instrument's declared size precision.

    Returns
    -------
    bool

    """
    # Quantity convergence assumes the declared precision and magnitude fit a double's 15
    # significant digits, as NT positions store their quantity as a double.
    with localcontext(prec=QUANTITY_CONTEXT_PRECISION):
        increment = Decimal(1).scaleb(-size_precision)

        return abs(value1 - value2).quantize(increment, rounding=ROUND_DOWN) == 0


def _quantized_repair_quantity(value: Decimal, size_precision: int) -> Decimal:
    # A repair is placed at the instrument's declared size precision, rounding toward zero
    # so it moves the cache toward the venue's truth and never past it.
    return value.quantize(Decimal(1).scaleb(-size_precision), rounding=ROUND_DOWN)


def _repair_target_sort_key(position: Position) -> tuple[int, str]:
    return position.ts_opened, position.id.value


def _trim_intents(
    positions: list[Position],
    quantity: Decimal,
    size_precision: int,
) -> tuple[list[PositionRepairIntent], Decimal]:
    # Build trims oldest `ts_opened` first (tiebreak position ID), each capped at the
    # target's own quantity so a repair can never over-close or flip its target.
    intents: list[PositionRepairIntent] = []
    remaining = quantity

    for position in sorted(positions, key=_repair_target_sort_key):
        if remaining <= 0:
            break

        take = _quantized_repair_quantity(
            min(remaining, position.quantity.as_decimal()),
            size_precision,
        )

        if take <= 0:
            continue

        intents.append(
            PositionRepairIntent(
                action=POSITION_REPAIR_TRIM,
                order_side=(
                    OrderSide.SELL if position.side == PositionSide.LONG else OrderSide.BUY
                ),
                quantity=take,
                target_position_id=position.id,
                target_strategy_id=position.strategy_id,
                avg_px=(Decimal(str(position.avg_px_open)) if position.avg_px_open else None),
            ),
        )
        remaining -= take

    return intents, remaining


def _unambiguous_avg_px(reports: list[PositionStatusReport]) -> Decimal | None:
    # Only use an average price all candidate reports agree on, so the result does not
    # depend on the order reports were delivered in.
    values = {report.avg_px_open for report in reports if report.avg_px_open is not None}

    if len(values) != 1:
        return None

    return next(iter(values))


def _side_deficit_opens(
    reports: list[PositionStatusReport],
    targets: list[Position],
    side: PositionSide,
    deficit: Decimal,
    size_precision: int,
) -> list[PositionRepairIntent]:
    # What the side's cache exposure leaves uncovered is opened under the venue IDs which
    # reported that side, each capped at what its own row holds beyond the cache positions
    # already carrying that ID, so a repair never grows an ID past its row. A remainder no
    # reported ID can carry is left unrepaired rather than fabricated unbound.
    cached_by_id: dict[PositionId, Decimal] = {}

    for position in targets:
        cached_by_id[position.id] = (
            cached_by_id.get(position.id, Decimal(0)) + position.quantity.as_decimal()
        )

    reported_by_id: dict[PositionId, Decimal] = {}

    for report in reports:
        venue_position_id = report.venue_position_id

        if venue_position_id is None or report.position_side != side:
            continue

        reported_by_id[venue_position_id] = (
            reported_by_id.get(venue_position_id, Decimal(0)) + report.quantity.as_decimal()
        )

    intents: list[PositionRepairIntent] = []
    remaining = deficit

    for venue_position_id in sorted(reported_by_id, key=lambda pid: pid.value):
        if remaining <= 0:
            break

        uncovered = reported_by_id[venue_position_id] - cached_by_id.get(
            venue_position_id,
            Decimal(0),
        )
        take = _quantized_repair_quantity(min(remaining, uncovered), size_precision)

        if take <= 0:
            continue

        matching = [
            report
            for report in reports
            if report.venue_position_id == venue_position_id and report.position_side == side
        ]
        intents.append(
            PositionRepairIntent(
                action=POSITION_REPAIR_OPEN,
                order_side=OrderSide.BUY if side == PositionSide.LONG else OrderSide.SELL,
                quantity=take,
                target_position_id=venue_position_id,
                target_strategy_id=None,
                avg_px=_unambiguous_avg_px(matching),
            ),
        )
        remaining -= take

    return intents


def _diff_by_venue_position_id(
    reports: list[PositionStatusReport],
    positions_open: list[Position],
    size_precision: int,
) -> list[PositionRepairIntent]:
    # An ID-bearing snapshot compares per side: the venue's exposure on a side is the sum
    # of its rows there, and the cache's is the sum of its open positions on that side
    # whatever ID they are held under, virtual IDs included. A venue position ID labels the
    # venue's own rows and is never a handle on a cache position, so counting the cache by
    # label hides the exposure held elsewhere and fabricates against a side which is
    # already covered.
    trims: list[PositionRepairIntent] = []
    opens: list[PositionRepairIntent] = []

    for side in (PositionSide.LONG, PositionSide.SHORT):
        venue_qty = sum(
            (report.quantity.as_decimal() for report in reports if report.position_side == side),
            Decimal(0),
        )
        targets = [position for position in positions_open if position.side == side]
        cached_qty = sum(
            (position.quantity.as_decimal() for position in targets),
            Decimal(0),
        )

        if quantities_equal_at_size_precision(cached_qty, venue_qty, size_precision):
            continue

        if cached_qty > venue_qty:
            side_trims, _ = _trim_intents(targets, cached_qty - venue_qty, size_precision)
            trims.extend(side_trims)
        else:
            opens.extend(
                _side_deficit_opens(
                    reports,
                    targets,
                    side,
                    venue_qty - cached_qty,
                    size_precision,
                ),
            )

    return trims + opens


def _diff_by_net_quantity(
    reports: list[PositionStatusReport],
    positions_open: list[Position],
    size_precision: int,
) -> list[PositionRepairIntent]:
    venue_net = sum((report.signed_decimal_qty for report in reports), Decimal(0))
    cached_net = sum((position.signed_decimal_qty() for position in positions_open), Decimal(0))

    if quantities_equal_at_size_precision(venue_net, Decimal(0), size_precision):
        # The venue holds nothing in this scope, so every cached position is excess
        intents, _ = _trim_intents(
            positions_open,
            sum(
                (position.quantity.as_decimal() for position in positions_open),
                Decimal(0),
            ),
            size_precision,
        )
        return intents

    if quantities_equal_at_size_precision(venue_net, cached_net, size_precision):
        return []

    delta = venue_net - cached_net

    if delta < 0:
        reducible = [p for p in positions_open if p.side == PositionSide.LONG]
        open_side = OrderSide.SELL
    else:
        reducible = [p for p in positions_open if p.side == PositionSide.SHORT]
        open_side = OrderSide.BUY

    intents, remaining = _trim_intents(reducible, abs(delta), size_precision)
    open_quantity = _quantized_repair_quantity(remaining, size_precision)

    # Trims absorb the discrepancy in whole increments, so what is left over here can be
    # the sub-increment remainder of the venue quantity, which no order could carry
    if open_quantity > 0:
        intents.append(
            PositionRepairIntent(
                action=POSITION_REPAIR_OPEN,
                order_side=open_side,
                quantity=open_quantity,
                target_position_id=None,
                target_strategy_id=None,
                avg_px=_unambiguous_avg_px(reports),
            ),
        )

    return intents


def diff_position_scope(
    reports: list[PositionStatusReport],
    positions_open: list[Position],
    size_precision: int,
) -> list[PositionRepairIntent]:
    """
    Return the target-safe repairs which converge the cache onto the venue snapshot.

    Comparison granularity follows what the venue reported: reports carrying venue
    position IDs are compared per side, summing the venue's rows on a side against the
    cache's open positions on that side whatever ID they are held under; reports without
    them are compared on the scope's net quantity. Quantities compare with strict
    equality at the instrument's declared size precision, so a difference finer than one
    size increment compares equal while a difference of one increment or more is a
    discrepancy. Repair quantities are placed at the same declared precision, rounded
    toward zero. A trim binds to the cache position it reduces; an opening binds to the
    venue position ID which reported the uncovered quantity, when the venue gave one.

    Parameters
    ----------
    reports : list[PositionStatusReport]
        The venue-reported positions for one (instrument, account) scope. An empty list
        means the venue holds no position in the scope.
    positions_open : list[Position]
        The cache-open positions for the same scope.
    size_precision : int
        The instrument's declared size precision, which quantities compare at.

    Returns
    -------
    list[PositionRepairIntent]
        The repairs to apply, trims before opens. Empty when the scope has converged.

    """
    with localcontext(prec=QUANTITY_CONTEXT_PRECISION):
        if any(report.venue_position_id is not None for report in reports):
            return _diff_by_venue_position_id(reports, positions_open, size_precision)

        return _diff_by_net_quantity(reports, positions_open, size_precision)
