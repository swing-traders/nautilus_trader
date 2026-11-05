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
The exposure a margin model is asked to value: an instrument's positions, its open orders and its
mark.
"""

from decimal import Decimal
from fractions import Fraction
from enum import Enum
from enum import unique

from nautilus_trader.core import nautilus_pyo3

from libc.stdint cimport uint8_t

from nautilus_trader.core.rust.model cimport OrderSide
from nautilus_trader.core.rust.model cimport OrderType
from nautilus_trader.core.rust.model cimport PositionSide
from nautilus_trader.model.events.position cimport PositionAdjusted
from nautilus_trader.model.instruments.base cimport Instrument
from nautilus_trader.model.objects cimport Price
from nautilus_trader.model.objects cimport Quantity
from nautilus_trader.model.orders.base cimport Order
from nautilus_trader.model.position cimport Position


@unique
class MarginBasis(Enum):
    """
    What a margin figure covers: what the venue holds (positions and orders), or what
    liquidation compares equity with (positions alone).
    """

    HELD = "HELD"
    MAINTENANCE = "MAINTENANCE"


cdef class Exposure:
    """
    An instrument's long and short positions, its open orders and its mark, as a margin model
    values them.

    Parameters
    ----------
    instrument : Instrument
        The instrument of the exposure.
    positions_open : list[Position]
        The open positions; closed ones are ignored.
    orders : list[tuple]
        The open orders as `(OrderType, OrderSide, quantity, leaves, price)` tuples.
    mark : Price, optional
        The instrument's mark price.
    absorbing : Position, optional
        An open position counted reduced by `absorbed`, as an opposite fill of that quantity
        would leave it.
    absorbed : Quantity, optional
        The quantity `absorbing` is reduced by.

    """

    def __init__(
        self,
        Instrument instrument not None,
        list positions_open not None,
        list orders not None,
        Price mark = None,
        Position absorbing = None,
        Quantity absorbed = None,
    ) -> None:
        cdef:
            Position position
            Quantity quantity
            list decorated = []
            int idx
        for idx, position in enumerate(positions_open):
            decorated.append((position.ts_opened, str(position.id), idx, position))
        decorated.sort()

        cdef list legs = []
        cdef dict quantity_raw = {PositionSide.LONG: 0, PositionSide.SHORT: 0}
        cdef dict notional = {PositionSide.LONG: Fraction(0), PositionSide.SHORT: Fraction(0)}
        for _, _, _, position in decorated:
            if not position.is_open_c():
                continue
            avg_px_open = Decimal(f"{position.avg_px_open:.{position.price_precision}f}")
            quantity = position.quantity
            signed_qty = position.signed_decimal_qty()
            if absorbing is not None and position.id == absorbing.id:
                quantity = Quantity.from_raw_c(quantity._mem.raw - absorbed._mem.raw, quantity._mem.precision)
                if quantity.is_zero():
                    continue
                if position.side == PositionSide.LONG:
                    signed_qty = quantity.as_decimal()
                else:
                    signed_qty = -quantity.as_decimal()
            legs.append((signed_qty, avg_px_open, position.ts_opened))
            quantity_raw[position.side] += quantity._mem.raw
            notional[position.side] += Fraction(quantity.as_decimal()) * _average_open_price(position)

        cdef uint8_t size_precision = instrument.size_precision
        if quantity_raw[PositionSide.LONG]:
            self.long_quantity = Quantity.from_raw_c(quantity_raw[PositionSide.LONG], size_precision)
            self.long_price = _as_decimal(notional[PositionSide.LONG] / Fraction(self.long_quantity.as_decimal()))
        if quantity_raw[PositionSide.SHORT]:
            self.short_quantity = Quantity.from_raw_c(quantity_raw[PositionSide.SHORT], size_precision)
            self.short_price = _as_decimal(notional[PositionSide.SHORT] / Fraction(self.short_quantity.as_decimal()))

        self.orders = tuple(orders)
        self.mark = mark

        # The net fold the accounts manager values a position's maintenance margin on
        net_signed_qty, net_avg_px = nautilus_pyo3.fold_net_position(legs)
        try:
            candidate = instrument.make_qty(abs(net_signed_qty))
        except ValueError:
            candidate = None
        self.net_side = PositionSide.FLAT
        if candidate is not None and not candidate.is_zero():
            self.net_quantity = candidate
            self.net_price = instrument.make_price(net_avg_px)
            if net_signed_qty > 0:
                self.net_side = PositionSide.LONG
            else:
                self.net_side = PositionSide.SHORT

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"long={self.long_quantity}@{self.long_price}, "
            f"short={self.short_quantity}@{self.short_price}, "
            f"orders={list(self.orders)}, "
            f"mark={self.mark})"
        )


cdef object _average_open_price(Position position):
    # The position's exact average open price, replayed from its fills and its recorded
    # adjustments as the position applies them: a fill from flat sets it, a fill adding to the
    # position averages in at the quantity held, and a reduction or a quantity adjustment leaves
    # it. The quantity held steps exactly as the position steps its own: a double, rounded to
    # the size precision after each fill and adjustment.
    cdef list adjustments = position.adjustments
    cdef int precision = position.size_precision
    cdef int applied = 0
    cdef int fills = 0
    cdef double held = 0.0
    average = Fraction(0)
    while applied < len(adjustments) and adjustments[applied].fills_before == fills:
        held = _adjusted(held, adjustments[applied], precision)
        applied += 1
    for fill in position.events:
        is_buy = fill.order_side == OrderSide.BUY
        if held == 0.0:
            average = Fraction(fill.last_px.as_decimal())
        elif (held > 0.0) == is_buy:
            weight = Fraction(Quantity(abs(held), precision).as_decimal())
            added = Fraction(fill.last_qty.as_decimal())
            average = (weight * average + added * Fraction(fill.last_px.as_decimal())) / (weight + added)
        if is_buy:
            held += fill.last_qty.as_double()
        else:
            held -= fill.last_qty.as_double()
        held = round(held, precision)
        if Quantity(abs(held), precision).is_zero():
            held = 0.0
        fills += 1
        while applied < len(adjustments) and adjustments[applied].fills_before == fills:
            held = _adjusted(held, adjustments[applied], precision)
            applied += 1

    if applied != len(adjustments) or Quantity(abs(held), precision) != position.quantity:
        raise ValueError(f"{position.id} quantity does not replay from its fills and adjustments")
    return average


cdef object _as_decimal(value):
    # One division, so the exact ratio is rounded once
    return Decimal(value.numerator) / Decimal(value.denominator)


cdef double _adjusted(double held, PositionAdjusted adjustment, int precision):
    if adjustment.quantity_change is None:
        return held
    held += float(adjustment.quantity_change)
    held = round(held, precision)
    if Quantity(abs(held), precision).is_zero():
        return 0.0
    return held


cpdef bint is_untriggered(Order order):
    """
    Return whether the order is a conditional order whose trigger has not been reached: one that
    declares a trigger price, and has not declared itself triggered.

    Parameters
    ----------
    order : Order
        The order to check.

    Returns
    -------
    bool

    """
    # A trailing order declares its trigger price before the trail has set one
    return hasattr(order, "trigger_price") and not getattr(order, "is_triggered", False)


cpdef tuple entry_at(
    Order order,
    bint reserves_untriggered,
    Quantity quantity,
    Quantity leaves,
    Price price,
):
    """
    Return the order as an exposure entry with the given quantity, leaves and price.

    Where the venue holds nothing for an untriggered conditional order the entry is ``None``
    until it triggers, and a triggered one with a limit price rests as a LIMIT.

    Parameters
    ----------
    order : Order
        The order for the entry.
    reserves_untriggered : bool
        If the venue holds margin for a conditional order before it triggers.
    quantity : Quantity
        The order's quantity.
    leaves : Quantity
        The order's working quantity.
    price : Price
        The order's price.

    Returns
    -------
    tuple[OrderType, OrderSide, Quantity, Quantity, Price] or ``None``

    """
    if reserves_untriggered:
        return (order.order_type, order.side, quantity, leaves, price)
    if is_untriggered(order):
        return None
    if hasattr(order, "trigger_price") and order.has_price_c():
        return (OrderType.LIMIT, order.side, quantity, leaves, price)
    return (order.order_type, order.side, quantity, leaves, price)


cpdef tuple order_entry(Order order, bint reserves_untriggered=True):
    """
    Return the order as an exposure entry, or ``None`` for an order that carries no margin: one
    not open, reduce-only, without a price or trigger, or untriggered where the venue holds
    nothing for it.

    Parameters
    ----------
    order : Order
        The order for the entry.
    reserves_untriggered : bool, default True
        If the venue holds margin for a conditional order before it triggers.

    Returns
    -------
    tuple[OrderType, OrderSide, Quantity, Quantity, Price] or ``None``

    """
    if not order.is_open_c() or order.is_reduce_only or (not order.has_price_c() and not order.has_trigger_price_c()):
        return None
    return entry_at(
        order,
        reserves_untriggered,
        order.quantity,
        order.leaves_qty,
        order.price if order.has_price_c() else order.trigger_price,
    )


cpdef list order_entries(list orders, bint reserves_untriggered=True):
    """
    Return the exposure entries of the given orders that carry margin.

    Parameters
    ----------
    orders : list[Order]
        The orders for the entries.
    reserves_untriggered : bool, default True
        If the venue holds margin for a conditional order before it triggers.

    Returns
    -------
    list[tuple]

    """
    cdef list entries = []
    cdef Order order
    cdef tuple entry
    for order in orders:
        entry = order_entry(order, reserves_untriggered)
        if entry is not None:
            entries.append(entry)
    return entries
