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

from nautilus_trader.core.rust.model cimport PositionSide
from nautilus_trader.model.instruments.base cimport Instrument
from nautilus_trader.model.objects cimport Price
from nautilus_trader.model.objects cimport Quantity
from nautilus_trader.model.orders.base cimport Order


cdef class Exposure:
    cdef readonly Quantity long_quantity
    """The summed quantity of the long positions (``None`` when none).\n\n:returns: `Quantity` or ``None``"""
    cdef readonly object long_price
    """The exact volume-weighted open price of the long positions (``None`` when none).\n\n:returns: `Decimal` or ``None``"""
    cdef readonly Quantity short_quantity
    """The summed quantity of the short positions (``None`` when none).\n\n:returns: `Quantity` or ``None``"""
    cdef readonly object short_price
    """The exact volume-weighted open price of the short positions (``None`` when none).\n\n:returns: `Decimal` or ``None``"""
    cdef readonly tuple orders
    """The open orders as `(OrderType, OrderSide, quantity, leaves, price)` tuples.\n\n:returns: `tuple`"""
    cdef readonly Price mark
    """The instrument's mark price (``None`` when unknown).\n\n:returns: `Price` or ``None``"""
    cdef readonly PositionSide net_side
    """The side of the net position.\n\n:returns: `PositionSide`"""
    cdef readonly Quantity net_quantity
    """The quantity of the net position (``None`` when flat).\n\n:returns: `Quantity` or ``None``"""
    cdef readonly Price net_price
    """The average open price of the net position (``None`` when flat).\n\n:returns: `Price` or ``None``"""


cpdef bint is_untriggered(Order order)
cpdef tuple entry_at(Order order, bint reserves_untriggered, Quantity quantity, Quantity leaves, Price price)
cpdef tuple order_entry(Order order, bint reserves_untriggered=*)
cpdef list order_entries(list orders, bint reserves_untriggered=*)
