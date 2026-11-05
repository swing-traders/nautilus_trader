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

from decimal import Decimal

from nautilus_trader.accounting.exposure cimport Exposure
from nautilus_trader.core.rust.model cimport OrderSide
from nautilus_trader.core.rust.model cimport OrderType
from nautilus_trader.core.rust.model cimport PositionSide
from nautilus_trader.model.instruments.base cimport Instrument
from nautilus_trader.model.objects cimport Currency
from nautilus_trader.model.objects cimport Money
from nautilus_trader.model.objects cimport Price
from nautilus_trader.model.objects cimport Quantity


cdef class MarginModel:
    cpdef Currency margin_currency(self, Instrument instrument)
    cpdef bint reserves_untriggered(self, Instrument instrument)
    cpdef bint checks_at_execution(self, Instrument instrument, OrderType order_type, OrderSide order_side)

    cpdef Money calculate_exposure_margin(
        self,
        Instrument instrument,
        Exposure exposure,
        basis,
        leverage,
        Currency to_currency,
        xrate_buy,
        xrate_sell,
        bint use_quote_for_inverse=*,
    )

    cpdef Money calculate_margin_init(
        self,
        Instrument instrument,
        Quantity quantity,
        Price price,
        leverage,
        bint use_quote_for_inverse=*,
    )

    cpdef Money calculate_margin_maint(
        self,
        Instrument instrument,
        PositionSide side,
        Quantity quantity,
        Price price,
        leverage,
        bint use_quote_for_inverse=*,
    )


cdef class StandardMarginModel(MarginModel):
    pass


cdef class LeveragedMarginModel(MarginModel):
    pass
