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
from nautilus_trader.core.correctness cimport Condition
from nautilus_trader.core.rust.model cimport PositionSide
from nautilus_trader.core.rust.model cimport PriceType
from nautilus_trader.model.instruments.base cimport Instrument
from nautilus_trader.model.objects cimport Currency
from nautilus_trader.model.objects cimport Money
from nautilus_trader.model.objects cimport Price
from nautilus_trader.model.objects cimport Quantity


cdef class MarginModel:
    """
    Abstract base class for margin calculation models.

    Different venues and instrument types may have varying approaches to
    calculating margin requirements. This abstraction allows for flexible
    margin calculation strategies.

    A model that declares `margin_from_exposure` is valued through `calculate_exposure_margin`
    alone, and its scalar calculations raise.
    """

    @property
    def margin_from_exposure(self):
        """
        If the model values an instrument's whole exposure through `calculate_exposure_margin`.

        Returns
        -------
        bool

        """
        return False

    @property
    def checks_free_margin(self):
        """
        If the venue refuses an order whose margin exceeds the account's free margin.

        Returns
        -------
        bool

        """
        return False

    @property
    def buy_price_type(self):
        """
        The price a buy's margin converts into the account currency at.

        Returns
        -------
        PriceType

        """
        return PriceType.BID

    @property
    def sell_price_type(self):
        """
        The price a sell's margin converts into the account currency at.

        Returns
        -------
        PriceType

        """
        return PriceType.ASK

    cpdef Currency margin_currency(self, Instrument instrument):
        """
        Return the currency the instrument's margin is stated in, or ``None`` when the instrument
        carries no margin.

        Parameters
        ----------
        instrument : Instrument
            The instrument for the margin.

        Returns
        -------
        Currency or ``None``

        """
        Condition.not_none(instrument, "instrument")

        if instrument.is_inverse:
            return instrument.get_base_currency()
        return instrument.quote_currency

    cpdef Money calculate_exposure_margin(
        self,
        Instrument instrument,
        Exposure exposure,
        basis,
        leverage: Decimal,
        Currency to_currency,
        xrate_buy: Decimal,
        xrate_sell: Decimal,
        bint use_quote_for_inverse=False,
    ):
        """
        Calculate the margin of an instrument's exposure in `to_currency`.

        Each side converts at its own rate before any comparison, and the sum is rounded once.

        Parameters
        ----------
        instrument : Instrument
            The instrument for the calculation.
        exposure : Exposure
            The positions, open orders and mark to value.
        basis : MarginBasis
            ``HELD`` for positions and orders, ``MAINTENANCE`` for positions alone.
        leverage : Decimal
            The account leverage for this instrument.
        to_currency : Currency
            The currency of the figure.
        xrate_buy : Decimal
            The rate a buy's margin converts at.
        xrate_sell : Decimal
            The rate a sell's margin converts at.
        use_quote_for_inverse : bool, default False
            If inverse instrument calculations use quote currency (instead of base).

        Returns
        -------
        Money

        """
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover

    cpdef Money calculate_margin_init(
        self,
        Instrument instrument,
        Quantity quantity,
        Price price,
        leverage: Decimal,
        bint use_quote_for_inverse=False,
    ):
        """
        Calculate the initial (order) margin requirement.

        Parameters
        ----------
        instrument : Instrument
            The instrument for the calculation.
        quantity : Quantity
            The order quantity.
        price : Price
            The order price.
        leverage : Decimal
            The account leverage for this instrument.
        use_quote_for_inverse : bool, default False
            If inverse instrument calculations use quote currency (instead of base).

        Returns
        -------
        Money
            The initial margin requirement.
        """
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover

    cpdef Money calculate_margin_maint(
        self,
        Instrument instrument,
        PositionSide side,
        Quantity quantity,
        Price price,
        leverage: Decimal,
        bint use_quote_for_inverse=False,
    ):
        """
        Calculate the maintenance (position) margin requirement.

        Parameters
        ----------
        instrument : Instrument
            The instrument for the calculation.
        side : PositionSide
            The position side.
        quantity : Quantity
            The position quantity.
        price : Price
            The current price.
        leverage : Decimal
            The account leverage for this instrument.
        use_quote_for_inverse : bool, default False
            If inverse instrument calculations use quote currency (instead of base).

        Returns
        -------
        Money
            The maintenance margin requirement.
        """
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover


cdef class StandardMarginModel(MarginModel):
    """
    Standard margin model that uses fixed percentages without leverage division.

    This model matches traditional broker behavior (e.g., Interactive Brokers)
    where margin requirements are fixed percentages of notional value regardless
    of account leverage. Leverage affects buying power but not margin requirements.

    Formula:
    - Initial Margin = notional_value * instrument.margin_init
    - Maintenance Margin = notional_value * instrument.margin_maint
    """

    cpdef Money calculate_margin_init(
        self,
        Instrument instrument,
        Quantity quantity,
        Price price,
        leverage: Decimal,
        bint use_quote_for_inverse=False,
    ):
        """
        Calculate initial margin using fixed percentage of notional value.
        """
        Condition.not_none(instrument, "instrument")
        Condition.not_none(quantity, "quantity")
        Condition.not_none(price, "price")

        notional = instrument.notional_value(
            quantity=quantity,
            price=price,
            use_quote_for_inverse=use_quote_for_inverse,
        ).as_decimal()

        margin = notional * instrument.margin_init

        if instrument.is_inverse and not use_quote_for_inverse:
            return Money(margin, instrument.get_base_currency())
        else:
            return Money(margin, instrument.quote_currency)

    cpdef Money calculate_margin_maint(
        self,
        Instrument instrument,
        PositionSide side,
        Quantity quantity,
        Price price,
        leverage: Decimal,
        bint use_quote_for_inverse=False,
    ):
        """
        Calculate maintenance margin using fixed percentage of notional value.
        """
        Condition.not_none(instrument, "instrument")
        Condition.not_none(quantity, "quantity")

        notional = instrument.notional_value(
            quantity=quantity,
            price=price,
            use_quote_for_inverse=use_quote_for_inverse,
        ).as_decimal()

        margin = notional * instrument.margin_maint

        if instrument.is_inverse and not use_quote_for_inverse:
            return Money(margin, instrument.get_base_currency())
        else:
            return Money(margin, instrument.quote_currency)


cdef class LeveragedMarginModel(MarginModel):
    """
    Leveraged margin model that divides margin requirements by leverage.

    This model represents the current Nautilus behavior and may be appropriate
    for certain crypto exchanges or specific trading scenarios where leverage
    directly reduces margin requirements.

    Formula:
    - Initial Margin = (notional_value / leverage) * instrument.margin_init
    - Maintenance Margin = (notional_value / leverage) * instrument.margin_maint
    """

    cpdef Money calculate_margin_init(
        self,
        Instrument instrument,
        Quantity quantity,
        Price price,
        leverage: Decimal,
        bint use_quote_for_inverse=False,
    ):
        """
        Calculate initial margin with leverage division.
        """
        Condition.not_none(instrument, "instrument")
        Condition.not_none(quantity, "quantity")
        Condition.not_none(price, "price")
        Condition.positive(leverage, "leverage")

        notional = instrument.notional_value(
            quantity=quantity,
            price=price,
            use_quote_for_inverse=use_quote_for_inverse,
        ).as_decimal()

        # Apply leverage division (current Nautilus behavior)
        adjusted_notional = notional / leverage
        margin = adjusted_notional * instrument.margin_init

        if instrument.is_inverse and not use_quote_for_inverse:
            return Money(margin, instrument.get_base_currency())
        else:
            return Money(margin, instrument.quote_currency)

    cpdef Money calculate_margin_maint(
        self,
        Instrument instrument,
        PositionSide side,
        Quantity quantity,
        Price price,
        leverage: Decimal,
        bint use_quote_for_inverse=False,
    ):
        """
        Calculate maintenance margin with leverage division.
        """
        Condition.not_none(instrument, "instrument")
        Condition.not_none(quantity, "quantity")
        Condition.positive(leverage, "leverage")

        notional = instrument.notional_value(
            quantity=quantity,
            price=price,
            use_quote_for_inverse=use_quote_for_inverse,
        ).as_decimal()

        # Apply leverage division (current Nautilus behavior)
        adjusted_notional = notional / leverage
        margin = adjusted_notional * instrument.margin_maint

        if instrument.is_inverse and not use_quote_for_inverse:
            return Money(margin, instrument.get_base_currency())
        else:
            return Money(margin, instrument.quote_currency)
