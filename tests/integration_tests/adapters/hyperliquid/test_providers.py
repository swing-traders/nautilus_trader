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

import json
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from nautilus_trader.adapters.hyperliquid.constants import HYPERLIQUID_VENUE
from nautilus_trader.adapters.hyperliquid.enums import HyperliquidProductType
from nautilus_trader.adapters.hyperliquid.providers import HyperliquidInstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.currencies import USDC
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class TestHyperliquidInstrumentProvider:
    def test_provider_initialization(self, mock_http_client):
        # Arrange & Act
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )

        # Assert
        assert provider is not None
        assert provider._product_types == {
            HyperliquidProductType.SPOT,
            HyperliquidProductType.PERP,
        }

    def test_provider_with_perp_only(self, mock_http_client):
        # Arrange & Act
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
            product_types=[HyperliquidProductType.PERP],
        )

        # Assert
        assert provider is not None
        assert HyperliquidProductType.PERP in provider._product_types

    def test_provider_with_spot_only(self, mock_http_client):
        # Arrange & Act
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
            product_types=[HyperliquidProductType.SPOT],
        )

        # Assert
        assert provider is not None
        assert HyperliquidProductType.SPOT in provider._product_types

    def test_provider_with_both_product_types(self, mock_http_client):
        # Arrange & Act
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
            product_types=[HyperliquidProductType.PERP, HyperliquidProductType.SPOT],
        )

        # Assert
        assert provider is not None
        assert HyperliquidProductType.PERP in provider._product_types
        assert HyperliquidProductType.SPOT in provider._product_types

    def test_provider_with_empty_product_types_raises(self, mock_http_client):
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="product_types must contain at least one entry"):
            HyperliquidInstrumentProvider(
                client=mock_http_client,
                config=InstrumentProviderConfig(),
                product_types=[],
            )

    def test_provider_without_client_raises(self):
        # Arrange & Act & Assert
        with pytest.raises(TypeError):
            HyperliquidInstrumentProvider(
                client=None,
                config=InstrumentProviderConfig(),
            )

    @pytest.mark.asyncio
    async def test_load_all_async_calls_client(self, mock_http_client):
        # Arrange
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )

        # Act
        await provider.load_all_async()

        # Assert
        mock_http_client.load_instrument_definitions.assert_called_once_with(
            include_spot=True,
            include_perps=True,
            include_perps_hip3=False,
            include_outcomes=False,
        )

    @pytest.mark.asyncio
    async def test_load_all_async_with_filters(self, mock_http_client):
        # Arrange
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )
        filters = {"symbol": "BTC"}

        # Act
        await provider.load_all_async(filters=filters)

        # Assert
        mock_http_client.load_instrument_definitions.assert_called_once_with(
            include_spot=True,
            include_perps=True,
            include_perps_hip3=False,
            include_outcomes=False,
        )

    def test_instruments_pyo3_returns_list(self, mock_http_client):
        # Arrange
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
        )

        # Act
        result = provider.instruments_pyo3()

        # Assert
        assert isinstance(result, list)

    @pytest.mark.parametrize(
        ("product_types", "expected_kwargs"),
        [
            (
                [HyperliquidProductType.OUTCOME],
                {
                    "include_spot": False,
                    "include_perps": False,
                    "include_perps_hip3": False,
                    "include_outcomes": True,
                },
            ),
            (
                [HyperliquidProductType.PERP],
                {
                    "include_spot": False,
                    "include_perps": True,
                    "include_perps_hip3": False,
                    "include_outcomes": False,
                },
            ),
            (
                [HyperliquidProductType.SPOT, HyperliquidProductType.OUTCOME],
                {
                    "include_spot": True,
                    "include_perps": False,
                    "include_perps_hip3": False,
                    "include_outcomes": True,
                },
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_load_all_async_passes_include_outcomes(
        self,
        mock_http_client,
        product_types,
        expected_kwargs,
    ):
        # Arrange
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
            product_types=product_types,
        )

        # Act
        await provider.load_all_async()

        # Assert
        mock_http_client.load_instrument_definitions.assert_called_once_with(**expected_kwargs)

    def test_instrument_product_type_recognizes_binary_option(self, mock_http_client):
        # Arrange
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
            product_types=[HyperliquidProductType.OUTCOME],
        )
        binary_option = _make_outcome_binary_option()

        # Act
        product_type = provider._instrument_product_type(binary_option)

        # Assert
        assert product_type is HyperliquidProductType.OUTCOME

    @pytest.mark.parametrize(
        ("filters", "expected"),
        [
            ({"market_types": ["outcome"]}, True),
            ({"market_types": ["spot", "perp"]}, False),
            ({"market_types": ["outcome", "spot"]}, True),
            ({}, True),
            (None, True),
        ],
    )
    def test_accept_instrument_outcome_market_type_filter(
        self,
        mock_http_client,
        filters,
        expected,
    ):
        # Arrange
        provider = HyperliquidInstrumentProvider(
            client=mock_http_client,
            config=InstrumentProviderConfig(),
            product_types=[HyperliquidProductType.OUTCOME],
        )
        binary_option = _make_outcome_binary_option()

        # Act
        accepted = provider._accept_instrument(binary_option, filters)

        # Assert
        assert accepted is expected


def _make_outcome_binary_option() -> BinaryOption:
    instrument_id = InstrumentId(Symbol("+50"), HYPERLIQUID_VENUE)
    price_increment = Price.from_str("0.0001")
    size_increment = Quantity.from_str("0.01")
    return BinaryOption(
        instrument_id=instrument_id,
        raw_symbol=Symbol("#50"),
        outcome="Yes",
        description="class:priceBinary|underlying:BTC|expiry:20260508-0600",
        asset_class=AssetClass.ALTERNATIVE,
        currency=USDC,
        price_precision=price_increment.precision,
        price_increment=price_increment,
        size_precision=size_increment.precision,
        size_increment=size_increment,
        activation_ns=0,
        expiration_ns=0,
        max_quantity=None,
        min_quantity=Quantity.from_int(1),
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        ts_event=0,
        ts_init=0,
    )


ACCOUNT_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
BTC_PERP_ID = InstrumentId.from_str("BTC-USD-PERP.HYPERLIQUID")
HIP3_PERP_ID = InstrumentId.from_str("xyz:XYZ100-USD-PERP.HYPERLIQUID")


def _make_pyo3_perp(instrument_id: str, raw_symbol: str) -> nautilus_pyo3.CryptoPerpetual:
    usdc = nautilus_pyo3.Currency.from_str("USDC")
    return nautilus_pyo3.CryptoPerpetual(
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument_id),
        raw_symbol=nautilus_pyo3.Symbol(raw_symbol),
        base_currency=nautilus_pyo3.Currency.from_str("BTC"),
        quote_currency=usdc,
        settlement_currency=usdc,
        is_inverse=False,
        price_precision=1,
        size_precision=5,
        price_increment=nautilus_pyo3.Price.from_str("0.1"),
        size_increment=nautilus_pyo3.Quantity.from_str("0.00001"),
        multiplier=None,
        lot_size=None,
        max_quantity=None,
        min_quantity=None,
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal("0.05"),
        margin_maint=Decimal("0.025"),
        maker_fee=None,
        taker_fee=None,
        ts_event=0,
        ts_init=0,
    )


def _fee_provider(mock_http_client, user_fees: dict | None, account_address: str | None):
    mock_http_client.load_instrument_definitions = AsyncMock(
        return_value=[
            _make_pyo3_perp(BTC_PERP_ID.value, "BTC"),
            _make_pyo3_perp(HIP3_PERP_ID.value, "xyz:XYZ100"),
        ],
    )
    mock_http_client.info_user_fees = AsyncMock(return_value=json.dumps(user_fees))
    return HyperliquidInstrumentProvider(
        client=mock_http_client,
        config=InstrumentProviderConfig(),
        product_types=[HyperliquidProductType.PERP, HyperliquidProductType.PERP_HIP3],
        account_address=account_address,
    )


USER_FEES = {
    "userCrossRate": "0.00045",
    "userAddRate": "0.00015",
    "activeReferralDiscount": "0.04",
}


@pytest.mark.asyncio
async def test_account_fees_stamp_canonical_perp(mock_http_client):
    # Arrange
    provider = _fee_provider(mock_http_client, USER_FEES, ACCOUNT_ADDRESS)

    # Act
    await provider.load_all_async()

    # Assert - taker 0.00045 x 0.96, maker 0.00015 x 0.96
    btc_perp = provider.find(BTC_PERP_ID)
    assert btc_perp.taker_fee == Decimal("0.000432")
    assert btc_perp.maker_fee == Decimal("0.000144")


@pytest.mark.asyncio
async def test_account_fees_negative_maker_rate_is_not_scaled(mock_http_client):
    # Arrange
    provider = _fee_provider(
        mock_http_client,
        {**USER_FEES, "userAddRate": "-0.00002"},
        ACCOUNT_ADDRESS,
    )

    # Act
    await provider.load_all_async()

    # Assert
    btc_perp = provider.find(BTC_PERP_ID)
    assert btc_perp.maker_fee == Decimal("-0.00002")
    assert btc_perp.taker_fee == Decimal("0.000432")


@pytest.mark.asyncio
async def test_account_fees_leave_hip3_perp_at_zero(mock_http_client):
    # Arrange
    provider = _fee_provider(mock_http_client, USER_FEES, ACCOUNT_ADDRESS)

    # Act
    await provider.load_all_async()

    # Assert
    hip3_perp = provider.find(HIP3_PERP_ID)
    assert hip3_perp.maker_fee == Decimal(0)
    assert hip3_perp.taker_fee == Decimal(0)


@pytest.mark.parametrize("missing", ["userCrossRate", "userAddRate", "activeReferralDiscount"])
@pytest.mark.asyncio
async def test_account_fees_missing_field_raises(mock_http_client, missing):
    # Arrange
    user_fees = {key: value for key, value in USER_FEES.items() if key != missing}
    provider = _fee_provider(mock_http_client, user_fees, ACCOUNT_ADDRESS)

    # Act, Assert
    with pytest.raises(Exception, match=missing):
        await provider.load_all_async()


@pytest.mark.asyncio
async def test_no_account_address_leaves_zero_fees(mock_http_client):
    # Arrange
    provider = _fee_provider(mock_http_client, USER_FEES, account_address=None)

    # Act
    await provider.load_all_async()

    # Assert
    btc_perp = provider.find(BTC_PERP_ID)
    assert btc_perp.maker_fee == Decimal(0)
    assert btc_perp.taker_fee == Decimal(0)
    mock_http_client.info_user_fees.assert_not_awaited()
