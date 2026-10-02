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

from nautilus_trader.cache.transformers import transform_instrument_from_pyo3
from nautilus_trader.cache.transformers import transform_instrument_to_pyo3
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.currencies import XAU
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import Cfd
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.test_kit.providers import TestInstrumentProvider


def _xauusd_cfd(**kwargs) -> Cfd:
    return Cfd(
        instrument_id=InstrumentId.from_str("XAUUSD.SIM"),
        raw_symbol=Symbol("XAUUSD"),
        asset_class=AssetClass.COMMODITY,
        base_currency=XAU,
        quote_currency=USD,
        price_precision=2,
        size_precision=2,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("0.01"),
        margin_init=Decimal("0.05"),
        margin_maint=Decimal("0.05"),
        ts_event=0,
        ts_init=0,
        info={},
        **kwargs,
    )


def test_default_multiplier_is_one():
    assert _xauusd_cfd().multiplier == Quantity.from_int(1)


def test_multiplier_scales_notional_value():
    quantity = Quantity.from_int(1)
    price = Price.from_str("2000.00")
    default = _xauusd_cfd()
    contract = _xauusd_cfd(multiplier=Quantity.from_int(100))

    assert contract.multiplier == Quantity.from_int(100)
    assert default.notional_value(quantity, price) == Money(
        quantity.as_decimal() * price.as_decimal(),
        USD,
    )
    assert contract.notional_value(quantity, price) == Money(
        quantity.as_decimal() * price.as_decimal() * 100,
        USD,
    )


def test_dict_round_trip_carries_multiplier():
    cfd = _xauusd_cfd(multiplier=Quantity.from_int(100), lot_size=Quantity.from_int(1))

    values = Cfd.to_dict(cfd)
    restored = Cfd.from_dict(values)

    assert values["multiplier"] == "100"
    assert restored.multiplier == Quantity.from_int(100)
    assert Cfd.to_dict(restored) == values


def test_provider_fixture_dict_is_upstream_plus_default_multiplier():
    assert Cfd.to_dict(TestInstrumentProvider.audusd_cfd()) == {
        "type": "Cfd",
        "id": "AUDUSD.OANDA",
        "raw_symbol": "AUD/USD",
        "asset_class": "FX",
        "quote_currency": "USD",
        "price_precision": 5,
        "price_increment": "0.00001",
        "size_precision": 0,
        "size_increment": "1",
        "multiplier": "1",
        "lot_size": "1000",
        "base_currency": "AUD",
        "max_quantity": None,
        "min_quantity": None,
        "max_notional": None,
        "min_notional": None,
        "max_price": None,
        "min_price": None,
        "margin_init": "0.03",
        "margin_maint": "0.03",
        "maker_fee": "0.00002",
        "taker_fee": "0.00002",
        "ts_event": 0,
        "ts_init": 0,
        "tick_scheme_name": None,
        "info": None,
    }


def test_catalog_round_trip_carries_multiplier(tmp_path):
    catalog = ParquetDataCatalog(tmp_path)
    cfd = _xauusd_cfd(multiplier=Quantity.from_int(100))

    catalog.write_data([cfd])
    [restored] = catalog.instruments(instrument_type=Cfd)

    assert restored.multiplier == Quantity.from_int(100)
    assert Cfd.to_dict(restored) == Cfd.to_dict(cfd)


def test_pyo3_round_trip_carries_multiplier():
    cfd = _xauusd_cfd(multiplier=Quantity.from_int(100))

    pyo3_cfd = transform_instrument_to_pyo3(cfd)
    restored = transform_instrument_from_pyo3(pyo3_cfd)

    assert pyo3_cfd.multiplier.as_decimal() == Decimal(100)
    assert restored.multiplier == Quantity.from_int(100)
    assert Cfd.to_dict(restored) == Cfd.to_dict(cfd)


def test_pyo3_round_trip_keeps_default_multiplier():
    cfd = _xauusd_cfd()

    restored = transform_instrument_from_pyo3(transform_instrument_to_pyo3(cfd))

    assert restored.multiplier == Quantity.from_int(1)
    assert Cfd.to_dict(restored) == Cfd.to_dict(cfd)
