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
Tests for the cache's venue position ID to position ID index.
"""

from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.position import Position
from nautilus_trader.test_kit.mocks.cache_database import MockCacheDatabase
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from nautilus_trader.test_kit.stubs.execution import TestExecStubs


AUDUSD_SIM = TestInstrumentProvider.default_fx_ccy("AUD/USD")

LEG_7 = PositionId("EURUSD.MT5-LONG-7")
LEG_8 = PositionId("EURUSD.MT5-LONG-8")
TICKET = PositionId("8133477")
SECOND_TICKET = PositionId("8133480")
LEG_8_TICKET = PositionId("8133490")


class RecordingCacheDatabase(MockCacheDatabase):
    """
    A mock cache database recording every venue position binding written through it.
    """

    def __init__(self) -> None:
        super().__init__()
        self.writes: list[tuple[PositionId, PositionId]] = []

    def index_venue_position(self, venue_position_id: PositionId, position_id: PositionId) -> None:
        self.writes.append((venue_position_id, position_id))
        super().index_venue_position(venue_position_id, position_id)


def _reload(database: MockCacheDatabase) -> Cache:
    # The execution engine's load sequence over a fresh cache
    cache = Cache(database=database)
    cache.clear_index()
    cache.cache_orders()
    cache.cache_positions()
    cache.build_index()
    return cache


class TestVenuePositionIndex:
    def setup_method(self) -> None:
        self.database = MockCacheDatabase()
        self.cache = Cache(database=self.database)
        self.cache.add_instrument(AUDUSD_SIM)

    def _add_position(self, position_id: PositionId, closed: bool) -> Position:
        open_order = TestExecStubs.market_order(
            instrument=AUDUSD_SIM,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(100_000),
            client_order_id=ClientOrderId(f"O-OPEN-{position_id}"),
        )
        self.cache.add_order(open_order, position_id)
        position = Position(
            instrument=AUDUSD_SIM,
            fill=TestEventStubs.order_filled(
                open_order,
                instrument=AUDUSD_SIM,
                position_id=position_id,
                trade_id=TradeId(f"E-OPEN-{position_id}"),
            ),
        )
        self.cache.add_position(position, OmsType.HEDGING)

        if closed:
            close_order = TestExecStubs.market_order(
                instrument=AUDUSD_SIM,
                order_side=OrderSide.SELL,
                quantity=Quantity.from_int(100_000),
                client_order_id=ClientOrderId(f"O-CLOSE-{position_id}"),
            )
            self.cache.add_order(close_order, position_id)
            position.apply(
                TestEventStubs.order_filled(
                    close_order,
                    instrument=AUDUSD_SIM,
                    position_id=position_id,
                    trade_id=TradeId(f"E-CLOSE-{position_id}"),
                ),
            )
            self.cache.update_position(position)

        return position

    def test_unbound_ids_answer_nothing(self) -> None:
        # Arrange, Act, Assert
        assert self.cache.position_id_for_venue(TICKET) is None
        assert self.cache.venue_position_ids(LEG_7) == frozenset()

    def test_binding_answers_both_directions(self) -> None:
        # Arrange, Act
        self.cache.add_venue_position_id(TICKET, LEG_7)

        # Assert
        assert self.cache.position_id_for_venue(TICKET) == LEG_7
        venue_position_ids = self.cache.venue_position_ids(LEG_7)
        assert isinstance(venue_position_ids, frozenset)
        assert venue_position_ids == frozenset({TICKET})

    def test_rebinding_the_same_pair_writes_nothing_new(self) -> None:
        # Arrange
        database = RecordingCacheDatabase()
        cache = Cache(database=database)
        cache.add_venue_position_id(TICKET, LEG_7)

        # Act
        cache.add_venue_position_id(TICKET, LEG_7)

        # Assert
        assert database.writes == [(TICKET, LEG_7)]
        assert cache.position_id_for_venue(TICKET) == LEG_7
        assert cache.venue_position_ids(LEG_7) == frozenset({TICKET})

    def test_bound_venue_id_naming_another_position_keeps_the_first_binding(self) -> None:
        # Arrange
        database = RecordingCacheDatabase()
        cache = Cache(database=database)
        cache.add_venue_position_id(TICKET, LEG_7)

        # Act
        cache.add_venue_position_id(TICKET, LEG_8)

        # Assert
        assert cache.position_id_for_venue(TICKET) == LEG_7
        assert cache.venue_position_ids(LEG_7) == frozenset({TICKET})
        assert cache.venue_position_ids(LEG_8) == frozenset()
        assert database.writes == [(TICKET, LEG_7)]

    def test_second_venue_id_for_a_bound_position_binds_it_too(self) -> None:
        # Arrange
        database = RecordingCacheDatabase()
        cache = Cache(database=database)
        cache.add_venue_position_id(TICKET, LEG_7)

        # Act
        cache.add_venue_position_id(SECOND_TICKET, LEG_7)

        # Assert
        assert cache.position_id_for_venue(TICKET) == LEG_7
        assert cache.position_id_for_venue(SECOND_TICKET) == LEG_7
        assert cache.venue_position_ids(LEG_7) == frozenset({TICKET, SECOND_TICKET})
        assert database.writes == [(TICKET, LEG_7), (SECOND_TICKET, LEG_7)]

    def test_bindings_survive_a_reload_through_the_database(self) -> None:
        # Arrange
        self._add_position(LEG_7, closed=False)
        self.cache.add_venue_position_id(TICKET, LEG_7)
        self.cache.add_venue_position_id(SECOND_TICKET, LEG_7)
        self.cache.add_venue_position_id(LEG_8_TICKET, LEG_8)

        # Act
        reloaded = _reload(self.database)

        # Assert
        assert reloaded.position_id_for_venue(TICKET) == LEG_7
        assert reloaded.position_id_for_venue(SECOND_TICKET) == LEG_7
        assert reloaded.position_id_for_venue(LEG_8_TICKET) == LEG_8
        assert reloaded.venue_position_ids(LEG_7) == frozenset({TICKET, SECOND_TICKET})
        assert reloaded.venue_position_ids(LEG_8) == frozenset({LEG_8_TICKET})

    def test_build_index_leaves_bindings_intact(self) -> None:
        # Arrange
        self._add_position(LEG_7, closed=False)
        self.cache.add_venue_position_id(TICKET, LEG_7)

        # Act
        self.cache.build_index()

        # Assert
        assert self.cache.position_id_for_venue(TICKET) == LEG_7
        assert self.cache.venue_position_ids(LEG_7) == frozenset({TICKET})

    def test_purge_position_drops_its_bindings_in_both_directions(self) -> None:
        # Arrange
        self._add_position(LEG_7, closed=True)
        self._add_position(LEG_8, closed=False)
        self.cache.add_venue_position_id(TICKET, LEG_7)
        self.cache.add_venue_position_id(SECOND_TICKET, LEG_7)
        self.cache.add_venue_position_id(LEG_8_TICKET, LEG_8)

        # Act
        self.cache.purge_position(LEG_7, purge_from_database=True)

        # Assert
        assert self.cache.position_id_for_venue(TICKET) is None
        assert self.cache.position_id_for_venue(SECOND_TICKET) is None
        assert self.cache.venue_position_ids(LEG_7) == frozenset()
        assert self.cache.position_id_for_venue(LEG_8_TICKET) == LEG_8
        assert self.cache.venue_position_ids(LEG_8) == frozenset({LEG_8_TICKET})

        reloaded = _reload(self.database)
        assert reloaded.position_id_for_venue(TICKET) is None
        assert reloaded.position_id_for_venue(SECOND_TICKET) is None
        assert reloaded.venue_position_ids(LEG_7) == frozenset()
        assert reloaded.position_id_for_venue(LEG_8_TICKET) == LEG_8
        assert reloaded.venue_position_ids(LEG_8) == frozenset({LEG_8_TICKET})

    def test_purging_an_open_position_keeps_its_bindings(self) -> None:
        # Arrange
        self._add_position(LEG_7, closed=False)
        self.cache.add_venue_position_id(TICKET, LEG_7)

        # Act
        self.cache.purge_position(LEG_7, purge_from_database=True)

        # Assert
        assert self.cache.position(LEG_7) is not None
        assert self.cache.position_id_for_venue(TICKET) == LEG_7
        assert self.cache.venue_position_ids(LEG_7) == frozenset({TICKET})
