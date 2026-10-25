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

import pickle
import struct
from collections import deque
from copy import deepcopy

import pytest

from nautilus_trader.indicators import AverageTrueRange
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.indicators import MovingAverageType
from nautilus_trader.indicators import SimpleMovingAverage
from nautilus_trader.indicators import WilderMovingAverage
from nautilus_trader.model.enums import PriceType
from nautilus_trader.test_kit.stubs.data import TestDataStubs


MA_TYPES = (SimpleMovingAverage, ExponentialMovingAverage, WilderMovingAverage)
TYPES = (*MA_TYPES, AverageTrueRange)
# Cython's generated state is used only to inspect ownership and inject refusal fixtures.
MA_FIELDS = (
    "_params", "alpha", "count", "has_inputs", "initialized", "name", "period", "price_type",
    "value",
)
FIELDS = {
    SimpleMovingAverage: ("_inputs", *MA_FIELDS[:1], *MA_FIELDS[2:]),
    ExponentialMovingAverage: MA_FIELDS,
    WilderMovingAverage: MA_FIELDS,
    AverageTrueRange: (
        "_ma", "_params", "_previous_close", "_use_previous", "_value_floor", "has_inputs",
        "initialized", "name", "period", "value",
    ),
}


def state(indicator):
    return dict(zip(FIELDS[type(indicator)], indicator.__reduce__()[2], strict=True))


def set_field(indicator, name, value):
    fields = state(indicator)
    fields[name] = value
    indicator.__setstate__(tuple(fields.values()))


def frozen(indicator):
    return pickle.dumps(indicator, protocol=5)


def identities(indicator):
    fields = state(indicator)
    result = (id(indicator), id(fields["_params"]))
    if "_inputs" in fields:
        result += (id(fields["_inputs"]),)
    if "_ma" in fields:
        result += identities(fields["_ma"])
    return result


def make(kind, period=3, **kwargs):
    if kind is AverageTrueRange:
        return kind(period, ma_type=MovingAverageType.WILDER, **kwargs)
    return kind(period, **kwargs)


def raw(indicator, value):
    if type(indicator) is AverageTrueRange:
        indicator.update_raw(value + 2, value - 1, value)
    else:
        indicator.update_raw(value)


def check_transfer(source, scratch, method, args):
    reference = deepcopy(source)
    before = frozen(source)
    source_ids = identities(source)
    scratch_ids = identities(scratch)
    assert set(source_ids).isdisjoint(scratch_ids)
    # Keep the destination dirty across repeated transfers.
    for _ in range(3):
        getattr(scratch, method)(*args)

    assert source.copy_state_to(scratch) is None

    assert frozen(source) == before
    assert frozen(scratch) == frozen(reference)
    assert identities(source) == source_ids
    assert identities(scratch) == scratch_ids
    getattr(scratch, method)(*args)
    getattr(reference, method)(*args)
    assert struct.pack("d", scratch.value) == struct.pack("d", reference.value)
    assert (scratch.has_inputs, scratch.initialized) == (
        reference.has_inputs, reference.initialized,
    )
    assert frozen(scratch) == frozen(reference)
    assert frozen(source) == before
    scratch.reset()
    assert frozen(source) == before
    assert identities(scratch) == scratch_ids


@pytest.mark.parametrize("kind", TYPES)
@pytest.mark.parametrize("period", [1, 3, 65])
def test_copy_matches_deepcopy_at_every_raw_prefix_and_reset(kind, period):
    source = make(kind, period)
    scratch = make(kind, period)
    for _ in range(2):
        for index in range(2 * period + 19):
            value = (1.0, 2.0, 3.0, 4.0, 1e16, 1.0, -1e16)[index % 7]
            args = (value + 2, value - 1, value) if kind is AverageTrueRange else (value,)
            check_transfer(source, scratch, "update_raw", args)
            scratch_before = frozen(scratch)
            source.update_raw(*args)
            assert frozen(scratch) == scratch_before
        scratch_before = frozen(scratch)
        source.reset()
        assert frozen(scratch) == scratch_before


@pytest.mark.parametrize("use_previous", [False, True])
@pytest.mark.parametrize("value_floor", [0.0, -0.0, 0.5, float("inf")])
def test_atr_previous_close_and_floor_match_deepcopy(use_previous, value_floor):
    source = make(AverageTrueRange, use_previous=use_previous, value_floor=value_floor)
    scratch = make(AverageTrueRange, use_previous=use_previous, value_floor=value_floor)
    for args in [(101.0, 99.0, 100.0), (110.0, 108.0, 109.0), (109.0, 109.0, 109.0)] * 7:
        check_transfer(source, scratch, "update_raw", args)
        source.update_raw(*args)


def test_atr_continuation_uses_copied_previous_close():
    source = make(AverageTrueRange)
    scratch = make(AverageTrueRange)
    source.update_raw(101.0, 99.0, 100.0)
    scratch.update_raw(1001.0, 999.0, 1000.0)
    reference = deepcopy(source)
    before = frozen(source)

    source.copy_state_to(scratch)
    scratch.update_raw(110.0, 108.0, 109.0)
    reference.update_raw(110.0, 108.0, 109.0)

    assert struct.pack("d", scratch.value) == struct.pack("d", reference.value)
    assert frozen(source) == before


@pytest.mark.parametrize("kind", TYPES)
def test_copy_matches_deepcopy_at_every_bar_prefix(kind):
    source = make(kind)
    scratch = make(kind)
    bar = TestDataStubs.bar_5decimal()
    for _ in range(20):
        check_transfer(source, scratch, "handle_bar", (bar,))
        source.handle_bar(bar)


@pytest.mark.parametrize("kind", MA_TYPES)
@pytest.mark.parametrize(("method", "price_type"), [
    ("handle_quote_tick", PriceType.BID),
    ("handle_quote_tick", PriceType.ASK),
    ("handle_quote_tick", PriceType.MID),
    ("handle_trade_tick", PriceType.BID),
    ("handle_trade_tick", PriceType.ASK),
    ("handle_trade_tick", PriceType.MID),
    ("handle_trade_tick", PriceType.LAST),
])
def test_copy_matches_deepcopy_at_every_tick_prefix(kind, price_type, method):
    source = make(kind, price_type=price_type)
    scratch = make(kind, price_type=price_type)
    for index in range(20):
        tick = (
            TestDataStubs.quote_tick(bid_price=1.0 + index / 100, ask_price=1.02 + index / 100)
            if method == "handle_quote_tick"
            else TestDataStubs.trade_tick()
        )
        check_transfer(source, scratch, method, (tick,))
        getattr(source, method)(tick)


@pytest.mark.parametrize("kind", TYPES)
@pytest.mark.parametrize("values", [
    [-0.0, 0.0, -0.0],
    [1e308, -1e308, 1e-308, -1e-308],
    [float("inf"), -float("inf"), float("nan"), 1.0],
])
def test_copy_preserves_binary64_extremes_and_reachable_nonfinite_state(kind, values):
    source = make(kind)
    scratch = make(kind)
    for value in values:
        args = (value, value, value) if kind is AverageTrueRange else (value,)
        check_transfer(source, scratch, "update_raw", args)
        source.update_raw(*args)


def refused(source, target, exception):
    source_before = frozen(source)
    source_ids = identities(source)
    target_before = frozen(target) if type(target) in TYPES else None
    target_ids = identities(target) if type(target) in TYPES else None
    with pytest.raises(exception):
        source.copy_state_to(target)
    assert frozen(source) == source_before
    assert identities(source) == source_ids
    if target_before is not None:
        assert frozen(target) == target_before
        assert identities(target) == target_ids


@pytest.mark.parametrize("kind", TYPES)
@pytest.mark.parametrize("target", [None, 42, "wrong"])
def test_copy_refuses_non_indicator_target(kind, target):
    refused(make(kind), target, TypeError)


@pytest.mark.parametrize(("kind", "other"), [
    (kind, other) for kind in TYPES for other in TYPES if kind is not other
])
def test_copy_refuses_different_exact_type(kind, other):
    refused(make(kind), make(other), TypeError)


@pytest.mark.parametrize("kind", TYPES)
def test_copy_refuses_self_and_period_mismatch(kind):
    source = make(kind)
    raw(source, 10)
    refused(source, source, ValueError)
    target = make(kind, 4)
    raw(target, 50)
    refused(source, target, ValueError)


@pytest.mark.parametrize("kind", MA_TYPES)
def test_copy_refuses_different_price_type(kind):
    refused(make(kind), make(kind, price_type=PriceType.MID), ValueError)


@pytest.mark.parametrize("kind", TYPES)
def test_copy_refuses_subclass_source_and_target_even_unbound(kind):
    class Derived(kind):
        pass

    source = make(kind)
    target = make(Derived) if kind is not AverageTrueRange else Derived(
        3, ma_type=MovingAverageType.WILDER,
    )
    raw(source, 10)
    target.handle_bar(TestDataStubs.bar_5decimal())
    before_source = frozen(source)
    before_target = pickle.dumps(target.__reduce__()[2], protocol=5)
    with pytest.raises(TypeError):
        kind.copy_state_to(source, target)
    with pytest.raises(NotImplementedError):
        kind.copy_state_to(target, source)
    assert frozen(source) == before_source
    assert pickle.dumps(target.__reduce__()[2], protocol=5) == before_target


@pytest.mark.parametrize("kind", TYPES)
@pytest.mark.parametrize("field", ["name", "_params"])
def test_copy_refuses_base_configuration_and_params_alias(kind, field):
    source = make(kind)
    target = make(kind)
    raw(source, 10)
    raw(target, 50)
    set_field(target, field, "different" if field == "name" else [123])
    refused(source, target, ValueError)
    target = make(kind)
    set_field(target, "_params", state(source)["_params"])
    refused(source, target, ValueError)


@pytest.mark.parametrize("kind", [ExponentialMovingAverage, WilderMovingAverage])
def test_copy_refuses_different_alpha_representation(kind):
    source = make(kind)
    target = make(kind)
    set_field(target, "alpha", -0.0)
    refused(source, target, ValueError)
    set_field(source, "alpha", 0.0)
    refused(source, target, ValueError)


def test_sma_refuses_shared_deque_and_wrong_bound():
    source = make(SimpleMovingAverage)
    raw(source, 10)
    for inputs in (state(source)["_inputs"], deque([50.0], maxlen=4)):
        target = make(SimpleMovingAverage)
        set_field(target, "_inputs", inputs)
        refused(source, target, ValueError)


@pytest.mark.parametrize(
    "option", [{"use_previous": False}, {"value_floor": 1.0}, {"value_floor": -0.0}],
)
def test_atr_refuses_different_options(option):
    refused(make(AverageTrueRange), make(AverageTrueRange, **option), ValueError)


def test_atr_refuses_child_type_mismatch_before_matching_unsupported_type():
    wilder = make(AverageTrueRange)
    simple = AverageTrueRange(3)
    refused(wilder, simple, ValueError)
    refused(simple, wilder, ValueError)
    refused(simple, AverageTrueRange(3), NotImplementedError)
    refused(wilder, state(wilder)["_ma"], TypeError)


def test_atr_refuses_matching_subclass_children():
    class DerivedWilder(WilderMovingAverage):
        pass

    source = make(AverageTrueRange)
    target = make(AverageTrueRange)
    set_field(source, "_ma", DerivedWilder(3))
    set_field(target, "_ma", DerivedWilder(3))
    before_source = state(source).copy()
    before_target = state(target).copy()
    source_child = pickle.dumps(state(source)["_ma"].__reduce__()[2], protocol=5)
    target_child = pickle.dumps(state(target)["_ma"].__reduce__()[2], protocol=5)
    with pytest.raises(NotImplementedError):
        source.copy_state_to(target)
    assert state(source) == before_source
    assert state(target) == before_target
    assert pickle.dumps(state(source)["_ma"].__reduce__()[2], protocol=5) == source_child
    assert pickle.dumps(state(target)["_ma"].__reduce__()[2], protocol=5) == target_child


@pytest.mark.parametrize(("field", "value"), [
    ("name", "different"),
    ("_params", [123]),
    ("period", 4),
    ("price_type", PriceType.MID),
    ("alpha", 0.0),
])
def test_atr_child_refusals_leave_both_graphs_unchanged(field, value):
    source = make(AverageTrueRange)
    target = make(AverageTrueRange)
    raw(source, 10)
    raw(target, 50)
    set_field(state(target)["_ma"], field, value)
    refused(source, target, ValueError)


@pytest.mark.parametrize("alias", ["child", "child_params", "cross_params", "reverse_cross_params"])
def test_atr_refuses_shared_mutable_child_graph(alias):
    source = make(AverageTrueRange)
    target = make(AverageTrueRange)
    raw(source, 10)
    raw(target, 50)
    if alias == "child":
        set_field(target, "_ma", state(source)["_ma"])
    elif alias == "child_params":
        set_field(state(target)["_ma"], "_params", state(state(source)["_ma"])["_params"])
    elif alias == "cross_params":
        set_field(state(target)["_ma"], "_params", state(source)["_params"])
    else:
        set_field(target, "_params", state(state(source)["_ma"])["_params"])
    refused(source, target, ValueError)
