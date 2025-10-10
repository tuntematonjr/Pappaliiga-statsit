import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_utils import weighted_median, weighted_percentile


def test_weighted_percentile_handles_unsorted_inputs():
    values = [3, 1, 2]
    weights = [1, 1, 1]

    p50 = weighted_percentile(values, weights, 50)

    assert p50 == 2


def test_weighted_percentile_prefers_heavier_weights():
    values = [10, 20, 30]
    weights = [1, 1, 8]

    p90 = weighted_percentile(values, weights, 90)

    assert p90 == 30


def test_weighted_percentile_returns_last_value_when_threshold_never_met():
    values = [5, 15]
    weights = [0.1, 0.1]

    result = weighted_percentile(values, weights, 100)

    assert result == 15


def test_weighted_percentile_falls_back_to_median_on_zero_weight_total():
    values = [10, 20, 30]
    weights = [0, 0, 0]

    result = weighted_percentile(values, weights, 75)

    assert result == 20


def test_weighted_median_delegates_to_percentile():
    values = [2, 8, 4]
    weights = [1, 2, 1]

    result = weighted_median(values, weights)

    assert math.isclose(result, weighted_percentile(values, weights, 50))


def test_weighted_percentile_handles_empty_values():
    assert weighted_percentile([], [], 50) == 0.0


def test_weighted_percentile_rejects_length_mismatches():
    with pytest.raises(ValueError):
        weighted_percentile([1, 2], [1], 50)


@pytest.mark.parametrize("invalid_p", [-1, 101])
def test_weighted_percentile_rejects_out_of_range_percentiles(invalid_p):
    with pytest.raises(ValueError):
        weighted_percentile([1], [1], invalid_p)


def test_weighted_percentile_rejects_negative_weights():
    with pytest.raises(ValueError):
        weighted_percentile([1, 2], [1, -1], 50)
