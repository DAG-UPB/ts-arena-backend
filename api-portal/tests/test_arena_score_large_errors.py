"""Finite input errors must not overflow while forming a graded outcome."""
import numpy as np
import pytest

from app.services import arena_score as arena


@pytest.mark.parametrize("a,b", [(1.0, 1.5), (1e308, 1.5e308),
                                 (1.5e308, 1e308), (1e308, 1e308)])
def test_pair_outcome_preserves_finite_error_ratio(a, b):
    scale = max(a, b)
    expected = (b / scale) / ((a / scale) + (b / scale))
    with np.errstate(over="raise", invalid="raise", divide="raise"):
        pairs = arena.pair_sums([1, 1], [1, 1], [1, 2], [a, b], [24, 24], [0, 0])
    assert pairs.n_pairs == 1
    assert pairs.wy / pairs.w == pytest.approx([expected])


def test_swapping_large_errors_complements_outcome():
    def outcome(a, b):
        pairs = arena.pair_sums([1, 1], [1, 1], [1, 2], [a, b], [24, 24], [0, 0])
        return (pairs.wy / pairs.w)[0]
    assert outcome(1e308, 1.5e308) + outcome(1.5e308, 1e308) == pytest.approx(1.0)


def test_large_scale_preserves_fitted_strength_gap():
    def gap(errors):
        pairs = arena.pair_sums([1, 1], [1, 1], [1, 2], errors, [24, 24], [0, 0])
        theta = arena.fit(np.array([0]), np.array([1]), pairs.w, pairs.wy, 2)
        return theta[0] - theta[1]
    assert gap([1e308, 1.5e308]) == pytest.approx(gap([1.0, 1.5]), abs=1e-5)
