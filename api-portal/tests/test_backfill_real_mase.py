"""The backfill's own bookkeeping: its drift checks must catch what they claim to."""
import argparse

from app.scripts.backfill_real_mase import Summary, _parse_args


def _row(model_id, series_id, mase, sql, scale=2.0, has_quantiles=False, status="complete"):
    return {
        "round_id": 1, "model_id": model_id, "series_id": series_id, "mase": mase, "sql_score": sql,
        "scale": scale, "has_quantiles": has_quantiles, "evaluation_status": status,
    }


def test_identity_check_flags_point_only_drift_only():
    summary = Summary()
    summary.record([
        _row(1, 10, 0.5, 0.5),
        _row(2, 10, 0.5, 0.6),                        # point-only, drifted
        _row(3, 10, 0.5, 0.9, has_quantiles=True),    # real quantiles: no identity to hold
        _row(4, 10, None, None, status="undefined_scale"),
    ])
    assert summary.identity_checked == 2
    assert summary.identity_mismatches == [(1, 2, 10, 0.5, 0.6)]
    assert summary.status == {"complete": 3, "undefined_scale": 1}


def test_scales_are_counted_per_series():
    summary = Summary()
    summary.record([_row(1, 10, 0.5, 0.5), _row(2, 10, 0.4, 0.4), _row(1, 11, None, None, scale=0.0)])
    assert summary.scale_series == 2
    assert summary.scale_undefined == 1


def test_arguments():
    args = _parse_args(["--dry-run", "--sample", "5", "--check-served"])
    assert isinstance(args, argparse.Namespace)
    assert args.dry_run and args.sample == 5 and args.check_served and not args.refresh
