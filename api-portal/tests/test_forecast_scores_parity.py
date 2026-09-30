"""The parity report between forecast_scores and forecasts.scores counts what it claims to."""
from app.scripts.forecast_scores_parity import format_report, summarise


def _row(model_id, series_id, *, in_old=True, in_new=True, old_mase=1.5, mae=1.5, naive_mae=1.0,
         old_rmse=2.0, new_rmse=2.0, old_points=2, new_points=2, old_status="complete",
         new_status="complete", new_final=True):
    return {
        "round_id": 1, "model_id": model_id, "series_id": series_id,
        "in_old": in_old, "in_new": in_new,
        "old_mase": old_mase if in_old else None,
        "old_rmse": old_rmse if in_old else None,
        "old_points": old_points if in_old else None,
        "old_status": old_status if in_old else None,
        "mae": mae if in_new else None,
        "new_rmse": new_rmse if in_new else None,
        "naive_mae": naive_mae if in_new else None,
        "new_points": new_points if in_new else None,
        "new_status": new_status if in_new else None,
        "new_final": new_final if in_new else None,
    }


def test_summary_counts_pairs_statuses_rmse_and_relative_mae():
    report = summarise([
        _row(1, 10),                                     # identical
        _row(2, 10, mae=3.0, old_mase=2.5),              # persistence value revised on series 10
        _row(3, 11, in_new=False),                       # final only in forecasts.scores
        _row(4, 11, in_old=False),                       # only in forecast_scores
        _row(5, 12, old_status="partial", new_points=3), # late actuals
        _row(6, 12, old_mase=float("inf")),              # the arena's inf is not compared
        _row(7, 12, naive_mae=0.0, new_rmse=2.5),        # nor a zero persistence error; actual revised
        _row(8, 13, old_rmse=None, new_rmse=None),       # no score on either side
    ])

    assert report["rounds"] == 1
    assert report["pairs_both"] == 6
    assert (report["only_old"], report["only_new"], report["new_not_final"]) == (1, 1, 0)
    assert report["status_agree"] == 5
    assert report["status_pairs"] == {("partial", "complete"): 1}
    assert report["points_agree"] == 5
    assert (report["rmse_compared"], report["rmse_exact"]) == (5, 4)
    assert report["rmse_mismatch_by_series"] == {12: 1}
    assert (report["relmae_compared"], report["relmae_exact"]) == (4, 3)
    assert report["relmae_mismatch_by_series"] == {10: 1}

    text = format_report(report)
    assert "RMSE agrees:              4/5" in text
    assert "mae / naive_mae == mase:  3/4" in text
    assert "partial -> complete: 1" in text
