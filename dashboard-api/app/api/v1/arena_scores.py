from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.arena_scores import parse_calculation_date, resolve_scope, validate_metric
from app.core.dependencies import get_api_key
from app.core.tracks import validate_track
from app.database.connection import get_db_connection
from app.repositories.arena_score_repository import ArenaScoreRepository
from app.repositories.mase_history_repository import MaseHistoryRepository
from app.schemas.arena_score import (
    ArenaScoreFiltersSchema,
    ArenaScoreLeaderboardSchema,
    ModelArenaScoresSchema,
)
from app.schemas.mase_history import MaseHistorySchema

router = APIRouter(prefix="/api/v1", tags=["Models"])


@router.get("/models/arena-scores", response_model=ArenaScoreLeaderboardSchema)
def get_arena_scores(
    definition_id: Optional[int] = Query(
        None,
        description="Filter by challenge definition ID (scope_type='definition')",
        examples=[1]
    ),
    frequency_horizon: Optional[str] = Query(
        None,
        description="Filter by frequency::horizon combination (scope_type='frequency_horizon'), e.g., '00:15:00::1 day'",
        examples=["00:15:00::1 day"]
    ),
    scope_type: Optional[str] = Query(
        None,
        description=(
            "Return every scope of this type in one response: 'definition' or "
            "'frequency_horizon'. Use instead of issuing one request per scope. "
            "`limit` then applies per scope. Each row carries scope_id / definition_id "
            "so results can be grouped. Cannot be combined with definition_id or "
            "frequency_horizon."
        ),
        examples=["definition"]
    ),
    calculation_date: Optional[str] = Query(
        None,
        description=(
            "Filter by calculation date (YYYY-MM-DD). Defaults to the latest date with "
            "scores for the requested scope (for `scope_type`: for any scope of that type) "
            "and metric."
        ),
        examples=["2026-10-14"]
    ),
    metric: str = Query(
        "point",
        description="Metric: 'point' (point accuracy, default) or 'probabilistic' (quantile loss; only models that send quantiles).",
        examples=["point"]
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of models per scope",
        examples=[100]
    ),
    track: Optional[str] = Query(
        None,
        description=(
            "Keep one track only: 'reference' (models implemented in ts-arena-models) or "
            "'open' (every other model). Omit for the combined board. Scores and "
            "rank_best / rank_worst are fitted over both tracks together; the filter only "
            "drops rows."
        ),
        examples=["reference"]
    ),
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Arena Score Leaderboard with Scope-Based Filtering.

    The Arena Score rates every model from pairwise comparisons of its errors with the
    errors of the other models on the same series. A score of 0 is the naive forecast,
    which repeats the last observed value, and higher is better. A difference in points is
    100 times the log of a typical ratio of errors, not a win probability: 10 points is
    about 10 % lower error, 69 points a factor of 2.

    **Scope Types:**
    - **Global** (default): No filter parameters - ranks all models together
    - **Definition**: Filter by `definition_id` - ranks the models of one challenge definition
    - **Frequency/Horizon**: Filter by `frequency_horizon` - ranks the models of all
      definitions with that frequency and horizon

    **Important:** Only ONE scope filter can be applied at a time. Providing both
    `definition_id` and `frequency_horizon` will result in an error.

    **Frequency/Horizon Format:**
    - Format: `frequency::horizon` (e.g., `00:15:00::1 day`)
    - Frequency examples: `00:15:00` (15 min), `01:00:00` (1 hour)
    - Horizon examples: `1 day`, `3 days`

    **Metric:**
    - `point` (default): ranks on the error of the point forecast
    - `probabilistic`: ranks on the quantile loss; only models that send quantiles are listed

    **Calculation Date:**
    - If not provided: returns the latest scores for the scope and metric
    - If provided (format: YYYY-MM-DD): returns the scores for that specific date
    - Use `/models/arena-score-filters` to see the month-end dates and the latest date

    **Ordering:** rows are sorted by score, best first. Models without a score come last,
    in alphabetical order. With `scope_type`, rows are grouped by scope.

    **Response:**
    ```json
    {
      "rankings": [
        {
          "model_id": 1,
          "model_name": "ExampleModel",
          "readable_id": "example-model",
          "architecture": "decoder-only",
          "model_size": 200,
          "organization_name": "UPB",
          "track": "reference",
          "scope_type": "global",
          "scope_id": null,
          "definition_id": null,
          "calculation_date": "2026-10-14",
          "score": 1042.3,
          "ci_lower": 1031.8,
          "ci_upper": 1052.9,
          "rank": 1,
          "rank_best": 1,
          "rank_worst": 3,
          "track_rank": 1,
          "track_rank_best": 1,
          "track_rank_worst": 2,
          "provisional": false,
          "low_coverage": false,
          "unranked_disconnected": false,
          "coverage": 0.93,
          "weeks_played": 28,
          "tasks_played": 4619,
          "rounds_played": 339,
          "mase_30d": 0.84,
          "participation_30d": 0.97,
          "sql_30d": 0.71,
          "sql_participation_30d": 0.9,
          "tasks_30d": 5639,
          "provisional_weeks": 4,
          "coverage_tau": 0.5
        }
      ],
      "scope": {
        "type": "global",
        "id": null
      },
      "metric": "point",
      "track": null,
      "calculation_date": "2026-10-14"
    }
    ```

    **Response Fields:**

    *Model Identifiers:*
    - `model_id`: Model identifier
    - `model_name`: Name of the model
    - `readable_id`: Readable model identifier
    - `architecture`: Model architecture type (e.g., decoder-only, encoder-decoder, ...)
    - `model_size`: Number of parameters in the model in millions
    - `organization_name`: Organization name (if applicable)

    *Track:*
    - `track`: `reference` for models implemented in ts-arena-models and run by TS-Arena
      on exactly the registration context, `open` for every other model

    *Scope:*
    - `scope_type`: `global`, `definition` or `frequency_horizon`
    - `scope_id`: `null` for global, the definition id for `definition`,
      `frequency::horizon` for `frequency_horizon`
    - `definition_id`: Definition id (definition scopes only)
    - `calculation_date`: Date of the calculation

    *Score:*
    - `score`: Arena Score. `null` for a model that is not comparable with the rest of the
      scope (`unranked_disconnected`)
    - `ci_lower`, `ci_upper`: 95 % confidence interval of the score. The interval is
      wider for models with few weeks played. `null` if no interval could be computed
    - `provisional_weeks`: Number of weeks a model needs before it gets a rank
    - `coverage_tau`: Minimum coverage a model needs to get a rank

    *Rank:*
    - `rank`: Place by score, 1 + the number of ranked models with a higher score. Models
      with equal scores share a place. It always lies within the rank range; show it
      together with the range, since the range says how settled the place is
    - `rank_best`: 1 + the number of ranked models whose interval lies entirely above this
      model's interval (lower bound above this model's upper bound)
    - `rank_worst`: 1 + the number of ranked models whose interval is not entirely below
      this model's interval (upper bound at or above this model's lower bound)
    - Show the range as `rank_best`-`rank_worst`, or a single number when both are equal.
      It says how far up or down the data so far can place the model: with overlapping
      intervals the order of two models is not settled. Models that overlap share the
      best rank, so ranks can skip numbers.
    - `rank`, `rank_best` and `rank_worst` are `null` unless the model is ranked. A model is not
      ranked while `provisional` or `low_coverage` is set, or if it has no score
    - `track_rank`, `track_rank_best`, `track_rank_worst`: The same three numbers computed
      among the ranked models of the same track only. `null` where `rank_best` is `null`

    *Status:*
    - `provisional`: The model has fewer than `provisional_weeks` weeks played. Score and
      interval are shown, but there is no rank
    - `low_coverage`: The model has not forecast enough of the scope's tasks since it
      joined: less than `coverage_tau` of the tasks of some challenge definition, or no
      scored round of some definition yet. There is no rank. Score and interval are still
      shown. `coverage` can be above `coverage_tau` in the second case
    - `unranked_disconnected`: The model has no comparison with the main group of models
      in the scope, so it has no score and no rank
    - `coverage`: The smallest share, over the challenge definitions of the scope that have
      scored rounds since the model joined, of those rounds' tasks the model forecast (0 to 1)
    - `weeks_played`: Calendar weeks in which the model was compared with other models
    - `tasks_played`: Tasks (one series in one round) the model forecast. Counts every
      forecast, also for tasks no other model forecast
    - `rounds_played`: Rounds the model forecast

    *Last 30 Days:*
    - `mase_30d`: Geometric mean MASE over the rounds that ended in the last 30 days.
      `null` below 80 % participation
    - `participation_30d`: Share of the scope's tasks in the last 30 days the model was
      scored on (0 to 1). `null` if the model has no scored task in the window
    - `sql_30d`: Geometric mean scaled quantile loss over the last 30 days, from forecasts
      with quantiles. `null` below 80 % participation, or if the model sent no quantiles
    - `sql_participation_30d`: Share of the scope's tasks in the last 30 days the model sent
      quantiles for (0 to 1). `null` if it sent none
    - `tasks_30d`: Tasks of the scope in the last 30 days. `null` if the model has no scored
      task in the window

    **Headers:**
    - X-API-Key: Valid API key required
    """
    validate_metric(metric)
    track = validate_track(track)
    resolved_type, resolved_id = resolve_scope(definition_id, frequency_horizon, scope_type)
    calc_date = parse_calculation_date(calculation_date)

    repo = ArenaScoreRepository(conn)
    rankings, resolved_date = repo.get_leaderboard(
        scope_type=resolved_type,
        scope_id=resolved_id,
        calculation_date=calc_date,
        metric=metric,
        limit=limit,
        track=track,
    )

    return {
        "rankings": rankings,
        "scope": {
            "type": resolved_type,
            "id": resolved_id,
        },
        "metric": metric,
        "track": track,
        "calculation_date": resolved_date,
    }


@router.get("/models/arena-score-filters", response_model=ArenaScoreFiltersSchema)
def get_arena_score_filters(
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get Available Filter Options for Arena Scores.

    This endpoint returns all available filter values that can be used with
    the `/models/arena-scores` endpoint.

    **Returns:**
    ```json
    {
      "definitions": [{"id": 1, "name": "Day-Ahead Power"}, {"id": 2, "name": "Week-Ahead Power"}],
      "frequency_horizons": ["00:15:00::1 day", "01:00:00::1 day", "01:00:00::3 days"],
      "calculation_dates": [
        {"calculation_date": "2026-10-14", "is_month_end": false},
        {"calculation_date": "2026-09-30", "is_month_end": true}
      ]
    }
    ```

    **Notes:**
    - Only values with Arena Scores are returned
    - `definitions` contains the definition IDs and names for scope_type='definition'
    - `frequency_horizons` contains the frequency::horizon combinations for scope_type='frequency_horizon'
    - `calculation_dates` contains the month-end dates and the latest date with a month-end indicator, sorted by date descending

    **Headers:**
    - X-API-Key: Valid API key required
    """
    repo = ArenaScoreRepository(conn)
    return repo.get_filter_options()


@router.get("/models/{model_id}/arena-scores", response_model=ModelArenaScoresSchema)
def get_model_arena_scores(
    model_id: int,
    metric: str = Query(
        "point",
        description="Metric: 'point' (point accuracy, default) or 'probabilistic' (quantile loss).",
        examples=["point"]
    ),
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get the Arena Score history of a model across all scopes it participated in.

    For each scope (global, definition, frequency/horizon) the response lists the model's
    Arena Score on every calculation date, with:
    - the score and its confidence interval
    - the place (`rank`) and the rank range (`rank_best` to `rank_worst`)
    - the status (`provisional`, `low_coverage`, `unranked_disconnected`) and coverage

    **Response Example:**
    ```json
    {
      "model_id": 123,
      "model_name": "ExampleModel",
      "track": "reference",
      "metric": "point",
      "scopes": [
        {
          "scope_type": "definition",
          "scope_id": "1",
          "definition_id": 1,
          "definition_name": "Day-Ahead Power Forecast",
          "daily_scores": [
            {
              "calculation_date": "2026-10-13",
              "score": 1037.5,
              "ci_lower": 1027.2,
              "ci_upper": 1047.8,
              "rank": 2,
              "rank_best": 1,
              "rank_worst": 4,
              "provisional": false,
              "low_coverage": false,
              "unranked_disconnected": false,
              "coverage": 0.91,
              "weeks_played": 27
            },
            {
              "calculation_date": "2026-10-14",
              "score": 1042.1,
              "ci_lower": 1031.9,
              "ci_upper": 1052.3,
              "rank": 1,
              "rank_best": 1,
              "rank_worst": 3,
              "provisional": false,
              "low_coverage": false,
              "unranked_disconnected": false,
              "coverage": 0.91,
              "weeks_played": 28
            }
          ]
        }
      ]
    }
    ```

    **Response Fields:**
    - Per scope: `scope_type` (`global`, `definition` or `frequency_horizon`), `scope_id`
      (`null` for global, the definition id, or `frequency::horizon`), and for definition
      scopes `definition_id` and `definition_name`
    - Per day: `score`, `ci_lower`, `ci_upper`, `rank`, `rank_best`, `rank_worst`, `provisional`,
      `low_coverage`, `unranked_disconnected`, `coverage` and `weeks_played`, as described
      for `/models/arena-scores`

    **Notes:**
    - Returns 404 if the model does not exist; a model without Arena Scores returns an empty
      `scopes` list
    - Scopes are ordered global, definitions by id, then frequency/horizon combinations
    - Daily scores are sorted by date (ascending)
    - `score`, `ci_lower`, `ci_upper` are `null` on days where the model is not comparable
      with the rest of the scope; `rank`, `rank_best` and `rank_worst` are `null` on days where the
      model is not ranked

    **Headers:**
    - X-API-Key: Valid API key required
    """
    validate_metric(metric)

    repo = ArenaScoreRepository(conn)
    result = repo.get_model_history(model_id, metric=metric)

    if not result:
        raise HTTPException(status_code=404, detail="Model not found")

    return result


@router.get("/models/mase-30d-history", response_model=MaseHistorySchema)
def get_mase_30d_history(
    definition_id: Optional[int] = Query(
        None,
        description="Challenge definition ID (scope_type='definition')",
        examples=[1]
    ),
    frequency_horizon: Optional[str] = Query(
        None,
        description="frequency::horizon combination (scope_type='frequency_horizon'), e.g., '00:15:00::1 day'",
        examples=["00:15:00::1 day"]
    ),
    track: Optional[str] = Query(
        None,
        description="Keep one track only: 'reference' or 'open'. Omit for both. The field the relative MASE compares with is always all models.",
        examples=["reference"]
    ),
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Weekly 30-day MASE of every model in one scope.

    One point per Monday 00:00 UTC since 2025-01-06. Each point covers the rounds that ended in
    the 30 days before, so consecutive points share 23 days. Without a filter the scope is
    global. Only one of `definition_id` and `frequency_horizon` can be given.

    - `mase_30d`: geometric mean MASE over the model's scored tasks in the window, the value
      the leaderboard shows as "MASE (30 days)". `null` below 80 % participation.
    - `mase_rel_30d`: the model's MASE relative to the field. Per task, the field is the
      geometric mean MASE of every model scored on it. 1 is the typical model, 0.9 is 10 %
      lower error. It removes the season and the series mix, which move all models together.
    - `participation_30d`: the model's share of the scope's tasks in the window.

    Models with no value in any week are left out. Past weeks are recomputed on every refresh
    from the current scores, so they follow rescoring and series exclusions.

    **Headers:**
    - X-API-Key: Valid API key required
    """
    track = validate_track(track)
    scope_type, scope_id = resolve_scope(definition_id, frequency_horizon, None)
    history = MaseHistoryRepository(conn).get_history(scope_type, scope_id, track)
    return {"scope": {"type": scope_type, "id": scope_id}, "track": track, **history}
