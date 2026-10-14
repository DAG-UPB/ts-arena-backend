from datetime import date
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from app.core.tracks import Track
from app.schemas.common import ModelFieldsSchema

ScopeType = Literal["global", "definition", "frequency_horizon"]


class ArenaScoreRowSchema(ModelFieldsSchema):
    """One model's Arena Score in one scope on one calculation date."""

    model_id: int = Field(..., description="Model identifier")
    model_name: str = Field(..., description="Name of the model")
    readable_id: Optional[str] = Field(None, description="Readable model identifier")
    architecture: Optional[str] = Field(None, description="Model architecture type")
    model_size: Optional[int] = Field(None, description="Number of parameters in millions")
    organization_name: Optional[str] = Field(None, description="Organization name (if applicable)")
    track: Track = Field(
        ...,
        description="'reference' for models implemented in ts-arena-models and run by TS-Arena, 'open' for every other model",
    )

    scope_type: ScopeType = Field(..., description="'global', 'definition' or 'frequency_horizon'")
    scope_id: Optional[str] = Field(
        None,
        description="Null for the global scope, the definition id for 'definition', 'frequency::horizon' for 'frequency_horizon'",
    )
    definition_id: Optional[int] = Field(None, description="Challenge definition id (definition scopes only)")
    calculation_date: date = Field(..., description="Date of the calculation")

    score: Optional[float] = Field(
        None,
        description="Arena Score, 0 = the naive forecast (last observed value repeated), higher is better, 1 point is about 1 % lower error. Null for a model that is not comparable with the rest of the scope",
    )
    ci_lower: Optional[float] = Field(
        None, description="Lower bound of the 95 % confidence interval of the score. Null if no interval could be computed"
    )
    ci_upper: Optional[float] = Field(
        None, description="Upper bound of the 95 % confidence interval of the score. Null if no interval could be computed"
    )
    rank: Optional[int] = Field(
        None,
        description="Place by score: 1 + the number of ranked models with a higher score; equal scores share a place. Lies within rank_best to rank_worst. Null unless ranked",
    )
    rank_best: Optional[int] = Field(
        None,
        description="Best rank the confidence interval allows: 1 + the number of ranked models clearly above (lower bound above this model's upper bound). Null unless ranked",
    )
    rank_worst: Optional[int] = Field(
        None,
        description="Worst rank the confidence interval allows: 1 + the number of ranked models not clearly below (upper bound at or above this model's lower bound). Null unless ranked",
    )
    track_rank: Optional[int] = Field(
        None, description="rank among the ranked models of the same track. Null unless ranked"
    )
    track_rank_best: Optional[int] = Field(
        None, description="rank_best among the ranked models of the same track. Null unless ranked"
    )
    track_rank_worst: Optional[int] = Field(
        None, description="rank_worst among the ranked models of the same track. Null unless ranked"
    )

    provisional: bool = Field(..., description="Fewer than provisional_weeks weeks played: score and interval are shown, no rank")
    low_coverage: bool = Field(
        ...,
        description="Not enough of the scope's tasks forecast since the model joined (less than coverage_tau for some definition, or no scored round of some definition yet): no rank",
    )
    unranked_disconnected: bool = Field(..., description="The model has no comparison with the main group of the scope: no score, no rank")
    coverage: Optional[float] = Field(
        None,
        description="Smallest share, over the scope's challenge definitions with scored rounds since the model joined, of those rounds' tasks the model forecast (0 to 1)",
    )

    weeks_played: int = Field(..., description="Calendar weeks in which the model was compared with other models")
    tasks_played: int = Field(
        ..., description="Tasks (one series in one round) the model forecast, including tasks no other model forecast"
    )
    rounds_played: int = Field(..., description="Rounds the model forecast")

    mase_30d: Optional[float] = Field(
        None, description="Geometric mean MASE over the last 30 days. Null below 80 % participation"
    )
    participation_30d: Optional[float] = Field(
        None, description="Share of the scope's tasks in the last 30 days that the model was scored on (0 to 1). Null if the model has no scored task in the window"
    )
    sql_30d: Optional[float] = Field(
        None, description="Geometric mean scaled quantile loss over the last 30 days. Null below 80 % participation or if the model sent no quantiles"
    )
    sql_participation_30d: Optional[float] = Field(
        None, description="Share of the scope's tasks in the last 30 days with a probabilistic forecast (0 to 1). Null if the model sent none"
    )
    tasks_30d: Optional[int] = Field(
        None, description="Tasks of the scope in the last 30 days. Null if the model has no scored task in the window"
    )

    provisional_weeks: Optional[int] = Field(None, description="Weeks a model needs to be ranked")
    coverage_tau: Optional[float] = Field(None, description="Minimum coverage a model needs to be ranked")


class ArenaScoreScopeSchema(BaseModel):
    type: ScopeType = Field(..., description="Scope type of the request")
    id: Optional[str] = Field(None, description="Scope id; null for the global scope and for a request across all scopes of a type")


class ArenaScoreLeaderboardSchema(BaseModel):
    """Arena Score leaderboard."""

    rankings: List[ArenaScoreRowSchema] = Field(..., description="Rows sorted by scope, then by score (best first)")
    scope: ArenaScoreScopeSchema
    metric: Literal["point", "probabilistic"]
    track: Optional[Track] = Field(None, description="Track filter of the request")
    calculation_date: Optional[date] = Field(None, description="Date of the rows; null if there are none")


class ArenaScoreDailySchema(BaseModel):
    calculation_date: date
    score: Optional[float] = None
    ci_lower: Optional[float] = None
    ci_upper: Optional[float] = None
    rank: Optional[int] = None
    rank_best: Optional[int] = None
    rank_worst: Optional[int] = None
    provisional: bool
    low_coverage: bool
    unranked_disconnected: bool
    coverage: Optional[float] = None
    weeks_played: int


class ArenaScoreScopeHistorySchema(BaseModel):
    scope_type: ScopeType
    scope_id: Optional[str] = None
    definition_id: Optional[int] = None
    definition_name: Optional[str] = None
    daily_scores: List[ArenaScoreDailySchema]


class ModelArenaScoresSchema(ModelFieldsSchema):
    """Arena Score history of one model."""

    model_id: int
    model_name: str
    track: Track
    metric: Literal["point", "probabilistic"]
    scopes: List[ArenaScoreScopeHistorySchema]


class ArenaScoreDefinitionSchema(BaseModel):
    id: int
    name: str


class ArenaScoreDateSchema(BaseModel):
    calculation_date: date
    is_month_end: bool


class ArenaScoreFiltersSchema(BaseModel):
    """Values the Arena Score endpoints accept as filters."""

    definitions: List[ArenaScoreDefinitionSchema]
    frequency_horizons: List[str]
    calculation_dates: List[ArenaScoreDateSchema]
