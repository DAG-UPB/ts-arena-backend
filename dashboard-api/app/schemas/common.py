from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List, Dict, Any


class ModelRankingSchema(BaseModel):
    """Global model ranking (Legacy - kept for backward compatibility)."""
    model_config = {"protected_namespaces": ()}
    
    model_id: int
    model_name: str
    n_completed: int
    avg_mase: float


class RankingResponseSchema(BaseModel):
    """Rankings for different time ranges (Legacy - kept for backward compatibility)."""
    ranges: Dict[str, List[ModelRankingSchema]]  # key = "Last 7 days", etc.


class EnhancedModelRankingSchema(BaseModel):
    """Enhanced model ranking with filter dimensions and statistics."""
    model_config = {"protected_namespaces": ()}
    
    model_name: str = Field(..., description="Name of the model")
    challenges_participated: int = Field(..., description="Number of challenges the model participated in", ge=0)
    avg_mase: Optional[float] = Field(None, description="Average MASE score across all series")
    stddev_mase: Optional[float] = Field(None, description="Standard deviation of MASE scores")
    min_mase: Optional[float] = Field(None, description="Minimum MASE score")
    max_mase: Optional[float] = Field(None, description="Maximum MASE score")
    domains_covered: List[str] = Field(default_factory=list, description="Unique domains covered")
    categories_covered: List[str] = Field(default_factory=list, description="Unique categories covered")
    subcategories_covered: List[str] = Field(default_factory=list, description="Unique subcategories covered")
    frequencies_covered: List[str] = Field(default_factory=list, description="Unique frequencies covered (ISO 8601)")
    horizons_covered: List[str] = Field(default_factory=list, description="Unique horizons covered (ISO 8601)")


class RankingFiltersSchema(BaseModel):
    """Applied filters for ranking request."""
    time_range: Optional[str] = Field(None, description="Time range filter (e.g., '30d')")
    domain: Optional[List[str]] = Field(None, description="Domain filters applied")
    category: Optional[List[str]] = Field(None, description="Category filters applied")
    subcategory: Optional[List[str]] = Field(None, description="Subcategory filters applied")
    frequency: Optional[List[str]] = Field(None, description="Frequency filters applied (ISO 8601)")
    horizon: Optional[List[str]] = Field(None, description="Horizon filters applied (ISO 8601)")
    min_rounds: Optional[int] = Field(None, description="Minimum rounds threshold applied")
    limit: Optional[int] = Field(None, description="Result limit applied")


class EnhancedRankingResponseSchema(BaseModel):
    """Rankings response with applied filters."""
    rankings: List[EnhancedModelRankingSchema] = Field(..., description="List of model rankings")
    filters_applied: Dict[str, Any] = Field(default_factory=dict, description="Filters that were applied to this query")


class HealthSchema(BaseModel):
    """Health Check Response."""
    status: str
    timestamp: datetime
    version: str


class APIInfoSchema(BaseModel):
    """API Info."""
    title: str
    version: str
    description: str
