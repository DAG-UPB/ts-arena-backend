# app/schemas/model_info.py
import re
from pydantic import BaseModel, ConfigDict, HttpUrl, field_validator
from typing import Optional
from datetime import datetime, date


# arXiv id formats: "2310.10688", "2310.10688v2", or the legacy "cs/0102003"
# (cf. https://info.arxiv.org/help/arxiv_identifier.html).
_ARXIV_ID_RE = re.compile(r"^(\d{4}\.\d{4,5}(v\d+)?|[a-z\-]+(\.[A-Z]{2})?/\d{7})$")


def _validate_optional_http_url(value: Optional[str]) -> Optional[str]:
    """Reject obviously-malformed URLs but allow None / empty."""
    if value is None or value == "":
        return None
    # Pydantic's HttpUrl validator handles scheme + structure; we just call it
    # explicitly so the field's storage type stays plain str (TEXT in PG).
    HttpUrl(value)  # raises on bad input
    return value


class ModelInfoBase(BaseModel):
    paper_url: Optional[str] = None
    repo_url: Optional[str] = None
    website_url: Optional[str] = None
    description: Optional[str] = None
    arxiv_id: Optional[str] = None

    @field_validator("paper_url", "repo_url", "website_url", mode="before")
    @classmethod
    def _check_url(cls, v):
        return _validate_optional_http_url(v)

    @field_validator("arxiv_id", mode="before")
    @classmethod
    def _check_arxiv_id(cls, v):
        if v is None or v == "":
            return None
        if not _ARXIV_ID_RE.match(str(v)):
            raise ValueError(
                "arxiv_id must look like '2310.10688', '2310.10688v2', or "
                "a legacy 'cs/0102003' identifier"
            )
        return v


class ModelInfoCreate(ModelInfoBase):
    name: str
    model_type: Optional[str] = None
    model_family: Optional[str] = None
    model_size: Optional[int] = None
    hosting: Optional[str] = None
    architecture: Optional[str] = None
    pretraining_data: Optional[str] = None
    publishing_date: Optional[date] = None
    parameters: Optional[dict] = None


class ModelInfoCreateInternal(ModelInfoCreate):
    organization_id: int


class ModelInfo(ModelInfoBase):
    name: str
    readable_id: Optional[str] = None
    model_type: Optional[str] = None
    model_family: Optional[str] = None
    model_size: Optional[int] = None
    hosting: Optional[str] = None
    architecture: Optional[str] = None
    pretraining_data: Optional[str] = None
    publishing_date: Optional[date] = None
    organization_id: Optional[int] = None
    parameters: Optional[dict] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)
