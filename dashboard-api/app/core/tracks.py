"""Reference Track / Open Track membership.

A model is in the Reference Track when it is owned by the admin user: only models
implemented in ts-arena-models are registered under that account, and they run on our
own infrastructure on exactly the registration context. Every other model is in the Open
Track. There is no stored column; the track is derived from ``model_info.user_id`` so it
can never drift from ownership.
"""
from typing import Literal, Optional

from fastapi import HTTPException

Track = Literal["reference", "open"]

REFERENCE_TRACK = "reference"
OPEN_TRACK = "open"
TRACKS = (REFERENCE_TRACK, OPEN_TRACK)

# auth.users.id of the admin account that owns every ts-arena-models model.
REFERENCE_OWNER_USER_ID = 1


def track_sql(model_info_alias: str) -> str:
    """SQL expression yielding the track of the ``models.model_info`` row under the alias."""
    return (
        f"(CASE WHEN {model_info_alias}.user_id = {REFERENCE_OWNER_USER_ID} "
        f"THEN '{REFERENCE_TRACK}' ELSE '{OPEN_TRACK}' END)"
    )


def track_for_user_id(user_id: Optional[int]) -> str:
    return REFERENCE_TRACK if user_id == REFERENCE_OWNER_USER_ID else OPEN_TRACK


def validate_track(track: Optional[str]) -> Optional[str]:
    """Reject an unknown track filter with a 400; None means both tracks."""
    if track is not None and track not in TRACKS:
        raise HTTPException(
            status_code=400,
            detail="Invalid track. Use 'reference' or 'open', or omit it for both tracks.",
        )
    return track
