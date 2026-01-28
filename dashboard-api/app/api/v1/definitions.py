from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from datetime import datetime

from app.core.dependencies import get_api_key
from app.core.utils import parse_comma_separated
from app.database.connection import get_db_connection
from app.repositories.challenge_repository import ChallengeRepository
from app.schemas.challenge import ChallengeDefinitionSchema

router = APIRouter(prefix="/api/v1/definitions", tags=["Definitions"])

@router.get("", response_model=List[ChallengeDefinitionSchema])
async def list_definitions(
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    List all challenge definitions (templates).
    
    **Headers:**
    - X-API-Key: Valid API Key
    """
    repo = ChallengeRepository(conn)
    return repo.list_definitions()

@router.get("/{definition_id}", response_model=ChallengeDefinitionSchema)
async def get_definition(
    definition_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    Get details of a specific challenge definition.
    
    **Headers:**
    - X-API-Key: Valid API Key
    """
    repo = ChallengeRepository(conn)
    definition = repo.get_definition(definition_id)
    
    if not definition:
        raise HTTPException(status_code=404, detail="Challenge definition not found")
        
    return definition

@router.get("/{definition_id}/rounds", response_model=List)
async def list_definition_rounds(
    definition_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    List all rounds belonging to a specific definition.
    
    **Headers:**
    - X-API-Key: Valid API Key
    """
    # Import locally to avoid circular dependencies if we move schemas around later
    from app.schemas.challenge import ChallengeRoundSchema
    
    repo = ChallengeRepository(conn)
    # Validate definition exists first
    if not repo.get_definition(definition_id):
        raise HTTPException(status_code=404, detail="Challenge definition not found")
        
    rounds = repo.list_rounds(definition_id=definition_id)
    return rounds


@router.get("/{definition_id}/series", response_model=List)
async def list_definition_series(
    definition_id: int,
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    List all time series associated with a definition (across all its rounds).
    """
    repo = ChallengeRepository(conn)
    # Validate definition exists
    if not repo.get_definition(definition_id):
        raise HTTPException(status_code=404, detail="Challenge definition not found")
        
    series = repo.get_definition_series(definition_id)
    return series
