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

@router.get("/{definition_id}/rounds")
async def list_definition_rounds(
    definition_id: int,
    status: Optional[str] = Query(
        None,
        description="Comma-separated status values to filter by (e.g., 'active,completed')"
    ),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    api_key: str = Depends(get_api_key),
    conn = Depends(get_db_connection)
):
    """
    List all rounds belonging to a specific definition with pagination.
    
    **Query Parameters:**
    - status: Comma-separated status values (e.g., 'active,completed')
    - page: Page number (default: 1)
    - page_size: Items per page (default: 20, max: 100)
    
    **Headers:**
    - X-API-Key: Valid API Key
    
    **Response:**
    ```json
    {
      "items": [...],
      "pagination": {
        "page": 1,
        "page_size": 20,
        "total_items": 100,
        "total_pages": 5,
        "has_next": true,
        "has_previous": false
      }
    }
    ```
    """
    from app.schemas.common import PaginatedResponse, PaginationMeta
    import math
    
    repo = ChallengeRepository(conn)
    # Validate definition exists first
    if not repo.get_definition(definition_id):
        raise HTTPException(status_code=404, detail="Challenge definition not found")
    
    # Parse comma-separated status values
    status_list = parse_comma_separated(status)
    
    result = repo.list_rounds(
        definition_id=definition_id,
        status=status_list,
        page=page,
        page_size=page_size
    )
    
    total_items = result['total_count']
    total_pages = math.ceil(total_items / page_size) if total_items > 0 else 0
    
    return PaginatedResponse(
        items=result['items'],
        pagination=PaginationMeta(
            page=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )
    )


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
