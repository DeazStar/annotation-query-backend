from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from app.api.deps import get_current_user, get_db_instance, get_schema_manager
from app.lib.db_resilience import QueryTimeoutError
from app.services.path_search import (
    PathRequestError,
    PathSearchResourceError,
    normalize_request,
    process_candidates,
)


router = APIRouter()
_paths_oauth = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)


def _paths_user(token: str = Depends(_paths_oauth)) -> str:
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    try:
        return get_current_user(token)
    except HTTPException as error:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized") from error


@router.post("/paths")
def find_paths(
    data: dict = Body(...),
    current_user_id: str = Depends(_paths_user),
    db_instance: Any = Depends(get_db_instance),
    schema_manager: Any = Depends(get_schema_manager),
):
    """Find bounded, deterministic paths; timeout returns 504 only (no partial paths)."""
    del current_user_id
    try:
        normalized = normalize_request(data, schema_manager)
    except PathRequestError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except PathSearchResourceError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    if not hasattr(db_instance, "find_paths"):
        raise HTTPException(status_code=500, detail="The configured backend does not support paths")
    try:
        result = db_instance.find_paths(normalized)
    except QueryTimeoutError as error:
        raise HTTPException(status_code=504, detail="Path search timed out") from error
    except PathRequestError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    try:
        response = process_candidates(result, normalized)
    except PathSearchResourceError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    requested_ids = {
        node["id"] for node in normalized["sources"] + normalized["targets"]
    }
    unresolved_ids = set(response["unresolved_ids"])
    if requested_ids and requested_ids.issubset(unresolved_ids):
        names = ", ".join(sorted(requested_ids))
        raise HTTPException(status_code=400, detail=f"Unknown node ids: {names}")
    return response
