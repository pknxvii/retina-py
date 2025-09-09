from abc import ABC
from typing import Optional
from fastapi import Request, HTTPException
from app.api.models.requests import HeaderData


class BaseController(ABC):
    """Base controller class with common functionality."""
    
    def __init__(self):
        pass
    
    @staticmethod
    def extract_headers(request: Request) -> HeaderData:
        """Extract common headers from request."""
        headers = request.headers
        return HeaderData(
            user_id=headers.get("X-User-Id"),
            organization_id=headers.get("X-Organization-Id")
        )
    
    @staticmethod
    def require_organization_id(organization_id: Optional[str]) -> str:
        """Validate that organization_id is present."""
        if not organization_id:
            raise HTTPException(
                status_code=400,
                detail="X-Organization-Id header is required"
            )
        return organization_id
    
    @staticmethod
    def require_user_id(user_id: Optional[str]) -> str:
        """Validate that user_id is present."""
        if not user_id:
            raise HTTPException(
                status_code=400,
                detail="X-User-Id header is required"
            )
        return user_id
