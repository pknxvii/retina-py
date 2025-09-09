from pydantic import BaseModel, Field
from typing import Optional, List, Literal


class GenerateUploadUrlRequest(BaseModel):
    doc_type: str = Field(..., description="Document type (pdf, txt, etc.)")


class IndexDocRequest(BaseModel):
    doc_id: str = Field(..., description="Document ID")
    object_path: str = Field(..., description="Object path in storage")


class QueryRequest(BaseModel):
    query: str = Field(..., description="The query to execute")
    targets: List[Literal["docstore", "sql"]] = Field(..., description="Target systems to query")


class HeaderData(BaseModel):
    user_id: Optional[str] = Field(None, description="User ID from X-User-Id header")
    organization_id: Optional[str] = Field(None, description="Organization ID from X-Organization-Id header")
