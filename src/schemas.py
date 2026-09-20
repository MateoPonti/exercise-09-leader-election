from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class NodeCreate(BaseModel):
    name: str
    host: str
    port: int = Field(gt=0, le=65535)

class NodeUpdate(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, gt=0, le=65535)

class NodeResponse(BaseModel):
    id: int
    name: str
    host: str
    port: int
    status: str
    created_at: datetime
    updated_at: datetime
    model_config = {"from_attributes": True}

class ElectionMessage(BaseModel):
    from_id: int

class CoordinatorMessage(BaseModel):
    leader_id: int
