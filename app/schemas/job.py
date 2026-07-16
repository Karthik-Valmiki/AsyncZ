from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class JobCreate(BaseModel):
    name: str = Field(..., description="Name of the job to execute")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Job payload or arguments")

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str
