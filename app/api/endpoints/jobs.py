from fastapi import APIRouter, status
from app.schemas.job import JobCreate, JobResponse
import uuid

router = APIRouter()

# In-memory storage for jobs as requested (MVP only)
# Format: {job_id: {"job_data": JobCreate_dict, "status": "accepted"}}
jobs_store = {}

@router.post("/", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(job: JobCreate):
    """
    Submit a new job to the asynchronous job scheduling system.
    """
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    
    # Store the job in memory (placeholder for future persistent storage/queue)
    jobs_store[job_id] = {
        "job_data": job.model_dump(),
        "status": "accepted"
    }
    
    return JobResponse(
        job_id=job_id,
        status="accepted",
        message="Job successfully submitted"
    )
