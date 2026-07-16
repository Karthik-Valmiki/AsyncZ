from fastapi import FastAPI
from app.api.endpoints import jobs

app = FastAPI(
    title="AsyncZ - Job Scheduling System",
    description="A basic asynchronous job scheduling system (Phase 1 MVP)",
    version="0.1.0",
)

app.include_router(jobs.router, prefix="/jobs", tags=["jobs"])

@app.get("/")
async def root():
    return {"message": "Welcome to AsyncZ Job Scheduling API"}
