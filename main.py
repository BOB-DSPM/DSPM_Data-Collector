from fastapi import FastAPI
from routers import resources, repository, explorer_router

app = FastAPI(title="AWS Resource Collector API")

app.include_router(resources.router, prefix="/api", tags=["AWS Resources"])
app.include_router(repository.router, prefix="/api", tags=["Repository Detail"])
app.include_router(explorer_router.router, prefix="/api", tags=["Repository Explorer"])

@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "message": "Service is healthy"}
