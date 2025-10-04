from fastapi import FastAPI
from routers import resources, repository

app = FastAPI(title="AWS Resource Collector API")

# 라우터 등록
app.include_router(resources.router, prefix="/api", tags=["AWS Resources"])
app.include_router(repository.router, prefix="/api", tags=["AWS Resources detail"])
