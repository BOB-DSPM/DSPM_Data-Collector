from fastapi import FastAPI
from routers import resources

app = FastAPI(title="AWS Resource Collector API")

# 라우터 등록
app.include_router(resources.router, prefix="/api", tags=["AWS Resources"])
