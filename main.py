from fastapi import FastAPI
from app.api.routes import router

#TODO: Add structlog
app = FastAPI(title="RAG Pipeline API")
app.include_router(router)