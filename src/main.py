from fastapi import FastAPI
from app.api.routes import router

app = FastAPI(title="RAG Pipeline API")
app.include_router(router)