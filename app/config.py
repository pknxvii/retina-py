import os

class Config:
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
    
    # Alternative database backend for Celery results
    # DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/celery_results")