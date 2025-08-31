# retina-py

# Env setup
conda create -n retina-py python=3.12
conda activate retina-py 

# Run fastapi locally
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Run celery worker
celery -A app.workers.ingestion_tasks.celery_app worker --loglevel=info
