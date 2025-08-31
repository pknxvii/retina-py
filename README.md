# retina-py

### env setup
conda create -n retina-py python=3.12
conda activate retina-py 

## run fastapi locally
uvicorn main:app --reload --host 0.0.0.0 --port 8000

## run celery worker
celery -A app.workers.ingestion_tasks.celery_app worker --loglevel=info
