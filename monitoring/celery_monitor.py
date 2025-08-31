#!/usr/bin/env python3
"""
Production monitoring script for Celery workers
"""

from app.workers.ingestion_tasks import celery_app
import time
import logging

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_worker_health():
    """Check if workers are healthy"""
    try:
        inspect = celery_app.control.inspect()
        
        # Check if workers are alive
        ping_result = inspect.ping()
        if not ping_result:
            logger.error("No workers are responding to ping")
            return False
            
        # Check worker stats
        stats = inspect.stats()
        for worker, stat in stats.items():
            pool = stat.get('pool', {})
            logger.info(f"Worker {worker}: {pool.get('processes', 0)} processes")
            
        # Check queue lengths
        active = inspect.active()
        reserved = inspect.reserved()
        
        total_active = sum(len(tasks) for tasks in active.values())
        total_reserved = sum(len(tasks) for tasks in reserved.values())
        
        logger.info(f"Active tasks: {total_active}, Reserved tasks: {total_reserved}")
        
        # Alert if queue is backing up
        if total_active + total_reserved > 100:
            logger.warning("High queue backlog detected!")
            
        return True
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False

if __name__ == "__main__":
    while True:
        check_worker_health()
        time.sleep(60)  # Check every minute
