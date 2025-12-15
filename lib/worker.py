"""Celery worker configuration and tasks."""
from celery import Celery

from lib.core.container import container

settings = container.settings

celery_app = Celery(
    "datasearch",
    broker=str(settings.REDIS_URL),
    backend=str(settings.REDIS_URL),
    include=["lib.crons.enrich", "lib.crons.cleanup"]
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,
    task_soft_time_limit=3000,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=50,
)
