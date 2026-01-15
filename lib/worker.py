from celery import Celery

from lib.core.container import container

settings = container.settings

celery_app = Celery(
    "datasearch",
    broker=str(settings.REDIS_URL),
    backend=str(settings.REDIS_URL),
    include=[
        "lib.crons.enrich",
        "lib.crons.cleanup",
        "lib.crons.enrichment.hf",
        "lib.crons.enrichment.kaggle",
    ]
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

celery_app.conf.beat_schedule = {
    'generate-embeddings-every-30min': {
        'task': 'enrich.generate_embeddings',
        'schedule': 1800.0,
        'args': (100,)
    },
    'fetch-hf-datasets-daily': {
        'task': 'hf.fetch_datasets',
        'schedule': 86400.0,
        'args': (1000, 1)
    },
    'enrich-kaggle-datasets-hourly': {
        'task': 'kaggle.enrich_pending',
        'schedule': 3600.0,
        'args': (50,)
    },
    'fetch-kaggle-latest-daily': {
        'task': 'kaggle.fetch_latest',
        'schedule': 86400.0,
        'args': (100, 'updated')
    },
}
