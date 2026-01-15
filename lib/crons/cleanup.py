from celery import shared_task

from lib.core.container import container


@shared_task(name="cleanup.check_broken_links")
def check_broken_links(batch_size: int = 100):
    """Checks for broken dataset links (Janitor Loop)."""
    logger = container.logger
    logger.info(f"Starting broken link check: batch_size={batch_size}")

    # TODO: Implement link checking logic
    # This will be implemented when the repository and validation layers are ready

    logger.info("Broken link check completed")
    return {"checked": 0, "broken": 0}


@shared_task(name="cleanup.remove_old_cache")
def remove_old_cache(max_age_hours: int = 24):
    """Removes old cache entries from Redis."""
    logger = container.logger
    logger.info(f"Starting cache cleanup: max_age={max_age_hours}h")

    # TODO: Implement cache cleanup logic

    logger.info("Cache cleanup completed")
    return {"removed": 0}
