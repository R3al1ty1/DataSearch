import asyncio
import math

from celery import shared_task

from lib.core.container import container


def _compute_scores(
    rows: list[tuple],
) -> dict:
    """Computes normalized static scores from raw metric data.

    Applies log1p to each metric, normalizes via min-max per metric,
    then combines with weights: download=0.5, view=0.3, like=0.2.
    """
    if not rows:
        return {}

    metrics = {
        row[0]: {
            'download': math.log1p(row[1] or 0),
            'view': math.log1p(row[2] or 0),
            'like': math.log1p(row[3] or 0),
        }
        for row in rows
    }

    for key in ('download', 'view', 'like'):
        values = [m[key] for m in metrics.values()]
        min_val = min(values)
        max_val = max(values)
        if max_val == min_val:
            for m in metrics.values():
                m[key] = 1.0
        else:
            range_val = max_val - min_val
            for m in metrics.values():
                m[key] = (m[key] - min_val) / range_val

    return {
        dataset_id: round(
            m['download'] * 0.5 + m['view'] * 0.3 + m['like'] * 0.2, 4
        )
        for dataset_id, m in metrics.items()
    }


@shared_task(
    name="search.compute_static_scores",
    autoretry_for=(Exception,),
    max_retries=3,
    default_retry_delay=60,
)
def compute_static_scores():
    """Recomputes static_score for all active enriched datasets."""
    logger = container.logger
    logger.info("Starting static score computation")

    async def _process():
        async with container.db.get_session() as session:
            rows = await container.dataset_repo.get_metric_data_for_scoring(session)

            if not rows:
                logger.info("No datasets found for scoring")
                return 0

            scores = _compute_scores(rows)
            updated = await container.dataset_repo.batch_update_static_scores(session, scores)
            await container.dataset_repo.commit(session)
            return updated

    updated = asyncio.run(_process())
    logger.info(f"Static score computation completed: {updated} datasets updated")

    return {"updated": updated}
