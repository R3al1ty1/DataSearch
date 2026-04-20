import logging
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from lib.services.datasets.models import EnrichmentResult, EnrichmentStage
from lib.services.datasets.repository import DatasetRepository, EnrichmentLogRepository
from lib.services.datasets.validation.link_checker import LinkCheckResult, LinkCheckerService


@dataclass
class CleanupBatchResult:
    checked: int
    deactivated: int
    errors: int


class CleanupService:
    def __init__(
        self,
        dataset_repo: DatasetRepository,
        enrichment_log_repo: EnrichmentLogRepository,
        link_checker: LinkCheckerService,
        logger: logging.Logger,
    ):
        self._dataset_repo = dataset_repo
        self._enrichment_log_repo = enrichment_log_repo
        self._link_checker = link_checker
        self._logger = logger

    async def run_cleanup_batch(
        self,
        session: AsyncSession,
        batch_size: int = 200,
        stale_after_hours: int = 48,
    ) -> CleanupBatchResult:
        self._logger.info(f"Starting dataset cleanup: batch_size={batch_size}, stale_after_hours={stale_after_hours}")

        datasets = await self._dataset_repo.get_stale_for_validation(session, batch_size, stale_after_hours)
        if not datasets:
            self._logger.info("Cleanup completed: no stale datasets found")
            return CleanupBatchResult(checked=0, deactivated=0, errors=0)

        results = await self._link_checker.check_batch([(d.id, d.url) for d in datasets])

        _, deactivated = await self._dataset_repo.bulk_update_check_results(session, results)

        errors = 0
        for result in results:
            if not result.is_reachable:
                self._logger.warning(
                    f"Dataset {result.dataset_id} deactivated: url={result.url}, reason={result.error_type}"
                )
                if result.error_type is not None:
                    errors += 1

            await self._enrichment_log_repo.log_enrichment(
                session=session,
                dataset_id=result.dataset_id,
                stage=EnrichmentStage.LINK_VALIDATION,
                result=EnrichmentResult.SUCCESS if result.is_reachable else EnrichmentResult.FAILED,
                attempt_number=1,
                duration_ms=result.duration_ms,
                error_message=result.error_type,
            )

        await session.commit()
        self._logger.info(f"Cleanup completed: checked={len(results)}, deactivated={deactivated}, errors={errors}")
        return CleanupBatchResult(checked=len(results), deactivated=deactivated, errors=errors)

    async def deactivate_dataset(
        self,
        session: AsyncSession,
        dataset_id: UUID,
        check_result: LinkCheckResult,
    ) -> None:
        await self._dataset_repo.bulk_update_check_results(session, [check_result])
        await self._enrichment_log_repo.log_enrichment(
            session=session,
            dataset_id=dataset_id,
            stage=EnrichmentStage.LINK_VALIDATION,
            result=EnrichmentResult.FAILED,
            attempt_number=1,
            duration_ms=check_result.duration_ms,
            error_message=check_result.error_type,
        )
        await session.commit()
        self._logger.warning(
            f"Dataset {dataset_id} deactivated: url={check_result.url}, reason={check_result.error_type}"
        )
