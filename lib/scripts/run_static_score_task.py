import asyncio

from lib.core.container import container


async def run_static_score_task() -> int:
    logger = container.logger
    logger.info("Starting static score computation")

    container.db.init()
    try:
        async with container.uow() as uow:
            updated = await container.static_score_service.compute_all(uow)
    finally:
        await container.db.close()

    logger.info(f"Static score computation completed: {updated} datasets updated")
    return updated


if __name__ == "__main__":
    asyncio.run(run_static_score_task())
