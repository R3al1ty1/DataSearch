from typing import TypeVar, Generic
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lib.models.base import Base

ModelType = TypeVar("ModelType", bound=Base)


class BaseRepository(Generic[ModelType]):
    """Base repository with common CRUD operations (stateless)."""

    def __init__(self, model: type[ModelType]):
        self.model = model

    async def get_by_id(self, session: AsyncSession, id: UUID) -> ModelType | None:
        """Get entity by ID."""
        result = await session.execute(
            select(self.model).where(self.model.id == id)
        )
        return result.scalar_one_or_none()

    async def create(self, session: AsyncSession, entity: ModelType) -> ModelType:
        """Create new entity."""
        session.add(entity)
        await session.flush()
        await session.refresh(entity)
        return entity

    async def update(self, session: AsyncSession, entity: ModelType) -> ModelType:
        """Update existing entity."""
        await session.flush()
        await session.refresh(entity)
        return entity

    async def delete(self, session: AsyncSession, entity: ModelType) -> None:
        """Delete entity."""
        await session.delete(entity)
        await session.flush()

    async def commit(self, session: AsyncSession) -> None:
        """Commit transaction."""
        await session.commit()

    async def rollback(self, session: AsyncSession) -> None:
        """Rollback transaction."""
        await session.rollback()
