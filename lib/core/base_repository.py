from typing import Generic, TypeVar
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lib.core.base_model import Base

ModelType = TypeVar("ModelType", bound=Base)

class BaseRepository(Generic[ModelType]):
    """Base repository with common CRUD operations (stateless)."""

    def __init__(self, model: type[ModelType], session: AsyncSession):
        self.model = model
        self.session = session

    async def get_by_id(self, id: UUID) -> ModelType | None:
        """Get entity by ID."""
        result = await self.session.execute(
            select(self.model).where(self.model.id == id)
        )
        return result.scalar_one_or_none()

    async def create(self, entity: ModelType) -> ModelType:
        """Create new entity."""
        self.session.add(entity)
        await self.session.flush()
        await self.session.refresh(entity)
        return entity

    async def update(self, entity: ModelType) -> ModelType:
        """Update existing entity."""
        await self.session.flush()
        await self.session.refresh(entity)
        return entity

    async def delete(self, entity: ModelType) -> None:
        """Delete entity."""
        await self.session.delete(entity)
        await self.session.flush()
