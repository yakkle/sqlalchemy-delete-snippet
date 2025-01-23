"""
https://docs.sqlalchemy.org/en/14/tutorial/orm_data_manipulation.html#orm-enabled-update-statements
https://github.com/mikey-no/pydantic-sqlalchemy-experiments/blob/93a7145b99a6dbef585be407d3b62e10deaa3390/main.py#L147
"""

import asyncio
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import Integer, ForeignKey, Column, String, event, select, update
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, lazyload
from sqlalchemy.orm.util import was_deleted

Base = declarative_base()


class Entry(Base):
    __tablename__ = "entry"
    entry_id = Column(Integer, primary_key=True)
    name = Column(String(50))

    def __repr__(self) -> str:
        return f"<Entry(entry_id={self.entry_id}, name={self.name})>"


class BaseEntry(BaseModel):
    entry_id: int

    class Config:
        orm_mode = True


class PydanticEntry(BaseModel):
    name: str

    class Config:
        orm_mode = True


async def update_entry(db: AsyncSession, entry_id):
    result = await db.execute(select(Entry).where(Entry.entry_id == entry_id))
    select_e1 = result.scalar_one_or_none()
    logger.debug(f"{select_e1=}")

    e1_data = BaseEntry.from_orm(select_e1)
    logger.debug(f"{e1_data=}")

    # query = db.sync_session.query(Entry)
    pydantic_entry = PydanticEntry(name="1 updateentry")
    logger.debug(f"{pydantic_entry.dict()=}")
    update_entry = e1_data.copy(update=pydantic_entry.dict())
    logger.debug(f"{update_entry=}")
    logger.debug(f"{update_entry.dict()=}")

    update_result = await db.execute(
        update(Entry)
        .where(Entry.entry_id == select_e1.entry_id)
        .values(update_entry.dict())
        .execution_options(synchronize_session="fetch")
    )
    logger.debug(f"{update_result.rowcount=}")
    # query.update(update_entry.dict())

    logger.debug(f"{select_e1=}")
    await db.refresh(select_e1)
    logger.debug(f"{select_e1=}")

    # result = await db.execute(select(Entry).where(Entry.entry_id == 1))
    # updated_e1 = result.scalar_one_or_none()
    # logger.debug(f"{updated_e1=}")


async def delete_entry(db: AsyncSession, entry_id):
    result = await db.execute(select(Entry).where(Entry.entry_id == entry_id))
    entry = result.scalar_one_or_none()
    logger.debug(f"{entry=}")

    await db.delete(entry)
    logger.debug(f"{db.deleted=}")
    logger.debug(f"{was_deleted(entry)=}")
    await db.flush()

    # result = await db.execute(select(Entry).where(Entry.entry_id == entry_id))
    # deleted_e1 = result.scalar_one_or_none()
    # logger.debug(f"{deleted_e1=}")

    logger.debug(f"{was_deleted(entry)=}")


async def async_main():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=True, future=True)

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        future=True,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )
    # Session = sessionmaker(bind=engine, class_=AsyncSession, future=True)
    db = Session()

    e1 = Entry(name="1 someentry")
    db.add(e1)
    await db.commit()

    e2 = Entry(name="2 someentry")
    # db.add_all([w1, e2])
    db.add(e2)
    await db.commit()

    await update_entry(db, e1.entry_id)
    await delete_entry(db, e2.entry_id)

    await db.close()


if __name__ == "__main__":
    asyncio.run(async_main())
