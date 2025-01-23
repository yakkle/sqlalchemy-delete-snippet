import asyncio
from loguru import logger
from sqlalchemy import Column, Integer, String, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

Base = declarative_base()


class Did(Base):
    __tablename__ = "did"

    id = Column(Integer, primary_key=True, index=True)
    did = Column(String, index=True)
    name = Column(String(50))

    def __repr__(self) -> str:
        return f"<Did(id={self.id}, did={self.did}, name={self.name})>"


async def trans_session(db: AsyncSession):
    did1 = Did(did="sample_did1234567890", name="yakkle")
    db.add(did1)
    await db.commit()
    logger.debug(f"{db.in_transaction()=}")

    logger.debug("start transaction")
    async with db.begin_nested() as trans:
        logger.debug(f"{db.in_transaction()=}")
        did2 = Did(did="sample_did987654321", name="hooray")
        db.add(did2)
        await db.flush([did2])
        logger.debug(f"{did2=}")

        result = await db.execute(select(Did))
        dids = result.scalars().all()
        logger.debug(f"before rollback: {dids=}")

        async with db.begin_nested() as trans2:
            logger.debug(f"{db.in_transaction()=}")
            did3 = Did(did="sample_did333333", name="brrrr")
            db.add(did3)
            await db.flush()

            result = await db.execute(select(Did))
            dids = result.scalars().all()
            logger.debug(f"before rollback: {dids=}")

            # await trans2.rollback()

        did4 = Did(did="sample_did44444444", name="444444")
        db.add(did4)
        await db.flush()

        result = await db.execute(select(Did))
        dids = result.scalars().all()
        logger.debug(f"before rollback: {dids=}")

        await trans.rollback()
        # await trans.commit()

    result = await db.execute(select(Did))
    dids = result.scalars().all()
    logger.debug(f"after rollback {dids=}")


async def trans_conn(db: AsyncSession):
    conn = await db.connection()
    # trans = await conn.begin()
    logger.debug(f"{db.in_transaction()=}")

    did1 = Did(id="sample_did1234567890", name="yakkle")
    db.add(did1)
    await db.commit()

    trans = await conn.begin_nested()
    logger.debug("start transaction")
    did2 = Did(id="sample_did987654321", name="hooray")
    db.add(did2)
    await db.commit()

    result = await db.execute(select(Did))
    dids = result.scalars().all()
    logger.debug(f"before rollback: {dids=}")

    await trans.rollback()
    # await trans.commit()

    result = await db.execute(select(Did))
    dids = result.scalars().all()
    logger.debug(f"after rollback {dids=}")


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

    await trans_session(db)
    # await trans_conn(db)

    await db.close()


# def main():
#     engine = create_engine("sqlite:///:memory:", echo=True, future=True)
#     Base.metadata.create_all(engine)
#     Session = sessionmaker(bind=engine, future=True)

#     db = Session()

#     w1 = Widget(name="somewidget")
#     e1 = Entry(name="1 someentry")
#     # e2 = Entry(name="2 someentry")
#     w1.favorite_entry = e1
#     w1.entries = [e1]
#     db.add_all([w1, e1])
#     db.commit()

#     delete_entry = w1.favorite_entry
#     w1.favorite_entry = None
#     db.delete(delete_entry)
#     db.commit()

#     logger.debug(f"{w1.favorite_entry_id=}")
#     logger.debug(f"{w1.favorite_entry=}")
#     logger.debug(f"{w1.entries=}")


if __name__ == "__main__":
    # main()
    asyncio.run(async_main())
