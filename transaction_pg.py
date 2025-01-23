import asyncio
from loguru import logger

import sqlalchemy
from sqlalchemy import Column, Integer, String, event
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncConnection,
    AsyncSession,
    AsyncTransaction,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from sqlalchemy.engine.url import URL
from sqlalchemy_utils.functions import quote

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


async def trans_conn(db: AsyncSession, trans: AsyncTransaction):
    conn: AsyncConnection = db.bind
    # trans = await conn.begin()

    logger.debug(f"{db.in_transaction()=}")
    logger.debug(f"{db.in_nested_transaction()=}")

    logger.debug(f"{conn.in_transaction()=}")
    logger.debug(f"{conn.in_nested_transaction()=}")

    did1 = Did(did="sample_did1234567890", name="yakkle")
    db.add(did1)
    await db.commit()

    result = await db.execute(select(Did))
    dids = result.scalars().all()
    logger.debug(f"dids: {dids=}")

    # trans = await conn.begin_nested()
    await db.commit()
    trans = await conn.begin()
    logger.debug("start transaction")

    logger.debug(f"{conn.in_transaction()=}")
    logger.debug(f"{conn.in_nested_transaction()=}")

    did2 = Did(did="sample_did987654321", name="hooray")
    db.add(did2)
    await db.commit()

    result = await db.execute(select(Did))
    dids = result.scalars().all()
    logger.debug(f"dids: {dids=}")

    trans2 = await conn.begin_nested()

    logger.debug("start transaction")

    logger.debug(f"{conn.in_transaction()=}")
    logger.debug(f"{conn.in_nested_transaction()=}")

    did3 = Did(did="sample_did3333333", name="33333")
    db.add(did3)
    await db.commit()

    result = await db.execute(select(Did))
    dids = result.scalars().all()
    logger.debug(f"before rollback: {dids=}")

    # await trans2.rollback()
    await trans.rollback()
    # await trans.commit()

    result = await db.execute(select(Did))
    dids = result.scalars().all()
    logger.debug(f"after rollback {dids=}")


async def async_main():
    db_url = URL.create(
        "postgresql+asyncpg",
        username="postgres",
        password="postgres",
        host="localhost",
        port="5432",
        database="postgres_test",
    )
    url = db_url._replace(database="postgres")
    _engine = create_async_engine(url, isolation_level="AUTOCOMMIT")

    async with _engine.begin() as conn:
        text = "CREATE DATABASE {} ENCODING '{}' TEMPLATE {}".format(
            quote(conn, db_url.database), "utf-8", quote(conn, "template1")
        )
        await conn.execute(sqlalchemy.text(text))

    engine = create_async_engine(db_url, echo=True, future=True)

    # connect to the database
    connection: AsyncConnection = await engine.connect()
    logger.warning(f"{connection=}")

    # begin a non-ORM transaction
    # trans: AsyncTransaction = await connection.begin()

    # bind an individual Session to the connection
    # session = Session(bind=connection)

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
    db: AsyncSession = Session(bind=connection)
    logger.warning(f"{db.connection=}")
    logger.warning(f"{db.bind=}")
    logger.warning(f"{db.get_bind()=}")

    ###    optional     ###

    # if the database supports SAVEPOINT (SQLite needs special
    # config for this to work), starting a savepoint
    # will allow tests to also use rollback within tests

    @event.listens_for(db.sync_session, "after_transaction_end")
    def restart_savepoint(s, transaction):
        if conn.closed:
            return

        if not conn.in_nested_transaction():
            conn.sync_connection.begin_nested()

    # await trans_session(db)
    await trans_conn(db, None)

    await db.close()

    await drop_database(db_url)


async def drop_database(db_url):
    url = db_url._replace(database="postgres")
    _engine = create_async_engine(url, isolation_level="AUTOCOMMIT")

    async with _engine.begin() as conn:
        pid_column = "pid"
        text = """
        SELECT pg_terminate_backend(pg_stat_activity.{pid_column})
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{database}'
        AND {pid_column} <> pg_backend_pid();
        """.format(
            pid_column=pid_column, database=db_url.database
        )
        await conn.execute(sqlalchemy.text(text))

        text = f"DROP DATABASE {quote(conn, db_url.database)}"
        logger.warning(f"{text=}")
        await conn.execute(sqlalchemy.text(text))


if __name__ == "__main__":
    asyncio.run(async_main())
