"""
https://docs.sqlalchemy.org/en/14/orm/relationship_persistence.html
https://github.com/sqlalchemy/sqlalchemy/discussions/10934
"""

import asyncio
from loguru import logger
from sqlalchemy import Integer, ForeignKey, Column, String, event, select
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, lazyload

Base = declarative_base()


class Entry(Base):
    __tablename__ = "entry"
    entry_id = Column(Integer, primary_key=True)
    widget_id = Column(Integer, ForeignKey("widget.widget_id"))
    name = Column(String(50))

    # entry = relationship("Widget", back_populates="entries")
    widget = relationship("Widget", foreign_keys=[widget_id], back_populates="entries")


class Widget(Base):
    __tablename__ = "widget"

    widget_id = Column(Integer, primary_key=True)
    favorite_entry_id = Column(
        # Integer, ForeignKey("entry.entry_id", name="fk_favorite_entry")
        Integer,
        ForeignKey("entry.entry_id", name="fk_favorite_entry", ondelete="SET NULL"),
    )
    name = Column(String(50))

    # entries = relationship(
    #     Entry, primaryjoin=widget_id == Entry.widget_id, lazy="selectin")
    entries = relationship(
        Entry,
        cascade="all",
        foreign_keys=[Entry.widget_id],
        back_populates="widget",
        lazy="dynamic",
        # lazy="selectin",
    )

    favorite_entry = relationship(
        Entry,
        primaryjoin=favorite_entry_id == Entry.entry_id,
        foreign_keys=favorite_entry_id,
        post_update=True,
    )


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

    w1 = Widget(name="somewidget")
    db.add(w1)
    await db.commit()

    widget_id = w1.widget_id
    logger.debug(f"{widget_id=}")

    e1 = Entry(widget_id=widget_id, name="1 someentry")
    db.add(e1)
    await db.commit()

    logger.debug(f"{w1.favorite_entry_id=}")

    w1.favorite_entry = e1
    # w1.entries = [e1]
    db.add(w1)
    await db.commit()
    await db.refresh(w1)

    logger.debug(f"{w1.favorite_entry_id=}")
    # logger.debug(f"{w1.entries=}")
    entries = (await db.scalars(w1.entries.statement)).all()
    logger.debug(f"{entries=}")

    e2 = Entry(widget_id=w1.widget_id, name="2 someentry")
    # db.add_all([w1, e2])
    db.add(e2)
    await db.commit()

    # db.expire(w1)
    # result = await db.execute(select(Widget).where(Widget.widget_id == w1.widget_id))
    # result.scalar_one_or_none()
    # w1.entries = await db.execute(select(Entry).where(Entry.widget_id == w1.widget_id))

    # w1_entries = await db.run_sync(lambda x: w1.entries)
    # w1_entries.append(e2)
    # await db.run_sync(lambda x: w1.entries.append(e2))
    # db.add(e2)
    # await db.commit()

    await db.refresh(w1)
    # logger.debug(f"{w1.entries=}")
    # entries = w1.entries
    entries = (await db.scalars(w1.entries.statement)).all()
    for entry in entries:
        logger.debug(f"{entry.name=}")

    # result = await db.execute(select(Widget).where(Widget.widget_id == w1.widget_id))
    result = await db.execute(
        select(Widget)
        .options(lazyload(Widget.entries))
        .where(Widget.widget_id == w1.widget_id)
    )
    select_w = result.scalar_one_or_none()
    entries = (await db.scalars(select_w.entries.statement)).all()
    logger.warning(f"{entries=}")

    delete_entry = w1.favorite_entry
    # w1.favorite_entry = None
    await db.delete(delete_entry)
    # await db.delete(e1)
    await db.commit()
    await db.refresh(w1)

    logger.debug(f"{w1.favorite_entry_id=}")
    logger.debug(f"{w1.favorite_entry=}")
    logger.debug(f"{w1.entries=}")

    await db.close()


def main():
    engine = create_engine("sqlite:///:memory:", echo=True, future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)

    db = Session()

    w1 = Widget(name="somewidget")
    e1 = Entry(name="1 someentry")
    # e2 = Entry(name="2 someentry")
    w1.favorite_entry = e1
    w1.entries = [e1]
    db.add_all([w1, e1])
    db.commit()

    delete_entry = w1.favorite_entry
    w1.favorite_entry = None
    db.delete(delete_entry)
    db.commit()

    logger.debug(f"{w1.favorite_entry_id=}")
    logger.debug(f"{w1.favorite_entry=}")
    logger.debug(f"{w1.entries=}")


if __name__ == "__main__":
    # main()
    asyncio.run(async_main())
