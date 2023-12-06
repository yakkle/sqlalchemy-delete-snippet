from loguru import logger
from sqlalchemy import JSON, Column, ForeignKey, Integer, String, create_engine, select, event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.schema import MetaData
from sqlalchemy.util import IdentitySet


Base = declarative_base()
Base.metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_`%(constraint_name)s`",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)

@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    logger.warning("set sqlite progma")
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)

    wallets = relationship("Wallet", cascade="all, delete", back_populates="user", lazy="selectin")


class Secret(Base):
    __tablename__ = "secret"

    id = Column(Integer, primary_key=True, index=True)
    type = Column(String, nullable=False)
    data = Column(MutableDict.as_mutable(JSON), nullable=False)

    wallets = relationship("Wallet", cascade="all, delete-orphan", back_populates="secret", lazy="selectin", passive_deletes=True)

    def __repr__(self) -> str:
        return f"<Secret(id={self.id}, type={self.type}, data={self.data})>"


class Wallet(Base):
    __tablename__ = "wallet"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("user.id"))
    secret_id = Column(Integer, ForeignKey("secret.id", ondelete="CASCADE"))

    secret = relationship("Secret", back_populates="wallets")
    user = relationship("User", back_populates="wallets")

    def __repr__(self) -> str:
        return (
            f"<Wallet(id={self.id}, secret_id={self.secret_id}, name={self.name})>"
        )

def delete_secret(db):
    s = db.execute(select(Secret)).scalar()
    logger.debug(f"{s=}")

    db.delete(s)

    s = db.execute(select(Secret)).scalar()
    logger.debug(f"{s=}")
    ws = db.execute(select(Wallet)).all()
    logger.debug(f"{ws=}")


def delete_wallets(db):
    ws = db.execute(select(Wallet)).scalars().all()
    logger.debug(f"{ws=}")
    for w in ws:
        logger.debug(f"{w=}")
        db.delete(w)
    db.commit()

    ws = db.execute(select(Wallet)).scalars().all()
    logger.debug(f"{ws=}")

    s = db.execute(select(Secret)).scalar()
    logger.debug(f"{s=}")
    logger.debug(f"{s.wallets=}")


def delete_wallet_orphan(db):
    s = db.execute(select(Secret)).scalar()
    logger.debug(f"{s=}")
    for w in s.wallets[:]:
        logger.debug(f"{w=}")
        s.wallets.remove(w)

    logger.debug(f"{s.wallets=}")

    ws = db.execute(select(Wallet)).scalars().all()
    logger.debug(f"{ws=}")


def delete_wallets_partial(db):
    ws = db.execute(select(Wallet)).scalars().all()
    logger.debug(f"{ws=}")
    db.delete(ws[0])
    db.commit()

    ws = db.execute(select(Wallet)).scalars().all()
    logger.debug(f"{ws=}")

    secrets = db.execute(select(Secret)).scalars().all()
    for secret in secrets:
        logger.debug(f"{secret=}")
        logger.debug(f"{secret.wallets=}")


def delete_user(db):
    users = db.execute(select(User)).scalars().all()
    logger.debug(f"{users=}")
    db.delete(users[0])

    ws = db.execute(select(Wallet)).scalars().all()
    logger.debug(f"{ws=}")

    s = db.execute(select(Secret)).scalar()
    logger.debug(f"{s=}")


def main():
    engine = create_engine("sqlite:///:memory:", echo=True, future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)

    db = Session()

    @event.listens_for(db, 'before_flush')
    def receive_before_flush(session, flush_context, instances):
        "listen for the 'before_flush' event"
        logger.warning(f"before_flush : {session=}, {flush_context=}, {instances=}")
        logger.warning(f"{session.deleted=}")
        if session.deleted:
            secret = None
            for ins in session.deleted:
                logger.warning(f"{ins=}")
                if isinstance(ins, Wallet):
                    secret = ins.secret
                    break

            if secret:
                logger.warning(f"{secret.wallets=}")
                intersect = IdentitySet(secret.wallets) & session.deleted
                logger.warning(f"{intersect}")
                if intersect == IdentitySet(secret.wallets):
                    logger.warning("set delete secret!")
                    session.delete(secret)

            # ws = session.execute(select(Wallet)).scalars().all()
            # logger.warning(f"{ws=}")

    @event.listens_for(db, 'persistent_to_deleted')
    def receive_persistent_to_deleted(session, instance):
        "listen for the 'persistent_to_deleted' event"
        logger.warning(f"persistent_to_deleted : {session=}, {instance=}")


    user = User(email="y@email.com")
    user1 = User(email="z@email.com")
    db.add(user)
    db.add(user1)
    db.commit()

    secret = Secret(type="keystore", data={"key": "value"})
    secret1 = Secret(type="keystore", data={"key1": "value1"})
    db.add(secret)
    db.add(secret1)
    db.commit()

    db.add_all([
        Wallet(user_id=user.id, secret_id=secret.id, name="test_wallet1"),
        Wallet(user_id=user.id, secret_id=secret.id, name="test_wallet2"),
        Wallet(user_id=user.id, secret_id=secret.id, name="test_wallet3"),
        Wallet(user_id=user1.id, secret_id=secret1.id, name="test_wallet4"),
    ])
    db.commit()

    logger.debug(f"{secret.wallets=}")

    # delete parent
    # delete_secret(db)

    # delete children
    # delete_wallets(db)

    # delete children with orphan
    # delete_wallet_orphan(db)

    # delete children partially
    # delete_wallets_partial(db)

    # delete user own wallets
    delete_user(db)


if __name__ == "__main__":
    main()