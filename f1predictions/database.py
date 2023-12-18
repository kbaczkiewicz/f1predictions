from sqlalchemy import create_engine, Engine
import sqlalchemy.orm as orm
from sqlalchemy.orm import sessionmaker, Session

_ENGINE = None
_SESSION = None


class Base(orm.DeclarativeBase):
    pass


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE:
        return _ENGINE

    connstr = "postgresql+psycopg2://root:toor@localhost:5432/f1predictions"
    _ENGINE = create_engine(connstr)

    return _ENGINE


def create_schema():
    Base.metadata.drop_all(get_engine())
    Base.metadata.create_all(get_engine())

def get_session():
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    session = sessionmaker(get_engine())
    _SESSION = session()

    return _SESSION


def clear_database():
    Base.metadata.drop_all(bind=get_engine())
    Base.metadata.create_all(bind=get_engine())
