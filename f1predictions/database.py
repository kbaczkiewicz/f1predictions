from sqlalchemy import create_engine, Engine, text
import sqlalchemy.orm as orm
from sqlalchemy.orm import sessionmaker

_ENGINE = None
_SESSION = None


class Base(orm.DeclarativeBase):
    pass


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE:
        return _ENGINE

    connstr = "postgresql+psycopg2://root:toor@postgres:5432/f1predictions"
    _ENGINE = create_engine(connstr)

    return _ENGINE


def create_schema():
    Base.metadata.drop_all(get_engine())
    Base.metadata.create_all(get_engine())


def get_session():
    session = sessionmaker(get_engine())

    return session


def clear_database():
    Connection = get_connection()
    with Connection() as conn:
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS drivers_rounds_results_view"))
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS drivers_seasons_results_view"))
        conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS opponents_rounds_results_view"))
        conn.commit()

    Base.metadata.drop_all(bind=get_engine())
    Base.metadata.create_all(bind=get_engine())


def get_connection():
    return get_engine().connect
