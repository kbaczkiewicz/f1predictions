from typing import Generator, Any
from sqlalchemy import text
from f1predictions.orm.config.database import get_session, get_connection


def load_data(models: Generator[Any, None, None]):
    Session = get_session()

    with Session() as db_session:
        for model in models:
            db_session.add(model)

        db_session.commit()
        db_session.flush()


def load_view(statement: text):
    Connection = get_connection()
    with Connection() as conn:
        conn.execute(statement)
        conn.commit()

