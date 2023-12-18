from typing import Generator

from f1predictions.database import get_session, clear_database


def load_data(models: dict[Generator]):
    clear_database()
    db_session = get_session()

    for model in models.values():
        for data in model:
            db_session.add(data)

    db_session.commit()

