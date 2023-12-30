from typing import Generator, Any

from f1predictions.database import get_session, clear_database
from f1predictions.utils import print_model


def load_data(models: Generator[Any, None, None], debug: bool = False):
    Session = get_session()

    with Session() as db_session:
        for model in models:
            if debug:
                print_model(model)
            db_session.add(model)

        db_session.commit()
        db_session.flush()
