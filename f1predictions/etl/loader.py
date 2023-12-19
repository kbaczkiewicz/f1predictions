from typing import Generator

from f1predictions.database import get_session, clear_database


def load_data(models: list[Generator]):
    clear_database()
    append_data(models)
    

def append_data(models: list[Generator]):
    Session = get_session()
        
    with Session() as db_session:
        for model in models:
            for data in model:
                pass
                db_session.add(data)

        db_session.commit()
        db_session.flush()
