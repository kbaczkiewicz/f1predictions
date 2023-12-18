from typing import Generator

from f1predictions.database import get_session, clear_database


def load_data(models: dict[Generator]):
    clear_database()
    append_data(models)
    

def append_data(models: dict[Generator]):
    Session = get_session()
        
    with Session() as db_session:
        for model in models.values():
            for data in model:
                pass
                db_session.add(data)

        db_session.commit()
