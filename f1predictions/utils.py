import pandas as pd
from sqlalchemy import select

from f1predictions.database import get_session
from f1predictions.model import DriverConstructor


def convert_time_to_ms(time: str) -> int:
    if "\\N" == time or pd.isnull(time):
        return 0
    minutes, rest = time.split(':')
    seconds, milliseconds = rest.split('.')

    return int(milliseconds) + (int(seconds) * 1000) + (int(minutes) * 60000)


def print_model(model):
    columns = [m.key for m in model.__table__.columns]
    details = ''
    for x in columns:
        details = details + str(x) + ': ' + str(getattr(model, x)) + " "

    print(details)


def create_drivers_constructors_dataframe() -> pd.DataFrame:
    Session = get_session()
    with Session() as Session:
        drivers_constructors = [(i.id, i.driver_id, i.constructor_id) for i in
                                Session.scalars(select(DriverConstructor))]
        drivers_constructors_df = pd.DataFrame({
            'id': [i[0] for i in drivers_constructors],
            'driverId': [i[1] for i in drivers_constructors],
            'constructorId': [i[2] for i in drivers_constructors]
        })

    return drivers_constructors_df


def find_driver_constructor_id(drivers_constructors_df: pd.DataFrame, driver_id: int, constructor_id: int) -> int:
    return int(drivers_constructors_df
     .where(drivers_constructors_df['driverId'] == driver_id)
     .where(drivers_constructors_df['constructorId'] == constructor_id)
     .dropna()
     ['id'].unique()[0])
