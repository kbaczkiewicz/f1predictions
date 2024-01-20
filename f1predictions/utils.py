import pandas as pd
from sqlalchemy import select

from f1predictions.orm.config.database import get_session
from f1predictions.orm.entity import DriverConstructor, Round


def convert_time_to_ms(time: str) -> int:
    if "\\N" == time or pd.isnull(time):
        return 0

    minutes, rest = time.split(':')
    seconds, milliseconds = rest.split('.')

    return int(milliseconds) + (int(seconds) * 1000) + (int(minutes) * 60000)


def create_drivers_constructors_dataframe() -> pd.DataFrame:
    Session = get_session()
    with Session() as session:
        drivers_constructors = [(i.id, i.driver_id, i.constructor_id, i.year) for i in
                                session.scalars(select(DriverConstructor))]

    return pd.DataFrame({
            'id': [i[0] for i in drivers_constructors],
            'driverId': [i[1] for i in drivers_constructors],
            'constructorId': [i[2] for i in drivers_constructors],
            'year': [i[3] for i in drivers_constructors]
        })


def create_rounds_dataframe() -> pd.DataFrame:
    Session = get_session()
    with Session() as session:
        rounds = [(i.id, i.year) for i in
                 session.scalars(select(Round))]

    return pd.DataFrame({
        'id': [i[0] for i in rounds],
        'year': [i[1] for i in rounds]
    })


def find_driver_constructor_id(drivers_constructors_df: pd.DataFrame, driver_id: int, constructor_id: int, year: int) -> int:
    df = (drivers_constructors_df
     .where(drivers_constructors_df['driverId'] == driver_id)
     .where(drivers_constructors_df['constructorId'] == constructor_id)
     .dropna()
    )

    if year is not None:
        df = df.where(drivers_constructors_df['year'] == year)

    return df['id'].dropna().unique()[0]







