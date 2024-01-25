import numpy as np
import pandas as pd

from sqlalchemy import select
from f1predictions.orm.config.database import get_session
from f1predictions.orm.entity import DriverConstructor, Round
from f1predictions.orm.query import DriverRatingQuery
from f1predictions.prediction.modelfactory import DriverRatingsModelFactory
from f1predictions.prediction.regressors import RandomForestRegressorBuilder


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


def find_driver_constructor_id(drivers_constructors_df: pd.DataFrame, driver_id: int, constructor_id: int,
                               year: int) -> int:
    df = (drivers_constructors_df
          .where(drivers_constructors_df['driverId'] == driver_id)
          .where(drivers_constructors_df['constructorId'] == constructor_id)
          .dropna()
          )

    if year is not None:
        df = df.where(drivers_constructors_df['year'] == year)

    return df['id'].dropna().unique()[0]


def get_driver_ratings_predictor(
        driver_rating_query: DriverRatingQuery,
        driver_ratings_model_factory: DriverRatingsModelFactory
) -> RandomForestRegressorBuilder:
    ratings = driver_rating_query.get_drivers_ratings()

    model = [driver_ratings_model_factory.create_driver_ratings_model(i.driver_id, i.year) for i in ratings]

    X = np.array([i.to_list() for i in model])
    y = np.array([i.rating for i in ratings])

    return RandomForestRegressorBuilder() \
        .set_model(X, y) \
        .set_criterion('absolute_error') \
        .create_train_test_set(0.33) \
        .create_regressor(12)



