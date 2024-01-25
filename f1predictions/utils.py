import numpy as np
import pandas as pd
from sqlalchemy import select
from f1predictions.orm.config.database import clear_database
from f1predictions.etl.transformer import *
from f1predictions.etl.loader import load_data, load_view
import f1predictions.etl.view_definitions as viewdef
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


def load_models():
    print("Clearing database...")
    clear_database()

    print("Loading drivers...")
    load_data(get_drivers_transformer().transform_to_model())
    print("Loading circuits...")
    load_data(get_circuits_transformer().transform_to_model())
    print("Loading statuses...")
    load_data(get_statuses_transformer().transform_to_model())
    print("Loading constructors...")
    load_data(get_constructors_transformer().transform_to_model())
    print("Loading races...")
    load_data(get_races_transformer().transform_to_model())
    print("Loading rounds...")
    load_data(get_rounds_transformer().transform_to_model())
    print("Loading drivers constructors...")
    load_data(get_drivers_constructors_transformer().transform_to_model())
    print("Loading drivers results...")
    load_data(get_race_drivers_results_transformer().transform_to_model())
    print("Loading constructors results...")
    load_data(get_race_constructors_results_transformer().transform_to_model())
    print("Loading qualifying results...")
    load_data(get_qualifying_results_transformer().transform_to_model())
    print("Loading lap times...")
    load_data(get_lap_times_transformer().transform_to_model())
    print("Loading drivers standings...")
    load_data(get_drivers_standings_transformer().transform_to_model())
    print("Loading constructor standings...")
    load_data(get_constructors_standings_transformer().transform_to_model())
    print("Loading saved drivers ratings...")
    load_data(get_drivers_ratings_transformer().transform_to_model())
    print("Loading saved drivers categories...")
    load_data(get_drivers_categories_transformer().transform_to_model())


def create_materialized_views():
    print('Creating drivers season results data materialized view')
    load_view(viewdef.drivers_seasons_results_view)
    print('Creating drivers\' opponents season results data materialized view')
    load_view(viewdef.opponents_seasons_results_view)
    print('Creating drivers round results data materialized view')
    load_view(viewdef.drivers_rounds_results_view)
    print('Creating drivers\' opponents round results data materialized view')
    load_view(viewdef.opponents_rounds_results_view)
