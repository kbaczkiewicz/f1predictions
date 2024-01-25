import f1predictions.etl.view_definitions as viewdef

from f1predictions.etl.transformer import get_drivers_transformer, get_rounds_transformer, get_statuses_transformer, \
   get_drivers_categories_transformer, get_drivers_constructors_transformer, \
   get_constructors_transformer, get_drivers_ratings_transformer, get_drivers_standings_transformer, \
   get_constructors_standings_transformer, get_races_transformer, get_race_drivers_results_transformer, \
   get_race_constructors_results_transformer, get_qualifying_results_transformer, \
   get_circuits_transformer, get_lap_times_transformer

from f1predictions.etl.loader import load_data, load_view
from f1predictions.orm.config.database import clear_database


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