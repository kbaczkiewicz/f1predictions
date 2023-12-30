from f1predictions.database import clear_database
from f1predictions.etl.transformer import (
    get_races_transformer,
    get_circuits_transformer,
    get_statuses_transformer,
    get_constructors_transformer,
    get_drivers_transformer,
    get_rounds_transformer,
    get_drivers_constructors_transformer, get_race_drivers_results_transformer, get_qualifying_results_transformer,
    get_race_constructors_results_transformer
)

from f1predictions.etl.loader import load_data
# print("Clearing database...")
# clear_database()
#
# print("Loading drivers...")
# load_data(get_drivers_transformer().transform_to_model())
# print("Loading circuits...")
# load_data(get_circuits_transformer().transform_to_model())
# print("Loading statuses...")
# load_data(get_statuses_transformer().transform_to_model())
# print("Loading constructors...")
# load_data(get_constructors_transformer().transform_to_model())
# print("Loading races...")
# load_data(get_races_transformer().transform_to_model())
# print("Loading rounds...")
# load_data(get_rounds_transformer().transform_to_model())
# print("Loading drivers constructors...")
# load_data(get_drivers_constructors_transformer().transform_to_model())
# print("Loading drivers results...")
# load_data(get_race_drivers_results_transformer().transform_to_model())
# print("Loading constructors results...")
# load_data(get_race_constructors_results_transformer().transform_to_model())
print("Loading qualifying results...")
load_data(get_qualifying_results_transformer().transform_to_model())
