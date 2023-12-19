from f1predictions.etl.transformer import (
    get_races_transformer,
    get_circuits_transformer,
    get_statuses_transformer,
    get_constructors_transformer,
    get_drivers_transformer,
    get_rounds_transformer,
    get_drivers_constructors_transformer
)

from f1predictions.etl.loader import load_data, append_data

models = [
    get_drivers_transformer().transform_to_model(),
    get_circuits_transformer().transform_to_model(),
    get_statuses_transformer().transform_to_model(),
    get_constructors_transformer().transform_to_model(),
    get_races_transformer().transform_to_model(),
]

models_append = [
    get_rounds_transformer().transform_to_model(),
    get_drivers_constructors_transformer().transform_to_model(),
]

load_data(models)
append_data(models_append)