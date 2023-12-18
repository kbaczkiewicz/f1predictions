from f1predictions.etl.transformer import (
    get_races_transformer,
    get_circuits_transformer,
    get_statuses_transformer,
    get_constructors_transformer,
    get_drivers_transformer
)

from f1predictions.etl.loader import load_data

models = {
    'races': get_races_transformer().transform_to_model(),
    'drivers': get_drivers_transformer().transform_to_model(),
    'circuits': get_circuits_transformer().transform_to_model(),
    'statuses': get_statuses_transformer().transform_to_model(),
    'constructors': get_constructors_transformer().transform_to_model(),
}

load_data(models)