import pandas as pd


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