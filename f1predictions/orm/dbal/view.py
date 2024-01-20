import pandas as pd
from sqlalchemy import text

from f1predictions.orm.config.database import get_connection


def get_drivers_rounds_results(driver_id: int, year: int) -> pd.DataFrame:
    stmt = """
        SELECT * FROM drivers_rounds_results_view WHERE driver_id = :driver_id AND year = :year
    """

    Connection = get_connection()
    with Connection() as conn:
        result = conn.execute(text(stmt), {'driver_id': driver_id, 'year': year})

    df = pd.DataFrame(result.fetchall())
    df.keys = result.keys()

    return df

def get_opponents_rounds_results(driver_id: int, year: int) -> pd.DataFrame:
    stmt = """
        SELECT * FROM opponents_rounds_results_view WHERE driver_id = :driver_id AND year = :year
    """

    Connection = get_connection()
    with Connection() as conn:
        result = conn.execute(text(stmt), {'driver_id': driver_id, 'year': year})

    df = pd.DataFrame(result.fetchall())
    df.keys = result.keys()

    return df

def get_drivers_seasons_results(driver_id: int, year: int) -> pd.DataFrame:
    stmt = """
        SELECT * FROM drivers_seasons_results_view WHERE driver_id = :driver_id AND year = :year
    """

    Connection = get_connection()
    with Connection() as conn:
        result = conn.execute(text(stmt), {'driver_id': driver_id, 'year': year})

    df = pd.DataFrame(result.fetchall())
    df.keys = result.keys()

    return df


def get_opponents_seasons_results(driver_id: int, year: int) -> pd.DataFrame:
    stmt = """
        SELECT * FROM opponents_seasons_results_view WHERE driver_id = :driver_id AND year = :year
    """

    Connection = get_connection()
    with Connection() as conn:
        result = conn.execute(text(stmt), {'driver_id': driver_id, 'year': year})

    df = pd.DataFrame(result.fetchall())
    df.keys = result.keys()

    return df