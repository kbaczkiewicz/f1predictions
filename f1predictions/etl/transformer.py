from abc import abstractmethod, ABC
from typing import Generator, override
from f1predictions.database import get_session, get_connection
from sqlalchemy import select, text
import pandas as pd
from f1predictions.etl.extractor import Extractor, DirectDataFrameExtractor
from f1predictions.model import Driver, Constructor, Status, Circuit, Race, Round, DriverConstructor, RaceDriverResult, RaceConstructorResult, RaceDriverStandings, RaceConstructorStandings, LapTimes, QualifyingResult
from f1predictions.utils import convert_time_to_ms, create_drivers_constructors_dataframe, find_driver_constructor_id, \
    create_rounds_dataframe


class Transformer(ABC):
    def __init__(self, extractor: Extractor):
        self.extractor = extractor

    @abstractmethod
    def transform_to_model(self) -> Generator:
        pass


class RelatedModelsTransformer(Transformer, ABC):
    def __init__(self, extractor: Extractor, related_model_data: dict[str, pd.DataFrame]):
        super().__init__(extractor)
        self.related_model_data = related_model_data


class DriversTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Driver, None, None]:
        df = self.extractor.extract()
        for i in range(len(df)):
            driver = Driver()
            driver.id = int(df.loc[i, 'driverId'])
            driver.name = str(df.loc[i, 'forename'])
            driver.surname = str(df.loc[i, 'surname'])

            yield driver


def get_drivers_transformer() -> DriversTransformer:
    extractor = Extractor('drivers.csv', ['driverId', 'forename', 'surname'])

    return DriversTransformer(extractor)


class ConstructorsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Constructor, None, None]:
        df = self.extractor.extract()
        for i in range(len(df)):
            constructor = Constructor()
            constructor.id = int(df.loc[i, 'constructorId'])
            constructor.name = str(df.loc[i, 'name'])

            yield constructor


def get_constructors_transformer() -> ConstructorsTransformer:
    extractor = Extractor('constructors.csv', ['constructorId', 'name'])

    return ConstructorsTransformer(extractor)


class StatusesTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Status, None, None]:
        df = self.extractor.extract()
        for i in range(len(df)):
            status = Status()
            status.id = int(df.loc[i, 'statusId'])
            status.status = str(df.loc[i, 'status'])

            yield status


def get_statuses_transformer() -> StatusesTransformer:
    extractor = Extractor('status.csv', ['statusId', 'status'])

    return StatusesTransformer(extractor)


class CircuitsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Circuit, None, None]:
        df = self.extractor.extract()
        for i in range(len(df)):
            circuit = Circuit()
            circuit.id = int(df.loc[i, 'circuitId'])
            circuit.name = str(df.loc[i, 'name'])

            yield circuit


def get_circuits_transformer() -> CircuitsTransformer:
    extractor = Extractor('circuits.csv', ['circuitId', 'name'])

    return CircuitsTransformer(extractor)


class RacesTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Race, None, None]:
        df = self.extractor.extract().drop_duplicates(['name']).reset_index()
        for i in range(len(df)):
            race = Race()
            race.id = i + 1
            race.name = str(df.loc[i, 'name'])
            race.circuit_id = int(df.loc[i, 'circuitId'])

            yield race


def get_races_transformer() -> RacesTransformer:
    extractor = Extractor('races.csv', ['name', 'circuitId'])

    return RacesTransformer(extractor)


class RoundsTransformer(RelatedModelsTransformer):
    @override
    def transform_to_model(self) -> Generator[Round, None, None]:
        df = self.extractor.extract().drop_duplicates().reset_index()
        races_df = self.related_model_data['races']
        for i in range(len(df)):
            race_name = df.loc[i, 'name']
            round_entity = Round()
            round_entity.id = int(df.loc[i, 'raceId'])
            round_entity.race_id = int(races_df.where(races_df['name'] == str(race_name)).drop_duplicates().dropna().values[0][0])
            round_entity.round_number = int(df.loc[i, 'round'])
            round_entity.year = int(df.loc[i, 'year'])
            yield round_entity


def get_rounds_transformer() -> RoundsTransformer:
    Session = get_session()
    extractor = Extractor('races.csv', ['raceId', 'name', 'round', 'year'])
    with Session() as session:
        races = session.scalars(select(Race))
        data = [(i.id, i.name) for i in races]
        races_dataframe = pd.DataFrame({'id': [i[0] for i in data], 'name': [i[1] for i in data]})

        return RoundsTransformer(extractor, {'races': races_dataframe})


class DriversConstructorsTransformer(RelatedModelsTransformer):
    @override
    def transform_to_model(self) -> Generator[DriverConstructor, None, None]:
        df = self.extractor.extract()
        rounds_df = self.related_model_data['rounds']
        df = df.join(rounds_df.set_index('raceId'), on='raceId').dropna().drop_duplicates(['driverId', 'constructorId', 'year'])[['driverId', 'constructorId', 'year']]
        identifier = 1
        for value in df.values:
            driver_constructor = DriverConstructor()
            driver_constructor.id = identifier
            driver_constructor.driver_id = int(value[0])
            driver_constructor.constructor_id = int(value[1])
            driver_constructor.year = int(value[2])
            identifier += 1

            yield driver_constructor


def get_drivers_constructors_transformer() -> DriversConstructorsTransformer:
    extractor = Extractor('results.csv', ['driverId', 'constructorId', 'raceId'])

    Session = get_session()
    with Session() as session:
        rounds = session.scalars(select(Round))
        data = [(i.id, i.year) for i in rounds]
    rounds_dataframe = pd.DataFrame({'raceId': [i[0] for i in data], 'year': [i[1] for i in data]})

    return DriversConstructorsTransformer(extractor, {'rounds': rounds_dataframe})


class RaceDriversResultsTransformer(RelatedModelsTransformer):
    @override
    def transform_to_model(self) -> Generator[RaceDriverResult, None, None]:
        df = self.extractor.extract()
        for i in range(len(df)):
            race_driver_result = RaceDriverResult()
            race = self.related_model_data['rounds'] \
                .where(self.related_model_data['rounds']['id'] == df.loc[i, 'raceId'])['year'].dropna()
            driver_constructor = find_driver_constructor_id(
                self.related_model_data['drivers_constructors'],
                int(df.loc[i, 'driverId']),
                int(df.loc[i, 'constructorId']),
                int(race.iloc[0])
            )

            fastest_lap_time = convert_time_to_ms(str(df.loc[i, 'fastestLapTime']))

            race_driver_result.id = int(df.loc[i, 'resultId'])
            race_driver_result.driver_constructor_id = driver_constructor
            race_driver_result.points = float(df.loc[i, 'points'])
            race_driver_result.fastest_lap_time = fastest_lap_time
            race_driver_result.round_id = int(df.loc[i, 'raceId'])
            race_driver_result.status_id = int(df.loc[i, 'statusId'])
            if "\\N" == df.loc[i, 'position']:
                race_driver_result.position = 0
            else:
                race_driver_result.position = int(df.loc[i, 'position'])

            if "\\N" == df.loc[i, 'fastestLapSpeed']:
                race_driver_result.fastest_lap_speed = 0.0
            else:
                race_driver_result.fastest_lap_speed = float(df.loc[i, 'fastestLapSpeed'])

            yield race_driver_result


def get_race_drivers_results_transformer():
    extractor = Extractor('results.csv', [
        'resultId',
        'raceId',
        'driverId',
        'constructorId',
        'points',
        'fastestLapTime',
        'fastestLapSpeed',
        'position',
        'statusId'
    ])

    return RaceDriversResultsTransformer(extractor, {
        'drivers_constructors': create_drivers_constructors_dataframe(),
        'rounds': create_rounds_dataframe()
    })


class RaceConstructorsResultsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[RaceConstructorResult, None, None]:
        df = self.extractor.extract()
        for i in range(len(df)):
            race_constructor_result = RaceConstructorResult()
            race_constructor_result.id = int(df.loc[i, 'constructorResultsId'])
            race_constructor_result.constructor_id = int(df.loc[i, 'constructorId'])
            race_constructor_result.round_id = int(df.loc[i, 'raceId'])
            race_constructor_result.points = float(df.loc[i, 'points'])

            yield race_constructor_result


def get_race_constructors_results_transformer():
    extractor = Extractor('constructor_results.csv', [
        'constructorResultsId',
        'raceId',
        'constructorId',
        'points',
    ])

    return RaceConstructorsResultsTransformer(extractor)


class QualifyingResultsTransformer(RelatedModelsTransformer):
    @override
    def transform_to_model(self) -> Generator[QualifyingResult, None, None]:
        df = self.extractor.extract()
        for i in range(len(df)):
            race = self.related_model_data['rounds'].where(self.related_model_data['rounds']['id'] == int(df.loc[i, 'raceId']))['year'].dropna()
            qualifying_result = QualifyingResult()
            driver_constructor = find_driver_constructor_id(
                self.related_model_data['drivers_constructors'],
                int(df.loc[i, 'driverId']),
                int(df.loc[i, 'constructorId']),
                int(race.iloc[0])
            )

            qualifying_result.id = int(df.loc[i, 'qualifyId'])
            qualifying_result.round_id = int(df.loc[i, 'raceId'])
            qualifying_result.driver_constructor_id = driver_constructor
            qualifying_result.position = int(df.loc[i, 'position'])
            qualifying_result.q1 = convert_time_to_ms(df.loc[i, 'q1'])
            qualifying_result.q2 = convert_time_to_ms(df.loc[i, 'q2'])
            qualifying_result.q3 = convert_time_to_ms(df.loc[i, 'q3'])

            yield qualifying_result


def get_qualifying_results_transformer():
    extractor = Extractor('qualifying.csv', [
        'qualifyId',
        'raceId',
        'driverId',
        'constructorId',
        'q1',
        'q2',
        'q3',
        'position'
    ])

    return QualifyingResultsTransformer(extractor, {
        'drivers_constructors': create_drivers_constructors_dataframe(),
        'rounds': create_rounds_dataframe()
    })


class LapTimesTransformer(RelatedModelsTransformer):
    @override
    def transform_to_model(self):
        df = self.extractor.extract()
        for i in range(len(df)):
            lap_time = LapTimes()
            lap_time.id = i + 1
            lap_time.lap = int(df.loc[i, 'lap'])
            lap_time.position = int(df.loc[i, 'position'])
            lap_time.time = int(df.loc[i, 'milliseconds'])
            lap_time.round_id = int(df.loc[i, 'raceId'])
            lap_time.driver_constructor_id = int(df.loc[i, 'raceId'])

            yield lap_time


def get_lap_times_transformer():
    extractor = Extractor('lap_times.csv', ['raceId', 'driverId', 'lap', 'position', 'milliseconds'])

    return LapTimesTransformer(extractor, {
        'drivers_constructors': create_drivers_constructors_dataframe(),
        'rounds': create_rounds_dataframe()
    })


class DriversStandingsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[RaceDriverStandings, None, None]:
        df = self.extractor.extract()
        index = 1
        for values in df.values:
            race_driver_standings = RaceDriverStandings()
            race_driver_standings.id = index
            race_driver_standings.driver_constructor_id = values[0]
            race_driver_standings.year = values[1]
            race_driver_standings.points = values[2]
            race_driver_standings.position = values[3]
            race_driver_standings.wins = values[4]
            index += 1

            yield race_driver_standings


def get_drivers_standings_transformer() -> DriversStandingsTransformer:
    standings_statement = """
    SELECT
        rr.year AS year,
        dc.id AS driver_constructor_id,
        SUM(dr.points) as sum_points,
        ROW_NUMBER() OVER (ORDER BY SUM(dr.points) DESC) AS wdc_position
    FROM race_driver_result dr
    JOIN driver_constructor dc ON dc.id = dr.driver_constructor_id
    JOIN round rr ON dr.round_id = rr.id
    WHERE rr.year = {}
    GROUP BY rr.year, dc.id
    """

    wins_statement = """
    SELECT
        dc.id AS driver_constructor_id,
        r.year AS year,
        COUNT(dr2.id) AS wins
    FROM race_driver_result dr
    LEFT JOIN race_driver_result dr2 ON dr2.id = dr.id AND dr2.position = 1
    JOIN round r ON r.id = dr.round_id
    JOIN driver_constructor dc ON dc.id = dr.driver_constructor_id
    WHERE r.year = {}
    GROUP BY dc.id, r.year
    """

    Connection = get_connection()

    full_standings = []
    for year in range(1950, 2024):
        standings_query = text(standings_statement.format(year))
        wins_query = text(wins_statement.format(year))

        with Connection() as conn:
            standings, wins = conn.execute(standings_query), conn.execute(wins_query)

        standings_df = pd.DataFrame(standings.fetchall())
        wins_df = pd.DataFrame(wins.fetchall())
        standings_df.keys = standings.columns
        wins_df.keys = wins.columns

        full_standings.append(pd.merge(standings_df, wins_df, on=['driver_constructor_id', 'year']).dropna()
                              [['driver_constructor_id', 'year', 'sum_points', 'wdc_position', 'wins']])

    return DriversStandingsTransformer(DirectDataFrameExtractor(pd.concat([i for i in full_standings])))

class ConstructorsStandingsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[RaceConstructorStandings, None, None]:
        df = self.extractor.extract().reset_index()
        index = 1
        for values in df.values:
            race_constructor_standings = RaceConstructorStandings()
            race_constructor_standings.id = index
            race_constructor_standings.constructor_id = values[1]
            race_constructor_standings.year = values[2]
            race_constructor_standings.points = values[3]
            race_constructor_standings.position = values[4]
            race_constructor_standings.wins = values[5]
            index += 1

            yield race_constructor_standings


def get_constructors_standings_transformer():
    standings_statement = """
    SELECT
        rr.year AS year,
        dc.constructor_id AS constructor_id,
        SUM(dr.points) as sum_points,
        ROW_NUMBER() OVER (ORDER BY SUM(dr.points) DESC) AS wcc_position
    FROM race_driver_result dr
    JOIN driver_constructor dc ON dc.id = dr.driver_constructor_id
    JOIN round rr ON dr.round_id = rr.id
    WHERE rr.year = {}
    GROUP BY rr.year, dc.constructor_id
    """

    wins_statement = """
    SELECT
        dc.constructor_id AS constructor_id,
        r.year AS year,
        COUNT(dr2.id) AS wins
    FROM race_driver_result dr
    LEFT JOIN race_driver_result dr2 ON dr2.id = dr.id AND dr2.position = 1
    JOIN round r ON r.id = dr.round_id
    JOIN driver_constructor dc ON dc.id = dr.driver_constructor_id
    WHERE r.year = {}
    GROUP BY dc.constructor_id, r.year
    """

    Connection = get_connection()
    full_standings = []
    for year in range(1950, 2024):
        standings_query = text(standings_statement.format(year))
        wins_query = text(wins_statement.format(year))

        with Connection() as conn:
            standings, wins = conn.execute(standings_query), conn.execute(wins_query)

        standings_df = pd.DataFrame(standings.fetchall())
        wins_df = pd.DataFrame(wins.fetchall())
        standings_df.keys = standings.columns
        wins_df.keys = wins.columns

        full_standings.append(pd.merge(standings_df, wins_df, on=['constructor_id', 'year']).dropna()
                              [['constructor_id', 'year', 'sum_points', 'wcc_position', 'wins']])

    return ConstructorsStandingsTransformer(DirectDataFrameExtractor(pd.concat([i for i in full_standings])))
