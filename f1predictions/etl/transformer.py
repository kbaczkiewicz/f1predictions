from abc import abstractmethod, ABC
from typing import Generator, override
from f1predictions.database import get_session, get_connection
from sqlalchemy import select, text
import pandas as pd
from f1predictions.etl.extractor import Extractor, RelatedModelsExtractor, DirectDataFrameExtractor
from f1predictions.model import Driver, Constructor, Status, Circuit, Race, Round, DriverConstructor, RaceDriverResult, RaceConstructorResult, RaceDriverStandings, RaceConstructorStandings, LapTimes, QualifyingResult
from f1predictions.utils import convert_time_to_ms, create_drivers_constructors_dataframe, find_driver_constructor_id


class Transformer(ABC):
    def __init__(self, exporter: Extractor):
        self.exporter = exporter

    @abstractmethod
    def transform_to_model(self) -> Generator:
        pass


class RelatedModelTransformer(Transformer, ABC):
    def __init__(self, exporter: Extractor, related_model_data: pd.DataFrame):
        super().__init__(exporter)
        self.related_model_data = related_model_data


class DriversTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Driver, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            driver = Driver()
            driver.id = int(df.loc[i, 'driverId'])
            driver.name = str(df.loc[i, 'forename'])
            driver.surname = str(df.loc[i, 'surname'])

            yield driver


def get_drivers_transformer() -> DriversTransformer:
    exporter = Extractor('drivers.csv', ['driverId', 'forename', 'surname'])

    return DriversTransformer(exporter)


class ConstructorsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Constructor, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            constructor = Constructor()
            constructor.id = int(df.loc[i, 'constructorId'])
            constructor.name = str(df.loc[i, 'name'])

            yield constructor


def get_constructors_transformer() -> ConstructorsTransformer:
    exporter = Extractor('constructors.csv', ['constructorId', 'name'])

    return ConstructorsTransformer(exporter)


class StatusesTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Status, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            status = Status()
            status.id = int(df.loc[i, 'statusId'])
            status.status = str(df.loc[i, 'status'])

            yield status


def get_statuses_transformer() -> StatusesTransformer:
    exporter = Extractor('status.csv', ['statusId', 'status'])

    return StatusesTransformer(exporter)


class CircuitsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Circuit, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            circuit = Circuit()
            circuit.id = int(df.loc[i, 'circuitId'])
            circuit.name = str(df.loc[i, 'name'])

            yield circuit


def get_circuits_transformer() -> CircuitsTransformer:
    exporter = Extractor('circuits.csv', ['circuitId', 'name'])

    return CircuitsTransformer(exporter)


class RacesTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Race, None, None]:
        df = self.exporter.extract().drop_duplicates(['name']).reset_index()
        for i in range(len(df)):
            race = Race()
            race.id = i + 1
            race.name = str(df.loc[i, 'name'])
            race.circuit_id = int(df.loc[i, 'circuitId'])

            yield race


def get_races_transformer() -> RacesTransformer:
    exporter = Extractor('races.csv', ['name', 'circuitId'])

    return RacesTransformer(exporter)


class RoundsTransformer(RelatedModelTransformer):
    @override
    def transform_to_model(self) -> Generator[Round, None, None]:
        df = self.exporter.extract().drop_duplicates().reset_index()
        for i in range(len(df)):
            race_name = df.loc[i, 'name']
            round_entity = Round()
            round_entity.id = int(df.loc[i, 'raceId'])
            round_entity.race_id = int(self.related_model_data.where(self.related_model_data['name'] == str(race_name)).drop_duplicates().dropna().values[0][0])
            round_entity.round_number = int(df.loc[i, 'round'])
            round_entity.year = int(df.loc[i, 'year'])
            yield round_entity


def get_rounds_transformer() -> RoundsTransformer:
    Session = get_session()
    exporter = Extractor('races.csv', ['raceId', 'name', 'round', 'year'])
    with Session() as session:
        races = session.scalars(select(Race))
        data = [(i.id, i.name) for i in races]
        races_dataframe = pd.DataFrame({'id': [i[0] for i in data], 'name': [i[1] for i in data]})

        return RoundsTransformer(exporter, races_dataframe)


class DriversConstructorsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[DriverConstructor, None, None]:
        df = self.exporter.extract().drop_duplicates(['driverId', 'constructorId']).dropna()
        identifier = 1
        for value in df.values:
            driver_constructor = DriverConstructor()
            driver_constructor.id = identifier
            driver_constructor.driver_id = int(value[0])
            driver_constructor.constructor_id = int(value[1])
            identifier += 1

            yield driver_constructor


def get_drivers_constructors_transformer() -> DriversConstructorsTransformer:
    exporter = Extractor('results.csv', ['driverId', 'constructorId'])

    return DriversConstructorsTransformer(exporter)


class RaceDriversResultsTransformer(RelatedModelTransformer):
    @override
    def transform_to_model(self) -> Generator[RaceDriverResult, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            race_driver_result = RaceDriverResult()
            driver_constructor = find_driver_constructor_id(
                self.related_model_data,
                int(df.loc[i, 'driverId']),
                int(df.loc[i, 'constructorId'])
            )

            fastest_lap_time = convert_time_to_ms(str(df.loc[i, 'fastestLapTime']))

            race_driver_result.id = int(df.loc[i, 'resultId'])
            race_driver_result.driver_constructor_id = driver_constructor
            race_driver_result.points = float(df.loc[i, 'points'])
            race_driver_result.fastest_lap_time = fastest_lap_time
            race_driver_result.round_id = int(df.loc[i, 'raceId'])
            if "\\N" == df.loc[i, 'position']:
                race_driver_result.position = 0
            else:
                race_driver_result.position = int(df.loc[i, 'position'])

            if "\\N" == df.loc[i, 'fastestLapSpeed']:
                race_driver_result.fastest_lap_speed = 0.0
            else:
                race_driver_result.fastest_lap_speed = float(df.loc[i, 'fastestLapSpeed'])

            yield  race_driver_result


def get_race_drivers_results_transformer():
    exporter = Extractor('results.csv', [
        'resultId',
        'raceId',
        'driverId',
        'constructorId',
        'points',
        'fastestLapTime',
        'fastestLapSpeed',
        'position'
    ])

    return RaceDriversResultsTransformer(exporter, create_drivers_constructors_dataframe())


class RaceConstructorsResultsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[RaceConstructorResult, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            race_constructor_result = RaceConstructorResult()
            race_constructor_result.id = int(df.loc[i, 'constructorResultsId'])
            race_constructor_result.constructor_id = int(df.loc[i, 'constructorId'])
            race_constructor_result.round_id = int(df.loc[i, 'raceId'])
            race_constructor_result.points = float(df.loc[i, 'points'])

            yield race_constructor_result


def get_race_constructors_results_transformer():
    exporter = Extractor('constructor_results.csv', [
        'constructorResultsId',
        'raceId',
        'constructorId',
        'points',
    ])

    return RaceConstructorsResultsTransformer(exporter)


class QualifyingResultsTransformer(RelatedModelTransformer):
    @override
    def transform_to_model(self) -> Generator[QualifyingResult, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            qualifying_result = QualifyingResult()
            driver_constructor = find_driver_constructor_id(
                self.related_model_data,
                int(df.loc[i, 'driverId']),
                int(df.loc[i, 'constructorId'])
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
    exporter = Extractor('qualifying.csv', [
        'qualifyId',
        'raceId',
        'driverId',
        'constructorId',
        'q1',
        'q2',
        'q3',
        'position'
    ])

    return QualifyingResultsTransformer(exporter, create_drivers_constructors_dataframe())


class LapTimesTransformer(RelatedModelTransformer):
    @override
    def transform_to_model(self):
        df = self.exporter.extract()
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
    exporter = Extractor('lap_times.csv', ['raceId', 'driverId', 'lap', 'position', 'milliseconds'])

    return LapTimesTransformer(exporter, create_drivers_constructors_dataframe())


class DriversStandingsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[RaceDriverStandings, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            race_driver_standings = RaceDriverStandings()
            race_driver_standings.id = i
            race_driver_standings.points = float(df.loc[i, 'sum_points'])
            race_driver_standings.position = int(df.loc[i, 'wdc_position'])
            race_driver_standings.wins = int(df.loc[i, 'wins'])
            race_driver_standings.year = int(df.loc[i, 'year'])
            race_driver_standings.driver_constructor_id = int(df.loc[i, 'driver_constructor_id'])

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
        COUNT(dr2.id) AS wins
    FROM race_driver_result dr
    LEFT JOIN race_driver_result dr2 ON dr2.id = dr.id AND dr2.position = 1
    JOIN round r ON r.id = dr.round_id
    JOIN driver_constructor dc ON dc.id = dr.driver_constructor_id
    WHERE r.year = {}
    GROUP BY dc.id
    """

    Connection = get_connection()

    full_standings = {}
    for year in range(1950, 2023):
        standings_query = text(standings_statement.format(year))
        wins_query = text(wins_statement.format(year))

        with Connection() as conn:
            standings, wins = conn.execute(standings_query), conn.execute(wins_query)

        standings_df = pd.DataFrame(standings.fetchall())
        wins_df = pd.DataFrame(wins.fetchall())
        standings_df.keys = standings.columns
        wins_df.keys = wins.columns

        full_standings.update(standings_df.join(wins_df.set_index('driver_constructor_id'), on='driver_constructor_id', lsuffix='_').dropna()[['driver_constructor_id', 'year', 'sum_points', 'wdc_position', 'wins']].to_dict())

    return DriversStandingsTransformer(DirectDataFrameExtractor(pd.DataFrame.from_dict(full_standings)))

class ConstructorsStandingsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[RaceConstructorStandings, None, None]:
        df = self.exporter.extract()
        for i in range(len(df)):
            race_constructor_standings = RaceConstructorStandings()
            race_constructor_standings.id = i
            race_constructor_standings.points = float(df.loc[i, 'sum_points'])
            race_constructor_standings.position = int(df.loc[i, 'wcc_position'])
            race_constructor_standings.wins = int(df.loc[i, 'wins'])
            race_constructor_standings.year = int(df.loc[i, 'year'])
            race_constructor_standings.constructor_id = int(df.loc[i, 'constructor_id'])

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
        COUNT(dr2.id) AS wins
    FROM race_driver_result dr
    LEFT JOIN race_driver_result dr2 ON dr2.id = dr.id AND dr2.position = 1
    JOIN round r ON r.id = dr.round_id
    JOIN driver_constructor dc ON dc.id = dr.driver_constructor_id
    WHERE r.year = {}
    GROUP BY dc.constructor_id
    """

    Connection = get_connection()
    full_standings = {}
    for year in range(1950, 2023):
        standings_query = text(standings_statement.format(year))
        wins_query = text(wins_statement.format(year))

        with Connection() as conn:
            standings, wins = conn.execute(standings_query), conn.execute(wins_query)

        standings_df = pd.DataFrame(standings.fetchall())
        wins_df = pd.DataFrame(wins.fetchall())
        standings_df.keys = standings.columns
        wins_df.keys = wins.columns

        full_standings.update(standings_df.join(wins_df.set_index('constructor_id'), on='constructor_id',lsuffix='_').dropna()[['constructor_id', 'year', 'sum_points', 'wcc_position', 'wins']].to_dict())

    return ConstructorsStandingsTransformer(DirectDataFrameExtractor(pd.DataFrame.from_dict(full_standings)))
