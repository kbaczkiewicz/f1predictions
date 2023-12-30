from abc import abstractmethod, ABC
from typing import Generator, override
from f1predictions.database import get_session
from sqlalchemy import select
import pandas as pd
import sys

from f1predictions.etl.exporter import Exporter
from f1predictions.model import Driver, Constructor, Status, Circuit, Race, Round, DriverConstructor, RaceDriverResult, RaceConstructorResult, RaceDriverStandings, RaceConstructorStandings, LapTimes, QualifyingResult
from f1predictions.utils import convert_time_to_ms


class Transformer(ABC):
    def __init__(self, exporter: Exporter):
        self.exporter = exporter

    @abstractmethod
    def transform_to_model(self) -> Generator:
        pass


class RelatedModelTransformer(Transformer, ABC):
    def __init__(self, exporter: Exporter, related_model_data: pd.DataFrame):
        super().__init__(exporter)
        self.related_model_data = related_model_data


class DriversTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Driver, None, None]:
        df = self.exporter.export()
        for i in range(len(df)):
            driver = Driver()
            driver.id = int(df.loc[i, 'driverId'])
            driver.name = str(df.loc[i, 'forename'])
            driver.surname = str(df.loc[i, 'surname'])

            yield driver


def get_drivers_transformer() -> DriversTransformer:
    exporter = Exporter('drivers.csv', ['driverId', 'forename', 'surname'])

    return DriversTransformer(exporter)


class ConstructorsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Constructor, None, None]:
        df = self.exporter.export()
        for i in range(len(df)):
            constructor = Constructor()
            constructor.id = int(df.loc[i, 'constructorId'])
            constructor.name = str(df.loc[i, 'name'])

            yield constructor


def get_constructors_transformer() -> ConstructorsTransformer:
    exporter = Exporter('constructors.csv', ['constructorId', 'name'])

    return ConstructorsTransformer(exporter)


class StatusesTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Status, None, None]:
        df = self.exporter.export()
        for i in range(len(df)):
            status = Status()
            status.id = int(df.loc[i, 'statusId'])
            status.status = str(df.loc[i, 'status'])

            yield status


def get_statuses_transformer() -> StatusesTransformer:
    exporter = Exporter('status.csv', ['statusId', 'status'])

    return StatusesTransformer(exporter)


class CircuitsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Circuit, None, None]:
        df = self.exporter.export()
        for i in range(len(df)):
            circuit = Circuit()
            circuit.id = int(df.loc[i, 'circuitId'])
            circuit.name = str(df.loc[i, 'name'])

            yield circuit


def get_circuits_transformer() -> CircuitsTransformer:
    exporter = Exporter('circuits.csv', ['circuitId', 'name'])

    return CircuitsTransformer(exporter)


class RacesTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[Race, None, None]:
        df = self.exporter.export().drop_duplicates(['name']).reset_index()
        for i in range(len(df)):
            race = Race()
            race.id = i + 1
            race.name = str(df.loc[i, 'name'])
            race.circuit_id = int(df.loc[i, 'circuitId'])

            yield race


def get_races_transformer() -> RacesTransformer:
    exporter = Exporter('races.csv', ['name', 'circuitId'])

    return RacesTransformer(exporter)


class RoundsTransformer(RelatedModelTransformer):
    @override
    def transform_to_model(self) -> Generator[Round, None, None]:
        df = self.exporter.export().drop_duplicates().reset_index()
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
    exporter = Exporter('races.csv', ['raceId', 'name', 'round', 'year'])
    with Session() as session:
        races = session.scalars(select(Race))
        data = [(i.id, i.name) for i in races]
        races_dataframe = pd.DataFrame({'id': [i[0] for i in data], 'name': [i[1] for i in data]})

        return RoundsTransformer(exporter, races_dataframe)


class DriversConstructorsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[DriverConstructor, None, None]:
        df = self.exporter.export().drop_duplicates(['driverId', 'constructorId']).dropna()
        identifier = 1
        for value in df.values:
            driver_constructor = DriverConstructor()
            driver_constructor.id = identifier
            driver_constructor.driver_id = int(value[0])
            driver_constructor.constructor_id = int(value[1])
            identifier += 1

            yield driver_constructor


def get_drivers_constructors_transformer() -> DriversConstructorsTransformer:
    exporter = Exporter('results.csv', ['driverId', 'constructorId'])

    return DriversConstructorsTransformer(exporter)


class RaceDriversResultsTransformer(RelatedModelTransformer):
    @override
    def transform_to_model(self) -> Generator[RaceDriverResult, None, None]:
        df = self.exporter.export()
        for i in range(len(df)):
            race_driver_result = RaceDriverResult()
            driver_constructor = (self.related_model_data
                                  .where(self.related_model_data['driverId'] == int(df.loc[i, 'driverId']))
                                  .where(self.related_model_data['constructorId'] == int(df.loc[i, 'constructorId']))
                                  [['id']].dropna())

            fastest_lap_time = convert_time_to_ms(str(df.loc[i, 'fastestLapTime']))

            race_driver_result.id = int(df.loc[i, 'resultId'])
            race_driver_result.driver_constructor_id = int(driver_constructor.values[0][0])
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
    exporter = Exporter('results.csv', [
        'resultId',
        'raceId',
        'driverId',
        'constructorId',
        'points',
        'fastestLapTime',
        'fastestLapSpeed',
        'position'
    ])

    Session = get_session()
    with Session() as Session:
        drivers_constructors = [(i.id, i.driver_id, i.constructor_id) for i in Session.scalars(select(DriverConstructor))]
        drivers_constructors_df = pd.DataFrame({
            'id': [i[0] for i in drivers_constructors],
            'driverId': [i[1] for i in drivers_constructors],
            'constructorId': [i[2] for i in drivers_constructors]
        })

    return RaceDriversResultsTransformer(exporter, drivers_constructors_df)


class RaceConstructorsResultsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[RaceConstructorResult, None, None]:
        df = self.exporter.export()
        for i in range(len(df)):
            race_constructor_result = RaceConstructorResult()
            race_constructor_result.id = int(df.loc[i, 'constructorResultsId'])
            race_constructor_result.constructor_id = int(df.loc[i, 'constructorId'])
            race_constructor_result.round_id = int(df.loc[i, 'raceId'])
            race_constructor_result.points = float(df.loc[i, 'points'])

            yield race_constructor_result


def get_race_constructors_results_transformer():
    exporter = Exporter('constructor_results.csv', [
        'constructorResultsId',
        'raceId',
        'constructorId',
        'points',
    ])

    return RaceConstructorsResultsTransformer(exporter)


class QualifyingResultsTransformer(RelatedModelTransformer):
    @override
    def transform_to_model(self) -> Generator[QualifyingResult, None, None]:
        df = self.exporter.export()
        for i in range(len(df)):
            qualifying_result = QualifyingResult()
            driver_constructor = (self.related_model_data
                                  .where(self.related_model_data['driverId'] == int(df.loc[i, 'driverId']))
                                  .where(self.related_model_data['constructorId'] == int(df.loc[i, 'constructorId']))
                                  [['id']].dropna())

            qualifying_result.id = int(df.loc[i, 'qualifyId'])
            qualifying_result.round_id = int(df.loc[i, 'raceId'])
            qualifying_result.driver_constructor_id = int(driver_constructor.values[0][0])
            qualifying_result.position = int(df.loc[i, 'position'])
            qualifying_result.q1 = convert_time_to_ms(df.loc[i, 'q1'])
            qualifying_result.q2 = convert_time_to_ms(df.loc[i, 'q2'])
            qualifying_result.q3 = convert_time_to_ms(df.loc[i, 'q3'])

            yield qualifying_result


def get_qualifying_results_transformer():
    exporter = Exporter('qualifying.csv', [
        'qualifyId',
        'raceId',
        'driverId',
        'constructorId',
        'q1',
        'q2',
        'q3',
        'position'
    ])

    Session = get_session()
    with Session() as Session:
        drivers_constructors = [(i.id, i.driver_id, i.constructor_id) for i in Session.scalars(select(DriverConstructor))]
        drivers_constructors_df = pd.DataFrame({
            'id': [i[0] for i in drivers_constructors],
            'driverId': [i[1] for i in drivers_constructors],
            'constructorId': [i[2] for i in drivers_constructors]
        })

    return QualifyingResultsTransformer(exporter, drivers_constructors_df)