from abc import abstractmethod, ABC
from typing import Generator, override
from f1predictions.database import get_session
from sqlalchemy import select
import pandas as pd
import sys

from f1predictions.etl.exporter import Exporter
from f1predictions.model import Driver, Constructor, Status, Circuit, Race, Round, DriverConstructor, RaceDriverResult, RaceConstructorResult, RaceDriverStandings, RaceConstructorStandings, LapTimes, QualifyingResult


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
            round = Round()
            round.id = i
            round.race_id = int(self.related_model_data.where(self.related_model_data['name'] == str(race_name)).drop_duplicates().dropna().values[0][0])
            round.round_number = int(df.loc[i, 'round'])
            round.year = int(df.loc[i, 'year'])
            yield round



def get_rounds_transformer() -> RacesTransformer:
    Session = get_session()
    exporter = Exporter('races.csv', ['name', 'round', 'year'])
    with Session() as session:
        races = session.scalars(select(Race))
        data = [(i.id, i.name) for i in races]
        races_dataframe = pd.DataFrame({'id': [i[0] for i in data], 'name': [i[1] for i in data]})

        return RoundsTransformer(exporter, races_dataframe)
    

class DriversConstructorsTransformer(Transformer):
    @override
    def transform_to_model(self) -> Generator[DriverConstructor, None, None]:
        df = self.exporter.export().drop_duplicates(['driverId', 'constructorId']).dropna()
        id = 1
        for value in df.values:
            driver_constructor = DriverConstructor()
            driver_constructor.id = id
            driver_constructor.driver_id = int(value[0])
            driver_constructor.constructor_id = int(value[1])
            id += 1

            yield driver_constructor


def get_drivers_constructors_transformer() -> DriversConstructorsTransformer:
    exporter = Exporter('results.csv', ['driverId', 'constructorId'])
    
    return DriversConstructorsTransformer(exporter)


class RaceDriversResultsTransformer(RelatedModelTransformer):
    @override
    def transform_to_model(self) -> Generator[Race, None, None]:
        df = self.exporter.export()
        for i in range(len(df)):
            race_driver_result = RaceDriverResult()
            driver_constructor = self.related_model_data.where \
                (self.related_model_data['driverId'] == df.loc[i, 'driverId'] and self.related_model_data['constructorId'] == df.loc[i, 'constructorId'])
            
            race_driver_result.id = i + 1
            race_driver_result.driver_constructor_id = driver_constructor.values[0][0]
            race_driver_result.round_id
            
            yield  race_driver_result