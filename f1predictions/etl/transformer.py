from abc import abstractmethod, ABC
from typing import Generator, override
from f1predictions.database import get_session
from sqlalchemy import select
import pandas as pd

from f1predictions.etl.exporter import Exporter
from f1predictions.model import Driver, Constructor, Status, Circuit, Race, Round


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
        df = self.exporter.export().drop_duplicates().reset_index()
        for i in range(1, len(df)):
            race = Race()
            race.id = i
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
        print('rounds')
        for i in range(len(df)):
            race_name = df.loc[i, 'name']
            print(self.related_model_data.head())
            break;
            round = Round()
            round.id = i
            #round.race_id = int(self.related_model_data.where(self.related_model_data['name'] == str(race_name)).drop_duplicates().reset_index().dropna().loc[0, 'id'])
            round.round_number = df.loc[i, 'round']
            round.year = df.loc[i, 'year']


        yield None



def get_rounds_transformer() -> RacesTransformer:
    Session = get_session()
    exporter = Exporter('races.csv', ['name', 'round', 'year'])
    with Session() as session:
        races = session.scalars(select(Race))
        
        races_dataframe = pd.DataFrame()
        print(races_dataframe.head())

        return RoundsTransformer(exporter, races_dataframe)