from abc import ABC
from typing import Sequence

from sqlalchemy import Select, Row
from sqlalchemy.orm import sessionmaker

from f1predictions.orm.entity import DriverRating, Driver


class Query(ABC):
    def __init__(self, session: sessionmaker):
        self._sessionmaker = session


class DriverQuery(Query):
    def get_drivers(self) -> Sequence[Driver]:
        with self._sessionmaker() as session:
            return session.scalars(Select(Driver)).all()

    def get_driver(self, driver_id: int) -> Driver:
        with self._sessionmaker() as session:
            return session.scalars(Select(Driver).filter_by(id=driver_id)).one_or_none()


class DriverRatingQuery(Query):
    def get_drivers_ratings(self) -> Sequence[DriverRating]:
        with self._sessionmaker() as session:
            return session.scalars(Select(DriverRating)).all()

    def get_driver_rating(self, driver_id: int, year: int) -> DriverRating:
        with self._sessionmaker() as session:
            return session.scalars(Select(DriverRating).filter_by(driver_id=driver_id, year=year)).one_or_none()

    def get_driver_rating_by_year(self, year: int) -> Sequence[DriverRating]:
        with self._sessionmaker() as session:
            return session.scalars(Select(DriverRating).filter_by(year=year)).all()
