from abc import ABC
from typing import Sequence

from sqlalchemy import Select
from sqlalchemy.orm import sessionmaker

from f1predictions.orm.entity import DriverRating, Driver, DriverCategory


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

    def get_drivers_ratings_by_year(self, year: int) -> Sequence[DriverRating]:
        with self._sessionmaker() as session:
            return session.scalars(Select(DriverRating).filter_by(year=year)).all()


class DriverCategoryQuery(Query):
    def get_drivers_categories(self) -> Sequence[DriverCategory]:
        with self._sessionmaker() as session:
            return session.scalars(Select(DriverCategory)).all()

    def get_driver_category(self, driver_id: int, year: int) -> DriverCategory:
        with self._sessionmaker() as session:
            return session.scalars(Select(DriverCategory).filter_by(driver_id=driver_id, year=year)).one_or_none()

    def get_drivers_categories_by_year(self, year: int) -> Sequence[DriverCategory]:
        with self._sessionmaker() as session:
            return session.scalars(Select(DriverCategory).filter_by(year=year)).all()