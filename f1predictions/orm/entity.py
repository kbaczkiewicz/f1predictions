from typing import Optional
from sqlalchemy import ForeignKey, String, Integer, Float
import sqlalchemy.orm as orm
from f1predictions.orm.config.database import Base


class Driver(Base):
    __tablename__ = 'driver'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(String(255))
    surname: orm.Mapped[str] = orm.mapped_column(String(255))


class Status(Base):
    __tablename__ = 'status'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    status: orm.Mapped[str] = orm.mapped_column(String(255))


class Constructor(Base):
    __tablename__ = 'constructor'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(String(255))


class DriverConstructor(Base):
    __tablename__ = 'driver_constructor'
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    year: orm.Mapped[int] = orm.mapped_column(Integer)

    driver_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('driver.id'))
    constructor_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('constructor.id'))


class Circuit(Base):
    __tablename__ = 'circuit'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(String(255))


class Race(Base):
    __tablename__ = 'race'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[String] = orm.mapped_column(String(50))

    circuit_id: orm.Mapped[int] = orm.mapped_column(ForeignKey("circuit.id"))


class Round(Base):
    __tablename__ = 'round'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    year: orm.Mapped[int] = orm.mapped_column(Integer)
    round_number: orm.Mapped[int] = orm.mapped_column(Integer)

    race_id: orm.Mapped[int] = orm.mapped_column(ForeignKey("race.id"))


class QualifyingResult(Base):
    __tablename__ = 'qualifying_result'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    position: orm.Mapped[int] = orm.mapped_column(Integer)
    q1: orm.Mapped[Optional[int]]
    q2: orm.Mapped[Optional[int]]
    q3: orm.Mapped[Optional[int]]

    round_id: orm.Mapped[int] = orm.mapped_column(ForeignKey("round.id"))
    driver_constructor_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('driver_constructor.id'))


class RaceDriverResult(Base):
    __tablename__ = 'race_driver_result'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    points: orm.Mapped[float] = orm.mapped_column(Float)
    position: orm.Mapped[int] = orm.mapped_column(Integer)
    fastest_lap_time: orm.Mapped[int] = orm.mapped_column(Integer)
    fastest_lap_speed: orm.Mapped[float] = orm.mapped_column(Float)

    driver_constructor_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('driver_constructor.id'))
    round_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('round.id'))
    status_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('status.id'))


class RaceConstructorResult(Base):
    __tablename__ = 'race_constructor_result'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    points: orm.Mapped[float] = orm.mapped_column(Float)

    constructor_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('constructor.id'))
    round_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('round.id'))


class RaceDriverStandings(Base):
    __tablename__ = 'race_driver_standings'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    year: orm.Mapped[int] = orm.mapped_column(Integer)
    points: orm.Mapped[float] = orm.mapped_column(Float)
    position: orm.Mapped[int] = orm.mapped_column(Integer)
    wins: orm.Mapped[int] = orm.mapped_column(Integer)

    driver_constructor_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('driver_constructor.id'))


class RaceConstructorStandings(Base):
    __tablename__ = 'race_constructor_standings'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    year: orm.Mapped[int] = orm.mapped_column(Integer)
    points: orm.Mapped[float] = orm.mapped_column(Float)
    position: orm.Mapped[int] = orm.mapped_column(Integer)
    wins: orm.Mapped[int] = orm.mapped_column(Integer)

    constructor_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('constructor.id'))


class LapTimes(Base):
    __tablename__ = 'lap_time'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    lap: orm.Mapped[int] = orm.mapped_column(Integer)
    position: orm.Mapped[int] = orm.mapped_column(Integer)
    time: orm.Mapped[int] = orm.mapped_column(Integer)

    driver_constructor_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('driver_constructor.id'))
    round_id: orm.Mapped[int] = orm.mapped_column(ForeignKey('round.id'))


class DriverRating(Base):
    __tablename__ = 'driver_rating'

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    rating: orm.Mapped[float] = orm.mapped_column(Float)
    year: orm.Mapped[int] = orm.mapped_column(Integer)

    driver_id = orm.mapped_column(ForeignKey('driver.id'))
