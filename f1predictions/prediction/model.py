class DriverRatingModel:
    driver_id: int
    name: str
    surname: str
    wins: int
    position: int
    average_quali_position: float
    q2s: int
    q3s: int
    poles: int
    front_rows: int
    podiums: int
    dnfs: int
    head_to_head_qualifying: bool
    percentage_constructor_points: float

    def __init__(
            self,
            driver_id,
            name,
            surname,
            wins,
            position,
            average_quali_position,
            q2s,
            q3s,
            poles,
            front_rows,
            podiums,
            dnfs,
            head_to_head_qualifying,
            percentage_constructor_points
    ):
        self.driver_id = driver_id
        self.name = name
        self.surname = surname
        self.wins = int(wins)
        self.position = int(position)
        self.average_quali_position = float(average_quali_position)
        self.q2s = int(q2s)
        self.q3s = int(q3s)
        self.poles = int(poles)
        self.front_rows = int(front_rows)
        self.podiums = int(podiums)
        self.dnfs = int(dnfs)
        self.head_to_head_qualifying = bool(head_to_head_qualifying)
        self.percentage_constructor_points = float(percentage_constructor_points)

    def to_list(self) -> list:
        return [
            self.wins,
            self.position,
            self.average_quali_position,
            self.q2s,
            self.q3s,
            self.poles,
            self.front_rows,
            self.podiums,
            self.dnfs,
            self.head_to_head_qualifying,
            self.percentage_constructor_points
        ]

