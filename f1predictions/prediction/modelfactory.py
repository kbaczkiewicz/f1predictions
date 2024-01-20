from f1predictions.orm.query import DriverQuery
from f1predictions.prediction.model import DriverRatingModel
from f1predictions.orm.dbal.view import *


class DriverRatingsModelFactory():
    def __init__(self, driver_query: DriverQuery):
        self._driver_query = driver_query

    def create_driver_ratings_model(self, driver_id: int, year: int) -> DriverRatingModel:
        df = self._build_dataframe(driver_id, year)
        driver = self._driver_query.get_driver(driver_id)
        h2h_qualifying = self.calculate_head_to_head_qualifying_result(df)

        return DriverRatingModel(
            driver.id,
            driver.name,
            driver.surname,
            df['wins'].iloc[0],
            df['season_position'].iloc[0],
            df['avg_qualifying_position'].iloc[0],
            df['q2_appearances'].iloc[0],
            df['q3_appearances'].iloc[0],
            df['pole_positions'].iloc[0],
            df['front_row_second'].iloc[0],
            df['podiums'].iloc[0],
            df['dnfs'].iloc[0],
            h2h_qualifying,
            0 if 0 == df['season_points_os'].iloc[0] else 100 * df['season_points'].iloc[0] / (
                    df['season_points'].iloc[0] + df['season_points_os'].iloc[0])
        )

    @staticmethod
    def _build_dataframe(driver_id, year) -> pd.DataFrame:
        return get_drivers_rounds_results(driver_id, year) \
            .join(get_opponents_rounds_results(driver_id, year).set_index('driver_id'), on='driver_id', rsuffix='_o') \
            .join(get_drivers_seasons_results(driver_id, year).set_index('driver_id'), on='driver_id', rsuffix='_s') \
            .join(get_opponents_seasons_results(driver_id, year).set_index('driver_id'), on='driver_id', rsuffix='_os')

    @staticmethod
    def calculate_head_to_head_qualifying_result(df) -> bool:
        return df['q3_appearances'].iloc[0] > df['q3_appearances_o'].iloc[0] \
            if round(df['avg_qualifying_position'].iloc[0]) == (df['avg_qualifying_position_o'].iloc[0]) \
            else round(df['avg_qualifying_position'].iloc[0]) < (df['avg_qualifying_position_o'].iloc[0])
