from sqlalchemy import text

drivers_seasons_results_view = text(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS drivers_seasons_results_view AS
            SELECT dc.driver_id,
                   dc.year,
                   SUM(rds.points)   AS season_points,
                   AVG(rds.position) AS season_position,
                   SUM(rds.wins)     as wins
            FROM race_driver_standings rds
                     JOIN driver_constructor dc ON rds.driver_constructor_id = dc.id
            GROUP BY dc.year, dc.driver_id
            ORDER BY dc.year
        WITH DATA
        """
)

opponents_seasons_results_view = text(
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS opponents_seasons_results_view AS
        SELECT d.driver_id,
               d.year,
               SUM(rds.points)   AS season_points,
               AVG(rds.position) AS season_position,
               SUM(rds.wins)     as wins
        FROM driver_constructor d
                 JOIN driver_constructor o
                      ON d.constructor_id = o.constructor_id AND d.driver_id != o.driver_id AND d.year = o.year
                 JOIN race_driver_standings rds ON rds.driver_constructor_id = o.id
        GROUP BY d.year, d.driver_id
        ORDER BY d.year
    WITH DATA
    """
)

drivers_rounds_results_view = text(
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS drivers_rounds_results_view AS
        SELECT dc.driver_id,
               dc.year,
               AVG(qs.position)                                                        AS avg_qualifying_position,
               SUM(CASE WHEN qs.q2 != 0 THEN 1 ELSE 0 END)                             AS q2_appearances,
               SUM(CASE WHEN qs.q3 != 0 THEN 1 ELSE 0 END)                             AS q3_appearances,
               COALESCE(SUM(qs1.position), 0)                                          AS pole_positions,
               COALESCE(SUM(qs2.position), 0)                                          AS front_row_second,
               SUM(CASE WHEN rdr.position <= 3 AND rdr.position > 0 THEN 1 ELSE 0 END) AS podiums,
               COUNT(rdr2.id)                                                          as dnfs
        FROM qualifying_result qs
                 JOIN race_driver_result rdr
                      ON qs.round_id = rdr.round_id AND rdr.driver_constructor_id = qs.driver_constructor_id
                 LEFT JOIN race_driver_result rdr2 ON rdr2.id = rdr.id AND rdr2.status_id NOT IN (SELECT id
                                                                                                  FROM status
                                                                                                  WHERE status = 'Finished'
                                                                                                     OR status.status LIKE '%Lap%')
                 JOIN driver_constructor dc ON qs.driver_constructor_id = dc.id
                 LEFT JOIN qualifying_result qs1 ON qs1.id = qs.id AND qs1.position = 1
                 LEFT JOIN qualifying_result qs2 ON qs2.id = qs.id AND qs2.position = 2
        GROUP BY dc.year, dc.driver_id
        ORDER BY dc.year
    """
)

opponents_rounds_results_view = text(
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS opponents_rounds_results_view AS
        SELECT d.driver_id,
               d.year,
               AVG(qs.position)                                                        AS avg_qualifying_position,
               SUM(CASE WHEN qs.q2 != 0 THEN 1 ELSE 0 END)                             AS q2_appearances,
               SUM(CASE WHEN qs.q3 != 0 THEN 1 ELSE 0 END)                             AS q3_appearances,
               COALESCE(SUM(qs1.position), 0)                                          AS pole_positions,
               COALESCE(SUM(qs2.position), 0)                                          AS front_row_second,
               SUM(CASE WHEN rdr.position <= 3 AND rdr.position > 0 THEN 1 ELSE 0 END) AS podiums,
               COUNT(rdr2.id)                                                          as dnfs
        FROM driver_constructor d
                 JOIN driver_constructor o
                      ON d.constructor_id = o.constructor_id AND d.driver_id != o.driver_id AND d.year = o.year
                 JOIN qualifying_result qs ON qs.driver_constructor_id = o.id
                 JOIN race_driver_result rdr
                      ON qs.round_id = rdr.round_id AND rdr.driver_constructor_id = qs.driver_constructor_id
                 LEFT JOIN race_driver_result rdr2 ON rdr2.id = rdr.id AND rdr2.status_id NOT IN (SELECT id
                                                                                                  FROM status
                                                                                                  WHERE status = 'Finished'
                                                                                                     OR status.status LIKE '%Lap%')
                 LEFT JOIN qualifying_result qs1 ON qs1.id = qs.id AND qs1.position = 1
                 LEFT JOIN qualifying_result qs2 ON qs2.id = qs.id AND qs2.position = 2
        GROUP BY d.year, d.driver_id
        ORDER BY d.year
    WITH DATA
    """
)
