WITH dt AS (
    SELECT
        date,
        year,
        month,
        CEIL(DOUBLE(month/3)) AS quarter,
        week_of_year,
        CASE
            WHEN week_of_year = 1 and month = 12 THEN year + 1
            WHEN (week_of_year = 52 OR week_of_year = 53) AND month = 1 THEN year - 1
            ELSE year
        END AS year_week_of_year,
        day_of_week,
        day_of_week_s,
        day_of_year,
        month_start,
        month_end
    FROM (
        SELECT
            date,
            YEAR(date) AS year,
            MONTH(date) AS month,
            WEEKOFYEAR(date) AS week_of_year,
            DATE_FORMAT(date, 'u') AS day_of_week,
            DATE_FORMAT(date, 'EEE') AS day_of_week_s,
            DATE_FORMAT(date, 'D') AS day_of_year,
            DATE_FORMAT(date, 'yyyy-MM-01') AS month_start,
            LAST_DAY(date) AS month_end
        FROM (
            SELECT
                DATE_ADD('1900-01-01', a.pos) AS date
            FROM
                (SELECT posexplode(split(repeat(",", 100000), ","))) a
        ) sub
    ) a
)

INSERT OVERWRITE TABLE d_date
SELECT
    dt.date,
    dt.year,
    dt.month,
    dt.quarter,
    dt.year_week_of_year,
    dt.week_of_year,
    dt.day_of_week,
    dt.day_of_week_s,
    dt.day_of_year,
    x.week_start,
    x.week_end,
    dt.month_start,
    dt.month_end
FROM dt
INNER JOIN (
    SELECT
        year_week_of_year,
        week_of_year,
        min(date) AS week_start,
        min(date) AS week_end,
    FROM dt
    GROUP BY
        year_week_of_year,
        week_of_year

    ) x
    ON x.year_week_of_year = dt.year_week_of_year
    AND x.week_of_year = dt.week_of_year
ORDER BY 1

