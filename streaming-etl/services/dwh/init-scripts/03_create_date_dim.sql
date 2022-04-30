CREATE TABLE date_dim (
    date_key int PRIMARY KEY NOT NULL,
    "date" date NOT NULL,
    day_of_week varchar(9) NOT NULL,
    calendar_month varchar(9) NOT NULL,
    weekday_indicator varchar(11) NOT NULL
);

INSERT INTO date_dim (
    SELECT
        CAST(to_char(ts, 'YYYYMMDD') AS int) AS date_key,
        CAST(ts AS date) AS "date",
        CASE date_part('dow', ts)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week,
        trim(to_char(ts, 'Month')) AS calendar_month,
        CASE date_part('dow', ts)
            WHEN 0 THEN 'Weekend'
            WHEN 6 THEN 'Weekend'
            ELSE 'Weekday'
        END AS weekday_indicator
    FROM generate_series(now() - INTERVAL '3 years', now(), '1 day') AS ts
);
