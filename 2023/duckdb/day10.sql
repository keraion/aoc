CREATE OR REPLACE TABLE input AS
SELECT DISTINCT repeat(' ', len(input_row) + 1) AS input_row
FROM
    read_csv(
        '2023/input/day10.txt', columns = { 'input_row': 'VARCHAR' }, delim = '~'
    )

UNION ALL

SELECT ' ' || input_row
FROM
    read_csv(
        '2023/input/day10.txt', columns = { 'input_row': 'VARCHAR' }, delim = '~'
    );


CREATE OR REPLACE TABLE map AS
SELECT
    rowid AS map_row,
    unnest(range(len(regexp_extract_all(input_row, '.')))) AS map_column,
    unnest(regexp_extract_all(input_row, '.')) AS map_value
FROM input;

CREATE OR REPLACE TABLE double_map AS
SELECT
    map_row,
    map_column,
    map_value
FROM map;

CREATE OR REPLACE TABLE mapper AS
WITH RECURSIVE map_nav AS (
    SELECT
        1 AS steps,
        o.map_row AS origin_row,
        o.map_column AS origin_column,
        o.map_value AS origin_value,
        n.map_row AS to_row,
        n.map_column AS to_column,
        n.map_value AS to_value,
        o.map_row + n.map_row AS bonus_row,
        o.map_column + n.map_column AS bonus_col,
        '|' AS bonus_value
    FROM map AS o
    INNER JOIN map AS n
        ON
            n.map_row = o.map_row - 1
            AND o.map_column = n.map_column
            AND n.map_value IN ('|', 'F', '7')
    WHERE o.map_value = 'S'

    UNION ALL

    SELECT
        1 AS steps,
        o.map_row AS origin_row,
        o.map_column AS origin_column,
        o.map_value AS origin_value,
        s.map_row AS to_row,
        s.map_column AS to_column,
        s.map_value AS to_value,
        o.map_row + s.map_row AS bonus_row,
        o.map_column + s.map_column AS bonus_col,
        '|' AS bonus_value
    FROM map AS o
    INNER JOIN map AS s
        ON
            s.map_row = o.map_row + 1
            AND o.map_column = s.map_column
            AND s.map_value IN ('|', 'L', 'J')
    WHERE o.map_value = 'S'

    UNION ALL

    SELECT
        1 AS steps,
        o.map_row AS origin_row,
        o.map_column AS origin_column,
        o.map_value AS origin_value,
        w.map_row AS to_row,
        w.map_column AS to_column,
        w.map_value AS to_value,
        o.map_row + w.map_row AS bonus_row,
        o.map_column + w.map_column AS bonus_col,
        '-' AS bonus_value
    FROM map AS o
    INNER JOIN map AS w
        ON
            o.map_row = w.map_row
            AND w.map_column = o.map_column - 1
            AND w.map_value IN ('-', 'L', 'F')
    WHERE o.map_value = 'S'

    UNION ALL

    SELECT
        1 AS steps,
        o.map_row AS origin_row,
        o.map_column AS origin_column,
        o.map_value AS origin_value,
        e.map_row AS to_row,
        e.map_column AS to_column,
        e.map_value AS to_value,
        o.map_row + e.map_row AS bonus_row,
        o.map_column + e.map_column AS bonus_col,
        '-' AS bonus_value
    FROM map AS o
    INNER JOIN map AS e
        ON
            o.map_row = e.map_row
            AND e.map_column = o.map_column + 1
            AND e.map_value IN ('-', '7', 'J')
    WHERE o.map_value = 'S'

    UNION ALL

    SELECT DISTINCT
        o.steps + 1 AS steps,
        o.to_row AS origin_row,
        o.to_column AS origin_column,
        o.to_value AS origin_value,
        x.map_row AS to_row,
        x.map_column AS to_column,
        x.map_value AS to_value,
        o.to_row + x.map_row AS bonus_row,
        o.to_column + x.map_column AS bonus_col,
        CASE
            WHEN
                x.map_row = o.to_row - 1
                AND o.to_value IN ('|', 'L', 'J')
                AND x.map_value IN ('|', 'F', '7')
                THEN '|'
            WHEN
                x.map_row = o.to_row + 1
                AND o.to_value IN ('|', 'F', '7')
                AND x.map_value IN ('|', 'L', 'J')
                THEN '|'
            WHEN
                x.map_column = o.to_column - 1
                AND o.to_value IN ('-', '7', 'J')
                AND x.map_value IN ('-', 'L', 'F')
                THEN '-'
            WHEN
                x.map_column = o.to_column + 1
                AND o.to_value IN ('-', 'L', 'F')
                AND x.map_value IN ('-', '7', 'J')
                THEN '-'
        END AS bonus_value
    FROM map_nav AS o
    INNER JOIN map AS x
        ON (
            x.map_row = o.to_row - 1
            AND o.to_column = x.map_column
            AND NOT (
                o.origin_row = x.map_row AND o.origin_column = x.map_column
            )
            AND o.to_value IN ('|', 'L', 'J')
            AND x.map_value IN ('|', 'F', '7')
        )

        OR (
            x.map_row = o.to_row + 1
            AND o.to_column = x.map_column
            AND NOT (
                o.origin_row = x.map_row AND o.origin_column = x.map_column
            )
            AND o.to_value IN ('|', 'F', '7')
            AND x.map_value IN ('|', 'L', 'J')
        )

        OR (
            o.to_row = x.map_row
            AND x.map_column = o.to_column - 1
            AND NOT (
                o.origin_row = x.map_row AND o.origin_column = x.map_column
            )
            AND o.to_value IN ('-', '7', 'J')
            AND x.map_value IN ('-', 'L', 'F')
        )

        OR (
            o.to_row = x.map_row
            AND x.map_column = o.to_column + 1
            AND NOT (
                o.origin_row = x.map_row AND o.origin_column = x.map_column
            )
            AND o.to_value IN ('-', 'L', 'F')
            AND x.map_value IN ('-', '7', 'J')
        )

    ANTI JOIN map_nav a
        ON a.origin_row = x.map_row
        AND a.origin_column = x.map_column
        AND a.steps <= o.steps + 1
)

SELECT *
FROM map_nav;

UPDATE double_map AS d
SET map_value = '.'
WHERE NOT EXISTS (
    SELECT 1
    FROM (
        SELECT
            origin_row,
            origin_column
        FROM mapper

        UNION ALL

        SELECT
            to_row,
            to_column
        FROM mapper
    ) AS x
    WHERE
        x.origin_row = d.map_row
        AND x.origin_column = d.map_column
) AND map_value != ' ';

CREATE OR REPLACE TABLE map_input AS
SELECT DISTINCT
    x.map_row,
    x.map_column,
    coalesce(m.bonus_value, x.map_value) AS map_value
FROM (
    SELECT
        x.map_row * 2 AS map_row,
        x.map_column * 2 AS map_column,
        x.map_value
    FROM double_map AS x

    UNION ALL

    SELECT
        x.map_row * 2 AS map_row,
        x.map_column * 2 + 1 AS map_column,
        ' ' AS map_value
    FROM double_map AS x

    UNION ALL

    SELECT
        x.map_row * 2 + 1 AS map_row,
        x.map_column * 2 + 1 AS map_column,
        ' ' AS map_value
    FROM double_map AS x

    UNION ALL

    SELECT
        x.map_row * 2 + 1 AS map_row,
        x.map_column * 2 AS map_column,
        ' ' AS map_value
    FROM double_map AS x
    ORDER BY map_row, map_column
) AS x
LEFT JOIN mapper AS m
    ON
        x.map_row = m.bonus_row
        AND x.map_column = m.bonus_col
ORDER BY x.map_row, x.map_column;

CREATE OR REPLACE TABLE outsider AS
WITH RECURSIVE tabx AS (

    SELECT
        o.map_row AS from_row,
        o.map_column AS from_column,
        o.map_value AS from_value,
        o.map_row AS to_row,
        o.map_column AS to_column,
        o.map_value AS to_value
    FROM map_input AS o
    WHERE (o.map_row, o.map_column) = (0, 0)

    UNION ALL

    SELECT
        o.map_row AS from_row,
        o.map_column AS from_column,
        o.map_value AS from_value,
        n.map_row AS to_row,
        n.map_column AS to_column,
        n.map_value AS to_value
    FROM map_input AS o
    INNER JOIN map_input AS n
        ON (
            n.map_row = o.map_row - 1
            AND o.map_column = n.map_column
        )
        OR (
            n.map_row = o.map_row + 1
            AND o.map_column = n.map_column
        )
        OR (
            o.map_row = n.map_row
            AND n.map_column = o.map_column - 1
        )
        OR (
            o.map_row = n.map_row
            AND n.map_column = o.map_column + 1
        )
    WHERE
        (o.map_row, o.map_column) = (0, 0)
        AND n.map_value NOT IN ('|', 'F', '7', '-', 'J', '7')

    UNION ALL

    SELECT DISTINCT
        o.to_row AS from_row,
        o.to_column AS from_column,
        o.to_value AS from_value,
        n.map_row AS to_row,
        n.map_column AS to_column,
        n.map_value AS to_value

    FROM tabx AS o
    INNER JOIN map_input AS n
        ON (
            (n.map_row = o.to_row - 1 AND o.to_column = n.map_column)
            OR (n.map_row = o.to_row + 1 AND o.to_column = n.map_column)
            OR (o.to_row = n.map_row AND n.map_column = o.to_column - 1)
            OR (o.to_row = n.map_row AND n.map_column = o.to_column + 1)
        )
        AND o.to_value IN ('.', ' ')
        AND n.map_value IN ('.', ' ')

    ANTI JOIN tabx a
        ON a.from_row = n.map_row
        AND a.from_column = n.map_column
)

SELECT DISTINCT
    to_row AS map_row,
    to_column AS map_column,
    to_value AS map_value
FROM tabx;

WITH part_2 AS (

    SELECT *
    FROM map_input
    WHERE map_value = '.'

    EXCEPT ALL

    SELECT *
    FROM outsider
    WHERE map_value = '.'
)

SELECT
    'Part 1' AS part,
    max(steps) AS answer
FROM (
    SELECT
        origin_row,
        origin_column,
        min(steps) AS steps
    FROM mapper
    GROUP BY ALL
)

UNION ALL

SELECT
    'Part 2' AS part,
    count(*) AS answer
FROM part_2;

-- File with visualization of expanded map
/*
COPY (
SELECT
    array_to_string(list(map_value), '')
FROM map_input
GROUP BY map_row
ORDER BY map_row
)
TO '2023/day10_scratch.txt' (HEADER FALSE);
*/
