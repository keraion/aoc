-- TODO: fix linting
CREATE OR REPLACE TABLE input AS
FROM
    read_csv(
        '2023/input/day13.txt',
        columns = { 'input_row': 'VARCHAR' },
        delim = '~'
    );


CREATE OR REPLACE TABLE hrows AS
SELECT
    *,
    row_number() OVER (PARTITION BY pattern ORDER BY rowid) AS r,
    count(*) OVER (PARTITION BY pattern) AS cnt
FROM (
    SELECT
        rowid,
        input_row,
        sum(CASE WHEN input_row IS null THEN 1 ELSE 0 END)
            OVER (ORDER BY rowid)
        AS pattern,
        lead(input_row) OVER (ORDER BY rowid) AS next_row
    FROM input
)
WHERE input_row IS NOT null;

CREATE OR REPLACE TABLE horizontals AS
SELECT *
FROM (
    SELECT
        *,
        unnest(range(r, 0, -1)) AS top_rows,
        unnest(range(r + 1, cnt + 1)) AS bottom_rows,
        least(len(range(r + 1, cnt + 1)), len(range(r, 0, -1))) AS mir_len
    FROM hrows
)
WHERE top_rows IS NOT null AND bottom_rows IS NOT null;

CREATE OR REPLACE TABLE vcol AS
SELECT
    *,
    regexp_extract_all(input_row, '.')[
        1:unnest(range(len(regexp_extract_all(input_row, '.')) - 1)) + 1
    ] AS col,
    regexp_extract_all(input_row, '.')[
        unnest(range(len(regexp_extract_all(input_row, '.')) - 1))
        + 2:len(regexp_extract_all(input_row, '.'))
        + 1
    ] AS col_rev
FROM hrows;

CREATE OR REPLACE TABLE vcol_2 AS
SELECT
    *,
    len(col) AS col_pos,
    list_reverse(col)[1:least(len(col), len(col_rev))] AS this_col,
    col_rev[1:least(len(col), len(col_rev))] AS next_col
FROM vcol
WHERE this_col = next_col;

SELECT
    'Part 1' AS part,
    sum(points) AS answer
FROM (
    SELECT
        a.pattern,
        a.r * 100 AS points
    FROM horizontals AS a
    INNER JOIN hrows AS t
        ON
            a.pattern = t.pattern
            AND a.top_rows = t.r
    INNER JOIN hrows AS b
        ON
            a.pattern = b.pattern
            AND a.bottom_rows = b.r
            AND t.input_row = b.input_row
    GROUP BY ALL
    HAVING count(*) = min(a.mir_len)

    UNION ALL

    SELECT
        pattern,
        col_pos
    FROM vcol_2
    GROUP BY ALL
    HAVING count(*) = max(cnt)
);

CREATE
OR REPLACE TABLE input AS
FROM
    read_csv(
        '2023/input/day13.txt',
        columns = { 'input_row': 'VARCHAR' },
        delim = '~'
    );


CREATE OR REPLACE TABLE hrows AS
SELECT
    *,
    hamming(input_row, next_row) AS hham
FROM (
    SELECT
        *,
        row_number() OVER (PARTITION BY pattern ORDER BY rowid) AS r,
        count(*) OVER (PARTITION BY pattern) AS cnt
    FROM (
        SELECT
            rowid,
            input_row,
            sum(CASE WHEN input_row IS null THEN 1 ELSE 0 END)
                OVER (ORDER BY rowid)
            AS pattern,
            lead(input_row) OVER (ORDER BY rowid) AS next_row
        FROM input
    )
    WHERE input_row IS NOT null
);

CREATE OR REPLACE TABLE horizontals AS
SELECT *
FROM (
    SELECT
        *,
        unnest(range(r, 0, -1)) AS top_rows,
        unnest(range(r + 1, cnt + 1)) AS bottom_rows,
        least(len(range(r + 1, cnt + 1)), len(range(r, 0, -1))) AS mir_len
    FROM hrows
)
WHERE top_rows IS NOT null AND bottom_rows IS NOT null;

CREATE OR REPLACE TABLE vcol AS
SELECT
    *,
    regexp_extract_all(input_row, '.')[
        1:unnest(range(len(regexp_extract_all(input_row, '.')) - 1)) + 1
    ] AS col,
    regexp_extract_all(input_row, '.')[
        unnest(range(len(regexp_extract_all(input_row, '.')) - 1))
        + 2:len(regexp_extract_all(input_row, '.'))
        + 1
    ] AS col_rev
FROM hrows;

CREATE OR REPLACE TABLE vcol_1 AS
SELECT
    *,
    len(col) AS col_pos,
    list_reverse(col)[1:least(len(col), len(col_rev))] AS this_col,
    col_rev[1:least(len(col), len(col_rev))] AS next_col
FROM vcol;

CREATE OR REPLACE TABLE vcol_2 AS
SELECT
    *,
    hamming(this_col, next_col) AS ham
FROM vcol_1
WHERE
    this_col = next_col
    OR ham = 1;

SELECT
    'Part 2' AS part,
    sum(points) AS answer
FROM (
    SELECT
        a.pattern,
        a.r * 100 AS points
    FROM horizontals AS a
    INNER JOIN hrows AS t
        ON
            a.pattern = t.pattern
            AND a.top_rows = t.r
    INNER JOIN hrows AS b
        ON
            a.pattern = b.pattern
            AND a.bottom_rows = b.r
            AND (
                t.input_row = b.input_row
                OR hamming(t.input_row, b.input_row) = 1
            )
    GROUP BY ALL
    HAVING
        count(*) = first(a.mir_len)
        AND sum((hamming(t.input_row, b.input_row) = 1)::int) = 1

    UNION ALL

    SELECT
        pattern,
        col_pos
    FROM vcol_2
    GROUP BY ALL
    HAVING
        count(*) = first(cnt)
        AND sum(ham) = 1
)
