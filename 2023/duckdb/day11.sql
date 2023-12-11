CREATE OR REPLACE TABLE input AS
SELECT input_row
FROM
    read_csv(
        '2023/input/day11.txt',
        columns = { 'input_row': 'VARCHAR' },
        delim = '~'
    );

CREATE OR REPLACE TABLE column_expand AS
SELECT
    col_num,
    2 AS pt1_col_num_counter,
    1000000 AS pt2_col_num_counter
FROM (
    SELECT
        unnest(range(len(regexp_extract_all(input_row, '.')))) AS col_num,
        unnest(regexp_extract_all(input_row, '.')) AS column_value
    FROM input
)
GROUP BY col_num
HAVING count(*) = count(*) FILTER (WHERE column_value = '.');

CREATE OR REPLACE TABLE row_expand AS
SELECT
    input_row,
    rowid AS row_num,
    sum(pt1_row_num_counter) OVER (ORDER BY rowid) - 1 AS pt1_row_num,
    sum(pt2_row_num_counter) OVER (ORDER BY rowid) - 1 AS pt2_row_num,
    unnest(range(len(regexp_extract_all(input_row, '.')))) AS col_num,
    unnest(regexp_extract_all(input_row, '.')) AS column_value
FROM (
    SELECT
        rowid,
        case when regexp_matches(input_row, '^[.]+$') then 2 else 1 end AS pt1_row_num_counter,
        case when regexp_matches(input_row, '^[.]+$') then 1000000 else 1 end AS pt2_row_num_counter,
        input_row
    FROM input
);

CREATE OR REPLACE TABLE expanded_universe AS
SELECT
    r.row_num,
    r.pt1_row_num,
    r.pt2_row_num,
    r.col_num,
    r.column_value,
    sum(coalesce(c.pt1_col_num_counter, 1)) OVER (PARTITION BY r.row_num ORDER BY r.col_num)
    - 1 AS pt1_col_num,
    sum(coalesce(c.pt2_col_num_counter, 1)) OVER (PARTITION BY r.row_num ORDER BY r.col_num)
    - 1 AS pt2_col_num
FROM row_expand AS r
LEFT OUTER JOIN column_expand AS c
    ON r.col_num = c.col_num;

SELECT
    sum(pt1_distance) AS part_1,
    sum(pt2_distance) AS part_2
FROM (
    SELECT
        abs(a.pt1_row_num - b.pt1_row_num)
        + abs(a.pt1_col_num - b.pt1_col_num) AS pt1_distance,
        abs(a.pt2_row_num - b.pt2_row_num)
        + abs(a.pt2_col_num - b.pt2_col_num) AS pt2_distance
    FROM expanded_universe AS a
    CROSS JOIN expanded_universe AS b
    WHERE
        a.column_value = '#'
        AND b.column_value = '#'
        AND ((a.row_num + 1) * (a.col_num + 1000001))
        < ((b.row_num + 1) * (b.col_num + 1000001))
        AND (a.row_num, a.col_num) != (b.row_num, b.col_num)
);
