CREATE OR REPLACE TABLE input_file as 
FROM read_csv('2023/input/day01.txt', columns = {'input_row': 'VARCHAR'}, delim ='~')
;

WITH tab_num AS (
    SELECT [
        'one',
        'two',
        'three',
        'four',
        'five',
        'six',
        'seven',
        'eight',
        'nine'
    ] AS x
),

num_ref AS (
    SELECT
        unnest(x) AS n_str,
        unnest(range(1, 10)) AS n
    FROM tab_num
    UNION ALL
    SELECT
        unnest(range(1, 10)) AS n_str,
        unnest(range(1, 10)) AS n
)

SELECT
    'Part 1' AS part,
    sum(
        (
            regexp_replace(input_row, '[^\d]+', '', 'g')[1]
            || regexp_replace(input_row, '[^\d]+', '', 'g')[-1]
        )::int
    ) AS answer
FROM
    input_file

UNION ALL

SELECT
    'Part 2' AS part,
    sum(n1.n * 10 + n2.n) AS answer
FROM input_file AS i
INNER JOIN num_ref AS n1
    ON
        regexp_extract(
            i.input_row, '\d|' || (SELECT array_to_string(x, '|') FROM tab_num)
        ) = n1.n_str
INNER JOIN num_ref AS n2
    ON regexp_extract(
        reverse(i.input_row),
        '\d|' || (SELECT reverse(array_to_string(x, '|')) FROM tab_num)
    ) = reverse(n2.n_str);
