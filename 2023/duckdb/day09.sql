CREATE OR REPLACE TABLE input AS
from read_csv('2023/input/day09.txt', columns = {'input_row': 'VARCHAR'}, delim ='~')
;

CREATE OR REPLACE TABLE item_patterns AS

WITH RECURSIVE pattern_generator AS (

    SELECT
        rowid,
        1 AS n,
        unnest(
            range(len(regexp_extract_all(input_row, '-?\d+')))
        ) AS item_number,
        unnest(regexp_extract_all(input_row, '-?\d+'))::int AS current_item,
        unnest(
            array_pop_front(regexp_extract_all(input_row, '-?\d+'))
        )::int AS next_item,
        (
            unnest(array_pop_front(regexp_extract_all(input_row, '-?\d+')))::int
            - unnest(regexp_extract_all(input_row, '-?\d+'))::int
        ) AS diff
    FROM input

    UNION ALL

    SELECT
        x.rowid,
        x.n + 1 AS n,
        x.item_number,
        x.diff,
        lead(x.diff) OVER (PARTITION BY x.rowid, x.n ORDER BY x.item_number),
        (
            lead(x.diff) OVER (PARTITION BY x.rowid, x.n ORDER BY x.item_number)
            - x.diff
        )
    FROM pattern_generator AS x
    QUALIFY bool_or(coalesce(x.diff, 0) != 0) OVER (PARTITION BY x.rowid, x.n)
)

SELECT
    rowid,
    n,
    row_number() OVER (PARTITION BY rowid ORDER BY n DESC) AS _rn,
    list(next_item ORDER BY item_number)[-1] AS last_item,
    list(current_item ORDER BY item_number)[1] AS first_item
FROM pattern_generator
WHERE next_item IS NOT null
GROUP BY rowid, n;


WITH RECURSIVE subtractor AS (
    SELECT
        *,
        last_item AS diff
    FROM item_patterns
    WHERE _rn = 1

    UNION ALL

    SELECT
        f.*,
        f.first_item - x.diff AS diff
    FROM subtractor AS x
    INNER JOIN item_patterns AS f
        ON
            x.rowid = f.rowid
            AND x._rn + 1 = f._rn
)

SELECT
    'Part 1' AS part,
    sum(last_item) AS answer
FROM item_patterns

UNION ALL

SELECT
    'Part 2' AS part,
    sum(diff) AS answer
FROM subtractor
WHERE n = 1;