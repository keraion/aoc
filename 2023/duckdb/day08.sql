CREATE OR REPLACE TABLE input AS
FROM read_csv('2023/day08.txt', columns = {'input_row': 'VARCHAR'}, delim ='~')
;

CREATE OR REPLACE TABLE steps AS 
SELECT
    unnest(input_row.regexp_extract_all('\w')) AS dir,
    generate_subscripts(input_row.regexp_extract_all('\w'), 1) - 1 AS step,
    len(input_row.regexp_extract_all('\w')) AS mod_steps
FROM input
WHERE rowid = 0;

CREATE OR REPLACE TABLE map AS 
SELECT
    regexp_extract_all(input_row, '\w{3}')[1] AS node,
    regexp_extract_all(input_row, '\w{3}')[2] AS lft,
    regexp_extract_all(input_row, '\w{3}')[3] AS rt
FROM input WHERE rowid > 1;

CREATE OR REPLACE TABLE part_1 AS

WITH RECURSIVE nodes AS (
    SELECT
        0 AS n,
        m.node,
        CASE s.dir WHEN 'R' THEN m.rt ELSE m.lft END AS to_node

    FROM map AS m
    CROSS JOIN steps AS s
    WHERE
        m.node = 'AAA'
        AND s.step = 0

    UNION ALL

    SELECT
        d.n + 1 AS n,
        d.to_node AS node,
        CASE s.dir WHEN 'R' THEN m.rt ELSE m.lft END AS to_node
    FROM nodes AS d
    INNER JOIN map AS m
        ON d.to_node = m.node
    INNER JOIN steps AS s
        ON
            s.step = ((d.n + 1) % s.mod_steps)
            AND d.to_node != 'ZZZ'
)

SELECT
    'Part 1' AS part,
    count(*) AS answer
FROM nodes;

CREATE OR REPLACE TABLE complete_steps AS 

WITH RECURSIVE nodes AS (
    SELECT
        0 AS n,
        m.node AS start_node,
        m.node,
        CASE s.dir WHEN 'R' THEN m.rt ELSE m.lft END AS to_node
    FROM map AS m
    CROSS JOIN steps AS s
    WHERE
        m.node LIKE '%A'
        AND s.step = 0
        AND m.node NOT LIKE '%Z'

    UNION ALL

    SELECT
        d.n + 1 AS n,
        d.start_node,
        d.to_node AS node,
        CASE s.dir
            WHEN 'R' THEN m.rt
            ELSE m.lft
        END AS to_node
    FROM nodes AS d
    INNER JOIN map AS m
        ON
            d.to_node = m.node
            AND m.node NOT LIKE '%Z'
    INNER JOIN steps AS s
        ON s.step = ((d.n + 1) % s.mod_steps)
)


SELECT max(n) + 1 AS done_step
FROM nodes
GROUP BY start_node;

WITH RECURSIVE tabx AS (
    SELECT
        rowid,
        done_step::int64 AS done_step
    FROM complete_steps
    WHERE rowid = 0

    UNION ALL

    SELECT
        c.rowid,
        lcm(c.done_step, x.done_step) AS done_step
    FROM tabx AS x
    INNER JOIN complete_steps AS c
        ON x.rowid + 1 = c.rowid
)

SELECT
    part,
    answer
FROM part_1

UNION ALL

SELECT
    'Part 2' AS part,
    max(done_step) AS answer
FROM tabx;
