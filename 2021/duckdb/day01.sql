WITH tab_increases AS (
    SELECT *
    FROM
        read_csv(
            '2021/input/day01.txt',
            columns = { 'input_row': 'VARCHAR' }, delim = '~'
        )
    QUALIFY input_row::int - LAG(input_row::int) OVER () > 0
),

tab_window_sum AS (
    SELECT
        sum(input_row::int)
            OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        AS total
    FROM
        read_csv(
            '2021/input/day01.txt', 
            columns = { 'input_row': 'VARCHAR' }, delim = '~'
        )
    QUALIFY len(list(input_row::int) 
        OVER (ROWS between 2 PRECEDING AND CURRENT ROW)) = 3
),

tab_window_increases AS (
    SELECT total - lag(total) OVER ()
    FROM tab_window_sum
    QUALIFY total - LAG(total) OVER () > 0
)

SELECT
    'Part 1' AS part,
    count(*) AS answer
FROM tab_increases

UNION ALL

SELECT
    'Part 2' AS part,
    count(*) AS answer
FROM tab_window_increases;
