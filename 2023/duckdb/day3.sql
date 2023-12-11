CREATE OR REPLACE TABLE input AS 
FROM read_csv('2023/day3.txt', columns = {'input_row': 'VARCHAR'}, delim ='~');

WITH row_locations AS (
    SELECT
        rowid AS row_num,
        unnest(
            list_transform(
                regexp_split_to_array(input_row, '[^\w.]'), x -> len(x)
            )
        ) AS part_symbol,
        generate_subscripts(
            regexp_split_to_array(input_row, '[^\w.]'), 1
        ) AS part_symbol_order,
        unnest(regexp_extract_all(input_row, '[^\w.]')) AS part_symbol_value,
        unnest(regexp_extract_all(input_row, '\d+')) AS num_value,
        unnest(
            list_transform(regexp_split_to_array(input_row, '\d+'), x -> len(x))
        ) AS num_symbol,
        unnest(
            list_transform(regexp_extract_all(input_row, '\d+'), x -> len(x))
        ) AS num_symbol_len,
        len(input_row) AS row_len
    FROM input
),

row_column_location AS (
    SELECT
        *,
        (sum(part_symbol) OVER (
            PARTITION BY row_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) + part_symbol_order)::int AS col_num,
        range(row_num - 1, row_num + 2) AS row_range
    FROM row_locations
    QUALIFY sum(part_symbol) OVER (PARTITION BY row_num 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) + part_symbol_order <= row_len
),

symbol_locations_base AS (
    SELECT
        *,
        unnest(row_range) AS row_check,
        range(col_num - 2, col_num + 1) AS col_range
    FROM row_column_location
),

symbol_locations AS (
    SELECT
        *,
        unnest(col_range) AS col_check
    FROM symbol_locations_base
),

number_locations_base AS (
    SELECT
        *,
        (sum(num_symbol + num_symbol_len) OVER (
            PARTITION BY row_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ))::int AS col_num
    FROM row_locations
    WHERE num_symbol_len IS NOT null
),

number_locations AS (
    SELECT
        row_num,
        num_value::int AS num_value,
        col_num - num_symbol_len AS base_col_num,
        unnest(range(col_num - num_symbol_len, col_num)) AS col_num
    FROM number_locations_base
),

distinct_hits AS (
    SELECT DISTINCT
        nl.row_num,
        nl.base_col_num,
        nl.num_value,
        sl.part_symbol_value
    FROM symbol_locations AS sl
    INNER JOIN number_locations AS nl
        ON
            sl.row_check = nl.row_num
            AND sl.col_check = nl.col_num
),

distinct_gears AS (
    SELECT DISTINCT ON (nl.row_num, nl.base_col_num)
        sl.row_num,
        sl.col_num,
        nl.row_num AS number_row_num,
        nl.base_col_num,
        nl.num_value
    FROM symbol_locations AS sl
    INNER JOIN number_locations AS nl
        ON
            sl.row_check = nl.row_num
            AND sl.col_check = nl.col_num
    WHERE sl.part_symbol_value = '*'
    QUALIFY dense_rank() OVER (PARTITION BY sl.row_num, sl.col_num ORDER BY nl.row_num, nl.base_col_num) 
     + dense_rank() OVER (PARTITION BY sl.row_num, sl.col_num ORDER BY nl.row_num DESC, nl.base_col_num DESC) - 1 = 2
),

gear_products AS (
    SELECT
        row_num,
        col_num,
        list_product(list(num_value)) AS products
    FROM distinct_gears
    GROUP BY ALL
)

SELECT
    'Part 1' AS part,
    sum(num_value) AS answer
FROM distinct_hits
UNION ALL
SELECT
    'Part 2' AS part,
    sum(products)::int AS answer
FROM gear_products;
