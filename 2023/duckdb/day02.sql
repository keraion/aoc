WITH game_rounds AS (
    SELECT
        string_split(input_row, ': ')[1] AS game,
        unnest(
            string_split(string_split(input_row, ': ')[2], ';')
        ) AS round_output
    FROM
        read_csv(
            '2023/input/day02.txt', columns = { 'input_row': 'VARCHAR' }, delim = '~'
        )
),

game_round_colors AS (
    SELECT
        *,
        trim(unnest(string_split(round_output, ', '))) AS color_outputs
    FROM game_rounds
),

game_round_color_info AS (
    SELECT
        *,
        string_split_regex(color_outputs, '\s+')[1]::int AS color_amount,
        string_split_regex(color_outputs, '\s+')[2] AS color_type
    FROM game_round_colors
),

impossible_games AS (
    SELECT regexp_extract(game, '\d+')::int AS game_int
    FROM game_round_color_info

    EXCEPT

    SELECT regexp_extract(game, '\d+')::int
    FROM game_round_color_info
    WHERE
        (color_type = 'red' AND color_amount > 12)
        OR (color_type = 'green' AND color_amount > 13)
        OR (color_type = 'blue' AND color_amount > 14)
),

game_round_color_max AS (
    SELECT
        game,
        color_type,
        max(color_amount) AS color_amount
    FROM game_round_color_info
    GROUP BY ALL
),

game_round_max_product AS (
    SELECT
        game,
        product(color_amount) AS color_amount
    FROM game_round_color_max
    GROUP BY game
)

SELECT
    'Part 1' AS part,
    sum(game_int)::int AS answer
FROM impossible_games

UNION ALL

SELECT
    'Part 2' AS part,
    sum(color_amount)::int AS answer
FROM game_round_max_product;
