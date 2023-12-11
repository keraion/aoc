with game_rounds as (
    select
        string_split(input_row, ': ')[1] as game,
        unnest(
            string_split(string_split(input_row, ': ')[2], ';')
        ) as round_output
    from
        read_csv('2023/day2.txt', columns = { 'input_row': 'VARCHAR' }, delim = '~')
),

game_round_colors as (
    select
        *,
        trim(unnest(string_split(round_output, ', '))) as color_outputs
    from game_rounds
),

game_round_color_info as (
    select
        *,
        string_split_regex(color_outputs, '\s+')[1]::int as color_amount,
        string_split_regex(color_outputs, '\s+')[2] as color_type
    from game_round_colors
),

impossible_games as (
    select regexp_extract(game, '\d+')::int as game_int
    from game_round_color_info

    except

    select regexp_extract(game, '\d+')::int
    from game_round_color_info
    where
        (color_type = 'red' and color_amount > 12)
        or (color_type = 'green' and color_amount > 13)
        or (color_type = 'blue' and color_amount > 14)
),

game_round_color_max as (
    select
        game,
        color_type,
        max(color_amount) as color_amount
    from game_round_color_info
    group by all
),

game_round_max_product as (
    select
        game,
        product(color_amount) as color_amount
    from game_round_color_max
    group by game
)

select
    'Part 1' as part,
    sum(game_int)::int as answer
from impossible_games

union all

select
    'Part 2',
    sum(color_amount)::int
from game_round_max_product
;
