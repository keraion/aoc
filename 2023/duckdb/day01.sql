CREATE OR REPLACE TABLE input_file as 
FROM read_csv('2023/day01.txt', columns = {'input_row': 'VARCHAR'}, delim ='~')
;

select
    sum(
        (
            regexp_replace(input_row, '[^\d]+', '', 'g')[1] || regexp_replace(input_row, '[^\d]+', '', 'g')[-1]
        )::int
    ) as part_1
from
    input_file
;

with tab_num as (
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
    ] as x
),

num_ref as (
    select unnest(x) n_str, unnest(range(1,10)) as n
    from tab_num
    union ALL
    select unnest(range(1,10)), unnest(range(1,10))
)

select sum(n1.n*10 + n2.n) as part_2
from input_file i
inner join num_ref n1
    on regexp_extract(i.input_row, '\d|' || (select array_to_string(x, '|') from tab_num)) = n1.n_str
inner join num_ref n2
    on regexp_extract(reverse(i.input_row), '\d|' || (select reverse(array_to_string(x, '|')) from tab_num)) = reverse(n2.n_str)
;