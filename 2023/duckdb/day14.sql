-- TODO: fix sqlfluff linting

CREATE or replace table input as
from
    read_csv(
        '2023/input/day14.txt',
        columns = { 'input_row': 'VARCHAR' },
        delim = '~'
    );

CREATE or replace table first_sample as
with recursive cycles as (
    
    select *,
        sum(case when val = '#' then 1 else 0 end) over (partition by col order by row_num) as row_block_group,
        sum(case when val = '#' then 1 else 0 end) over (partition by row_num order by col) as col_block_group,
        case 
            when val = '#' then 0
            when val = 'O' then 1
            when val = '.' then 2
        end as order_val,
        case 
            when val = '#' then 0
            when val = 'O' then 2
            when val = '.' then 1
        end as rev_order_val,
        count(*) over () as val_count
    from (
        select 
            rowid + 1 as row_num, 
            unnest(range(len(input_row))) as col, 
            input_row[unnest(range(len(input_row))) + 1] as val,
            0 as n
        from input
    )

    union all

    from (select * replace (sum(case when val = '#' then 1 else 0 end) over (partition by col order by row_num) as row_block_group) 
        from (select * replace (row_number() over (partition by row_num order by col_block_group, rev_order_val, col) as col)
        from (select * replace (sum(case when val = '#' then 1 else 0 end) over (partition by row_num order by col) as col_block_group) 
            from (select * replace (row_number() over (partition by col order by row_block_group, rev_order_val, row_num) as row_num)
            from (select * replace (sum(case when val = '#' then 1 else 0 end) over (partition by col order by row_num) as row_block_group) 
                from (select * replace (row_number() over (partition by row_num order by col_block_group, order_val, col) as col)
                from (select * replace (sum(case when val = '#' then 1 else 0 end) over (partition by row_num order by col) as col_block_group) 
                    from (select * replace (row_number() over (partition by col order by row_block_group, order_val, row_num) as row_num)

                    from (
                        select 
                            * replace (
                                n + 1 as n,
                                sum(case when val = '#' then 1 else 0 end) over (partition by col order by row_num) as row_block_group
                            )
                        from cycles
                        where n < 250 -- increase this if needed, maybe find a better computational way to break early
                    )) -- north
                )) -- west
            )) -- south
        )) -- east
    )
)

select n, 
    array_to_string(list(c order by row_num), '') as map, 
    list(c order by row_num) as map_list
from (
    select 
        n, 
        row_num, 
        array_to_string(list(val order by col), '') as c,
    from cycles
    group by n, row_num
    order by n, row_num
)
group by n
;

select 'Part 1' as part, sum(_rn) as answer
from (
    select *,
        row_number() over (partition by col order by block_group desc, order_val desc, rowid desc) as _rn
    from (
        select *, 
            sum(case when val = '#' then 1 else 0 end) over (partition by col order by rowid) block_group,
            case 
                when val = '#' then 0
                when val = 'O' then 1
                when val = '.' then 2
            end as order_val
        from (
            select 
                rowid, 
                unnest(range(len(input_row))) + 1 as col, 
                input_row[unnest(range(len(input_row))) + 1] val
            from input
        )
    )
    order by col, block_group, order_val, rowid
)
where val = 'O'

union all

select 'Part 2' as part, sum(y) as answer
from (
    select n, (unnest(list_reverse(range(len(map_list)))) + 1) * len(regexp_extract_all(unnest(map_list), 'O')) as y
    from first_sample
    where n = (
        select list(n)[1] + ((1000000000 - list(n)[1]) % (list(n)[-1] - list(n)[1]))
        from (
            from (
                select *, row_number() over (partition by map order by n) as rpt
                from first_sample
                qualify row_number() over (partition by map order by n) in (2,3)
            )
            qualify row_number() over (partition by rpt order by n) = 1
        )
    )
)
;