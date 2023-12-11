CREATE OR REPLACE TABLE input AS
FROM read_csv('2023/day05.txt', columns = {'input_row': 'VARCHAR'}, delim ='~')
;

create or replace table seeds as

SELECT 
    string_split(input_row, ':')[1] AS thing_type,
    unnest(string_split_regex(trim(string_split(input_row, ':')[2]), '\s+'))::int64 AS seed_value
FROM input 
WHERE input_row LIKE 'seeds:%'
;

CREATE OR REPLACE TABLE seed_explode AS

WITH seed_ranges AS (
    SELECT 
        string_split(input_row, ':')[1] AS thing_type,
        unnest(string_split_regex(trim(string_split(input_row, ':')[2]), '\s+')) AS seeds,
        generate_subscripts(string_split_regex(trim(string_split(input_row, ':')[2]), '\s+'), 1) AS seeds_pos
    FROM input 
    WHERE input_row LIKE 'seeds:%'
),

seed_groupings AS (
    SELECT 
        *,
        (seeds_pos - 1) // 2 AS seed_group,
        (seeds_pos - 1) % 2 AS seed_group_pos
    FROM seed_ranges
),

seed_as_ranges AS (
    SELECT 
        seed_group,
        first(seeds ORDER BY seed_group_pos)::int64 AS seed_source,
        first(seeds ORDER BY seed_group_pos DESC)::int64 AS seed_end
    FROM seed_groupings
    GROUP BY seed_group
)

SELECT 
    seed_source AS seed_start,
    seed_source + seed_end - 1 AS seed_end
FROM seed_as_ranges
;

CREATE OR REPLACE TABLE range_map AS

with map_grouper AS (
    SELECT
        *,
        sum((not regexp_matches(input_row, '\d+'))::int64) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS map_type
    FROM input
    WHERE rowid > 1
    and input_row IS NOT NULL
),

map_header_grouper AS (
    SELECT 
        *,
        coalesce(lag(map_type) OVER () != map_type, true) AS is_header
    FROM map_grouper
),

map_detail AS (
    SELECT
        h.input_row AS map_type_name,
        h.map_type::int64 AS map_type,
        unnest(struct_pack(
            dest := regexp_extract_all(d.input_row, '\d+')[1]::int64,
            source := regexp_extract_all(d.input_row, '\d+')[2]::int64,
            range_ := regexp_extract_all(d.input_row, '\d+')[3]::int64
        ))
    FROM map_header_grouper h
    INNER JOIN map_header_grouper d
    ON h.map_type = d.map_type
    and h.is_header = true
    and d.is_header = false
)

SELECT 
    map_type,
    source AS map_start,
    source + range_ - 1 AS map_end,
    dest - source AS delta
FROM map_detail
;

WITH RECURSIVE seed_range AS 
(
    SELECT distinct
        1 as n,
        seed_start AS old_seed_start,
        seed_end AS old_seed_end,
        case when map_src != 'seeds' then map_start end AS map_start,
        case when map_src != 'seeds' then map_end end AS map_end,
        case when map_src = 'maps' then delta else 0 end AS delta,
        map_start + case when map_src = 'maps' then delta else 0 end AS seed_start,
        map_end + case when map_src = 'maps' then delta else 0 end AS seed_end
    FROM(
        unpivot
        (
            SELECT
                seed_start,
                seed_end,
                case 
                    when coalesce(lag(map_ends) OVER (PARTITION BY seed_start ORDER BY map_starts) + 1, seed_start) <= map_starts - 1
                    then coalesce(lag(map_ends) OVER (PARTITION BY seed_start ORDER BY map_starts) + 1, seed_start)
                end AS lag_start,
                case 
                    when coalesce(lag(map_ends) OVER (PARTITION BY seed_start ORDER BY map_starts) + 1, seed_start) <= map_starts - 1
                    then map_starts - 1
                end AS lag_end,
                map_starts,
                map_ends,
                case 
                    when coalesce(lead(map_starts) OVER (PARTITION BY seed_start ORDER BY map_starts) - 1, seed_end) >= map_ends + 1
                    then map_ends + 1
                end AS lead_start,
                case 
                    when coalesce(lead(map_starts) OVER (PARTITION BY seed_start ORDER BY map_starts) - 1, seed_end) >= map_ends + 1
                    then coalesce(lead(map_starts) OVER (PARTITION BY seed_start ORDER BY map_starts) - 1, seed_end)
                end AS lead_end,
                seed_start AS seed_start_set,
                seed_end AS seed_end_set,
                deltas AS delta
            FROM (
                SELECT 
                    seed_start,
                    seed_end,
                    unnest(list_concat([null], list(y_list ORDER BY x)[2])) AS map_starts,
                    unnest(list_concat([null], list(y_list ORDER BY x)[3])) AS map_ends,
                    unnest(list_concat([null], list(delta_list ORDER BY x)[2])) AS deltas,
                FROM (
                    SELECT 
                        seed_start, 
                        seed_end, 
                        x, 
                        list(y ORDER BY y) AS y_list, 
                        list(delta ORDER BY y) AS delta_list,
                        list(seed_start) AS start_list
                    FROM (
                        SELECT 
                            * exclude delta replace
                            (
                                case 
                                    when seed_start > y then seed_start
                                    when seed_end < y then seed_end
                                    else y
                                end AS y 
                            ),
                            sum(case when x = '_1_map_start' then 1 when x = '_2_map_end' then -1 else 0 end * delta) OVER (PARTITION BY seed_start, seed_end ORDER BY y, x) AS delta,
                            sum(case when x = '_1_map_start' then 1 when x = '_2_map_end' then -1 else 0 end * delta) OVER (PARTITION BY seed_start, seed_end ORDER BY y, x)
                                + case when x = '_2_map_end' then delta else 0 end AS delta_w_end
                        FROM (
                            unpivot (
                                SELECT 
                                    s.seed_start, 
                                    s.seed_end,
                                    s.seed_start AS _0_seed_map_start, 
                                    s.seed_end AS _3_seed_map_end,
                                    r.map_start AS _1_map_start, 
                                    r.map_end AS _2_map_end,
                                    r.delta
                                FROM seed_explode s
                                LEFT JOIN range_map r
                                ON r.map_start <= s.seed_end
                                and s.seed_start <= r.map_end
                                and r.map_type = 1
                            ) ON _0_seed_map_start, _3_seed_map_end, _1_map_start, _2_map_end
                            into name x
                                values y
                        )
                    )
                    WHERE x in ('_1_map_start', '_2_map_end', '_0_seed_map_start')
                    GROUP BY seed_start, seed_end, x
                )
                GROUP BY all
            )
        )
        ON  (lag_start, lag_end) AS lags, (map_starts, map_ends) AS maps, (lead_start, lead_end) AS leads, (seed_start_set, seed_end_set) AS seeds
        into name map_src
            value map_start, map_end
    )
    qualify dense_rank() OVER (PARTITION BY seed_start ORDER BY case when map_src == 'seeds' then 1 else 0 end) = 1

    UNION ALL

    SELECT distinct
        n + 1 as n,
        seed_start AS old_seed_start,
        seed_end AS old_seed_end,
        case when map_src != 'seeds' then map_start end AS map_start,
        case when map_src != 'seeds' then map_end end AS map_end,
        case when map_src = 'maps' then delta else 0 end AS delta,
        map_start + case when map_src = 'maps' then delta else 0 end AS seed_start,
        map_end + case when map_src = 'maps' then delta else 0 end AS seed_end
    FROM(
        unpivot
        (
            SELECT
                seed_start,
                seed_end,
                n,
                case 
                    when coalesce(lag(map_ends) OVER (PARTITION BY seed_start ORDER BY map_starts) + 1, seed_start) <= map_starts - 1
                    then coalesce(lag(map_ends) OVER (PARTITION BY seed_start ORDER BY map_starts) + 1, seed_start)
                end AS lag_start,
                case 
                    when coalesce(lag(map_ends) OVER (PARTITION BY seed_start ORDER BY map_starts) + 1, seed_start) <= map_starts - 1
                    then map_starts - 1
                end AS lag_end,
                map_starts,
                map_ends,
                case 
                    when coalesce(lead(map_starts) OVER (PARTITION BY seed_start ORDER BY map_starts) - 1, seed_end) >= map_ends + 1
                    then map_ends + 1
                end AS lead_start,
                case 
                    when coalesce(lead(map_starts) OVER (PARTITION BY seed_start ORDER BY map_starts) - 1, seed_end) >= map_ends + 1
                    then coalesce(lead(map_starts) OVER (PARTITION BY seed_start ORDER BY map_starts) - 1, seed_end)
                end AS lead_end,
                seed_start AS seed_start_set,
                seed_end AS seed_end_set,
                deltas AS delta
            FROM (
                SELECT 
                    seed_start,
                    seed_end,
                    n,
                    unnest(list_concat([null], list(y_list ORDER BY x)[2])) AS map_starts,
                    unnest(list_concat([null], list(y_list ORDER BY x)[3])) AS map_ends,
                    unnest(list_concat([null], list(delta_list ORDER BY x)[2])) AS deltas,
                FROM (
                    SELECT 
                        seed_start, 
                        seed_end, 
                        n,
                        x, 
                        list(y ORDER BY y) AS y_list, 
                        list(delta ORDER BY y) AS delta_list,
                        list(seed_start) AS start_list
                    FROM (
                        SELECT 
                            * exclude delta replace
                            (
                                case 
                                    when seed_start > y then seed_start
                                    when seed_end < y then seed_end
                                    else y
                                end AS y 
                            ),
                            sum(case when x = '_1_map_start' then 1 when x = '_2_map_end' then -1 else 0 end * delta) OVER (PARTITION BY seed_start, seed_end ORDER BY y, x) AS delta,
                            sum(case when x = '_1_map_start' then 1 when x = '_2_map_end' then -1 else 0 end * delta) OVER (PARTITION BY seed_start, seed_end ORDER BY y, x)
                                + case when x = '_2_map_end' then delta else 0 end AS delta_w_end
                        FROM (
                            unpivot (
                                SELECT 
                                    s.seed_start, 
                                    s.seed_end,
                                    s.n,
                                    s.seed_start AS _0_seed_map_start, 
                                    s.seed_end AS _3_seed_map_end,
                                    r.map_start AS _1_map_start, 
                                    r.map_end AS _2_map_end,
                                    r.delta
                                FROM seed_range s
                                LEFT JOIN range_map r
                                ON r.map_start <= s.seed_end
                                and s.seed_start <= r.map_end
                                and r.map_type = s.n + 1
                                where s.n + 1 <= 7
                            ) ON _0_seed_map_start, _3_seed_map_end, _1_map_start, _2_map_end
                            into name x
                                values y
                        )
                    )
                    WHERE x in ('_1_map_start', '_2_map_end', '_0_seed_map_start')
                    GROUP BY seed_start, seed_end, x, n
                )
                GROUP BY all
            )
        )
        ON  (lag_start, lag_end) AS lags, (map_starts, map_ends) AS maps, (lead_start, lead_end) AS leads, (seed_start_set, seed_end_set) AS seeds
        into name map_src
            value map_start, map_end
    )
    qualify dense_rank() OVER (PARTITION BY seed_start ORDER BY case when map_src == 'seeds' then 1 else 0 end) = 1
)

SELECT 
    'Part 1' AS part,
    min(s.seed_value + coalesce(s1.delta, 0) + coalesce(s2.delta, 0) + coalesce(s3.delta, 0) + coalesce(s4.delta, 0) + coalesce(s5.delta, 0) + coalesce(s6.delta, 0) + coalesce(s7.delta, 0)) AS answer
FROM seeds s
LEFT JOIN range_map AS s1
    ON s.seed_value BETWEEN s1.map_start AND s1.map_end
    AND s1.map_type = 1
LEFT JOIN range_map AS s2
    ON s.seed_value + coalesce(s1.delta, 0) BETWEEN s2.map_start AND s2.map_end
    AND s2.map_type = 2
LEFT JOIN range_map AS s3
    ON s.seed_value + coalesce(s1.delta, 0) + coalesce(s2.delta, 0) BETWEEN s3.map_start AND s3.map_end
    AND s3.map_type = 3
LEFT JOIN range_map AS s4
    ON s.seed_value + coalesce(s1.delta, 0) + coalesce(s2.delta, 0) + coalesce(s3.delta, 0) BETWEEN s4.map_start AND s4.map_end
    AND s4.map_type = 4
LEFT JOIN range_map AS s5
    ON s.seed_value + coalesce(s1.delta, 0) + coalesce(s2.delta, 0) + coalesce(s3.delta, 0) + coalesce(s4.delta, 0) BETWEEN s5.map_start AND s5.map_end
    AND s5.map_type = 5
LEFT JOIN range_map AS s6
    ON s.seed_value + coalesce(s1.delta, 0) + coalesce(s2.delta, 0) + coalesce(s3.delta, 0) + coalesce(s4.delta, 0) + coalesce(s5.delta, 0) BETWEEN s6.map_start AND s6.map_end
    AND s6.map_type = 6
LEFT JOIN range_map AS s7
    ON s.seed_value + coalesce(s1.delta, 0) + coalesce(s2.delta, 0) + coalesce(s3.delta, 0) + coalesce(s4.delta, 0) + coalesce(s5.delta, 0) + coalesce(s6.delta, 0) BETWEEN s7.map_start AND s7.map_end
    and s7.map_type = 7

UNION ALL

select 'Part 2' AS part, min(seed_start) as answer
from seed_range
where n = 7
;