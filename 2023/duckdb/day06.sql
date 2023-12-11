WITH tabx AS (
    SELECT
        'Part 1' AS part,
        string_split(input_row, ':')[1] AS value_type,
        unnest(
            range(
                len(regexp_extract_all(string_split(input_row, ':')[2], '\d+'))
            )
        ) AS race,
        unnest(
            regexp_extract_all(string_split(input_row, ':')[2], '\d+')
        ) AS row_value
    FROM
        read_csv(
            '2023/day06.txt', columns = { 'input_row': 'VARCHAR' }, delim = '~'
        )

    UNION ALL

    SELECT
        'Part 2' AS part,
        string_split(input_row, ':')[1] AS value_type,
        unnest(
            range(
                len(
                    regexp_extract_all(
                        regexp_replace(
                            string_split(input_row, ':')[2], '\s+', '', 'g'
                        ),
                        '\d+'
                    )
                )
            )
        ) AS race,
        unnest(
            regexp_extract_all(
                regexp_replace(string_split(input_row, ':')[2], '\s+', '', 'g'),
                '\d+'
            )
        ) AS row_value
    FROM
        read_csv(
            '2023/day06.txt', columns = { 'input_row': 'VARCHAR' }, delim = '~'
        )
),

taby AS (
    SELECT
        t.part,
        t.race,
        t.row_value::int64 AS time_value,
        d.row_value::int64 AS distance
    FROM tabx AS t
    INNER JOIN tabx AS d
        ON
            t.race = d.race
            AND t.part = d.part
            AND t.value_type = 'Time'
            AND d.value_type = 'Distance'
)

SELECT
    part,
    product(cnt)::int64 AS answer
FROM (
    SELECT
        part,
        race,
        count(*) AS cnt
    FROM (
        SELECT
            part,
            race,
            time_value,
            unnest(list_reverse(range(time_value + 1))) AS ms,
            unnest(range(time_value + 1))
            * unnest(list_reverse(range(time_value + 1)))
            > distance AS win
        FROM taby
    )
    WHERE win
    GROUP BY part, race
)
GROUP BY part;
