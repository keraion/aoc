CREATE OR REPLACE TABLE input as FROM read_csv('2023/input/day04.txt', columns = {'input_row': 'VARCHAR'}, delim ='~');

CREATE OR REPLACE TABLE base AS
SELECT
    regexp_extract_all(string_split(input_row, ':')[1], '\d+')[
        1
    ]::int AS card_num,
    unnest(list_concat(range(
        regexp_extract_all(string_split(input_row, ':')[1], '\d+')[1]::int + 1,
        regexp_extract_all(string_split(input_row, ':')[1], '\d+')[1]::int + 1
        + len(
            list_intersect(
                regexp_extract_all(
                    string_split(string_split(input_row, ':')[2], '|')[1], '\d+'
                ),
                regexp_extract_all(
                    string_split(string_split(input_row, ':')[2], '|')[2], '\d+'
                )
            )
        )
    ), [NULL])) AS next_card
FROM input;

WITH RECURSIVE looper AS (
    SELECT
        card_num,
        next_card
    FROM base

    UNION ALL

    SELECT
        n.card_num,
        n.next_card
    FROM looper AS l
    INNER JOIN base AS n
        ON
            l.next_card = n.card_num
            AND n.next_card IS NOT NULL
)

SELECT
    'Part 1' AS part,
    sum((2 ** (len(list_intersect(
        regexp_extract_all(
            string_split(string_split(input_row, ':')[2], '|')[1], '\d+'
        ),
        regexp_extract_all(
            string_split(string_split(input_row, ':')[2], '|')[2], '\d+'
        )
    )) - 1))::int) AS answer
FROM input

UNION ALL

SELECT
    'Part 2' AS part,
    count(*) AS answer
FROM looper;
