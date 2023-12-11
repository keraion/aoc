CREATE OR REPLACE TABLE input AS
FROM read_csv('2023/day07.txt', columns = {'input_row': 'VARCHAR'}, delim ='~')
;

WITH tabx AS (
    SELECT
        i.rowid,
        string_split(i.input_row, ' ')[2]::int64 AS bid,
        j.sub,

        array_to_string(
            list_transform(
                string_split(string_split(i.input_row, ' ')[1], ''), x
                -> CASE x
                    WHEN 'A' THEN 'E'
                    WHEN 'K' THEN 'D'
                    WHEN 'Q' THEN 'C'
                    WHEN 'J' THEN 'B'
                    WHEN 'T' THEN 'A'
                    ELSE x
                END
            ), ''
        ) AS part_1_hand_listing,

        array_to_string(
            list_transform(
                string_split(string_split(i.input_row, ' ')[1], ''), x
                -> CASE x
                    WHEN 'A' THEN 'E'
                    WHEN 'K' THEN 'D'
                    WHEN 'Q' THEN 'C'
                    WHEN 'J' THEN '1'
                    WHEN 'T' THEN 'A'
                    ELSE x
                END
            ), ''
        ) AS part_2_hand_listing,

        map_values(
            list_histogram(
                string_split(string_split(i.input_row, ' ')[1], '')
            )
        ) AS hand_part_1_list,
        
        map_values(list_histogram(
            list_transform(
                string_split(string_split(i.input_row, ' ')[1], ''), x
                -> CASE x
                    WHEN 'J' THEN j.sub
                    ELSE x
                END
            )
        ))
        AS hand_part_2_list
    FROM input AS i
    CROSS JOIN (
        VALUES
        ('A'),
        ('K'),
        ('Q'),
        ('T'),
        ('9'),
        ('8'),
        ('7'),
        ('6'),
        ('5'),
        ('4'),
        ('3'),
        ('2')
    ) AS j (sub)
)

SELECT 
    sum(score_part_1) AS part_1_answer,
    sum(score_part_2) AS part_2_answer
FROM (
    SELECT
        *,
        row_number() OVER (ORDER BY part_1_hand_type, part_1_hand_listing) * bid AS score_part_1,
        row_number() OVER (ORDER BY part_2_hand_type, part_2_hand_listing) * bid AS score_part_2
    FROM (
        SELECT
            rowid,
            first(part_1_hand_type) AS part_1_hand_type,
            max(part_2_hand_type) AS part_2_hand_type,
            first(part_1_hand_listing) AS part_1_hand_listing,
            first(part_2_hand_listing) AS part_2_hand_listing,
            first(bid) AS bid
        FROM (
            SELECT
                *,
                CASE
                    WHEN list_intersect(list_sort(hand_part_1_list, 'DESC'), [5]) = [5] THEN 7
                    WHEN list_intersect(list_sort(hand_part_1_list, 'DESC'), [4, 1]) = [4, 1] THEN 6
                    WHEN list_intersect(list_sort(hand_part_1_list, 'DESC'), [3, 2]) = [3, 2] THEN 5
                    WHEN list_intersect(list_sort(hand_part_1_list, 'DESC'), [3, 1, 1]) = [3, 1, 1] THEN 4
                    WHEN list_intersect(list_sort(hand_part_1_list, 'DESC'), [2, 2, 1]) = [2, 2, 1] THEN 3
                    WHEN list_intersect(list_sort(hand_part_1_list, 'DESC'), [2, 1, 1, 1]) = [2, 1, 1, 1] THEN 2
                    WHEN list_intersect(list_sort(hand_part_1_list, 'DESC'), [1, 1, 1, 1, 1]) = [1, 1, 1, 1, 1] THEN 1
                END AS part_1_hand_type,
                CASE
                    WHEN list_intersect(list_sort(hand_part_2_list, 'DESC'), [5]) = [5] THEN 7
                    WHEN list_intersect(list_sort(hand_part_2_list, 'DESC'), [4, 1]) = [4, 1] THEN 6
                    WHEN list_intersect(list_sort(hand_part_2_list, 'DESC'), [3, 2]) = [3, 2] THEN 5
                    WHEN list_intersect(list_sort(hand_part_2_list, 'DESC'), [3, 1, 1]) = [3, 1, 1] THEN 4
                    WHEN list_intersect(list_sort(hand_part_2_list, 'DESC'), [2, 2, 1]) = [2, 2, 1] THEN 3
                    WHEN list_intersect(list_sort(hand_part_2_list, 'DESC'), [2, 1, 1, 1]) = [2, 1, 1, 1] THEN 2
                    WHEN list_intersect(list_sort(hand_part_2_list, 'DESC'), [1, 1, 1, 1, 1]) = [1, 1, 1, 1, 1] THEN 1
                END AS part_2_hand_type
            FROM tabx
        )
        GROUP BY rowid
    )
);