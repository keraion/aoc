import polars as pl

hand_type_df = pl.DataFrame(
    {
        "values": [
            "5",
            "41",
            "32",
            "311",
            "221",
            "2111",
            "11111",
        ],
        "rank": [1, 2, 3, 4, 5, 6, 7],
    }
)
replacement_df = pl.DataFrame(
    {"sub": [f"{n}" for n in range(2, 10)] + ["A", "C", "D", "E"]}
)

sort_map = {
    "A": "E",
    "K": "D",
    "Q": "C",
    "J": "B",
    "T": "A",
}

input_df = (
    pl.read_csv("2023/day07.txt", has_header=False, separator=" ")
    .with_row_count()
    .with_columns(
        pl.col("column_1")
        .str.extract_all(r"\w")
        .list.eval(pl.first().replace(sort_map))
        .list.join(""),
        pl.col("column_1")
        .str.extract_all(r"\w")
        .explode()
        .value_counts()
        .implode()
        .over("row_nr")
        .list.eval(pl.first().struct.field("counts"))
        .list.sort(descending=True)
        .cast(pl.List(pl.Utf8))
        .list.join("")
        .alias("hist"),
    )
    .with_columns(
        pl.col("column_1").str.replace_all("B", "1").alias("column_1_pt_2"),
    )
    .join(replacement_df, how="cross")
    .with_row_count("rn_")
    .with_columns(
        pl.col("column_1_pt_2")
        .str.replace_all("1", pl.col("sub"))
        .str.extract_all(r"\w")
        .explode()
        .value_counts()
        .implode()
        .over("rn_")
        .list.eval(pl.first().struct.field("counts"))
        .list.sort(descending=True)
        .cast(pl.List(pl.Utf8))
        .list.join("")
        .alias("column_1_pt_2_hist"),
    )
    .join(hand_type_df, left_on="hist", right_on="values", how="inner")
    .join(hand_type_df, left_on="column_1_pt_2_hist", right_on="values", how="inner")
    .group_by("row_nr")
    .agg(
        pl.col(
            "column_1",
            "column_2",
            "column_1_pt_2",
            "hist",
            "column_1_pt_2_hist",
            "rank",
        ).first(),
        pl.col("rank_right").min(),
    )
    .sort("rank", "column_1", descending=[True, False])
    .with_columns(
        (pl.col("column_2") * pl.col("rank").cum_count().add(1)).alias("p1_rank"),
    )
    .sort("rank_right", "column_1_pt_2", descending=[True, False])
    .with_columns(
        (pl.col("column_2") * pl.col("rank").cum_count().add(1)).alias("p2_rank"),
    )
)

print(input_df)
print("Part 1:", input_df.select("p1_rank").sum())
print("Part 2:", input_df.select("p2_rank").sum())
