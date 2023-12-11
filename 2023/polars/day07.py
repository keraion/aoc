import polars as pl

hand_type_df = pl.DataFrame(
    {
        "hand_values": [
            "5",
            "41",
            "32",
            "311",
            "221",
            "2111",
            "11111",
        ],
        "hand_rank": [1, 2, 3, 4, 5, 6, 7],
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
    pl.read_csv(
        "2023/input/day07.txt",
        has_header=False,
        separator=" ",
        schema={"cards": pl.Utf8, "bid": pl.Int64},
    )
    .with_row_count()
    .with_columns(
        pl.col("cards")
        .str.extract_all(r"\w")
        .list.eval(pl.first().replace(sort_map))
        .list.join(""),
        pl.col("cards")
        .str.extract_all(r"\w")
        .explode()
        .value_counts()
        .implode()
        .over("row_nr")
        .list.eval(pl.first().struct.field("counts"))
        .list.sort(descending=True)
        .cast(pl.List(pl.Utf8))
        .list.join("")
        .alias("cards_pt_1_hist"),
    )
    .with_columns(
        pl.col("cards").str.replace_all("B", "1").alias("cards_pt_2"),
    )
    .join(replacement_df, how="cross")
    .with_row_count("rn_")
    .with_columns(
        pl.col("cards_pt_2")
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
        .alias("cards_pt_2_hist"),
    )
    .join(hand_type_df, left_on="cards_pt_1_hist", right_on="hand_values", how="inner")
    .join(hand_type_df, left_on="cards_pt_2_hist", right_on="hand_values", how="inner")
    .group_by("row_nr")
    .agg(
        pl.col(
            "cards",
            "bid",
            "cards_pt_2",
            "cards_pt_1_hist",
            "cards_pt_2_hist",
            "hand_rank",
        ).first(),
        pl.col("hand_rank_right").min(),
    )
    .sort("hand_rank", "cards", descending=[True, False])
    .with_columns(
        (pl.col("bid") * pl.col("hand_rank").cum_count().add(1)).alias("p1_rank"),
    )
    .sort("hand_rank_right", "cards_pt_2", descending=[True, False])
    .with_columns(
        (pl.col("bid") * pl.col("hand_rank").cum_count().add(1)).alias("p2_rank"),
    )
)

print("Part 1:", input_df.select("p1_rank").sum())
print("Part 2:", input_df.select("p2_rank").sum())
