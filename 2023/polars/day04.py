import polars as pl

df = pl.read_csv("2023/input/day04.txt", has_header=False, separator=":").with_columns(
    pl.col("column_1").str.extract(r"(\d+)").cast(pl.Int64).alias("card_num"),
    pl.col("column_2")
    .str.split("|")
    .list.first()
    .str.extract_all(r"\d+")
    .alias("winners"),
    pl.col("column_2")
    .str.split("|")
    .list.last()
    .str.extract_all(r"\d+")
    .alias("selected"),
)

df_next_cards = df.select(
    pl.col("card_num"),
    pl.int_ranges(
        pl.col("card_num") + 1,
        pl.col("card_num")
        + 1
        + pl.col("winners").list.set_intersection(pl.col("selected")).list.len(),
    ).alias("next_card"),
).explode("next_card")

df_next_cards = df_next_cards.with_columns(pl.col("next_card").alias("next_card_0"))
df_base = df_next_cards.with_columns(
    (
        pl.col("card_num").unique().count() + pl.col("next_card").drop_nulls().count()
    ).alias("hit_count")
)

n = 0
while len(df_base.drop_nulls(f"next_card_{n}")) > 0:
    df_base = df_base.join(
        df_next_cards,
        how="left",
        left_on=f"next_card_{n}",
        right_on="card_num",
        suffix=f"_{n+1}",
    ).with_columns(
        pl.col("hit_count") + pl.col(f"next_card_{n+1}").drop_nulls().count()
    )

    n += 1

print(
    "Part 1:",
    df.select(
        (
            2
            ** (
                pl.col("winners").list.set_intersection(pl.col("selected")).list.len()
                - 1
            )
        ).cast(pl.Int64, strict=False)
    ).sum(),
)

print("Part 2:", df_base.select("hit_count").max())
