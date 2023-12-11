import polars as pl

df = (
    pl.read_csv("2023/day02.txt", has_header=False, separator=":")
    .with_columns(
        pl.col("column_2")
        .str.strip_chars()
        .str.split(";")
        .explode()
        .str.strip_chars()
        .str.split(",")
        .explode()
        .str.strip_chars()
        .str.split(" ")
        .list.to_struct(fields=["amount", "color"])
        .implode()
        .over("column_2"),
    )
    .explode("column_2")
    .unnest("column_2")
)

print(
    "Part 1:",
    df.filter(
        ~pl.col("column_1").is_in(
            df.filter(
                pl.any_horizontal(
                    (
                        (pl.col("color") == pl.lit("red"))
                        & (pl.col("amount").cast(pl.Int64) > 12)
                    ),
                    (
                        (pl.col("color") == pl.lit("green"))
                        & (pl.col("amount").cast(pl.Int64) > 13)
                    ),
                    (
                        (pl.col("color") == pl.lit("blue"))
                        & (pl.col("amount").cast(pl.Int64) > 14)
                    ),
                )
            ).to_series()
        )
    )
    .select(pl.col("column_1").str.extract(r"(\d+)").cast(pl.Int64))
    .unique()
    .sum(),
)

print(
    "Part 2:",
    df.group_by("column_1", "color")
    .agg(pl.col("amount").cast(pl.Int64).max())
    .group_by("column_1")
    .agg(pl.col("amount").product())
    .select("amount")
    .sum(),
)
