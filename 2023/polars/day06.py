import polars as pl

print(
    pl.read_csv("2023/input/day06.txt", has_header=False, separator=":")
    .select(
        pl.col("column_1").str.strip_chars(),
        pl.col("column_2").str.extract_all(r"\d+").cast(pl.List(pl.Int64)),
        pl.col("column_2")
        .str.replace_all(r"\s+", "")
        .str.extract(r"(\d+)")
        .cast(pl.Int64)
        .alias("part_2"),
    )
    .transpose(column_names="column_1")
    .with_row_count("Part", 1)
    .explode("Time", "Distance")
    .with_columns(
        pl.int_ranges(0, pl.col("Time") + 1).alias("forward"),
        pl.int_ranges(0, pl.col("Time") + 1).list.reverse().alias("rev"),
    )
    .explode("forward", "rev")
    .filter((pl.col("forward") * pl.col("rev")) > pl.col("Distance"))
    .group_by("Part", "Time")
    .agg(pl.count().alias("wins"))
    .group_by("Part")
    .agg(pl.col("wins").product())
)
