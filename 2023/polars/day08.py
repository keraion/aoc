# For the love, this is not the way to use this library.

import polars as pl
from math import lcm


def get_next_node(looper_df: pl.DataFrame, step_df, map_df, n):
    return (
        looper_df.join(
            step_df.filter(pl.col("row_nr") == (n % pl.col("row_nr").count())),
            how="cross",
        )
        .select(
            pl.when(pl.col("direction") == pl.lit("R"))
            .then(pl.col("right"))
            .otherwise(pl.col("left"))
            .alias("node"),
            pl.col("start"),
            pl.col("end"),
        )
        .with_columns(
            pl.when(pl.col("node").str.ends_with("Z"))
            .then(pl.col("end"))
            .otherwise(pl.lit(n + 2))
            .alias("end")
        )
        .join(map_df, on="node")
    )


def part_1(step_df, map_df):
    n = 0
    loop_df = map_df.filter(pl.col("node").str.starts_with("AAA")).with_columns(
        pl.col("node").alias("start"), pl.lit(n).alias("end")
    )
    while len(loop_df.filter(pl.col("node") == pl.lit("ZZZ"))) == 0:
        loop_df = loop_df.pipe(get_next_node, step_df, map_df, n)
        n += 1
    return loop_df.select(pl.lit(n).alias("end"))


def part_2(step_df, map_df):
    n = 0
    loop_df: pl.DataFrame = map_df.filter(
        pl.col("node").str.ends_with("A")
    ).with_columns(
        pl.col("node").alias("start"),
        pl.lit(n).alias("end"),
    )
    zmap_df = map_df.with_columns(
        pl.when(pl.col("node").str.ends_with("Z"))
        .then(pl.col("node"))
        .otherwise(pl.col("left"))
        .alias("left"),
        pl.when(pl.col("node").str.ends_with("Z"))
        .then(pl.col("node"))
        .otherwise(pl.col("right"))
        .alias("right"),
    )

    while len(loop_df.filter(pl.col("node").str.ends_with("Z"))) != len(loop_df):
        loop_df = loop_df.pipe(get_next_node, step_df, zmap_df, n)
        n += 1

    # Maybe I'll do the lcm in pure polars later
    return lcm(*loop_df.select(pl.col("end")).to_series().to_list())


def main():
    input_df = pl.read_csv(
        "2023/day08.txt",
        has_header=False,
        separator="~",
    )

    step_df = (
        input_df.head(1)
        .select(
            pl.col("column_1").str.extract_all(r"\w").alias("direction"),
        )
        .explode("direction")
        .with_row_count()
    )
    map_df = (
        input_df.tail(-2)
        .with_columns(
            pl.col("column_1")
            .str.extract_all(r"\b\w{3}\b")
            .list.to_struct(fields=["node", "left", "right"])
        )
        .unnest("column_1")
    )

    print("Part 1", part_1(step_df, map_df))
    print("Part 2", part_2(step_df, map_df))


if __name__ == "__main__":
    main()