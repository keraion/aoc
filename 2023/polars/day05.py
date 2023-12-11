import polars as pl


def map_df_ranges(
    source_map_df: pl.DataFrame | pl.LazyFrame,
    maps_df: pl.DataFrame | pl.LazyFrame,
    n: int,
):
    return (
        source_map_df.join(
            pl.concat(
                [
                    maps_df.filter(pl.col("map_set") == n).drop("map_set"),
                    source_map_df.select(
                        pl.col("source_start"),
                        pl.col("source_end"),
                        pl.lit(0).alias("delta"),
                    ),
                ],
                how="vertical_relaxed",
            ),
            how="cross",
        )
        .filter(
            (
                (pl.col("source_start") <= pl.col("source_end_right"))
                & (pl.col("source_start_right") <= pl.col("source_end"))
            )
        )
        .with_columns(
            pl.col("source_start_right").clip(
                pl.col("source_start"), pl.col("source_end")
            ),
            pl.col("source_end_right").clip(
                pl.col("source_start"), pl.col("source_end")
            )
            + 1,
        )
        .pipe(map_melter)
        .filter(
            (
                (pl.col("source_start_right") <= pl.col("value"))
                & (pl.col("value") < pl.col("source_end_right"))
            )
        )
        .select(
            (pl.col("value") + pl.col("delta_right")).alias("source_start"),
            (pl.col("value_end") + pl.col("delta_right")).alias("source_end"),
            pl.lit(0).alias("delta"),
        )
    )

def loop_map_df_ranges(
        
):
    ...

def map_melter(map_splitter_df: pl.DataFrame | pl.LazyFrame):
    return map_splitter_df.join(
        map_splitter_df.melt(
            ["source_start", "source_end", "delta_right"],
            ["source_start_right", "source_end_right"],
        )
        .sort("value", "variable")
        .with_columns(
            (
                pl.col("delta_right")
                * pl.when(pl.col("variable") == pl.lit("source_start_right"))
                .then(1)
                .otherwise(-1)
            )
            .cum_sum()
            .over("source_start", "source_end"),
        )
        .group_by("source_start", "source_end", "value")
        .agg(
            pl.coalesce(
                pl.col("delta_right").filter(pl.col("delta_right") != 0).first(), 0
            ),
        )
        .sort("value")
        .with_columns(
            pl.col("value")
            .shift(-1)
            .over("source_start", "source_end")
            .sub(1)
            .alias("value_end")
        ),
        on=["source_start", "source_end", "delta_right"],
        how="inner",
    )


def left_join_range(df: pl.LazyFrame, lazy_maps_df: pl.LazyFrame, n):
    return (
        df.sort("seeds")
        .join_asof(
            lazy_maps_df.filter(pl.col("map_set") == n),
            left_on="seeds",
            right_on="source_start",
        )
        .select(
            pl.col("seeds").add(
                pl.when(pl.col("seeds").le(pl.col("source_end")))
                .then(pl.col("delta"))
                .otherwise(0)
            )
        )
    )


def part_01(seeds_df: pl.DataFrame, maps_df: pl.DataFrame):
    return (
        seeds_df.explode("seeds")
        .select(
            pl.col("seeds").alias("source_start"),
            pl.col("seeds").alias("source_end"),
            pl.lit(0).alias("delta"),
        )
        # A range loop over could be used here but let's use pipe
        .pipe(map_df_ranges, maps_df, 1, 7)
        .pipe(map_df_ranges, maps_df, 2)
        .pipe(map_df_ranges, maps_df, 3)
        .pipe(map_df_ranges, maps_df, 4)
        .pipe(map_df_ranges, maps_df, 5)
        .pipe(map_df_ranges, maps_df, 6)
        .pipe(map_df_ranges, maps_df, 7)
        .select("source_start").min()
    )


def part_02(seeds_df: pl.DataFrame, maps_df: pl.DataFrame):
    return (
        seeds_df.select(
            pl.col("seeds").explode().gather_every(2).alias("source_start"),
            (
                pl.col("seeds").explode().gather_every(2)
                + pl.col("seeds").list.slice(1).explode().gather_every(2)
                - 1
            ).alias("source_end"),
            pl.lit(0).alias("delta"),
        )
        .pipe(map_df_ranges, maps_df, 1)
        .pipe(map_df_ranges, maps_df, 2)
        .pipe(map_df_ranges, maps_df, 3)
        .pipe(map_df_ranges, maps_df, 4)
        .pipe(map_df_ranges, maps_df, 5)
        .pipe(map_df_ranges, maps_df, 6)
        .pipe(map_df_ranges, maps_df, 7)
        .select("source_start").min()
    )


def part_01_bruteforce(seeds_df_in: pl.DataFrame, maps_df_in: pl.DataFrame):
    lazy_maps_df = maps_df_in.sort("source_start").lazy()

    return (
        seeds_df_in.lazy()
        .explode("seeds")
        .pipe(left_join_range, lazy_maps_df, 1)
        .pipe(left_join_range, lazy_maps_df, 2)
        .pipe(left_join_range, lazy_maps_df, 3)
        .pipe(left_join_range, lazy_maps_df, 4)
        .pipe(left_join_range, lazy_maps_df, 5)
        .pipe(left_join_range, lazy_maps_df, 6)
        .pipe(left_join_range, lazy_maps_df, 7)
        .min()
    ).collect(streaming=True)


def generate_map_df(df_in: pl.DataFrame):
    return (
        df_in.with_columns(
            pl.col("column_1").is_null().cum_sum().alias("map_set"),
            pl.col("column_1")
            .str.extract_all(r"\d+")
            .cast(pl.List(pl.Int64))
            .alias("map_values"),
        )
        .filter((pl.col("map_set") > 0) & (pl.col("map_values").list.len() > 0))
        .with_columns(
            pl.col("map_values").list.to_struct(fields=["dest", "source", "length"])
        )
        .unnest("map_values")
        .select(
            pl.col("map_set"),
            pl.col("source").alias("source_start"),
            (pl.col("source") + pl.col("length") - 1).alias("source_end"),
            (pl.col("dest") - pl.col("source")).alias("delta"),
        )
    )

def main(input_filename):
    input_df = pl.read_csv(input_filename, has_header=False, separator="~")
    seeds_df = input_df.filter(pl.col("column_1").str.starts_with("seeds:")).select(
        pl.col("column_1")
        .str.extract_all(r"\d+")
        .cast(pl.List(pl.Int64))
        .alias("seeds")
    )

    maps_df = generate_map_df(input_df)

    print("Part 1 Bruteforce:", part_01_bruteforce(seeds_df, maps_df))
    print("Part 1:", part_01(seeds_df, maps_df))
    print("Part 2:", part_02(seeds_df, maps_df))


if __name__ == "__main__":
    main("2023/day05_example.txt")
    main("2023/input/day05.txt")