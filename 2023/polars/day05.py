import polars as pl


def map_df_ranges(
    source_map_df: pl.DataFrame | pl.LazyFrame,
    maps_df: pl.DataFrame | pl.LazyFrame,
    n: int,
):
    map_splitter_df = (
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
    )

    melt_splitter_df = (
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
        )
    )

    map_splitter_df = (
        map_splitter_df.join(
            melt_splitter_df,
            on=["source_start", "source_end", "delta_right"],
            how="inner",
        )
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

    return map_splitter_df


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


def part_01(seeds_df, maps_df):
    part1_df = seeds_df.explode("seeds").select(
        pl.col("seeds").alias("source_start"),
        pl.col("seeds").alias("source_end"),
        pl.lit(0).alias("delta"),
    )

    for n in range(7):
        part1_df = map_df_ranges(part1_df, maps_df, n + 1)

    return part1_df


def part_02(seeds_df, maps_df):
    part2_df = seeds_df.select(
        pl.col("seeds").explode().gather_every(2).alias("source_start"),
        (
            pl.col("seeds").explode().gather_every(2)
            + pl.col("seeds").list.slice(1).explode().gather_every(2)
            - 1
        ).alias("source_end"),
        pl.lit(0).alias("delta"),
    )

    for n in range(7):
        part2_df = map_df_ranges(part2_df, maps_df, n + 1)

    return part2_df


def part_01_bruteforce(seeds_df, maps_df):
    lazy_maps_df = maps_df.sort("source_start").lazy()

    part1_brute_df = (
        seeds_df.lazy()
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

    return part1_brute_df


def generate_map_df(input_df: pl.DataFrame):
    maps_df = (
        input_df.with_columns(
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

    return maps_df


if __name__ == "__main__":
    cfg = pl.Config()
    cfg.set_tbl_cols(25)
    cfg.set_fmt_table_cell_list_len(25)

    input_df = pl.read_csv("2023/day05.txt", has_header=False, separator="~")
    seeds_df = input_df.filter(pl.col("column_1").str.starts_with("seeds:")).select(
        pl.col("column_1")
        .str.extract_all(r"\d+")
        .cast(pl.List(pl.Int64))
        .alias("seeds")
    )

    maps_df = generate_map_df(input_df)

    part1_brute_df = part_01_bruteforce(seeds_df, maps_df)
    print("Part 1 Bruteforce:", part1_brute_df)

    part1_df = part_01(seeds_df, maps_df)
    print("Part 1:", part1_df.select("source_start").min())

    part2_df = part_02(seeds_df, maps_df)
    print("Part 2:", part2_df.select("source_start").min())
