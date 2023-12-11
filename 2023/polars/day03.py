import polars as pl

part_len_expr = pl.col("parts").str.len_bytes()
pos_expr = part_len_expr.cum_sum() - part_len_expr

df = (
    pl.read_csv("2023/day03.txt", has_header=False, separator=":")
    .with_row_count()
    .with_columns(
        pl.col("column_1").str.extract_all(r"\.+|\d+|[^.\d]").alias("parts"),
        pl.col("column_1").str.len_bytes().alias("row_len"),
    )
    .explode("parts")
    .with_row_count("part_nr")
    .with_columns(
        pos_expr.alias("pos"),
        part_len_expr.alias("pos_len"),
        (pos_expr // pl.col("row_len")).alias("row_num"),
        (pos_expr % pl.col("row_len")).alias("col_num"),
    )
    .filter(~pl.col("parts").str.contains(r"\."))
    .with_columns((pl.col("col_num") + pl.col("pos_len")).alias("col_end"))
)

is_symbols = pl.col("parts").str.contains(r"[^\d]")

symbols_df = (
    df.filter(is_symbols)
    .with_columns(
        pl.concat_list([pl.col("row_num") + n for n in range(-1, 2)]).alias("row_span"),
        pl.concat_list([pl.col("col_num") + n for n in range(-1, 2)]).alias("col_span"),
    )
    .explode("row_span")
    .explode("col_span")
)

numbers_df = (
    df.filter(~is_symbols)
    .with_columns(
        pl.col("row_num").cast(pl.Int64).alias("row_span"),
        pl.int_ranges("col_num", "col_end").alias("col_span"),
        pl.col("parts").str.to_integer().alias("number_value"),
    )
    .explode("col_span")
)

print(
    "Part 1:",
    numbers_df.join(symbols_df, how="semi", on=["row_span", "col_span"])
    .unique(["part_nr"])
    .select("number_value")
    .sum(),
)

print(
    "Part 2:",
    symbols_df.filter(pl.col("parts") == pl.lit("*"))
    .join(numbers_df, how="inner", on=["row_span", "col_span"])
    .unique(["part_nr_right"])
    .group_by("part_nr")
    .agg(
        pl.col("number_value").count().alias("number_count"),
        pl.col("number_value").product().alias("number_product"),
    )
    .filter(pl.col("number_count") == 2)
    .select(pl.col("number_product"))
    .sum(),
)
