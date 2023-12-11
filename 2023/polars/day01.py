import polars as pl

df = pl.read_csv("2023/day01.txt", has_header=False, separator=":")

rev_str_expr = pl.col("column_1").str.split("").list.reverse().list.join("")

print(
    "Part 1:",
    df.select(
        pl.concat_str(
            pl.col("column_1").str.extract(r"(\d)"),
            rev_str_expr.str.extract(r"(\d)"),
        )
        .str.to_integer()
        .sum()
    ),
)

num_dict = {
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
}

print(
    "Part 2:",
    df.select(
        pl.concat_str(
            pl.col("column_1")
            .str.extract(rf"(\d|{'|'.join(num_dict.keys())})")
            .replace(num_dict),
            rev_str_expr.str.extract(rf"(\d|{'|'.join(num_dict.keys())[::-1]})")
            .str.split("")
            .list.reverse()
            .list.join("")
            .replace(num_dict),
        )
        .str.to_integer()
        .sum()
    ),
)
