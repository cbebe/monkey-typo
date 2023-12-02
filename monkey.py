import os
import re
from glob import glob

import polars as pl

DTYPES = {
    'mode': pl.Categorical,
    'mode2': pl.Categorical,
    'isPb': pl.Utf8,
    'restartCount': pl.UInt8,
    'afkDuration': pl.UInt8,
    'quoteLength': pl.Int64,
}


def cast_dtypes(df: pl.DataFrame):
    return df.with_columns([pl.col(x).cast(t) for x, t in DTYPES.items()])


def append_to_results(src='./results.parquet', pathname='/home/chrlz/Downloads/results*.csv'):
    return (
        pl.concat([
            get_all(pathname).pipe(format_as_monkey),
            (
                pl.read_parquet(src).pipe(cast_dtypes)
                .pipe(format_df).pipe(format_as_monkey)),
        ])
        .unique()
        .with_columns(
            pl.when(pl.col('quoteLength') == -1)
            .then(pl.lit(None))
            .otherwise(pl.col('quoteLength'))
            .name.keep(),
            ((pl.col('timestamp') / 1000).round().cast(pl.Int64) * 1000)
        ).group_by('_id')
        .max()
        .pipe(format_as_monkey)
        .sort('timestamp')
    )


def get_all(pathname: str):
    return (
        pl.concat([get_df(x) for x in glob(pathname)])
        .drop('index')
        .unique()
        .sort('timestamp', descending=True)
        .pipe(add_index)
        .sort('index', descending=True)
    )


def format_as_monkey(df: pl.DataFrame):
    return (
        df.with_columns(pl.col('timestamp').dt.epoch(time_unit='ms'))
        .sort('timestamp', descending=True)
        .drop('index')
    )


def format_df(df: pl.DataFrame):
    return (
        df.pipe(add_index)
        .with_columns(pl.from_epoch('timestamp', time_unit='ms'))
        .sort('timestamp')
    )


def get_df(pathname: str, separator='|'):
    return pl.read_csv(pathname, separator=separator, dtypes=DTYPES).pipe(format_df)


def dropsame_lazy(df: pl.LazyFrame):
    """
    Drops all the columns that only have one unique value, but with the Lazy API.
    Except it actually needs to collect to find the unique values.
    """
    # LMFAO LazyAPI
    same_values = (
        df.select((pl.all().unique().len() == 1).all())
        .melt()
        .filter(pl.col('value') == False)
        .select('variable')
        .collect()
        .to_series()
        .to_list()
    )
    return df.select(same_values)


def dropsame(df: pl.DataFrame):
    """
    Drops all the columns that only have one unique value.
    """
    return df[[s.name for s in df if not (len(s.unique()) == 1)]]


def find_latest_csv(pathname: str):
    def match_path(x: str):
        return re.match('.*?([0-9]+).*', os.path.basename(x))
    res = max(
        glob(pathname),
        key=lambda x: int(m[1]) if (m := match_path(x)) else 0,
        default=None
    )
    if not res:
        raise Exception('No file found with glob')
    return res


def add_index(df: pl.DataFrame, col: str = 'index'):
    pdf = df.to_pandas(use_pyarrow_extension_array=True)
    pdf[col] = pdf.index
    return pl.from_pandas(pdf)


def to_struct(col: str, high='high', low='low', split=12):
    def split_expr(name: str, offset: int):
        return (
            pl.col(col)
            .str.slice(offset, split)
            .str.to_integer(base=16)
            .cast(pl.UInt64)
            .alias(name)
        )

    return pl.struct([split_expr(high, 0), split_expr(low, split)])


def to_int(col: str, high='high', low='low'):
    def to_hex(x: int):
        return hex(x)[2:]
    return (
        pl.col(col).struct.field(high).map_elements(to_hex) +
        pl.col(col).struct.field(low).map_elements(to_hex)
    )
