import os
import re
from glob import glob

import polars as pl


def get_all(pathname: str):
    return (
        pl.concat([get_df(x) for x in glob(pathname)])
        .drop('index')
        .unique()
        .sort('timestamp')
        .pipe(add_index)
        .sort('index', descending=True)
    )

def get_df(pathname: str):
    return (
        pl.read_csv(pathname, separator='|', dtypes={
            'mode': pl.Categorical,
            'mode2': pl.Categorical,
            'isPb': pl.Boolean,
            'restartCount': pl.UInt8,
            'afkDuration': pl.UInt8,
        })
        .pipe(add_index)
        .drop('tags')
        .with_columns(pl.from_epoch('timestamp', time_unit='ms'))
        .sort('timestamp')
    )


def dropsame_lazy(df: pl.LazyFrame):
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
    return pl.struct([
        pl.col(col).str.slice(0, split).str.to_integer(
            base=16).cast(pl.UInt64).alias(high),
        pl.col(col).str.slice(split, split).str.to_integer(
            base=16).cast(pl.UInt64).alias(low),
    ])


def to_int(col: str, high='high', low='low'):
    def to_hex(x: int):
        return hex(x)[2:]
    return (
        pl.col(col).struct.field(high).map_elements(to_hex) +
        pl.col(col).struct.field(low).map_elements(to_hex)
    )
