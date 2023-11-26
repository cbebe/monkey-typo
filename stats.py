#!.venv/bin/python

"""
stats.py

Prints MonkeyType stats.
"""

import polars as pl

from monkey import find_latest_csv, get_df

if __name__ == "__main__":
    max_len = 10
    pathname = '/home/chrlz/Downloads/results*.csv'
    csv = find_latest_csv(pathname)
    df = get_df(csv)
    mean = df['wpm'].mean()
    restarts = df['restartCount'].sum()/len(df)
    print(f'Mean: {mean}')
    print(f'Restarts: {restarts}')
    worst = (
        df.sort('wpm')
        .head(max_len)
        .sort('index', descending=True)
        .with_columns(
            pl.col('wpm').cast(pl.Utf8).str.pad_start(6),
            pl.col('index').cast(pl.Utf8).str.pad_start(5).str.pad_end(7),
        )
        .select(wpm_w='wpm', index_w='index')
    )
    earliest = (
        df.head(max_len)
        .with_columns(
            pl.col('wpm').cast(pl.Utf8).str.pad_start(6),
            pl.col('index').cast(pl.Utf8).str.pad_start(5).str.pad_end(7),
        )
        .select(wpm_e='wpm', index_e='index')
    )
    print('Worst:\t\tEarliest:')
    print(pl.concat([worst, earliest],
          how='horizontal').write_csv(separator='\t'))
