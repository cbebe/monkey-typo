#!.venv/bin/python

"""
stats.py

Prints MonkeyType stats.
"""

import sys

import polars as pl

import monkey as mk

if __name__ == "__main__":
    max_len = 15
    pathname = '/home/chrlz/Downloads/results*.csv'
    # pathname = 'C:/Users/maple/Downloads/results*.csv'
    full_df = mk.append_to_results()
    df = full_df.pipe(mk.get_recent)
    csv = mk.find_latest_csv(pathname)
    mean = df['wpm'].mean()
    restarts = df['restartCount'].sum()/len(df)
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
    with sys.stdout as f:
        f.write(csv + '\n')
        f.write(f'Restarts: {restarts}\n')
        full_df.pipe(mk.write_results, f)
        f.write('Worst:\t\tEarliest:\n')
        f.write(
            pl.concat([worst, earliest], how='horizontal')
            .write_csv(separator='\t') + '\n'
        )
