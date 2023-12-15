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

    def to_wpm(df: pl.DataFrame, suffix: str):
        return (
            df.select(
                pl.col('wpm').cast(pl.Utf8)
                .str.pad_start(6).alias('wpm_' + suffix),
                pl.col('index').cast(pl.Utf8)
                .str.pad_start(5).str.pad_end(7).alias('index_' + suffix),
            )
        )

    worst = (
        df.sort('wpm')
        .head(max_len)
        .sort('index', descending=True)
        .pipe(to_wpm, 'w')
    )
    best = (
        df.sort('wpm', descending=True)
        .head(max_len)
        .sort('index', descending=True)
        .pipe(to_wpm, 'b')
    )
    earliest = df.head(max_len).pipe(to_wpm, 'e')
    with sys.stdout as f:
        f.write(csv + '\n')
        f.write(f'Restarts: {restarts}\n')
        full_df.pipe(mk.write_results, f)
        f.write('Best:\t\tWorst:\t\tEarliest:\n')
        f.write(
            pl.concat([best, worst, earliest], how='horizontal')
            .write_csv(separator='\t') + '\n'
        )
