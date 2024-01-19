#!.venv/bin/python

"""
append.py

Write the latest results to results.parquet
"""

from monkey import append_to_results, format_df, write_results

if __name__ == "__main__":
    pathname = '/home/chrlz/dox/dl/results*.csv'
    # pathname = 'C:/Users/maple/Downloads/results*.csv'
    df = append_to_results(pathname=pathname)
    recent = (
        df.tail(1000-len(df))
        .sort('timestamp', descending=True)
        .pipe(format_df)
    )
    mean = recent['wpm'].mean()
    worst = recent.sort('wpm').select('wpm', 'index')[0].to_dicts()[0]
    best = recent.sort('wpm', descending=True).select(
        'wpm', 'index')[0].to_dicts()[0]
    with open('results.txt', 'w') as f:
        df.pipe(write_results, f)
    df.write_parquet('./results.parquet')
