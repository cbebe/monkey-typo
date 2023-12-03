#!.venv/bin/python

"""
append.py

Write the latest results to results.parquet
"""

from monkey import append_to_results, format_df

if __name__ == "__main__":
    df = append_to_results()
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
        f.write(f'Total rows: {len(df)}\n')
        f.write(f'Mean (recent 1000): {mean}\n')
        f.write(f'PB (recent 1000): {best["wpm"]} ({best["index"]})\n')
        f.write(f'Worst (recent 1000): {worst["wpm"]} ({worst["index"]})\n')
    df.write_parquet('./results.parquet')
