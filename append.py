#!.venv/bin/python

"""
append.py

Write the latest results to results.tsv
"""

from monkey import append_to_results

if __name__ == "__main__":
    append_to_results().write_parquet('./results.parquet')
