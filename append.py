#!.venv/bin/python

"""
append.py

Write the latest results to results.csv
"""

from monkey import append_to_results

if __name__ == "__main__":
    append_to_results().write_csv('./results.csv', separator='|')
