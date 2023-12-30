use std::ops::Div;
use std::ops::Mul;
use std::path::Path;

use polars::enable_string_cache;
use polars::lazy::dsl::col;
use polars::lazy::dsl::Expr;
use polars::prelude::*;

// Need const fn in SortOptions::default
fn descending() -> SortOptions {
    SortOptions {
        descending: true,
        ..SortOptions::default()
    }
}

fn format_as_monkey(df: LazyFrame) -> LazyFrame {
    df.with_column(col("timestamp").dt().timestamp(TimeUnit::Milliseconds))
        .sort("timestamp", descending())
        .drop_columns(["index"])
}

const DTYPES: [(&'static str, DataType); 24] = [
    ("_id", DataType::Utf8),
    ("isPb", DataType::Utf8),
    ("wpm", DataType::Float64),
    ("acc", DataType::Float64),
    ("rawWpm", DataType::Float64),
    ("consistency", DataType::Float64),
    ("charStats", DataType::Utf8),
    ("mode", DataType::Categorical(None)),
    ("mode2", DataType::Categorical(None)),
    ("quoteLength", DataType::Int64),
    ("restartCount", DataType::Int64),
    ("testDuration", DataType::Float64),
    ("afkDuration", DataType::Int64),
    ("incompleteTestSeconds", DataType::Float64),
    ("punctuation", DataType::Boolean),
    ("numbers", DataType::Boolean),
    ("language", DataType::Utf8),
    ("funbox", DataType::Utf8),
    ("difficulty", DataType::Utf8),
    ("lazyMode", DataType::Boolean),
    ("blindMode", DataType::Boolean),
    ("bailedOut", DataType::Boolean),
    ("tags", DataType::Utf8),
    ("timestamp", DataType::Int64),
];

fn schema() -> Option<SchemaRef> {
    let mut schema = Schema::new();
    for (col, dt) in DTYPES {
        schema.with_column(col.into(), dt);
    }
    Some(schema.into())
}

fn format_df(lf: LazyFrame) -> LazyFrame {
    lf.with_row_count("index", None)
        .with_column(col("timestamp".into()).cast(DataType::Datetime(TimeUnit::Milliseconds, None)))
        .sort("timestamp", SortOptions::default())
}

fn get_df(path_glob: &Path, separator: u8) -> PolarsResult<LazyFrame> {
    LazyCsvReader::new(path_glob)
        .with_separator(separator)
        .with_schema(schema())
        .finish()
        .map(|lf| {
            lf.unique(None, UniqueKeepStrategy::Any)
                .with_column(
                    col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
                )
                .sort("timestamp", descending())
                .with_row_count("index", None)
                .sort("index", descending())
        })
}

fn get_main(src: &Path) -> PolarsResult<LazyFrame> {
    LazyFrame::scan_parquet(src, ScanArgsParquet::default()).map(|lf| {
        format_as_monkey(format_df(
            lf.with_columns(
                DTYPES
                    .iter()
                    .map(|(c, dt)| col((*c).into()).cast(dt.clone()))
                    .collect::<Vec<_>>(),
            ),
        ))
    })
}

fn append_to_results(src: &Path, path_glob: &Path) -> PolarsResult<LazyFrame> {
    concat(
        [format_as_monkey(get_df(path_glob, b'|')?), get_main(src)?],
        UnionArgs::default(),
    )
    .map(|lf| {
        lf.unique(None, UniqueKeepStrategy::Any)
            .with_columns([
                when(col("quoteLength".into()).eq(Expr::Literal(LiteralValue::Int64(-1))))
                    .then(Expr::Literal(LiteralValue::Null))
                    .otherwise(col("quoteLength")),
                col("timestamp".into())
                    .div(Expr::Literal(LiteralValue::Int64(1000)))
                    .mul(Expr::Literal(LiteralValue::Int64(1000))),
            ])
            .group_by([col("_id")])
            .agg([all().max()])
    })
    .map(|lf| format_as_monkey(lf).sort("timestamp".into(), SortOptions::default()))
}

fn main() -> PolarsResult<()> {
    enable_string_cache();
    let src = Path::new("/home/chrlz/repos/monkey-typo/results.parquet");
    let path_glob = Path::new("/home/chrlz/dox/dl/results*.csv");
    // cannot create series from UInt8??
    let df = append_to_results(&src, &path_glob)?.collect()?;
    println!("{df}");
    Ok(())
}
