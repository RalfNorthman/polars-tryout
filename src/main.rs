use mimalloc::MiMalloc;
use polars::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let iris = LazyCsvReader::new("iris.csv".to_string())
        .has_header(true)
        .finish()?;

    let aggr = iris
        .filter(col("sepal_length").gt(lit(5.0_f64)))
        .groupby([col("species")])
        .agg([all().sum()])
        .collect()?;

    println!("{aggr}");

    Ok(())
}
