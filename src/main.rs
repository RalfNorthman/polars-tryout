use mimalloc::MiMalloc;
use polars::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let iris = LazyCsvReader::new("iris.csv".to_string())
        .has_header(true)
        .finish()?;

    let sel = iris
        .clone()
        .select([
            col("species").unique().sort(false).head(Some(2)),
            col("sepal_length")
                .filter(col("species").eq(lit("virginica")))
                .median(),
        ])
        .collect()?;

    let aggr = iris
        .filter(col("sepal_length").gt(lit(5.0_f64)))
        .groupby([col("species")])
        .agg([all().sum()])
        .collect()?;

    println!("{aggr}");
    println!("{sel}");

    Ok(())
}
