use mimalloc::MiMalloc;
use polars::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let iris = LazyCsvReader::new("iris.csv".to_string())
        .has_header(true)
        .finish()?;

    let df = iris
        .select([
            col("species"),
            col("sepal_length"),
            col("sepal_length")
                .median()
                .over([col("species")])
                .alias("group_median"),
        ])
        .with_column(
            (col("sepal_length") / col("group_median") * lit(100.0_f64)).alias("percent_median"),
        )
        .collect()?;

    println!("{df}");

    Ok(())
}
