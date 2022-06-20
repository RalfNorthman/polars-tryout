use mimalloc::MiMalloc;
use polars::prelude::*;

const SPECIES: &str = "species";
const SEP_LEN: &str = "sepal_length";

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let iris = LazyCsvReader::new("iris.csv".to_string())
        .has_header(true)
        .finish()?;

    let df = iris
        .select([
            col(SPECIES),
            col(SEP_LEN),
            col(SEP_LEN)
                .median()
                .over([col(SPECIES)])
                .alias("group_median"),
        ])
        .with_column((col(SEP_LEN) / col("group_median") * lit(100.0_f64)).alias("percent_median"))
        .groupby([col(SPECIES)])
        .agg([
            col("percent_median").min().suffix("_min"),
            col("percent_median").max().suffix("_max"),
        ])
        .collect()?;

    println!("{df}");

    Ok(())
}
