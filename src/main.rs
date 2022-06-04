use mimalloc::MiMalloc;
use polars::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let colors = LazyCsvReader::new("csvs/colors.csv".into())
        .has_header(true)
        .finish()?;
    let filtered = colors.filter(col("id").lt(lit(6_i64))).collect()?;
    println!("{}", filtered);
    Ok(())
}
