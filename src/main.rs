use mimalloc::MiMalloc;
use polars::prelude::*;
use std::fs::File;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let colors = CsvReader::from_path("csvs/colors.csv")?
        .has_header(true)
        .finish();
    Ok(())
}
