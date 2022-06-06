use anyhow::{Context, Result};
use mimalloc::MiMalloc;
use polars::prelude::*;
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;
use std::io::prelude::*;
use std::io::Cursor;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn load(table: &str) -> Result<DataFrame> {
    let client = Client::new();
    let response = client
        .get(format!("http://localhost:8070/api/tables/{table}"))
        .header(ACCEPT, "application/parquet")
        .send()
        .with_context(|| format!("Failure sending http GET for '{table}'"))?
        .bytes()
        .with_context(|| format!("Failure taking bytes from response for '{table}'"))?;

    let mut cursor = Cursor::new(Vec::new());
    cursor
        .write_all(&response)
        .with_context(|| format!("Failure writing to cursor for '{table}'"))?;

    let ipc = ParquetReader::new(cursor)
        .finish()
        .with_context(|| format!("Failure finishing parquet-read for '{table}'"))?;
    Ok(ipc)
}

fn main() -> Result<()> {
    let mut colors = load("colors")?;
    colors.try_apply("is_trans", |s| s.cast(&DataType::Categorical(None)))?;
    let parts = load("parts")?;
    let inventory_parts = load("inventory_parts")?;
    let inventories = load("inventories")?;
    let sets = load("sets")?;

    let some_glitters = colors
        .lazy()
        .filter(col("is_trans").eq(lit("t")))
        .sort_by_exprs(vec![col("name")], vec![false])
        .limit(5);
    let rand_sets = sets.sample_n(5, false, Some(42))?;

    let all = collect_all([some_glitters])?;
    for df in all {
        println!("{}", df);
    }
    println!("{}", rand_sets);
    Ok(())
}
