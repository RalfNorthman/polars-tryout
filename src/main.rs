use anyhow::{Context, Result};
use mimalloc::MiMalloc;
use polars::prelude::*;
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;
use std::fs::File;
use std::io::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn load(table: &str) -> Result<DataFrame> {
    let client = Client::new();
    let res = client
        .get(format!("http://localhost:8070/api/tables/{table}"))
        .header(ACCEPT, "application/vnd.apache.arrow.file")
        .send()
        .with_context(|| format!("Failure sending http GET for '{table}'"))?
        .bytes()
        .with_context(|| format!("Failure taking bytes from respones for '{table}'"))?;

    let mut file = File::create(format!("{table}.file"))
        .with_context(|| format!("Failure creating file for '{table}'"))?;
    file.write_all(&res)
        .with_context(|| format!("Failure writing bytes to file for '{table}'"))?;
    let ipc = IpcReader::new(file)
        .finish()
        .with_context(|| format!("Failure finishing ipc-read for '{table}'"))?;
    Ok(ipc)
}

fn main() -> Result<()> {
    let mut colors = load("colors")?;
    colors.try_apply("is_trans", |s| s.cast(&DataType::Categorical(None)))?;
    let parts = load("parts")?;
    let inventory_parts = load("inventory_parts")?;
    let inventories = load("inventories")?;
    let sets = load("sets")?;
    let themes = load("themes")?;

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
