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

    let df = ParquetReader::new(cursor)
        .finish()
        .with_context(|| format!("Failure finishing parquet-read for '{table}'"))?;
    Ok(df)
}

fn main() -> Result<()> {
    let mut colors = load("colors")?;
    colors
        .try_apply("is_trans", |s| s.cast(&DataType::Categorical(None)))
        .context(format!(
            "Failure casting colors.is_trans to categorical type."
        ))?;
    let parts = load("parts")?;
    let inventory_parts = load("inventory_parts")?;
    let inventories = load("inventories")?;
    let sets = load("sets")?;

    let some_glitters = colors
        .clone()
        .lazy()
        .filter(col("is_trans").eq(lit("t")))
        .sort_by_exprs(vec![col("name")], vec![false])
        .limit(5);
    let rand_sets = sets.sample_n(5, false, Some(42))?;

    let joined = sets
        .lazy()
        .rename(&["name"], &["set_name"])
        .inner_join(
            inventories.lazy().rename(&["id"], &["inventory_id"]),
            col("set_num"),
            col("set_num"),
        )
        .inner_join(
            inventory_parts.lazy(),
            col("inventory_id"),
            col("inventory_id"),
        )
        .inner_join(
            parts.lazy().rename(&["name"], &["part_name"]),
            col("part_num"),
            col("part_num"),
        )
        .inner_join(
            colors
                .lazy()
                .rename(&["id", "name"], &["color_id", "color_name"]),
            col("color_id"),
            col("color_id"),
        )
        .select(&[
            col("set_num"),
            col("set_name"),
            col("year"),
            col("part_num"),
            col("part_name"),
            col("color_name"),
            col("quantity"),
        ])
        .limit(5);

    let mut collected = collect_all([some_glitters, joined])?;
    collected.push(rand_sets);
    for df in collected {
        println!("{}", df);
    }
    Ok(())
}
