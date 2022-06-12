use anyhow::{Context, Result};
use mimalloc::MiMalloc;
use polars::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn load(table: &str) -> Result<LazyFrame> {
    let path = format!("csvs/{table}.csv");
    let df = LazyCsvReader::new(path).has_header(true).finish()?;
    Ok(df)
}

fn main() -> Result<()> {
    let colors = load("colors")?;
    let mut colors = colors.collect()?;
    colors
        .try_apply("is_trans", |s| s.cast(&DataType::Categorical(None)))
        .context("Failure casting colors.is_trans to categorical type.".to_string())?;
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
    let rand_sets = sets
        .clone()
        .collect()?
        .sample_n(5, false, false, Some(42))?;

    let joined = sets
        .rename(&["name"], &["set_name"])
        .inner_join(
            inventories.rename(&["id"], &["inventory_id"]),
            col("set_num"),
            col("set_num"),
        )
        .inner_join(inventory_parts, col("inventory_id"), col("inventory_id"))
        .inner_join(
            parts.rename(&["name"], &["part_name"]),
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
