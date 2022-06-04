use mimalloc::MiMalloc;
use polars::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let load = |s: &str| LazyCsvReader::new(s.into()).has_header(true).finish();

    let mut colors = load("csvs/colors.csv")?.collect()?;
    colors.try_apply("is_trans", |s| s.cast(&DataType::Categorical(None)))?;
    let parts = load("csvs/parts.csv")?;
    let inventory_parts = load("csvs/inventory_parts.csv")?;
    let inventories = load("csvs/inventories.csv")?;
    let sets = load("csvs/sets.csv")?;
    let themes = load("csvs/themes.csv")?;

    let some_glitters = colors
        .lazy()
        .filter(col("is_trans").eq(lit("t")))
        .sort_by_exprs(vec![col("name")], vec![false])
        .limit(5);
    let rand_sets = sets.collect()?.sample_n(5, false, Some(42))?;

    let all = collect_all([some_glitters])?;
    for df in all {
        println!("{}", df);
    }
    println!("{}", rand_sets);
    Ok(())
}
