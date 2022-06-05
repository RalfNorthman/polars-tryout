use anyhow::Result;
use mimalloc::MiMalloc;
use polars::prelude::*;
use reqwest::header::ACCEPT;
use reqwest::Client;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

async fn load(table: &str) -> Result<DataFrame> {
    let client = Client::new();
    let res = client
        .get(format!("http://localhost:8070/api/tables{table}"))
        .header(ACCEPT, "application/vnd.apache.arrow.file")
        .send()
        .await?
        .bytes()
        .await?;

    let mut file = File::create("response").await?;
    file.write_all(&res).await?;
    let file = file.into_std().await;
    let ipc = IpcReader::new(file).finish()?;
    Ok(ipc)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut colors = load("colors").await?;
    colors.try_apply("is_trans", |s| s.cast(&DataType::Categorical(None)))?;
    let parts = load("parts").await?;
    let inventory_parts = load("inventory_parts").await?;
    let inventories = load("inventories").await?;
    let sets = load("sets").await?;
    let themes = load("themes").await?;

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
