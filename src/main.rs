use mimalloc::MiMalloc;
use polars::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let df1 = df![
        "a" => [-6_i8, -3, 0],
    ]?
    .lazy();

    let df2 = df![
        "b1" => [-6_i8, -4, -2, 0],
        "b2" => [-6_i8, -4, -2, 0],
    ]?
    .lazy();

    let df = df1.inner_join(df2, col("a"), col("b1")).collect()?;

    println!("{df}");

    Ok(())
}
