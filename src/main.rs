use anyhow::Result;
use mimalloc::MiMalloc;
use modulo::Mod;
use polars::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let a: Vec<i8> = ((-127i8)..=127).filter(|n| n.modulo(7) == 0).collect();
    let b: Vec<i8> = ((-127i8)..=127).filter(|n| n.modulo(3) == 0).collect();

    let df1 = df![
        "a" => a,
    ]?
    .lazy();

    let df2 = df![
        "b" => b,
    ]?
    .lazy();

    let df = df1.inner_join(df2, col("a"), col("b")).collect()?;

    dbg!((-119i8).modulo(7) == 0);
    dbg!((-119i8).modulo(3) == 0);

    dbg!(-7i8.modulo(7) == 0);
    dbg!(-3i8.modulo(3) == 0);

    println!("{df}");

    Ok(())
}
