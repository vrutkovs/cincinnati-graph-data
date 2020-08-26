pub mod check_releases;
pub mod verify_yaml;
pub use anyhow::Result as Fallible;

async fn run_all_tests() -> Fallible<()> {
    let found_versions = verify_yaml::run().await?;
    check_releases::run(found_versions).await?;
    Ok(())
}

fn main() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    std::process::exit(match runtime.block_on(run_all_tests()) {
        Ok(_) => 0,
        Err(e) => {
            println!("{}", e);
            1
        }
    })
}
