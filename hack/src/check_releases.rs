use cincinnati::plugins::internal::release_scrape_dockerv2::plugin;
use cincinnati::plugins::internal::release_scrape_dockerv2::registry;

use anyhow::Result as Fallible;
use anyhow::{bail, Context};
use semver::Version;
use std::collections::HashSet;

pub async fn run(found_versions: HashSet<Version>) -> Fallible<()> {
  let settings = plugin::ReleaseScrapeDockerv2Settings::default();
  let cache = registry::cache::new();
  let registry = registry::Registry::try_from_str(&settings.registry)
    .context(format!("Parsing {} as Registry", &settings.registry))?;

  println!("Scraping Quay registry");
  let releases = registry::fetch_releases(
    &registry,
    &settings.repository,
    settings.username.as_ref().map(String::as_ref),
    settings.password.as_ref().map(String::as_ref),
    cache,
    &settings.manifestref_key,
    settings.fetch_concurrency,
  )
  .await
  .context("failed to fetch all release metadata")?;

  println!("Verifying all releases are uploaded");
  for v in found_versions.iter() {
    if releases
      .iter()
      .find(|&r| r.metadata.version == *v)
      .is_none()
    {
      bail!("Version {} is not found in scraped images", v)
    }
  }

  Ok(())
}
