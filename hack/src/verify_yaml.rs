use cincinnati::plugins::internal::openshift_secondary_metadata_parser::plugin;
use regex::Regex;
use semver::Version;
use serde::de::DeserializeOwned;
use serde_yaml;
use std::collections::HashSet;
use std::path::PathBuf;
use tokio;

pub use anyhow::Result as Fallible;
use anyhow::{bail, Context};

pub async fn run() -> Fallible<HashSet<Version>> {
  let data_dir = PathBuf::from("..");
  let extension_re = Regex::new("ya+ml").unwrap();
  // Collect a list of mentioned versions
  let mut found_versions: HashSet<Version> = HashSet::new();

  println!("Verifying blocked edge files are valid");
  let blocked_edge_path = data_dir.join(plugin::BLOCKED_EDGES_DIR).canonicalize()?;
  let blocked_edge_vec =
    walk_files::<plugin::graph_data_model::BlockedEdge>(&blocked_edge_path, &extension_re).await?;
  for v in blocked_edge_vec.iter() {
    found_versions.insert(v.to.clone());
  }

  println!("Verifying channel files are valid");
  let channel_path = data_dir.join(plugin::CHANNELS_DIR).canonicalize().unwrap();
  let channels_vec =
    walk_files::<plugin::graph_data_model::Channel>(&channel_path, &extension_re).await?;
  for c in channels_vec.iter() {
    for v in c.versions.iter() {
      found_versions.insert(v.clone());
    }
  }

  Ok(found_versions)
}

pub async fn walk_files<T>(path: &PathBuf, extension_re: &Regex) -> Fallible<Vec<T>>
where
  T: DeserializeOwned,
{
  use tokio::stream::Stream;
  use tokio::stream::StreamExt;

  let mut file_err_vec: Vec<std::io::Error> = vec![];
  let mut extension_err_vec: Vec<std::io::Error> = vec![];
  let mut serialize_err_vec: Vec<std::io::Error> = vec![];

  let mut paths = tokio::fs::read_dir(&path)
    .await
    .context(format!("Reading directory {:?}", &path))?
    .filter_map(|tried_direntry| match tried_direntry {
      Ok(direntry) => Some(direntry),
      Err(e) => {
        file_err_vec.push(e);
        None
      }
    })
    .filter_map(|direntry| {
      let path = direntry.path();
      if let Some(extension) = &path.extension() {
        if extension_re.is_match(extension.to_str().unwrap_or_default()) {
          Some(path)
        } else {
          extension_err_vec.push(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
              "File {:?} has invalid extension: {}",
              &path,
              extension.to_str().unwrap_or_default()
            ),
          ));
          None
        }
      } else {
        extension_err_vec.push(std::io::Error::new(
          std::io::ErrorKind::Other,
          format!("{:?} does not have an extension", &path,),
        ));
        None
      }
    });

  let mut path_vec: Vec<T> = Vec::with_capacity(match paths.size_hint() {
    (_, Some(upper)) => upper,
    (lower, None) => lower,
  });

  while let Some(path) = paths.next().await {
    match tokio::fs::read(&path).await {
      Ok(yaml) => match serde_yaml::from_slice::<T>(&yaml) {
        Ok(value) => path_vec.push(value),
        Err(e) => {
          serialize_err_vec.push(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to deserialize file at {:?}: {}", &path, e),
          ));
        }
      },
      Err(e) => {
        serialize_err_vec.push(std::io::Error::new(
          std::io::ErrorKind::Other,
          format!("Couldn't read file {:?}: {}", &path, e),
        ));
      }
    }
  }

  let mut has_errors = false;
  for v in vec![
    ("file", file_err_vec),
    ("extension", extension_err_vec),
    ("serialization", serialize_err_vec),
  ] {
    if v.1.len() > 0 {
      println!("Found {} errors:", v.0);
      println!("{:?}", v.1);
      has_errors = true;
    }
  }
  match has_errors {
    true => bail!("Exiting due to errors"),
    false => Ok(path_vec),
  }
}
