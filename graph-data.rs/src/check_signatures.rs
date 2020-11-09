use anyhow::Result as Fallible;
use anyhow::{format_err, Context};
use futures::stream::{FuturesOrdered, StreamExt};
use gpgrv::{verify_message, Keyring};
use lazy_static::lazy_static;
use reqwest::{Client, ClientBuilder};
use semver::Version;
use std::collections::HashSet;
use std::fs::{read_dir, File};
use std::io::{BufReader, Seek, SeekFrom};
use std::ops::Range;
use std::str::FromStr;
use std::time::Duration;
use tempfile::tempfile;
use url::Url;

use cincinnati::plugins::prelude_plugin_impl::TryFutureExt;
use cincinnati::Release;
lazy_static! {
  // base url for signature storage - see https://github.com/openshift/cluster-update-keys/blob/master/stores/store-openshift-official-release-mirror
  static ref BASE_URL: Url =
    Url::parse("https://mirror.openshift.com/pub/openshift-v4/signatures/openshift/release/")
      .expect("could not parse url");
}

// Signature file request timeout
static DEFAULT_TIMEOUT_SECS: u64 = 30;

// CVO has maxSignatureSearch = 10 in pkg/verify/verify.go
static MAX_SIGNATURES: u64 = 10;

// Skip some versions from 4.0 / 4.1 / 4.2 times
// https://issues.redhat.com/browse/ART-2397
static SKIP_VERSIONS: &[&str] = &[
  "4.1.0-rc.3+amd64",
  "4.1.0-rc.5+amd64",
  "4.1.0-rc.4+amd64",
  "4.1.0-rc.0+amd64",
  "4.1.0-rc.8+amd64",
  "4.1.37+amd64",
  "4.2.11+amd64",
  "4.3.0-rc.0+amd64",
  "4.6.0-fc.3+s390x",
];

// Location of public keys
static PUBKEYS_DIR: &str = "/usr/local/share/public-keys/";

// Signature format
#[derive(Deserialize, Serialize)]
struct SignatureImage {
  #[serde(rename = "docker-manifest-digest")]
  digest: String,
}

#[derive(Deserialize, Serialize)]
struct SignatureCritical {
  image: SignatureImage,
}

#[derive(Deserialize, Serialize)]
struct Signature {
  critical: SignatureCritical,
}

fn payload_from_release(release: &Release) -> Fallible<String> {
  match release {
    Release::Concrete(c) => Ok(c.payload.clone()),
    _ => Err(format_err!("not a concrete release")),
  }
}

async fn fetch_url(client: &Client, sha: &str, i: u64) -> Fallible<String> {
  let url = BASE_URL
    .join(format!("{}/", sha.replace(":", "=")).as_str())?
    .join(format!("signature-{}", i).as_str())?;
  let res = client
    .get(url.clone())
    .send()
    .map_err(|e| anyhow::anyhow!(e.to_string()))
    .await?;

  let url_s = url.to_string();
  let status = res.status();
  match status.is_success() {
    true => Ok(res.text().await?),
    false => Err(format_err!("Error fetching {} - {}", url_s, status)),
  }
}

async fn verify_signature(keyring: &Keyring, body: &str, digest: &str) -> Fallible<()> {
  let mut temp = tempfile().context("Creating temp file for signature")?;
  verify_message(body.as_bytes(), &mut temp, keyring).context("Verifying signature")?;
  temp.seek(SeekFrom::Start(0)).unwrap();
  let signature: Signature = serde_json::from_reader(temp).context("Deserializing message")?;
  let actual_digest = signature.critical.image.digest;
  if actual_digest == digest {
    Ok(())
  } else {
    return Err(format_err!(
      "Valid signature, but digest mismatches: {}",
      actual_digest
    ));
  }
}

async fn find_signatures_for_version(
  client: &Client,
  keyring: &Keyring,
  release: &Release,
) -> Fallible<()> {
  let mut errors = vec![];
  let payload = payload_from_release(release)?;
  let digest = payload
    .split("@")
    .last()
    .ok_or_else(|| format_err!("could not parse payload '{:?}'", payload))?;

  let mut attempts = Range {
    start: 1,
    end: MAX_SIGNATURES,
  };
  loop {
    if let Some(i) = attempts.next() {
      match fetch_url(client, digest, i).await {
        Ok(body) => match verify_signature(&keyring, body.as_str(), digest).await {
          Ok(_) => return Ok(()),
          Err(e) => errors.push(e),
        },
        Err(e) => errors.push(e),
      }
    } else {
      return Err(format_err!(
        "Failed to verify signature for {} - {}: {:#?}",
        release.version(),
        payload,
        errors
      ));
    }
  }
}

fn is_release_in_versions(versions: &HashSet<Version>, release: &Release) -> bool {
  // Check that release version is not in skip list
  if SKIP_VERSIONS.contains(&release.version()) {
    return false;
  }
  // Strip arch identifier
  let stripped_version = release
    .version()
    .split("+")
    .next()
    .ok_or(release.version())
    .unwrap();
  let version = Version::from_str(stripped_version).unwrap();
  versions.contains(&version)
}

fn add_public_keys_to_keyring(keyring: &mut Keyring) -> Fallible<()> {
  for entry in read_dir(PUBKEYS_DIR).context("Reading public keys dir")? {
    let path = &entry?.path();
    let path_str = match path.to_str() {
      None => continue,
      Some(p) => p,
    };
    let input = BufReader::new(File::open(path).context(format!("Reading {}", path_str))?);
    keyring
      .append_keys_from_armoured(input)
      .context(format!("Appending {}", path_str))?;
  }
  Ok(())
}

pub async fn run(
  releases: &Vec<Release>,
  found_versions: &HashSet<semver::Version>,
) -> Fallible<()> {
  println!("Checking release signatures");

  // Initialize keyring
  let mut keyring = Keyring::new();
  add_public_keys_to_keyring(&mut keyring)?;

  let client: Client = ClientBuilder::new()
    .gzip(true)
    .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
    .build()
    .context("Building reqwest client")?;

  // Filter scraped images - skip CI images
  let tracked_versions: Vec<&cincinnati::Release> = releases
    .into_iter()
    .filter(|ref r| is_release_in_versions(found_versions, &r))
    .collect::<Vec<&cincinnati::Release>>();

  let results: Vec<Fallible<()>> = tracked_versions
    //Attempt to find signatures for filtered releases
    .into_iter()
    .map(|ref r| find_signatures_for_version(&client, &keyring, r))
    .collect::<FuturesOrdered<_>>()
    .collect::<Vec<Fallible<()>>>()
    .await
    // Filter to keep errors only
    .into_iter()
    .filter(|e| e.is_err())
    .collect();
  if results.is_empty() {
    Ok(())
  } else {
    Err(format_err!("Signature check errors: {:#?}", results))
  }
}
