use anyhow::Result as Fallible;
use anyhow::{format_err, Context};
use bytes::buf::BufExt;
use bytes::Bytes;
use futures::stream::{FuturesOrdered, StreamExt};
use lazy_static::lazy_static;
use reqwest::{Client, ClientBuilder};
use semver::Version;
use std::collections::HashSet;
use std::fs::{read_dir, File};
use std::ops::Range;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

use pgp::composed::message::Message;
use pgp::composed::signed_key::SignedPublicKey;
use pgp::Deserializable;

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

/// Keyring is a collection of public keys
type Keyring = Vec<SignedPublicKey>;

/// Extract payload value from Release if its a Concrete release
fn payload_from_release(release: &Release) -> Fallible<String> {
  match release {
    Release::Concrete(c) => Ok(c.payload.clone()),
    _ => Err(format_err!("not a concrete release")),
  }
}

/// Fetch signature contents by building a URL for signature store
async fn fetch_url(client: &Client, sha: &str, i: u64) -> Fallible<Bytes> {
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
    true => Ok(res.bytes().await?),
    false => Err(format_err!("Error fetching {} - {}", url_s, status)),
  }
}

/// Verify that signature is valid and contains expected digest
async fn verify_signature(
  public_keys: &Keyring,
  body: Bytes,
  expected_digest: &str,
) -> Fallible<()> {
  let msg = Message::from_bytes(body.reader()).context("Parsing message")?;

  // Verify signature using provided public keys
  if !public_keys.iter().any(|ref k| msg.verify(k).is_ok()) {
    return Err(format_err!("No matching key found to decrypt {:#?}", msg));
  }

  // Deserialize the message
  let contents = match msg.get_content().context("Reading contents")? {
    None => return Err(format_err!("Empty message received")),
    Some(m) => m,
  };
  let signature: Signature = serde_json::from_slice(&contents).context("Deserializing message")?;
  let actual_digest = signature.critical.image.digest;
  if actual_digest == expected_digest {
    Ok(())
  } else {
    return Err(format_err!(
      "Valid signature, but digest mismatches: {}",
      actual_digest
    ));
  }
}

/// Generate URLs for signature store and attempt to find a valid signature
async fn find_signatures_for_version(
  client: &Client,
  public_keys: &Keyring,
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
        Ok(body) => match verify_signature(public_keys, body, digest).await {
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

/// Iterate versions and return true if Release is included
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

/// Create a Keyring from a dir of public keys
fn load_public_keys() -> Fallible<Keyring> {
  let mut result: Keyring = vec![];
  for entry in read_dir(PUBKEYS_DIR).context("Reading public keys dir")? {
    let path = &entry?.path();
    let path_str = match path.to_str() {
      None => continue,
      Some(p) => p,
    };
    let file = File::open(path).context(format!("Reading {}", path_str))?;
    let (pubkey, _) =
      SignedPublicKey::from_armor_single(file).context(format!("Parsing {}", path_str))?;
    match pubkey.verify() {
      Err(err) => return Err(format_err!("{:?}", err)),
      Ok(_) => result.push(pubkey),
    };
  }
  Ok(result)
}

pub async fn run(
  releases: &Vec<Release>,
  found_versions: &HashSet<semver::Version>,
) -> Fallible<()> {
  println!("Checking release signatures");

  // Initialize keyring
  let public_keys = load_public_keys()?;

  // Prepare http client
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
    .map(|ref r| find_signatures_for_version(&client, &public_keys, r))
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
