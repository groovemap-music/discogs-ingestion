//! Explicit, one-shot local input for operator and released-image smoke runs.
//!
//! Production extraction continues to use [`super::downloader::Downloader`]. This source is
//! selected only by the `--local-manifest` CLI option and deliberately accepts the narrow
//! versioned smoke-manifest contract rather than turning arbitrary local files into trusted
//! extractor input.

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncReadExt;

use crate::state_marker::StateMarker;
use crate::types::{DataType, S3FileInfo};

use super::downloader::DataSource;

const CONTRACT_NAME: &str = "groovemap.discogs-extractor-smoke";
const CONTRACT_VERSION: u64 = 1;

#[derive(Debug, Deserialize)]
struct Manifest {
    contract: String,
    version: u64,
    scope: String,
    input: ManifestInput,
}

#[derive(Debug, Deserialize)]
struct ManifestInput {
    path: String,
    media_type: String,
    sha256: String,
    data_type: String,
    records: u64,
}

/// A checksum-verified, single-file source selected explicitly by an operator.
#[derive(Debug)]
pub struct LocalManifestSource {
    output_directory: PathBuf,
    source_path: PathBuf,
    file_name: String,
    expected_sha256: String,
    size: u64,
    state_marker: Option<StateMarker>,
    marker_path: Option<PathBuf>,
}

impl LocalManifestSource {
    /// Load and validate a v1 extractor smoke manifest.
    pub async fn from_manifest(manifest_path: &Path, output_directory: PathBuf) -> Result<Self> {
        let manifest_path = fs::canonicalize(manifest_path)
            .await
            .with_context(|| format!("Local manifest does not exist or is not accessible: {}", manifest_path.display()))?;
        let manifest_directory = manifest_path.parent().context("Local manifest path has no parent directory")?;
        let body = fs::read_to_string(&manifest_path)
            .await
            .with_context(|| format!("Failed to read local manifest: {}", manifest_path.display()))?;
        let manifest: Manifest =
            serde_json::from_str(&body).with_context(|| format!("Failed to parse local manifest JSON: {}", manifest_path.display()))?;

        ensure!(manifest.contract == CONTRACT_NAME, "Unsupported local manifest contract: {}", manifest.contract);
        ensure!(manifest.version == CONTRACT_VERSION, "Unsupported local manifest version: {} (expected {})", manifest.version, CONTRACT_VERSION);
        ensure!(manifest.scope == "single_file", "Unsupported local manifest scope: {}", manifest.scope);
        ensure!(manifest.input.media_type == "application/gzip", "Unsupported local manifest input media type: {}", manifest.input.media_type);
        ensure!(manifest.input.records > 0, "Local manifest input must declare at least one record");
        validate_checksum(&manifest.input.sha256)?;

        let relative_path = Path::new(&manifest.input.path);
        let mut components = relative_path.components();
        let file_name = match (components.next(), components.next()) {
            (Some(Component::Normal(name)), None) => name.to_str().context("Local manifest input path must be valid UTF-8")?.to_string(),
            _ => bail!("Local manifest input path must be one relative file name: {}", manifest.input.path),
        };
        let data_type = validate_file_name(&file_name)?;
        ensure!(data_type.as_str() == manifest.input.data_type, "Local manifest data_type does not match input file name");

        let source_path = fs::canonicalize(manifest_directory.join(relative_path))
            .await
            .with_context(|| format!("Local manifest input does not exist or is not accessible: {}", manifest.input.path))?;
        ensure!(
            source_path.parent() == Some(manifest_directory),
            "Local manifest input resolves outside the manifest directory: {}",
            manifest.input.path
        );
        let metadata = fs::metadata(&source_path)
            .await
            .with_context(|| format!("Failed to inspect local manifest input: {}", source_path.display()))?;
        ensure!(metadata.is_file(), "Local manifest input is not a regular file: {}", source_path.display());

        let source = Self {
            output_directory,
            source_path,
            file_name,
            expected_sha256: manifest.input.sha256,
            size: metadata.len(),
            state_marker: None,
            marker_path: None,
        };
        source.verify_source().await?;
        Ok(source)
    }

    async fn verify_source(&self) -> Result<()> {
        let actual = sha256_file(&self.source_path).await?;
        ensure!(
            actual == self.expected_sha256,
            "Local manifest checksum mismatch for {}: expected {}, got {}",
            self.file_name,
            self.expected_sha256,
            actual
        );
        Ok(())
    }

    async fn stage_source(&self) -> Result<()> {
        fs::create_dir_all(&self.output_directory)
            .await
            .with_context(|| format!("Failed to create local-manifest output directory: {}", self.output_directory.display()))?;
        self.verify_source().await?;

        let destination = self.output_directory.join(&self.file_name);
        if fs::canonicalize(&destination).await.ok().as_deref() == Some(self.source_path.as_path()) {
            return Ok(());
        }

        let temporary = self.output_directory.join(format!(".{}.local-manifest.tmp", self.file_name));
        fs::copy(&self.source_path, &temporary)
            .await
            .with_context(|| format!("Failed to stage local manifest input: {}", self.file_name))?;
        let staged_checksum = sha256_file(&temporary).await?;
        if staged_checksum != self.expected_sha256 {
            let _ = fs::remove_file(&temporary).await;
            bail!("Staged local manifest checksum mismatch for {}: expected {}, got {}", self.file_name, self.expected_sha256, staged_checksum);
        }
        fs::rename(&temporary, &destination)
            .await
            .with_context(|| format!("Failed to install local manifest input: {}", destination.display()))?;
        Ok(())
    }
}

fn validate_checksum(checksum: &str) -> Result<()> {
    ensure!(
        checksum.len() == 64 && checksum.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "Local manifest sha256 must be 64 lowercase hexadecimal characters"
    );
    Ok(())
}

fn validate_file_name(file_name: &str) -> Result<DataType> {
    let body = file_name
        .strip_prefix("discogs_")
        .and_then(|name| name.strip_suffix(".xml.gz"))
        .with_context(|| format!("Local manifest input is not a Discogs XML gzip file: {file_name}"))?;
    let (version, data_type) = body.split_once('_').with_context(|| format!("Local manifest input has no versioned data type: {file_name}"))?;
    ensure!(version.len() == 8 && version.bytes().all(|byte| byte.is_ascii_digit()), "Local manifest input version must be YYYYMMDD: {file_name}");
    let data_type: DataType = data_type.parse().map_err(anyhow::Error::msg)?;
    ensure!(DataType::discogs().contains(&data_type), "Local manifest input names a non-Discogs data type: {data_type}");
    Ok(data_type)
}

async fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).await.with_context(|| format!("Failed to open local manifest input: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await.with_context(|| format!("Failed to read local manifest input: {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

#[async_trait]
impl DataSource for LocalManifestSource {
    async fn list_s3_files(&mut self) -> Result<Vec<S3FileInfo>> {
        // Reverify here as well as at construction so a changed input never reaches the
        // state-marker or processing paths.
        self.verify_source().await?;
        Ok(vec![S3FileInfo { name: self.file_name.clone(), size: self.size }])
    }

    fn get_latest_monthly_files(&self, files: &[S3FileInfo]) -> Result<Vec<S3FileInfo>> {
        ensure!(files.len() == 1 && files[0].name == self.file_name, "Local manifest source received an unexpected file list");
        Ok(files.to_vec())
    }

    async fn download_discogs_data(&mut self) -> Result<Vec<String>> {
        self.stage_source().await?;
        let marker_path = self.marker_path.clone().context("Local manifest state-marker path was not configured")?;
        let marker = self.state_marker.as_mut().context("Local manifest state marker was not configured")?;
        marker.start_download(1);
        marker.start_file_download(&self.file_name);
        marker.file_downloaded(&self.file_name, self.size);
        marker.file_bytes_verified(&self.file_name, &self.expected_sha256);
        marker.complete_download();
        marker.save(&marker_path).await?;
        Ok(vec![self.file_name.clone()])
    }

    fn set_state_marker(&mut self, state_marker: StateMarker, marker_path: PathBuf) {
        self.state_marker = Some(state_marker);
        self.marker_path = Some(marker_path);
    }

    fn take_state_marker(&mut self) -> Option<StateMarker> {
        self.state_marker.take()
    }
}

#[cfg(test)]
#[path = "tests/local_manifest_tests.rs"]
mod tests;
