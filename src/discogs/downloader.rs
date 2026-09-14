use anyhow::{Context, Result};
use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tracing::{Instrument, debug, error, info, warn};

use async_trait::async_trait;

use crate::polite_http::{PoliteClient, PoliteConfig};
use crate::state_marker::StateMarker;
use crate::types::{LocalFileInfo, S3FileInfo};

// S3 file names already contain the full key (e.g., "data/2026/discogs_...xml.gz")
// so no prefix stripping or re-prepending is needed.
const DISCOGS_DATA_URL: &str = "https://data.discogs.com/";
// This retry loop only fires for *post-connect* failures — partial reads,
// flush/sync errors, transport drops mid-stream. Rate-limit (HTTP 429 / 503)
// handling lives upstream in `polite_http::PoliteClient` and never reaches
// this loop, so 3 attempts at 2s base is plenty to ride out a brief network
// hiccup. Higher values bloat CI runtime — integration tests in `tests/`
// see `cfg(not(test))` and pay the full backoff per failing-download test.
const MAX_DOWNLOAD_RETRIES: u32 = 3;

#[cfg(not(test))]
const RETRY_BASE_DELAY_MS: u64 = 2_000;
#[cfg(test)]
const RETRY_BASE_DELAY_MS: u64 = 10;

pub struct Downloader {
    pub output_directory: PathBuf,
    pub metadata: HashMap<String, LocalFileInfo>,
    base_url: String,
    pub state_marker: Option<StateMarker>,
    pub marker_path: Option<PathBuf>,
    cached_files: Option<Vec<S3FileInfo>>,
    client: PoliteClient,
}

#[cfg_attr(feature = "test-support", mockall::automock)]
#[async_trait]
pub trait DataSource: Send + Sync {
    async fn list_s3_files(&mut self) -> Result<Vec<S3FileInfo>>;
    fn get_latest_monthly_files(&self, files: &[S3FileInfo]) -> Result<Vec<S3FileInfo>>;
    async fn download_discogs_data(&mut self) -> Result<Vec<String>>;
    fn set_state_marker(&mut self, state_marker: StateMarker, marker_path: PathBuf);
    fn take_state_marker(&mut self) -> Option<StateMarker>;
}

impl Downloader {
    pub async fn new(output_directory: PathBuf) -> Result<Self> {
        Self::new_with_base_url(output_directory, DISCOGS_DATA_URL.to_string()).await
    }

    /// Create a new downloader with a custom base URL (primarily for testing)
    #[doc(hidden)]
    pub async fn new_with_base_url(output_directory: PathBuf, base_url: String) -> Result<Self> {
        let metadata = load_metadata(&output_directory)?;
        let client = PoliteClient::new(Self::polite_config())?;

        Ok(Self { output_directory, metadata, base_url, state_marker: None, marker_path: None, cached_files: None, client })
    }

    /// Polite-client tuning for `data.discogs.com`. Tests override `min_gap`
    /// to keep them fast; production uses the upstream-friendly defaults.
    fn polite_config() -> PoliteConfig {
        let mut cfg = PoliteConfig::discogs();
        if cfg!(test) {
            cfg.min_gap = std::time::Duration::from_millis(10);
        }
        cfg
    }

    /// Set the state marker for tracking download progress (builder pattern, used by integration tests)
    pub fn with_state_marker(mut self, state_marker: StateMarker, marker_path: PathBuf) -> Self {
        self.state_marker = Some(state_marker);
        self.marker_path = Some(marker_path);
        self
    }

    /// Save state marker to disk if present
    async fn save_state_marker(&mut self) {
        if let (Some(marker), Some(path)) = (&mut self.state_marker, &self.marker_path)
            && let Err(e) = marker.save(path).await
        {
            warn!("⚠️ Failed to save state marker: {}", e);
        }
    }

    pub async fn download_discogs_data(&mut self) -> Result<Vec<String>> {
        info!("📥 Starting download of Discogs data dumps...");

        // Create output directory if it doesn't exist
        fs::create_dir_all(&self.output_directory).await.context("Failed to create output directory")?;

        // List available files from S3
        let available_files = self.list_s3_files().await?;

        // Get latest monthly dump
        let latest_files = self.get_latest_monthly_files(&available_files)?;

        if latest_files.is_empty() {
            warn!("⚠️ No monthly data files found");
            return Ok(Vec::new());
        }

        let month = extract_month_from_filename(&latest_files[0].name);
        info!("📅 Latest available month: {}", month);

        // Fetch the Discogs-published CHECKSUM file so each data file's SHA-256 can be
        // verified against an authoritative source rather than only the self-generated
        // hash computed from whatever bytes happened to arrive (discogsography-cu2.106).
        // Best-effort: if the CHECKSUM file can't be fetched or parsed, log and proceed
        // without verification rather than blocking the whole dump on a transient
        // fetch failure — this is additive hardening, not a hard gate.
        let published_checksums = match Self::find_checksum_entry(&available_files, &latest_files) {
            Some(checksum_file) => match self.fetch_checksums(&checksum_file).await {
                Ok(map) => {
                    info!("🔐 Fetched published CHECKSUM file ({} entries)", map.len());
                    Some(map)
                }
                Err(e) => {
                    warn!("⚠️ Failed to fetch/parse published CHECKSUM file — proceeding without integrity verification: {}", e);
                    None
                }
            },
            None => {
                warn!("⚠️ No CHECKSUM file found alongside the latest monthly dump — proceeding without integrity verification");
                None
            }
        };

        // Start download phase tracking if state marker is available
        if let Some(ref mut marker) = self.state_marker {
            marker.start_download(latest_files.len());
        }
        self.save_state_marker().await;

        let mut downloaded_files = Vec::new();

        for file_info in &latest_files {
            let filename = std::path::Path::new(&file_info.name).file_name().and_then(|name| name.to_str()).unwrap_or("unknown_file");

            let mut needs_download = self.should_download(file_info).await?;

            // should_download() only proves the local file hasn't changed since it was
            // downloaded — it can't tell a genuine dump from a previously-trusted bad
            // 200-response body (the exact hole this bead closes). Cross-check the
            // locally-trusted checksum against the published one whenever available, and
            // force a re-download on mismatch instead of trusting it forever.
            if !needs_download
                && let Some(ref published) = published_checksums
                && let Some(expected) = published.get(filename)
            {
                let locally_trusted = self.metadata.get(filename).map(|info| info.checksum.as_str());
                if locally_trusted != Some(expected.as_str()) {
                    warn!("⚠️ Locally-trusted checksum for {} does not match the published CHECKSUM — forcing re-download", filename);
                    needs_download = true;
                }
            }

            if needs_download {
                // Start tracking file download
                if let Some(ref mut marker) = self.state_marker {
                    marker.start_file_download(filename);
                }
                self.save_state_marker().await;

                // Acquisition and processing are separate phases of the run, so a file's
                // download cannot share a span with its parse — the download loop has
                // finished every file before the first one is parsed. The download therefore
                // opens its own `extract {source} {entity}` root, with `download` as its
                // child, and the processing phase opens a second one for the same file.
                let entity = super::extract_data_type(filename).map(|data_type| data_type.as_str()).unwrap_or("unknown");
                let extract_span = crate::telemetry::extract_span(entity);
                let download_span = extract_span.in_scope(crate::telemetry::download_span);

                match self.download_file(file_info).instrument(download_span).instrument(extract_span).await {
                    Ok(downloaded_size) => {
                        // Verify the downloaded bytes against the Discogs-published CHECKSUM
                        // before trusting them. Without this, a 200-response whose body isn't
                        // the real dump (CDN/interstitial error page, a proxy-truncated stream)
                        // would be hashed by download_file, recorded as metadata truth, and
                        // never re-downloaded until the next monthly release or a manual
                        // file/metadata deletion.
                        if let Some(ref published) = published_checksums
                            && let Some(expected) = published.get(filename)
                        {
                            let actual = self.metadata.get(filename).map(|info| info.checksum.clone());
                            if actual.as_deref() != Some(expected.as_str()) {
                                error!(
                                    "❌ Checksum mismatch for {}: expected {} from published CHECKSUM, got {:?}. Deleting corrupt download.",
                                    filename, expected, actual
                                );
                                self.metadata.remove(filename);
                                let local_path = self.output_directory.join(filename);
                                if local_path.exists()
                                    && let Err(e) = fs::remove_file(&local_path).await
                                {
                                    warn!("⚠️ Failed to remove corrupt file {}: {}", filename, e);
                                }
                                return Err(anyhow::anyhow!("Checksum verification failed for {} against published CHECKSUM", filename));
                            }
                            debug!("🔐 Verified {} against published CHECKSUM", filename);
                        }

                        info!("✅ Successfully downloaded: {}", filename);

                        // Persist metadata immediately so a later file's failure can't
                        // discard this file's checksum. download_file only records the
                        // checksum in the in-memory metadata map; without this incremental
                        // save the durable .discogs_metadata.json is written once, after
                        // the whole loop — so a failure on file N loses the checksums of
                        // files 1..N-1, forcing full multi-GB re-downloads on restart
                        // (discogsography-cu2.65). Best-effort: a save failure only costs a
                        // re-download next run, so warn and continue rather than abort.
                        if let Err(e) = self.save_metadata() {
                            warn!("⚠️ Failed to persist metadata after downloading {}: {}", filename, e);
                        }

                        // Track file download in state marker with actual downloaded size, and
                        // bind the marker to the byte-image now on disk. If these bytes differ
                        // from the ones an earlier session already processed (the whole point of
                        // a forced re-download), that stale Completed processing status is
                        // dropped so pending_files() re-queues the file — otherwise the
                        // corrected data would never be parsed or published.
                        let verified_checksum = self.metadata.get(filename).map(|info| info.checksum.clone());
                        if let Some(ref mut marker) = self.state_marker {
                            marker.file_downloaded(filename, downloaded_size);
                            if let Some(ref checksum) = verified_checksum {
                                marker.file_bytes_verified(filename, checksum);
                            }
                        }
                        self.save_state_marker().await;

                        downloaded_files.push(filename.to_string());
                    }
                    Err(e) => {
                        error!("❌ Failed to download {}: {}", filename, e);
                        return Err(e).context(format!("Failed to download {}", filename));
                    }
                }
            } else {
                info!("✅ Already have latest version of: {}", filename);

                // Track existing file in state marker with actual file size. The locally
                // trusted checksum is recorded too, so the first run after this change
                // establishes the provenance that later re-downloads are compared against.
                let local_path = self.output_directory.join(filename);
                let file_size = tokio::fs::metadata(&local_path).await.map(|m| m.len()).unwrap_or(0);
                let local_checksum = self.metadata.get(filename).map(|info| info.checksum.clone());
                if let Some(ref mut marker) = self.state_marker {
                    marker.file_downloaded(filename, file_size);
                    if let Some(ref checksum) = local_checksum {
                        marker.file_bytes_verified(filename, checksum);
                    }
                }
                self.save_state_marker().await;

                downloaded_files.push(filename.to_string());
            }
        }

        // Complete download phase tracking if state marker is available
        if let Some(ref mut marker) = self.state_marker {
            marker.complete_download();
        }
        self.save_state_marker().await;

        // Save updated metadata
        self.save_metadata()?;

        Ok(downloaded_files)
    }

    async fn scrape_file_list_from_discogs(&self) -> Result<HashMap<String, Vec<S3FileInfo>>> {
        info!("🌐 Fetching file list from Discogs website...");

        // Step 1: Fetch the main page to get available years
        let response = self.client.get(&self.base_url).await.context("Failed to fetch Discogs website")?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Discogs website returned HTTP {} for {}", response.status(), self.base_url));
        }
        let html = response.text().await.context("Failed to read HTML response")?;

        // Extract year directories (e.g., 2026/, 2025/, etc.)
        let year_pattern = Regex::new(r#"href="\?prefix=data%2F(\d{4})%2F""#).context("Failed to compile year regex")?;

        let mut years: Vec<String> = year_pattern.captures_iter(&html).filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string())).collect();

        if years.is_empty() {
            return Err(anyhow::anyhow!("No year directories found on Discogs website"));
        }

        // Sort years in descending order (most recent first)
        years.sort_by(|a, b| b.cmp(a));
        info!("📅 Found {} year directories, checking recent years...", years.len());

        // Step 2: Fetch files from recent years (check last 2 years)
        let mut ids: HashMap<String, Vec<S3FileInfo>> = HashMap::new();

        // Compile regex once outside the loop
        // Pattern matches: ?download=data%2F2026%2Fdiscogs_20260101_artists.xml.gz
        let file_pattern = Regex::new(r#"\?download=data%2F\d{4}%2F(discogs_(\d{8})_[^"]+)"#).context("Failed to compile file regex")?;

        // Per-year outcome accounting. Without it there is no baseline against which the
        // final version count can be judged, so a year silently contributing nothing is
        // indistinguishable from a year that genuinely has nothing.
        let mut years_checked = 0usize;
        let mut years_contributing = 0usize;

        for year in years.iter().take(2) {
            let year_url = format!("{}?prefix=data%2F{}%2F", self.base_url, year);
            years_checked += 1;

            match self.client.get(&year_url).await {
                Ok(year_response) if year_response.status().is_success() => {
                    // A body read can fail independently of the response (connection reset or
                    // truncation after headers), and PoliteClient never retries body reads.
                    // Dropping the year silently here was the one traceless failure arm in this
                    // function — the newest dump would vanish with nothing in the logs.
                    let year_html = match year_response.text().await {
                        Ok(html) => html,
                        Err(e) => {
                            warn!("⚠️ Failed to read year {} directory body: {}", year, e);
                            continue;
                        }
                    };

                    {
                        let mut file_count = 0;
                        for cap in file_pattern.captures_iter(&year_html) {
                            if let (Some(filename_match), Some(version_match)) = (cap.get(1), cap.get(2)) {
                                let filename = filename_match.as_str();
                                let version_id = version_match.as_str();

                                // URL decode the filename
                                let decoded_filename = urlencoding::decode(filename).context("Failed to URL decode filename")?.to_string();

                                // Construct full S3 key
                                let s3_key = format!("data/{}/{}", year, decoded_filename);

                                ids.entry(version_id.to_string()).or_default().push(S3FileInfo { name: s3_key, size: 0 });

                                file_count += 1;
                            }
                        }

                        if file_count > 0 {
                            info!("📋 Found {} files in year {} directory", file_count, year);
                            years_contributing += 1;
                        } else {
                            // A readable page that matches nothing is the other silent path:
                            // a Discogs markup change would drop years just as invisibly, and
                            // unlike a transport error it would not self-heal on the next check.
                            warn!(
                                "⚠️ No dump files matched in the year {} directory ({} bytes of HTML) — the Discogs listing markup may have changed",
                                year,
                                year_html.len()
                            );
                        }
                    }
                }
                Ok(year_response) => {
                    warn!("⚠️ Discogs returned HTTP {} for year {} directory", year_response.status(), year);
                    continue;
                }
                Err(e) => {
                    warn!("⚠️ Failed to fetch year {} directory: {}", year, e);
                    continue;
                }
            }
        }

        if ids.is_empty() {
            return Err(anyhow::anyhow!(
                "No files found on Discogs website (none of the {} recent year directories contributed files)",
                years_checked
            ));
        }

        if years_contributing < years_checked {
            warn!("⚠️ Only {}/{} recent year directories contributed files — the newest dump may be missing", years_contributing, years_checked);
        }

        info!("📊 Found {} unique versions from website ({}/{} year directories contributed)", ids.len(), years_contributing, years_checked);

        Ok(ids)
    }

    pub async fn list_s3_files(&mut self) -> Result<Vec<S3FileInfo>> {
        if let Some(ref cached) = self.cached_files {
            debug!("📋 Using cached file list ({} files)", cached.len());
            return Ok(cached.clone());
        }

        info!("🔍 Listing available files from Discogs website...");

        // Scrape file list from Discogs website instead of S3 listing
        // This avoids the AccessDenied error from S3's ListBucket restriction
        let ids_map = self.scrape_file_list_from_discogs().await?;

        // Flatten the map into a single list of files for compatibility
        let files: Vec<S3FileInfo> = ids_map.into_values().flat_map(|files| files.into_iter()).collect();

        info!("Found {} relevant files from website", files.len());
        self.cached_files = Some(files.clone());
        Ok(files)
    }

    pub fn get_latest_monthly_files(&self, files: &[S3FileInfo]) -> Result<Vec<S3FileInfo>> {
        // Group files by their ID (date part like "20250801") - matching Python logic
        let mut ids: std::collections::HashMap<String, Vec<S3FileInfo>> = std::collections::HashMap::new();

        for file in files {
            // Extract basename before splitting — the full S3 key may contain path separators.
            // `file.name` is an S3 object key scraped from the Discogs public bucket listing —
            // operator-controlled infrastructure, not user input — and `.file_name()` discards
            // any path components, so no path escapes this function.
            let basename = std::path::Path::new(&file.name).file_name().and_then(|f| f.to_str()).unwrap_or(&file.name); // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
            let parts: Vec<&str> = basename.split('_').collect();
            if parts.len() >= 2 {
                let id = parts[1].to_string();
                ids.entry(id).or_default().push(file.clone());
            }
        }

        info!("Found {} unique version IDs", ids.len());

        // Get the most recent version (sorted in reverse order)
        let mut sorted_ids: Vec<_> = ids.keys().collect();
        sorted_ids.sort_by(|a, b| b.cmp(a));

        for id in sorted_ids {
            let files_for_id = ids.get(id).unwrap();
            // Check if we have a complete set - exactly like Python logic
            // Python requires exactly 5 files total (1 CHECKSUM + 4 data files)
            if files_for_id.len() != 5 {
                warn!("⚠️ Skipping version {} — expected 5 files, found {}", id, files_for_id.len());
                continue;
            }

            // Only return data files (not CHECKSUM) for processing, with filename only
            let data_files: Vec<_> = files_for_id
                .iter()
                .filter(|f| f.name.ends_with(".xml.gz"))
                .map(|f| S3FileInfo { name: f.name.clone(), size: f.size })
                .collect();

            debug!("Version {} has {} data files", id, data_files.len());

            if data_files.len() == 4 {
                // We expect exactly 4 data files
                info!("📅 Using version {} with {} data files", id, data_files.len());
                return Ok(data_files);
            }
        }

        warn!("No complete version found with all expected data files");
        Ok(Vec::new())
    }

    /// Locate the CHECKSUM entry sharing the same version id as `data_files`, among the
    /// unfiltered `files` list (`get_latest_monthly_files` drops the CHECKSUM entry from
    /// its own return value, so callers that need it look it up here instead).
    fn find_checksum_entry(files: &[S3FileInfo], data_files: &[S3FileInfo]) -> Option<S3FileInfo> {
        let sample = data_files.first()?;
        let sample_basename = std::path::Path::new(&sample.name).file_name().and_then(|f| f.to_str())?;
        let version_id = sample_basename.split('_').nth(1)?;

        files
            .iter()
            .find(|f| {
                let basename = std::path::Path::new(&f.name).file_name().and_then(|n| n.to_str()).unwrap_or("");
                basename.contains("CHECKSUM") && basename.split('_').nth(1) == Some(version_id)
            })
            .cloned()
    }

    /// Fetch and parse the Discogs-published CHECKSUM file for a version, mapping each data
    /// filename to its published SHA-256. Standard `sha256sum`-style lines are expected:
    /// `<hex64>  <filename>` (optionally `*filename` for binary mode).
    async fn fetch_checksums(&self, checksum_file: &S3FileInfo) -> Result<HashMap<String, String>> {
        let download_url = format!("{}?download={}", self.base_url, urlencoding::encode(&checksum_file.name));

        let response = self.client.get(&download_url).await.context("Failed to fetch CHECKSUM file")?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("CHECKSUM file fetch returned HTTP {}", response.status()));
        }
        let text = response.text().await.context("Failed to read CHECKSUM file body")?;

        let mut checksums = HashMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(2, char::is_whitespace);
            let hash = parts.next().unwrap_or("");
            let name_part = parts.next().unwrap_or("").trim();
            let name_part = name_part.strip_prefix('*').unwrap_or(name_part);
            if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) && !name_part.is_empty() {
                let basename = std::path::Path::new(name_part).file_name().and_then(|n| n.to_str()).unwrap_or(name_part);
                checksums.insert(basename.to_string(), hash.to_lowercase());
            }
        }

        if checksums.is_empty() {
            return Err(anyhow::anyhow!("No checksum entries parsed from CHECKSUM file"));
        }

        Ok(checksums)
    }

    pub async fn should_download(&self, file_info: &S3FileInfo) -> Result<bool> {
        // Extract just the base filename for local checks. `file_info.name` is an S3 object
        // key scraped from the Discogs public bucket listing (operator-controlled
        // infrastructure, not user input); `.file_name()` discards any path components so
        // `local_path` below can never escape `output_directory`.
        let filename = std::path::Path::new(&file_info.name).file_name().and_then(|name| name.to_str()).unwrap_or("unknown_file"); // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
        let local_path = self.output_directory.join(filename);

        // Check if file exists locally
        if !local_path.exists() {
            return Ok(true);
        }

        // Check metadata
        if let Some(local_info) = self.metadata.get(filename) {
            // Note: file_info.size is 0 from scraping, so we can't compare sizes
            // File exists (checked above), validate checksum
            let checksum = calculate_file_checksum(&local_path).await?;
            if checksum != local_info.checksum {
                warn!("⚠️ Checksum mismatch for {}", file_info.name);
                return Ok(true);
            }

            // File exists with correct checksum
            return Ok(false);
        }

        // No metadata, download to be safe
        Ok(true)
    }

    /// Download a single file (exposed for testing)
    /// Returns the number of bytes downloaded
    #[doc(hidden)]
    pub async fn download_file(&mut self, file_info: &S3FileInfo) -> Result<u64> {
        use futures::StreamExt;

        // Use the full S3 key directly (name already contains the full path)
        let s3_key = &file_info.name;
        // Extract just the base filename for local storage (remove path components). `file_info.name`
        // is an S3 object key scraped from the Discogs public bucket listing (operator-controlled
        // infrastructure, not user input); `.file_name()` discards any path components so
        // `local_path` below can never escape `output_directory`.
        let filename = std::path::Path::new(&file_info.name).file_name().and_then(|name| name.to_str()).unwrap_or("unknown_file"); // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
        let local_path = self.output_directory.join(filename);

        info!("⬇️ Downloading {}...", filename);

        // Construct Discogs download URL (URL encode the S3 key)
        let download_url = format!("{}?download={}", self.base_url, urlencoding::encode(s3_key));

        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 1..=MAX_DOWNLOAD_RETRIES {
            if attempt > 1 {
                // Remove any partial file left by the previous attempt
                if local_path.exists()
                    && let Err(e) = fs::remove_file(&local_path).await
                {
                    warn!("⚠️ Failed to remove partial file before retry: {}", e);
                }
                let delay_ms = RETRY_BASE_DELAY_MS * (1u64 << (attempt - 2));
                warn!("🔄 Retry {}/{} for {} (waiting {}ms)...", attempt - 1, MAX_DOWNLOAD_RETRIES - 1, filename, delay_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }

            // Create progress bar (unknown size from scraping)
            let pb = ProgressBar::new_spinner();
            pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} [{elapsed_precise}] {bytes} ({bytes_per_sec})").unwrap());

            // Download via the polite client — gates the request on min_gap and
            // server-driven Retry-After so a 429 doesn't burn a retry slot here.
            let response = match self.client.get(&download_url).await {
                Ok(r) => r,
                Err(e) => {
                    let msg = format!("Failed to start HTTP download from {}: {}", download_url, e);
                    warn!("⚠️ Attempt {}/{}: {}", attempt, MAX_DOWNLOAD_RETRIES, msg);
                    last_error = Some(anyhow::anyhow!(msg));
                    continue;
                }
            };

            if !response.status().is_success() {
                let msg = format!("HTTP error: {}", response.status());
                warn!("⚠️ Attempt {}/{}: {}", attempt, MAX_DOWNLOAD_RETRIES, msg);
                last_error = Some(anyhow::anyhow!(msg));
                continue;
            }

            // `local_path` is built above from the sanitized basename (path components already
            // stripped via `.file_name()`) joined with the operator-controlled `output_directory`,
            // so it always resolves inside `output_directory`.
            let mut file = File::create(&local_path).await.context("Failed to create local file")?; // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
            let mut hasher = Sha256::new();
            let mut downloaded: u64 = 0;
            // Bytes already folded into `groovemap.extraction.download.bytes`. The counter is
            // advanced in deltas at the existing 10 s log cadence (and once at the end) rather
            // than once per 8 KiB chunk, which would be millions of adds per dump.
            let mut bytes_reported: u64 = 0;
            let download_start = std::time::Instant::now();
            let mut last_progress_log = download_start;

            // Stream the response body
            let mut stream = response.bytes_stream();
            let mut stream_error: Option<anyhow::Error> = None;

            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(chunk) => {
                        hasher.update(&chunk);
                        if let Err(e) = file.write_all(&chunk).await {
                            stream_error = Some(anyhow::anyhow!("Failed to write chunk to file: {}", e));
                            break;
                        }
                        downloaded += chunk.len() as u64;
                        pb.set_position(downloaded);

                        // Log progress every 10 seconds for syslog visibility
                        let now = std::time::Instant::now();
                        if now.duration_since(last_progress_log).as_secs() >= 10 {
                            let elapsed_secs = download_start.elapsed().as_secs_f64();
                            let speed = if elapsed_secs > 0.0 {
                                (downloaded as f64 / 1_048_576.0) / elapsed_secs
                            } else {
                                0.0
                            };
                            info!("📥 {} — {:.1} MB received ({:.1} MB/s)", filename, downloaded as f64 / 1_048_576.0, speed);
                            crate::telemetry::record_download_bytes(downloaded - bytes_reported);
                            bytes_reported = downloaded;
                            last_progress_log = now;
                        }
                    }
                    Err(e) => {
                        stream_error = Some(anyhow::anyhow!("Failed to read HTTP response chunk: {}", e));
                        break;
                    }
                }
            }

            if let Some(err) = stream_error {
                warn!("⚠️ Attempt {}/{} failed for {}: {}", attempt, MAX_DOWNLOAD_RETRIES, filename, err);
                last_error = Some(err);
                continue;
            }

            if let Err(e) = file.flush().await {
                warn!("⚠️ Attempt {}/{} failed to flush {}: {}", attempt, MAX_DOWNLOAD_RETRIES, filename, e);
                last_error = Some(anyhow::anyhow!("Failed to flush file: {}", e));
                continue;
            }
            if let Err(e) = file.sync_data().await {
                warn!("⚠️ Attempt {}/{} failed to sync {}: {}", attempt, MAX_DOWNLOAD_RETRIES, filename, e);
                last_error = Some(anyhow::anyhow!("Failed to sync file: {}", e));
                continue;
            }

            pb.finish_with_message("Download complete");

            crate::telemetry::record_download_bytes(downloaded - bytes_reported);

            info!("✅ Downloaded {} ({:.2} MB)", filename, downloaded as f64 / 1_048_576.0);

            // Calculate checksum
            let checksum = hex::encode(hasher.finalize());

            // Update metadata with actual downloaded size
            self.metadata.insert(
                filename.to_string(),
                LocalFileInfo {
                    path: local_path.to_string_lossy().to_string(),
                    checksum,
                    version: extract_month_from_filename(filename),
                    size: downloaded,
                },
            );

            return Ok(downloaded);
        }

        // Clean up partial file left by the final failed attempt
        if local_path.exists()
            && let Err(e) = fs::remove_file(&local_path).await
        {
            warn!("⚠️ Failed to remove partial file after all retries: {}", e);
        }

        crate::telemetry::record_error(crate::telemetry::Stage::Download);
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Download failed after {} attempts", MAX_DOWNLOAD_RETRIES)))
    }

    /// Persist the checksum-trust database.
    ///
    /// Written to a temp file, fsynced, then atomically renamed over the final path —
    /// the same durability contract as [`StateMarker::save`]. A plain in-place write
    /// leaves a truncated (or zero-byte) file if the process is killed mid-write, and
    /// a corrupt metadata file used to wedge startup permanently.
    pub fn save_metadata(&self) -> Result<()> {
        let metadata_file = self.output_directory.join(".discogs_metadata.json");
        let tmp_file = self.output_directory.join(".discogs_metadata.json.tmp");
        let json = serde_json::to_string_pretty(&self.metadata).context("Failed to serialize metadata")?;

        {
            use std::io::Write;
            // Path is built from operator-controlled config plus a hardcoded literal.
            let mut file = std::fs::File::create(&tmp_file).context("Failed to create metadata temp file")?; // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path
            file.write_all(json.as_bytes()).context("Failed to write metadata temp file")?;
            // fsync before rename so the rename cannot expose an empty file after a crash.
            file.sync_all().context("Failed to fsync metadata temp file")?;
        }

        std::fs::rename(&tmp_file, &metadata_file).context("Failed to save metadata")?;

        Ok(())
    }
}

fn load_metadata(output_directory: &Path) -> Result<HashMap<String, LocalFileInfo>> {
    // Filename here is a hardcoded literal, not derived from any external input.
    let metadata_file = output_directory.join(".discogs_metadata.json");

    if !metadata_file.exists() {
        return Ok(HashMap::new());
    }

    let json = std::fs::read_to_string(&metadata_file).context("Failed to read metadata file")?; // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path

    match serde_json::from_str(&json) {
        Ok(metadata) => Ok(metadata),
        Err(e) => {
            // The metadata file is a download cache, not a hard integrity requirement:
            // treating a corrupt one as fatal crash-looped the service forever. Quarantine
            // it and start fresh — the worst case is re-verifying/re-downloading dumps,
            // which `should_download` already handles safely via checksums.
            warn!("⚠️ Failed to parse metadata file, starting with empty metadata: {}", e);

            let corrupt_path = output_directory.join(".discogs_metadata.json.corrupt");
            match std::fs::rename(&metadata_file, &corrupt_path) {
                Ok(()) => warn!("⚠️ Quarantined corrupt metadata file to: {}", corrupt_path.display()),
                Err(rename_err) => warn!("⚠️ Failed to quarantine corrupt metadata file: {}", rename_err),
            }

            Ok(HashMap::new())
        }
    }
}

/// Compute a SHA-256 checksum for a local file. Callers only ever pass paths built from
/// `output_directory` joined with a sanitized basename (see `should_download`/`download_file`,
/// which strip path components via `.file_name()`), so this never escapes `output_directory`.
async fn calculate_file_checksum(path: &Path) -> Result<String> {
    let mut file = File::open(path).await.context("Failed to open file for checksum")?; // nosemgrep: rust.actix.path-traversal.tainted-path.tainted-path

    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 8192];

    loop {
        let n = tokio::io::AsyncReadExt::read(&mut file, &mut buffer).await.context("Failed to read file for checksum")?;

        if n == 0 {
            break;
        }

        hasher.update(&buffer[..n]);
    }

    Ok(hex::encode(hasher.finalize()))
}

fn extract_month_from_filename(filename: &str) -> String {
    // Extract YYYYMMDD from filename like discogs_20241201_artists.xml.gz
    if let Some(date_part) = filename.split('_').nth(1)
        && date_part.len() >= 6
    {
        return date_part[0..6].to_string(); // YYYYMM
    }
    Utc::now().format("%Y%m").to_string()
}

#[async_trait]
impl DataSource for Downloader {
    async fn list_s3_files(&mut self) -> Result<Vec<S3FileInfo>> {
        Downloader::list_s3_files(self).await
    }

    fn get_latest_monthly_files(&self, files: &[S3FileInfo]) -> Result<Vec<S3FileInfo>> {
        Downloader::get_latest_monthly_files(self, files)
    }

    async fn download_discogs_data(&mut self) -> Result<Vec<String>> {
        Downloader::download_discogs_data(self).await
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
#[path = "tests/downloader_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/downloader_reliability_contract_tests.rs"]
mod reliability_contract_tests;
