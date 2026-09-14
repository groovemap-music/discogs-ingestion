use super::*;
use serde_json::{Value, json};
use tempfile::TempDir;

const FIXTURE_ROOT: &str = "contracts/extractor-smoke/v1";

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT).join(name)
}

fn fixture_manifest() -> Value {
    serde_json::from_slice(&std::fs::read(fixture_path("manifest.json")).unwrap()).unwrap()
}

fn write_manifest(directory: &Path, manifest: &Value) -> PathBuf {
    let path = directory.join("manifest.json");
    std::fs::write(&path, serde_json::to_vec_pretty(manifest).unwrap()).unwrap();
    path
}

fn copy_fixture(directory: &Path) {
    std::fs::copy(fixture_path("discogs_20000101_releases.xml.gz"), directory.join("discogs_20000101_releases.xml.gz")).unwrap();
}

#[tokio::test]
async fn landed_manifest_is_accepted_and_staged_only_after_marker_configuration() {
    let output = TempDir::new().unwrap();
    let mut source = LocalManifestSource::from_manifest(&fixture_path("manifest.json"), output.path().to_path_buf()).await.unwrap();

    let listed = source.list_s3_files().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "discogs_20000101_releases.xml.gz");
    assert_eq!(source.get_latest_monthly_files(&listed).unwrap().len(), 1);
    assert!(!output.path().join(&listed[0].name).exists());

    let marker_path = output.path().join(".extraction_status_20000101.json");
    source.set_state_marker(StateMarker::new("20000101".to_string()), marker_path.clone());
    assert_eq!(source.download_discogs_data().await.unwrap(), vec![listed[0].name.clone()]);
    assert!(output.path().join(&listed[0].name).is_file());
    assert!(marker_path.is_file());

    let marker = source.take_state_marker().unwrap();
    assert_eq!(marker.download_phase.files_downloaded, 1);
    assert_eq!(marker.download_phase.downloads_by_file[&listed[0].name].checksum.as_deref(), Some(source.expected_sha256.as_str()));
}

#[tokio::test]
async fn missing_manifest_and_missing_input_fail_closed() {
    let directory = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    let missing_manifest = directory.path().join("missing.json");
    let error = LocalManifestSource::from_manifest(&missing_manifest, output.path().to_path_buf()).await.unwrap_err();
    assert!(error.to_string().contains("does not exist or is not accessible"));

    let manifest_path = write_manifest(directory.path(), &fixture_manifest());
    let error = LocalManifestSource::from_manifest(&manifest_path, output.path().to_path_buf()).await.unwrap_err();
    assert!(error.to_string().contains("input does not exist or is not accessible"));
}

#[tokio::test]
async fn checksum_mismatch_fails_before_staging_or_state_changes() {
    let directory = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    copy_fixture(directory.path());
    let mut manifest = fixture_manifest();
    manifest["input"]["sha256"] = json!("0".repeat(64));
    let manifest_path = write_manifest(directory.path(), &manifest);

    let error = LocalManifestSource::from_manifest(&manifest_path, output.path().to_path_buf()).await.unwrap_err();
    assert!(error.to_string().contains("checksum mismatch"));
    assert!(std::fs::read_dir(output.path()).unwrap().next().is_none());
}

#[tokio::test]
async fn malformed_or_escaping_manifest_entries_fail_closed() {
    let directory = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    copy_fixture(directory.path());

    let cases = [
        ("contract", json!("other.contract")),
        ("version", json!(2)),
        ("scope", json!("monthly_set")),
        ("input.path", json!("../discogs_20000101_releases.xml.gz")),
        ("input.data_type", json!("artists")),
        ("input.media_type", json!("application/xml")),
        ("input.records", json!(0)),
    ];

    for (field, value) in cases {
        let mut manifest = fixture_manifest();
        match field.split_once('.') {
            Some((section, name)) => manifest[section][name] = value,
            None => manifest[field] = value,
        }
        let manifest_path = write_manifest(directory.path(), &manifest);
        assert!(LocalManifestSource::from_manifest(&manifest_path, output.path().to_path_buf()).await.is_err(), "{field} must fail closed");
    }
}

#[tokio::test]
async fn changed_source_is_reverified_before_listing() {
    let directory = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    copy_fixture(directory.path());
    let manifest_path = write_manifest(directory.path(), &fixture_manifest());
    let mut source = LocalManifestSource::from_manifest(&manifest_path, output.path().to_path_buf()).await.unwrap();

    std::fs::write(directory.path().join("discogs_20000101_releases.xml.gz"), b"changed after validation").unwrap();
    let error = source.list_s3_files().await.unwrap_err();
    assert!(error.to_string().contains("checksum mismatch"));
}
