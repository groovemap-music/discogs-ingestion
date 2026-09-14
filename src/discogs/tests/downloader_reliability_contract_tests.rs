//! Mechanical contract for downloader behavior shared with MusicBrainz ingestion.

use super::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

const TEST_FILE_KEY: &str = "data/discogs_20260101_artists.xml.gz";
const TEST_FILENAME: &str = "discogs_20260101_artists.xml.gz";

fn downloader_with_read_timeout(output: PathBuf, base_url: String, read_timeout: Duration) -> Downloader {
    let client = PoliteClient::new(PoliteConfig {
        min_gap: Duration::ZERO,
        max_retry_after: Duration::from_millis(50),
        max_throttle_retries: 1,
        request_timeout: Duration::from_secs(1),
        read_timeout,
    })
    .unwrap();

    Downloader { output_directory: output, metadata: HashMap::new(), base_url, state_marker: None, marker_path: None, cached_files: None, client }
}

async fn read_request(socket: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&request).into_owned()
}

fn listing_html() -> (&'static str, &'static str) {
    (
        r#"<a href="?prefix=data%2F2026%2F">2026/</a>"#,
        r#"
        <a href="?download=data%2F2026%2Fdiscogs_20260101_artists.xml.gz">artists</a>
        <a href="?download=data%2F2026%2Fdiscogs_20260101_labels.xml.gz">labels</a>
        <a href="?download=data%2F2026%2Fdiscogs_20260101_masters.xml.gz">masters</a>
        <a href="?download=data%2F2026%2Fdiscogs_20260101_releases.xml.gz">releases</a>
        <a href="?download=data%2F2026%2Fdiscogs_20260101_CHECKSUM.txt">checksum</a>
        "#,
    )
}

#[tokio::test]
async fn partial_attempt_restarts_from_zero_after_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured_requests = requests.clone();
    let fresh_body = b"complete replacement body";

    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured_requests.lock().await.push(read_request(&mut socket).await);
            if attempt == 0 {
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: close\r\n\r\nstale-partial-prefix")
                    .await
                    .unwrap();
            } else {
                let headers = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", fresh_body.len());
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(fresh_body).await.unwrap();
            }
            socket.shutdown().await.unwrap();
        }
    });

    let temp_dir = TempDir::new().unwrap();
    let mut downloader = downloader_with_read_timeout(temp_dir.path().to_path_buf(), format!("http://{address}/"), Duration::from_secs(1));
    let file_info = S3FileInfo { name: TEST_FILE_KEY.to_string(), size: fresh_body.len() as u64 };
    let started = Instant::now();

    let bytes = downloader.download_file(&file_info).await.unwrap();
    server.await.unwrap();

    assert!(started.elapsed() >= Duration::from_millis(RETRY_BASE_DELAY_MS));
    assert_eq!(bytes, fresh_body.len() as u64);
    assert_eq!(tokio::fs::read(temp_dir.path().join(TEST_FILENAME)).await.unwrap(), fresh_body);
    let requests = requests.lock().await;
    assert_eq!(requests.len(), 2, "one truncated attempt must consume exactly one retry");
    assert!(
        requests.iter().all(|request| !request.to_ascii_lowercase().contains("\r\nrange:")),
        "Discogs retries restart the file rather than issuing byte-range resumes"
    );
}

#[tokio::test]
async fn stalled_body_times_out_three_attempts_cleans_destination_and_reports_error() {
    let defaults = PoliteConfig::discogs();
    assert_eq!(defaults.request_timeout, Duration::from_secs(120));
    assert_eq!(defaults.read_timeout, Duration::from_secs(120));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured_requests = requests.clone();

    let server = tokio::spawn(async move {
        let mut stalled_connections = Vec::new();
        for _ in 0..MAX_DOWNLOAD_RETRIES {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured_requests.lock().await.push(read_request(&mut socket).await);
            stalled_connections.push(tokio::spawn(async move {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: keep-alive\r\n\r\npartial").await.unwrap();
                socket.flush().await.unwrap();
                tokio::time::sleep(Duration::from_secs(10)).await;
            }));
        }
        stalled_connections
    });

    let temp_dir = TempDir::new().unwrap();
    let read_timeout = Duration::from_millis(50);
    let mut downloader = downloader_with_read_timeout(temp_dir.path().to_path_buf(), format!("http://{address}/"), read_timeout);
    let file_info = S3FileInfo { name: TEST_FILE_KEY.to_string(), size: 1024 };
    let started = Instant::now();

    let error = downloader.download_file(&file_info).await.unwrap_err();
    let elapsed = started.elapsed();
    let stalled_connections = server.await.unwrap();
    for connection in stalled_connections {
        connection.abort();
    }

    assert!(elapsed >= read_timeout * MAX_DOWNLOAD_RETRIES);
    assert!(elapsed < Duration::from_secs(2), "bounded read timeouts took {elapsed:?}");
    assert_eq!(requests.lock().await.len(), MAX_DOWNLOAD_RETRIES as usize);
    assert!(!temp_dir.path().join(TEST_FILENAME).exists(), "terminal failure must remove the final partial file");
    assert!(!downloader.metadata.contains_key(TEST_FILENAME), "failed bytes must never become trusted metadata");
    assert!(format!("{error:#}").contains("Failed to read HTTP response chunk"), "terminal error must retain the body-read cause: {error:#}");
}

#[tokio::test]
async fn terminal_error_names_file_and_http_root_cause() {
    let mut server = mockito::Server::new_async().await;
    let temp_dir = TempDir::new().unwrap();
    let (index_html, year_html) = listing_html();
    let _index = server.mock("GET", "/").with_status(200).with_body(index_html).create_async().await;
    let _year = server.mock("GET", "/?prefix=data%2F2026%2F").with_status(200).with_body(year_html).create_async().await;
    let checksum_body = format!("{}  {TEST_FILENAME}\n", "0".repeat(64));
    let _checksum = server
        .mock("GET", "/?download=data%2F2026%2Fdiscogs_20260101_CHECKSUM.txt")
        .with_status(200)
        .with_body(checksum_body)
        .create_async()
        .await;
    let failed_download = server
        .mock("GET", "/?download=data%2F2026%2Fdiscogs_20260101_artists.xml.gz")
        .with_status(500)
        .expect(MAX_DOWNLOAD_RETRIES as usize)
        .create_async()
        .await;

    let mut downloader = Downloader::new_with_base_url(temp_dir.path().to_path_buf(), format!("{}/", server.url())).await.unwrap();
    let error = downloader.download_discogs_data().await.unwrap_err();
    let chain = format!("{error:#}");

    failed_download.assert_async().await;
    assert!(chain.contains(&format!("Failed to download {TEST_FILENAME}")), "missing file context: {chain}");
    assert!(chain.contains("HTTP error: 500 Internal Server Error"), "missing HTTP root cause: {chain}");
}

#[tokio::test]
async fn published_checksum_mismatch_deletes_untrusted_download() {
    let mut server = mockito::Server::new_async().await;
    let temp_dir = TempDir::new().unwrap();
    let (index_html, year_html) = listing_html();
    let _index = server.mock("GET", "/").with_status(200).with_body(index_html).create_async().await;
    let _year = server.mock("GET", "/?prefix=data%2F2026%2F").with_status(200).with_body(year_html).create_async().await;
    let checksum_body = format!("{}  {TEST_FILENAME}\n", "0".repeat(64));
    let _checksum = server
        .mock("GET", "/?download=data%2F2026%2Fdiscogs_20260101_CHECKSUM.txt")
        .with_status(200)
        .with_body(checksum_body)
        .create_async()
        .await;
    let corrupt_download = server
        .mock("GET", "/?download=data%2F2026%2Fdiscogs_20260101_artists.xml.gz")
        .with_status(200)
        .with_body("not the published dump")
        .expect(1)
        .create_async()
        .await;

    let mut downloader = Downloader::new_with_base_url(temp_dir.path().to_path_buf(), format!("{}/", server.url())).await.unwrap();
    let error = downloader.download_discogs_data().await.unwrap_err();

    corrupt_download.assert_async().await;
    assert!(format!("{error:#}").contains(&format!("Checksum verification failed for {TEST_FILENAME} against published CHECKSUM")));
    assert!(!temp_dir.path().join(TEST_FILENAME).exists(), "checksum-mismatched bytes must be deleted");
    assert!(!downloader.metadata.contains_key(TEST_FILENAME), "checksum-mismatched bytes must not remain trusted");
}
