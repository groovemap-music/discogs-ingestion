#[test]
fn runtime_image_uses_repository_identity_and_safe_boundaries() {
    let dockerfile = include_str!("../Dockerfile");
    assert!(dockerfile.contains("org.opencontainers.image.title=\"discogs-ingestion\""));
    assert!(dockerfile.contains("github.com/groovemap-music/discogs-ingestion"));
    assert!(dockerfile.contains("org.opencontainers.image.licenses=\"MIT\""));
    assert_eq!(dockerfile.matches("cargo build --release --locked").count(), 2);
    assert!(dockerfile.contains("USER ${UID}:${GID}"));
    assert!(dockerfile.contains("CMD [\"curl\", \"-f\", \"http://localhost:8000/health\"]"));
    assert!(dockerfile.contains("COPY contracts/extractor-smoke /usr/share/discogs-ingestion/contracts/extractor-smoke"));
    assert!(!dockerfile.lines().any(|line| line.trim_start().starts_with("ENV LOCAL_MANIFEST=")));
    assert!(!dockerfile.lines().any(|line| {
        line.trim_start().starts_with("ENV ")
            && ["PASSWORD", "USERNAME", "SECRET", "TOKEN", "CREDENTIAL", "PRIVATE_KEY"].iter().any(|key| line.contains(key))
    }));
}
