// Library exports for testing

pub mod config;
pub mod discogs;
pub mod generated {
    pub mod catalog_contract;
}
pub mod extractor;
pub mod health;
pub mod logging;
pub mod message_queue;
pub mod polite_http;
pub mod runtime;
pub mod state_marker;
pub mod telemetry;
pub mod types;

// Frozen compatibility paths for downstream Discogs callers.
pub use discogs::downloader as discogs_downloader;
pub use discogs::normalize;
pub use discogs::parser;
pub use discogs::rules;

// Additional test modules
#[cfg(test)]
#[path = "tests/message_queue_unit_tests.rs"]
mod message_queue_unit_tests;
