# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

## v0.3.1 (2026-09-14)

### Fix

- **release**: recover the v0.3.0 publication rejected before workflow admission

## v0.3.0 (2026-09-14)

The `v0.3.0` workflow failed before publishing artifacts or images. The tag is
retained as an immutable record of that release attempt.

### Feat

- **image**: package local smoke contract
- **extractor**: add local manifest input seam
- **telemetry**: span the extraction path and propagate context to consumers
- **telemetry**: export OTLP traces from the tracing subscriber
- **telemetry**: emit process and tokio runtime gauges
- **telemetry**: port wave-2 runtime metrics and tracing
- **build**: cache Rust compilation with sccache

### Fix

- **telemetry**: let one test own the runtime the tokio gauges report on
- **ci**: accept commitizen's no-eligible-commits bump-preview state
- **deps**: refresh rust:1.98-slim digest and pin Python 3.14.7 (#3)

### Refactor

- **runtime**: remove narrating orchestration comments

## v0.2.1 (2026-09-04)

### Fix

- **image**: copy the vendored media taxonomy into the builder stage

## v0.2.0 (2026-09-04)

### Feat

- **contracts**: carry the media block in release fixtures and document it
- **rules**: flag Discogs format names the media taxonomy does not know
- **media**: add the Rust media mapper and attach the canonical media block
- **contracts**: fail contract-check on vendored taxonomy drift
- **contracts**: vendor the media taxonomy
- **telemetry**: export OTLP metrics from the extraction pipeline

### Fix

- **discogs**: invalidate Docker library cache
- **discogs**: seed Docker library target
- **toolchain**: bind pinned tools through mise
- **split**: freeze provider compatibility baseline

### Refactor

- **discogs**: make repository provider-exclusive
- **contracts**: generate source-owned v1 exports
- **runtime**: partition provider-owned modules

## v0.1.1 (2026-08-31)

### Fix

- **release**: use supported files-only flag
- **ci**: accept release-boundary bump states

## v0.1.0 (2026-08-31)

The `v0.1.0` workflow failed before publishing artifacts or images. The tag is
retained as an immutable record of that release attempt.
