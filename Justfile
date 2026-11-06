set shell := ["bash", "-euo", "pipefail", "-c"]

cargo_audit_tool := "cargo:cargo-audit@0.22.2"
cargo_cyclonedx_tool := "cargo:cargo-cyclonedx@0.5.9"
cargo_deny_tool := "cargo:cargo-deny@0.20.2"
cargo_llvm_cov_tool := "cargo:cargo-llvm-cov@0.9.0"

default:
    @just --list

bootstrap:
    mise install --jobs 1
    mise install --jobs 1 {{cargo_audit_tool}} {{cargo_cyclonedx_tool}} {{cargo_deny_tool}} {{cargo_llvm_cov_tool}}
    # .mise.toml [env] sets RUSTC_WRAPPER=sccache, so the pinned sccache must be on PATH.
    sccache --version

setup: bootstrap
    cargo fetch --locked

check: bootstrap format-check lint test contract-check repository-check repository-tests build-check license-check secret-scan bump-preview

format:
    cargo fmt --all

format-check:
    cargo fmt --all -- --check

lint:
    cargo clippy --all-targets --all-features --locked -- -D warnings

test:
    cargo test --all-features --locked

source-characterization:
    cargo test --all-features --locked --test extractor_di_test discogs
    cargo test --all-features --locked discogs::parser::tests::
    cargo test --all-features --locked discogs::normalize::tests::
    cargo test --all-features --locked discogs::rules::tests::

# Real-broker tests (#[ignore]d in `just test`): start a disposable RabbitMQ at the deployment
# pin, run them, and always remove the container and its anonymous volumes. Needs docker.
rabbitmq_image := "rabbitmq@sha256:c200fd496482cda18c412be4e6f244830e1e75949b8cf5f0e887cea9ea456109"

test-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    name="gm-ingest-it-$$"
    trap 'docker rm -f -v "$name" >/dev/null 2>&1 || true' EXIT
    docker run -d --name "$name" --user rabbitmq -p 127.0.0.1::5672 {{rabbitmq_image}} >/dev/null
    port="$(docker port "$name" 5672/tcp | head -n1 | sed 's/.*://')"
    for _ in $(seq 1 60); do
        docker exec "$name" rabbitmq-diagnostics -q ping >/dev/null 2>&1 && break
        sleep 2
    done
    docker exec "$name" rabbitmq-diagnostics -q ping >/dev/null
    export GM_TEST_AMQP_URL="amqp://guest:guest@127.0.0.1:${port}" GM_TEST_RABBITMQ_CONTAINER="$name"
    timeout 600 cargo test --all-features --locked --lib -- --ignored --test-threads=1

coverage: bootstrap
    cargo clean
    if [[ "$(uname -s)" == Linux ]]; then export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-C link-arg=-fuse-ld=bfd"; fi; mise exec {{cargo_llvm_cov_tool}} -- cargo llvm-cov --all-features --locked --lcov --output-path lcov.info

contract:
    mise exec -- python contracts/generate.py

contract-check:
    mise exec -- python contracts/generate.py --check

repository-check:
    mise exec -- python scripts/check-repository.py

# Keep: this focused policy test is the fast feedback path for publication-history
# tooling; `repository-tests` also retains it in the full repository gate.
publication-history-test:
    PYTHONDONTWRITEBYTECODE=1 mise exec -- python -m unittest discover -s tests -p 'test_publication_history.py'

repository-tests:
    PYTHONDONTWRITEBYTECODE=1 mise exec -- python -m unittest discover -s tests -p 'test_*.py'

# Keep: operators use this non-publishing rehearsal to produce sanitized-history
# evidence before the separately approved history cutover described in the runbook.
history-rehearsal source-repository output-directory:
    PLANNING_ARCHIVE_REPO="${PLANNING_ARCHIVE_REPO}" bash scripts/rehearse-publication-history.sh "{{ source-repository }}" "{{ output-directory }}"

build:
    cargo build --release --locked

install-check:
    test -x target/release/discogs-ingestion
    install_root="$(mktemp -d)"; trap 'rm -rf "${install_root}"' EXIT; install -m 0755 target/release/discogs-ingestion "${install_root}/discogs-ingestion"; "${install_root}/discogs-ingestion" --help >/dev/null

build-check:
    cargo check --all-targets --all-features --locked

license-check: bootstrap
    mise exec {{cargo_deny_tool}} -- cargo deny --log-level error check licenses bans sources

secret-scan:
    mise exec -- gitleaks git --redact --no-banner
    mise exec -- gitleaks dir . --redact --no-banner

audit: bootstrap
    mise exec {{cargo_audit_tool}} -- cargo audit

cyclonedx: bootstrap
    mise exec {{cargo_cyclonedx_tool}} -- cargo cyclonedx --format json

image:
    docker build --pull --tag discogs-ingestion:local .

bump-preview:
    mise exec -- python scripts/check_bump_preview.py

# Update Cargo metadata and changelog only; do not commit, tag, push, or publish.
bump:
    mise exec -- uvx --from commitizen==4.9.1 cz bump --files-only --changelog --yes --check-consistency

release-dry-run: check
    bash scripts/release-dry-run.sh
