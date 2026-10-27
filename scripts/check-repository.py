#!/usr/bin/env python3
"""Credential-free repository boundary and metadata checks."""

from __future__ import annotations

import json
import re
import subprocess
import tomllib
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
AUTOMATION_CI_REVISION = "dbf22ed83d486e9484fc8c6385c7c3e5995700e4"
AUTOMATION_RELEASE_REVISION = "833cb464507678c38ab78bd4718ce697399463e9"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def markdown_anchors(path: Path) -> set[str]:
    """Return GitHub-style anchors for the ordinary headings used by maintained docs."""
    anchors: set[str] = set()
    duplicates: dict[str, int] = {}
    for heading in re.findall(r"^#{1,6}\s+(.+?)\s*#*\s*$", path.read_text(encoding="utf-8"), re.MULTILINE):
        plain = re.sub(r"<[^>]+>", "", heading)
        plain = re.sub(r"[`*_~]", "", plain).strip().lower()
        slug = re.sub(r"[^\w\s-]", "", plain)
        slug = re.sub(r"\s+", "-", slug)
        duplicate = duplicates.get(slug, 0)
        duplicates[slug] = duplicate + 1
        anchors.add(slug if duplicate == 0 else f"{slug}-{duplicate}")
    return anchors


def check_local_markdown_links(index: Path) -> None:
    """Require every local README/index link and Markdown anchor to resolve."""
    text = index.read_text(encoding="utf-8")
    for raw_target in re.findall(r"(?<!!)\[[^]]+\]\(([^)\s]+)", text):
        target = raw_target.strip("<>")
        parsed = urlsplit(target)
        if parsed.scheme or parsed.netloc:
            continue

        relative_path = unquote(parsed.path)
        destination = index if not relative_path else (index.parent / relative_path).resolve()
        require(destination.is_relative_to(ROOT), f"Markdown link escapes the repository in {index.relative_to(ROOT)}: {target}")
        require(destination.exists(), f"Broken Markdown link in {index.relative_to(ROOT)}: {target}")

        if parsed.fragment:
            require(destination.is_file(), f"Markdown anchor does not target a file in {index.relative_to(ROOT)}: {target}")
            anchor = unquote(parsed.fragment).lower()
            require(anchor in markdown_anchors(destination), f"Broken Markdown anchor in {index.relative_to(ROOT)}: {target}")


def check_conceptual_diagrams() -> None:
    """Keep maintained conceptual diagrams in reviewable, source-native Mermaid."""
    diagram_docs = (
        ROOT / "README.md",
        ROOT / "docs" / "extraction.md",
        ROOT / "docs" / "extraction-rules-guide.md",
        ROOT / "docs" / "state-marker-system.md",
        ROOT / "docs" / "state-marker-periodic-updates.md",
        ROOT / "docs" / "decisions" / "0001-producer-normalization-boundary.md",
    )
    non_mermaid_diagram_fences = {"blockdiag", "d2", "dot", "graphviz", "nomnoml", "plantuml", "puml", "seqdiag"}

    for path in diagram_docs:
        text = path.read_text(encoding="utf-8")
        require("```mermaid" in text, f"Maintained conceptual diagram must use Mermaid in {path.relative_to(ROOT)}")

    for path in (ROOT / "README.md", *(ROOT / "docs").rglob("*.md")):
        languages = {language.lower() for language in re.findall(r"^\s*```([\w+-]+)\s*$", path.read_text(encoding="utf-8"), re.MULTILINE)}
        forbidden = sorted(languages & non_mermaid_diagram_fences)
        require(not forbidden, f"Conceptual diagrams must use Mermaid in {path.relative_to(ROOT)}; found {', '.join(forbidden)}")


cargo = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))["package"]
require(cargo["license"] == "MIT", "Cargo package must use the approved MIT license")
require(cargo["repository"] == "https://github.com/groovemap-music/discogs-ingestion", "stale repository URL")
require(cargo["publish"] is False, "crate publication must remain disabled")

dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
require(len(re.findall(r"^FROM .+@sha256:[0-9a-f]{64}(?: AS \w+)?$", dockerfile, re.MULTILINE)) == 2, "both image stages must be digest-pinned")
require("COPY Cargo.toml ./" in dockerfile and "COPY src ./src" in dockerfile, "Dockerfile still assumes a monorepo-relative root")
require(re.search(r"^ARG UID=1000$", dockerfile, re.MULTILINE) is not None, "Dockerfile must pin the default UID")
require(re.search(r"^ARG GID=1000$", dockerfile, re.MULTILINE) is not None, "Dockerfile must pin the default GID")
require("useradd -r -l -u ${UID}" in dockerfile, "runtime user must use the configured UID")
require(re.search(r"^USER \$\{UID\}:\$\{GID\}$", dockerfile, re.MULTILINE) is not None, "runtime USER must match the owned directories")
require('org.opencontainers.image.title="discogs-ingestion"' in dockerfile, "container image title must match the repository")
require("RUST_EXTRACTOR_CONFIG" not in dockerfile, "unused legacy extractor configuration variable must stay removed")

release_workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
require("repository-name: discogs-ingestion" in release_workflow, "release workflow must use the repository identity")
require("publish-image: true" in release_workflow, "release workflow must publish the repository-named image")

polite_http = (ROOT / "src" / "polite_http.rs").read_text(encoding="utf-8")
require("groovemap-discogs-ingestion/" in polite_http, "default User-Agent must identify GrooveMap catalog ingestion")

for runtime_identity_source in (ROOT / "src" / "main.rs", ROOT / "src" / "health.rs"):
    text = runtime_identity_source.read_text(encoding="utf-8").lower()
    require("rust-extractor" not in text, f"legacy runtime identity remains in {runtime_identity_source.relative_to(ROOT)}")

contract = json.loads((ROOT / "contracts/catalog-events/v1/contract.json").read_text(encoding="utf-8"))
require(contract["version"] == 1, "unexpected catalog contract version")
require((ROOT / "contracts/catalog-events/v1/bindings/python/catalog_contract.py").is_file(), "generated Python binding is absent")

for forbidden in (ROOT / "target", ROOT / "dist", ROOT / ".env"):
    require(not forbidden.is_file(), f"generated or local file is tracked at {forbidden.name}")

for private_planning in (
    ROOT / ".planning",
    ROOT / "docs" / "superpowers",
    ROOT / "docs" / "specs",
):
    require(
        not private_planning.exists(),
        f"private planning material must not be published at {private_planning.relative_to(ROOT)}",
    )

for documentation_index in (ROOT / "README.md", ROOT / "docs" / "README.md"):
    check_local_markdown_links(documentation_index)

check_conceptual_diagrams()

ci_workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
release_workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
automation_ci = (
    "groovemap-music/automation/.github/workflows/reusable-ci.yml@"
    f"{AUTOMATION_CI_REVISION}"
)
automation_release = (
    "groovemap-music/automation/.github/workflows/reusable-release.yml@"
    f"{AUTOMATION_RELEASE_REVISION}"
)

require(f"uses: {automation_ci}" in ci_workflow, "CI must pin the reviewed shared workflow commit")
require(f"uses: {automation_release}" in release_workflow, "release must pin the reviewed shared workflow commit")
require(
    re.search(r"(?m)^  attestations: write$", release_workflow) is not None,
    "release caller must grant artifact and image attestation permission",
)
require("pull_request:" in ci_workflow, "ordinary and Dependabot pull requests must share the pull_request trigger")
require("pull_request_target" not in ci_workflow, "pull_request_target is forbidden")
require(not re.search(r"github\.actor|dependabot\[bot\]", ci_workflow, re.IGNORECASE), "CI must not branch on the pull-request actor")
require("fallback-command" not in ci_workflow, "CI must not provide a reduced validation fallback")
require("secrets: inherit" not in ci_workflow, "CI must map only the Codecov secret it consumes")

jobs_block = ci_workflow.split("\njobs:\n", maxsplit=1)[1]
require(
    re.findall(r"^  ([A-Za-z0-9_-]+):\s*$", jobs_block, re.MULTILINE) == ["required"],
    "normal and Dependabot pull requests must use one identical required job graph",
)
for marker in (
    "language: rust",
    "setup-command: just setup",
    "check-command: just check",
    "coverage-command: just coverage",
    "audit-command: just audit",
    "license-command: just license-check",
    "secret-scan-command: just secret-scan",
    "package-command: just build",
    "install-command: just install-check",
    "image-command: just image",
    "coverage-files: lcov.info",
    "upload-codecov: true",
    "CODECOV_TOKEN: ${{ secrets.CODECOV_TOKEN }}",
):
    require(marker in ci_workflow, f"CI contract marker is missing: {marker}")

for workflow in (ci_workflow, release_workflow):
    for reference in re.findall(r"^\s*uses:\s*([^\s#]+)", workflow, re.MULTILINE):
        require(
            re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_./-]+)?@[0-9a-f]{40}", reference)
            is not None,
            f"workflow reference must use a full commit SHA: {reference}",
        )

tracked_paths = {
    path
    for path in (
        subprocess.run(
            ["git", "ls-files"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
    )
}
require(not any("renovate" in path.lower() for path in tracked_paths), "Renovate configuration must not be present")
require(not any("claude" in path.lower() for path in tracked_paths), "legacy Claude automation must not be present")
require("lcov.info" not in tracked_paths, "generated coverage evidence must not be tracked")

justfile = (ROOT / "Justfile").read_text(encoding="utf-8")
require("-C link-arg=-fuse-ld=bfd" in justfile, "Linux coverage must override the crashing bundled rust-lld linker")
require("cargo llvm-cov --all-features --locked --lcov" in justfile, "Rust coverage must remain enabled")
rust_toolchain = (ROOT / "rust-toolchain.toml").read_text(encoding="utf-8")
require("llvm-tools-preview" in rust_toolchain, "the pinned Rust toolchain must install coverage support noninteractively")

print("repository boundary and metadata checks passed")
