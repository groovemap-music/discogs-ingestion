# Publication readiness

Publication rehearsal happens only in separate local backup and sanitized clones. The
sanitized history removes `.planning/`, `docs/superpowers/`, and `docs/specs/` from every
reachable ref. The public extraction architecture in [`extraction.md`](extraction.md) is
preserved byte-for-byte.

Removed planning records map to the private `planning-archive` repository at commit
`daf82a149aaa382b3cebbd4b43d3c82e53d4128e`. The checked-in rehearsal tooling records the
source and sanitized commits, complete ref and commit maps, object dispositions, strict
repository checks, secret scans, and a fresh sanitized validation log. The immutable local
evidence digest is recorded on the corresponding Beadhive publication-evidence bead rather
than committed with generated recovery material.

History cutover and repository publication are separate operator approvals. A successful
rehearsal does not rewrite a remote, change visibility, publish an image or package, create a
tag, or create a release.

Run the focused `just publication-history-test` while changing the attestation or
rehearsal tooling. Operators run
`just history-rehearsal <source-repository> <output-directory>` with
`PLANNING_ARCHIVE_REPO` set to the local planning-archive checkout. Both recipes are
deliberately retained: the focused test shortens policy feedback, and the rehearsal is
the documented non-publishing evidence path even though neither is called by another
repository-local recipe.
