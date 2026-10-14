# cairn-cell

`cairn-cell` is the leaf crate for build-cell policy and lifecycle primitives.
It is named for the domain rather than the mechanism used to materialize a
checkout or any single lifecycle operation.

Today, the crate owns checkout population:

- `PopulateConfig`, the `materialization.populate.copy` globs from
  `.cairn/config.yaml`
- `populate()` and `populate_real()`, which warm a newly materialized cell with
  Git-ignored artifacts from the source repository
- the filesystem backend boundary used to test population without touching a
  real checkout

Population copies ignored paths that match a copy rule and skips the rest,
leaving them to setup commands. It always excludes `.cairn`, preserves existing
destinations, and treats individual entry failures as best-effort results.
Invalid glob configuration remains a fatal error.

Cairn creates no symlinks inside build cells. The former `populate.symlink`
strategy shared one main-checkout directory across every cell, which coupled
cells to each other and gave every owned-storage walker a link to follow. A
config that still names it is rejected at load with a message pointing at
`copy`, rather than silently ignored. A link that already exists inside a
copied tree (for example `node_modules/.bin`) is reproduced as a link, never
followed.

The crate does not currently own cell provisioning, checkout integrity checks,
repair, adoption, or reclaim sweeps. Those lifecycle operations remain in
`cairn-exec` and should move here only when an extraction is justified by a
concrete need.
