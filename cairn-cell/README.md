# cairn-cell

`cairn-cell` is the leaf crate for build-cell policy and lifecycle primitives.
It is named for the domain rather than the mechanism used to materialize a
checkout or any single lifecycle operation.

Today, the crate owns checkout population:

- `PopulateConfig`, the `populate.copy` and `populate.symlink` globs from
  `.cairn/config.yaml`
- `populate()` and `populate_real()`, which warm a newly materialized cell with
  Git-ignored artifacts from the source repository
- the filesystem backend boundary used to test population without touching a
  real checkout

Population applies copy rules before symlink rules, always excludes `.cairn`,
preserves existing destinations, and treats individual entry failures as
best-effort results. Invalid glob configuration remains a fatal error.

The crate does not currently own cell provisioning, checkout integrity checks,
repair, adoption, or reclaim sweeps. Those lifecycle operations remain in
`cairn-exec` and should move here only when an extraction is justified by a
concrete need.
