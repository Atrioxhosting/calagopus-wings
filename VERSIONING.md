# Atriox Wings versions

`application/Cargo.toml` is the version source for Wings. Use
`<upstream major>.<upstream minor>.<upstream patch>+atriox.<revision>`.
The first three components follow upstream Calagopus; Atriox changes only the
revision after `+atriox.`.

- For an Atriox patch, increase the Atriox revision.
- For an upstream patch update, keep the Atriox revision. For example,
  `1.2.2+atriox.3` becomes `1.2.3+atriox.3`.
- For an upstream major or minor update, reset the Atriox revision to zero.
  For example, `1.2.3+atriox.3` becomes `1.3.0+atriox.0`.

Update `Cargo.lock` with the manifest. The runtime version, Panel User-Agent,
diagnostics and backup metadata use Cargo's package version. Release Git tags
must match it exactly, including `+atriox.N`. Docker image tags use `_` in
place of `+`, because Docker tags cannot contain `+`; the OCI image version
label preserves the exact Cargo version.
