# Bandwidth development deployment

Build `wings-rs` from this repository and `tundra-node` from `vendor/tundra` with Rust 1.97 or newer. The vendored Tundra source is the daemon counterpart of the `tundra-common` path dependency in `application/Cargo.toml`.

On each development Wings node, install the built `wings-rs` binary as `/usr/local/bin/wings` and the built `tundra-node` binary as `/usr/local/bin/calagopus-tundra-bandwidth`. In `/etc/calagopus-wings/config.yml`, set `tundra.binary` to `/usr/local/bin/calagopus-tundra-bandwidth`. Keep `tundra.image: debian:trixie-slim`; the daemon is copied into the existing Tundra data mount when Wings reconciles its application container. Restart only `wings.service`. Wings will update its owned Tundra application container when the binary hash changes.

Bandwidth ledgers live in `bandwidth/` under the configured Wings `system.root_directory`, named by service UUID. Keep that directory with the Wings data when recovering a node; Wings writes ledger checkpoints atomically and archives completed billing periods there.
