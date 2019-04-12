#!/bin/bash
set -v

cargo test --features "with-postgres with-sqlite"
cargo test --features "with-postgres"
cargo test --features "with-sqlite"
