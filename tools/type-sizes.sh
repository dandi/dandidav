#!/bin/bash
# Produces a listing of the types defined by dandidav (including anonymous
# types like futures returned by async functions) along with their sizes,
# ordered by decreasing size
#
# Requires:
#
# - nightly Rust
# - top-type-sizes <https://crates.io/crates/top-type-sizes>

set -eux -o pipefail

outfile="${1:?Usage: $0 outfile}"

cargo clean -p dandidav

RUSTFLAGS=-Zprint-type-sizes cargo +nightly build -j 1 | top-type-sizes \
    --sort-fields \
    --filter '@src/|\b(dandi|dav|httputil|paths|s3|streamutil|zarrman)::' \
    --remove-wrappers \
    --hide-less 8 \
    > "$outfile"
