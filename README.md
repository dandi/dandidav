[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![CI Status](https://github.com/jwodder/dandidav/actions/workflows/test.yml/badge.svg)](https://github.com/jwodder/dandidav/actions/workflows/test.yml)
[![codecov.io](https://codecov.io/gh/jwodder/dandidav/branch/master/graph/badge.svg)](https://codecov.io/gh/jwodder/dandidav)
[![Minimum Supported Rust Version](https://img.shields.io/badge/MSRV-1.70-orange)](https://www.rust-lang.org)
[![MIT License](https://img.shields.io/github/license/jwodder/dandidav.svg)](https://opensource.org/licenses/MIT)

[GitHub](https://github.com/jwodder/dandidav) | [Issues](https://github.com/jwodder/dandidav/issues)

This is a [Rust](https://www.rust-lang.org) implementation of a readonly WebDAV
interface to [DANDI Archive](https://dandiarchive.org).

Building & Running
==================

1. [Install Rust and Cargo](https://www.rust-lang.org/tools/install).

2. Clone this repository and `cd` into it.

3. Run `cargo build` to build the binary.  The intermediate build artifacts
   will be cached in `target/` in order to speed up subsequent builds.

    - Alternatively, run `cargo build --release` or `cargo build -r` to build
      with optimizations enabled.

4. Run with `cargo run` (or `cargo run --release` if built with `--release`) to
   run the server.  If any server CLI options are supplied, they must be
   separated from `cargo run [--release]` by a `--` argument.

    - The WebDAV server will be accessible for as long as the program is left
      running.  Shut it down by hitting Ctrl-C.

5. If necessary, the actual binary can be found in `target/debug/dandidav` (or
   `target/release/dandidav` if built with `--release`).  It should run on any
   system with the same OS and architecture as it was built on.

Usage
=====

    cargo run [-r] -- [<options>]

`dandidav` serves a WebDAV interface to the DANDI Archive at
http://127.0.0.1:8080 by default.  It can be accessed by any WebDAV client or
in a normal web browser.  (If your client asks you about login details, you may
log in without authentication/as a guest.)

Options
-------

- `--api-url <URL>` — Specify the API URL of the DANDI Archive instance to
  serve [default: `https://api.dandiarchive.org/api`]

- `--ip-addr <IPADDR>` — Specify the IP address for the server to listen on
  [default: 127.0.0.1]

- `-p <PORT>`, `--port <PORT>` — Specify the port for the server to listen on
  [default: 8080]

- `-T <TITLE>`, `--title <TITLE>` — Specify the site name to include in the
  `<title>`s of HTML pages for collections
