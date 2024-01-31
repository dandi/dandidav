[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![CI Status](https://github.com/dandi/dandidav/actions/workflows/test.yml/badge.svg)](https://github.com/dandi/dandidav/actions/workflows/test.yml)
[![codecov.io](https://codecov.io/gh/dandi/dandidav/branch/main/graph/badge.svg)](https://codecov.io/gh/dandi/dandidav)
[![Minimum Supported Rust Version](https://img.shields.io/badge/MSRV-1.70-orange)](https://www.rust-lang.org)
[![MIT License](https://img.shields.io/github/license/dandi/dandidav.svg)](https://opensource.org/licenses/MIT)

[GitHub](https://github.com/dandi/dandidav) | [Issues](https://github.com/dandi/dandidav/issues)

This is a [Rust](https://www.rust-lang.org) implementation of a readonly
[WebDAV](https://webdav.org) interface to [DANDI
Archive](https://dandiarchive.org), intended as a replacement for the Python
implementation [dandi-webdav](https://github.com/dandi/dandi-webdav).

Features
========

- Support for readonly operations from [RFC
  4918](http://www.webdav.org/specs/rfc4918.html), [DAV compliance
  classes](http://www.webdav.org/specs/rfc4918.html#dav.compliance.classes) 1
  and 3.
    - Not supported: Locking, mutating requests

Advantages over dandi-webdav
----------------------------

- Accessing a non-collection resource results in a 307 redirect to S3.

    - Note that HTML pages for collections link directly to S3 URLs, but this
      is just to save on a request; if you directly access, say,
      <http://localhost:8080/dandisets/000027/draft/sub-RAT123/sub-RAT123.nwb>,
      either by editing the browser address bar or via the command line, you
      will see the redirect.

    - Note that redirects first go to Archive API `/download` URLs, which
      redirect again to S3.  This is necessary in order to obtain a signed S3
      URL that specifies the download file name so that downloaded asset blobs
      will be given the file name of their asset instead of the blob ID.

- The Archive instance to serve can be specified on the command line.

- Paths to resources under Dandiset versions are resolved all at once, rather
  than component-by-component; cf.
  [dandi-webdav#5](https://github.com/dandi/dandi-webdav/issues/5).

- The S3 bucket used for Zarrs is determined by parsing Zarrs' `contentUrls`
  rather than having to be hardcoded.

- The S3 client is cached rather than being reinitialized for every Zarr.


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
