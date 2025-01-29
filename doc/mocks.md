Mocks, Stubs, and Specs
=======================

`dandidav`'s tests involve making queries to an instance of itself that in turn
makes requests to a mocked version of the DANDI Archive.  The requests handled
& responses returned by this mock archive are defined by *response stubs*, JSON
files located under `src/testdata/stubs/`.  As these stubs necessarily involve
a nontrivial amount of repeated information, they are not created manually but
are instead generated from *specs*, YAML files located under `mockspecs/` that
describe the contents of the mock archive.

Spec Schemata & Layout
----------------------

The specs consist of:

- A `dandiset.yaml` file that lists the Dandisets in the mock archive and their
  versions

- An `assets/{dandiset_id}/{version_id}.yaml` file for each Dandiset version
  that lists the assets in that version

The `genrandassets.py` script in the `tools/` directory can be used to randomly
generate new asset listings.

### `dandiset.yaml`

The `dandiset.yaml` file contains a list of Dandiset definitions, each of which
is an object with the following fields:

- `identifier` — Dandiset ID (string)
- `created` — timestamp)
- `modified` — timestamp
- `contact_person` — string
- `embargo_status` — string
- `versions` — list of version definitions, each of which is an object with the following fields:

    - `version` — version ID (string)
    - `name` – string
    - `status` — string
    - `created` — timestamp
    - `modified` — timestamp
    - `metadata` — arbitrary JSON-compatible data
    - `assets` *(optional)* — A list of asset paths that the tests will query
      for and thus that appropriate stubs should be generated for.
    - `asset_dirs` *(optional)* — A list of asset directory paths (sans trailing
      slash) that the tests will query for and thus that appropriate stubs should
      be generated for.  A path of `null` denotes the root directory.

    There must be one version with a `version` value of "draft".  The non-draft
    version with the latest `created` date, if any, becomes the most recent
    published version.

### Asset Listings

Asset files contains a list of assets, each of which is an object with the following fields:

- `asset_id` — string
- `blob` — string or `null`
- `zarr` — string or `null`
- `path` — string
- `size` — integer
- `created` — timestamp
- `modified` — timestamp
- `metadata` — Arbitrary JSON-compatible data; this should include the fields
  `encodingFormat`, `contentUrl`, and `digest`

Stub Schema & Layout
--------------------

Response stubs are laid out in a directory hierarchy mirroring their request
paths, with the last URL path component (after stripping trailing slashes)
becoming the basename of a JSON file; for example, responses to requests for
`/api/dandisets/000001/` are looked up in `api/dandisets/000001.json` within
the stub directory.

A single response stub file contains a JSON list of objects, each of which has
a `params` fields and a `response` field.  The `params` field is an object with
string values specifying the parsed query parameters of the requests to which
the associated `response` is the response.  Parameters must match exactly: a
query string of `path=foo&metadata=1&order=path` will only match a `params`
value of `{"path": "foo", "metadata": "1", "order": "path"}` (ignoring element
order), while additional parameters in the query string will cause the given
`params` to not match.

If the mock archive server cannot find a stub file or response that matches a
given request, it will respond with a 404.

Generating Stubs from Specs
---------------------------

The `genstubs` package in `crates/genstubs` defines a Rust program for
generating stubs from specs.  It can be run via:

    cargo run -p genstubs -- $specdir $stubdir

where `$specdir` is the directory containing the specs and `$stubdir` is the
directory in which to place the stubs.

Alternatively, simply run `cargo genstubs` in the root of this repository, and
`genstubs` will be run with the appropriate arguments, using an alias defined
in the repository's `.cargo/config.toml`.
