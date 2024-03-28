In Development
--------------
- Set `Access-Control-Allow-Origin: *` header in all responses
- Log current memory usage before & after handling each request

v0.3.0 (2024-03-15)
-------------------
- Respond to undecodable "Depth" header values with a 400 response instead of
  acting like no value was specified
- Improve logging:
    - Log a message before & after each HTTP request made to the Dandi Archive,
      to datasets.datalad.org, or to S3 when determining a bucket's region
    - Emit logs on stderr instead of stdout
    - Disable log coloration when stderr is not a terminal
    - Suppress noisy & irrelevant log messages from various dependencies
    - Log errors that cause 404 and 500 responses
    - Use local timezone offset for log timestamps
- Added breadcrumbs to HTML views of collections
- `FAST_NOT_EXIST` components are now checked for case-insensitively
- Add links to version & asset metadata to the web view
- Adjust the format of timestamps in the web view: Always use UTC, show the
  timezone as "Z", prevent line breaking in the middle, wrap in `<time>` tag
- Format sizes in the web view in "1.23 MiB" style
- Zarr entries under `/zarrs/` are now served with ".zarr" extensions
- Use `<thead>` and `<tbody>` in collection tables in web view
- Add `--prefer-s3-redirects` option for redirecting requests for blob assets
  directly to S3 instead of to Archive URLs that redirect to signed S3 URLs

v0.2.0 (2024-02-07)
-------------------
- Serve Zarr entries via manifests from
  <https://datasets.datalad.org/?dir=/dandi/zarr-manifests> at "`/zarrs/`"

v0.1.0 (2024-02-01)
-------------------
Initial release
