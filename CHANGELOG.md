In Development
--------------
- Respond to undecodable "Depth" header values with a 400 response instead of
  acting like no value was specified
- Improve logging:
    - Log a message before & after each HTTP request made to the Dandi Archive,
      to datasets.datalad.org, or to S3 when determining a bucket's region
    - Emit logs on stderr instead of stdout
    - Disable log coloration when stderr is not a terminal
    - Suppress noisy & irrelevant log messages from various dependencies
    - Log errors that cause 404 and 500 responses
- Added breadcrumbs to HTML views of collections
- `FAST_NOT_EXIST` components are now checked for case-insensitively

v0.2.0 (2024-02-07)
-------------------
- Serve Zarr entries via manifests from
  <https://datasets.datalad.org/?dir=/dandi/zarr-manifests> at "`/zarrs/`"

v0.1.0 (2024-02-01)
-------------------
Initial release
