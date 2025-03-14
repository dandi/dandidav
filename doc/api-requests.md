How `dandidav` Uses the DANDI Archive API
=========================================

*This document is up-to-date as of 2025 March 6.*

When `dandidav` receives a request for a path under `/dandisets/`, the requests
it makes to the DANDI Archive API for each type of `dandidav` request path are
as follows.

Note that there are two different approaches that `dandidav` uses for fetching
resources:

- When responding to a `GET` request or a `PROPFIND` request with a `Depth`
  header of `1` (hereafter referred to as a "deep request"), `dandidav`
  requests both the resource identified by the request path and (if that
  resource is a collection) all of its immediate child resources.

- When responding to a `PROPFIND` request with a `Depth` header of `0`
  (hereafter referred to as a "shallow request"), `dandidav` requests only the
  resource identified by the request path, not any of its children.

Note that `dandidav` immediately rejects all `PROPFIND` requests with a `Depth`
header of `infinity`.

Dandiset Index
--------------

> **dandidav path:** `/dandisets/`

For deep requests, `dandidav` paginates over the API's `/dandisets/` endpoint.

For shallow requests, `dandidav` does not make any API requests.

Dandiset Top Level
------------------

> **dandidav path:** `/dandisets/{dandiset_id}/`

For both deep and shallow requests, `dandidav` makes an API request to `/dandisets/{dandiset_id}/`.

Releases Index
--------------

> **dandidav path:** `/dandisets/{dandiset_id}/releases/`

For deep requests, `dandidav` paginates over the API's `/dandisets/{dandiset_id}/versions/` endpoint.

For shallow requests, `dandidav` does not make any API requests.

Top Level of a Dandiset Version
-------------------------------

> **dandidav paths:**
>
> - `/dandisets/{dandiset_id}/draft/`
> - `/dandisets/{dandiset_id}/latest/`
> - `/dandisets/{dandiset_id}/releases/{version_id}/`

For both deep and shallow requests, if the request path is
`/dandisets/{dandiset_id}/latest/`, an initial request is made to
`/dandisets/{dandiset_id}/` to get the version ID of the latest published
version.

For deep requests, `dandidav` makes API requests to the following endpoints:

- `/dandisets/{dandiset_id}/versions/{version_id}/info/`
- `/webdav/assets/atpath/?dandiset_id={dandiset_id}&version_id={version_id}&children=true&metadata=true`
  (paginated)

For shallow requests, `dandidav` makes an API request to
`/dandisets/{dandiset_id}/versions/{version_id}/info/`.

Metadata File
-------------

> **dandidav paths:**
>
> - `/dandisets/{dandiset_id}/draft/dandiset.yaml`
> - `/dandisets/{dandiset_id}/latest/dandiset.yaml`
> - `/dandisets/{dandiset_id}/releases/{version_id}/dandiset.yaml`

For both deep and shallow requests, if the request path is
`/dandisets/{dandiset_id}/latest/`, an initial request is made to
`/dandisets/{dandiset_id}/` to get the version ID of the latest published
version.  Then, for both deep and shallow requests, `dandidav` makes an API
request to `/dandisets/{dandiset_id}/versions/{version_id}/`.

Asset Path
----------

> **dandidav paths:**
>
> - `/dandisets/{dandiset_id}/draft/{path}`
> - `/dandisets/{dandiset_id}/latest/{path}`
> - `/dandisets/{dandiset_id}/releases/{version_id}/{path}`
>
> Note that any trailing slashes at the end of `path` are ignored and are
> stripped before passing to the Archive.

For both deep and shallow requests, if the request path is
`/dandisets/{dandiset_id}/latest/`, an initial request is made to
`/dandisets/{dandiset_id}/` to get the version ID of the latest published
version.

Then, for each initial subpath of `path` that ends with a non-final component
ending in ".zarr" or ".ngff" (case insensitive), an API request is made to
`/webdav/assets/atpath/?dandiset_id={dandiset_id}&version_id={version_id}&path={subpath}&metadata=true`,
and the result is handled as follows:

- If the response consists of a blob asset, then the user requested a path
  under a blob, and so no more requests are made to the Archive, and `dandidav`
  returns a 404 response.

- If the response consists of a Zarr asset, `dandidav` has found a Zarr asset,
  and the checking of initial subpaths terminates.  A request is then made to
  S3 to fetch information about the resource at the path equal to the
  remainder.

- If the response consists of a folder, the next initial subpath is checked.

- If the response is a 404, no more requests are made to the Archive, and
  `dandidav` returns a 404 response.

If all initial subpaths are checked without finding an asset or a 404, then
`path` is treated as an asset path with no intra-Zarr subpath.  Shallow
requests then make a request to
`/webdav/assets/atpath/?dandiset_id={dandiset_id}&version_id={version_id}&path={path}&metadata=true`,
while deep requests make a paginated request to the same endpoint but with
`children=true` added to the query parameters.  For deep requests, if a Zarr
asset is found at `path`, further requests are made to S3 to fetch information
about the Zarr's entries.

Other Notes
-----------

- Paginated requests to the Archive use a `page_size` of 10,000 and are
  requested serially.

- `dandidav` requires the following information from the Archive about each
  type of resource, not always at the same time.  Required information about
  the relationships between resources is not listed here.

    - Dandisets:
        - identifier (to serve as the resource name)
        - creation & modification timestamps

    - Versions:
        - identifier (to serve as the resource name)
        - size
        - creation & modification timestamps
        - metadata (to serve the virtual `dandiset.yaml` resources)

    - Asset folders:
        - path/name

    - Assets:
        - path/name
        - ID (for computing the metadata URL to display in the web view)
        - blob ID and Zarr ID (to determine the asset type)
        - size
        - creation & modification timestamps
        - metadata:

            - `digest` — For blob assets, the `"dandi:dandi-etag"` digest is
              used as the resource's ETag in `PROPFIND` responses

            - `contentUrl` — As a reminder, this array contains both an Archive
              download URL and a direct S3 URL.  Which one is used depends on
              the asset type and the situation:

                - Blob assets: When rendering links in the web (HTML) view, the
                  Archive download URL is used, as this results in a redirect
                  to a signed S3 URL that sets the Content-Disposition header,
                  thereby ensuring that the user's browser downloads the asset
                  to a file of the same name.  When responding to `GET`
                  requests for blob assets, either URL may be used, depending
                  on the presence of the `--prefer-s3-redirects` command-line
                  option.

                - Zarr assets: The S3 URL is parsed by `dandidav` to locate the
                  Zarr's entries on S3.

            - `encodingFormat` — Used as the resource's Content Type in
              `PROPFIND` responses
