Overview of `dandidav` Architecture
===================================

*This document is up-to-date as of 2024 November 12.*

> [!NOTE]
> A new architecture is currently being planned for the code.  See [issue
> #67](https://github.com/dandi/dandidav/issues/67) for more information.

This document is an overview — and only an overview — of how most of the code
in `dandidav` fits together.  For a fuller understanding of the code, peruse
the source code documentation; you can view the documentation in rendered form
in a web browser by running `cargo doc --open --no-deps` inside a clone of this
repository.

General
-------

- `dandidav` is implemented as an [`axum`](https://github.com/tokio-rs/axum)
  server in which almost all request-handling is done by a `Service`.  (We need
  to use a `Service` rather than `axum`'s normal method routers because the
  latter do not support WebDAV's `PROPFIND` verb.)  The service itself is
  defined at [`src/main.rs`, lines 116-122][service-fn], and it simply passes
  requests to the [`DandiDav::handle_request()`][handle-request] method for the
  actual handling.

    - The only requests not handled by the service are those for the CSS
      stylesheet at `/.static/styles.css`, which is not WebDAV-enabled and thus
      should not support `PROPFIND` or return the WebDAV-specific headers
      present in all other responses.

- If any error occurs during the processing of a request, it will almost always
  "bubble up" to [`DandiDav::handle_request()`][handle-request], which will log
  the error and convert it into a 404, 500, or 502 response, as appropriate.

    - Panics (which are handled by `axum`) should only ever occur if there is
      an actual bug in the code.

- Most `GET` and `PROPFIND` requests to `dandidav` require fetching &
  displaying information about both the resource identified by the request URL
  and that resource's immediate children (if it has any); the exception is
  `PROPFIND` requests with a `Depth` header of `0`, for which information about
  child resources is not fetched.  In order to support both types of requests
  efficiently, many methods & types come in two variants: one variant with the
  suffix `_with_children()`/`WithChildren` for fetching or representing a
  resource and its children, and one variant without the suffix for fetching or
  representing just the resource itself.

    - Requests with a `Depth` header of `infinity` (including missing `Depth`
      headers) are replied to immediately with a 403 response; this is handled
      by [the extraction of the `Depth` header value][extract-depth].

- File sizes are represented as `i64` instead of `u64` for compatibility with
  the official Rust AWS S3 SDK.


`DandiDav`
----------

- The [`DandiDav`][] type is responsible for handling WebDAV and plain HTTP
  requests by:

    - Parsing the request URL path into a [`DavPath`][] describing the type of
      resource the URL refers to and the resource's parameters

        - Splitting a path under a Dandiset version of the form
          `{asset_path}/{zarr_resource_path}` is not done at this stage; that
          is performed by the `DandiClient`.

    - Making calls to a `DandiClient` or `ZarrManClient` instance as
      appropriate to fetch information about the given resource and possibly
      its child resources

    - Rendering the resulting information in the form appropriate to the
      resource and request method

- The entry point to `DandiDav`'s functionality is its
  [`handle_request()`][handle-request] method, which parses the request, passes
  the results to either `DandiDav::get()` or `DandiDav::propfind()` depending
  on the request verb, and then performs error handling and setting of
  universal response headers on the result.

- [`DandiDav::get()`][] and [`DandiDav::propfind()`][] both call
  `DandiDav::get_resource_with_children()` to fetch information about the
  requested resource and, if that resource is a collection, its immediate
  children; as an exception, if a `PROPFIND` request was received with `Depth:
  0`, `DandiDav::propfind()` will instead call `DandiDav::get_resource()`,
  which does not obtain information about child resources.  `DandiDav::get()`
  and `DandiDav::propfind()` then display the obtained information in different
  ways: `DandiDav::get()` renders collections as HTML tables and
  non-collections as redirects (or, for `dandiset.yaml`, serialized YAML),
  while `DandiDav::propfind()` always returns a "multistatus" WebDAV XML
  document.

    - When rendering information about blob assets (either on their own or
      within a parent resource), a choice must be made about what URL to
      provide as the download URL.  The `contentUrl` metadata field of a blob
      asset is assumed to contain two download URLs, one pointing to S3 and one
      pointing to an Archive API endpoint that redirects to a signed version of
      the S3 URL that sets a filename in the `Content-Disposition` header.  The
      latter type of URL is generally more desirable, as it means that any
      users downloading the URL via a browser will end up with a file with the
      same filename as the asset, while a direct, unsigned S3 URL will be
      downloaded as a file with the same filename as the S3 key, which is
      usually the blob ID and not user-friendly.  Hence, by default, a `GET`
      request to `dandidav` for a blob asset will be responded to with a
      redirect to the Archive API download URL.  Unfortunately, certain WebDAV
      clients (i.e., [davfs2](https://savannah.nongnu.org/bugs/?65376)) do not
      support WebDAV servers that return redirects to other redirects, so the
      `--prefer-s3-redirects` CLI option was added to `dandidav` (and is
      currently used by the webdav.dandiarchive.org deployment) to instead make
      these `GET` requests redirect directly to the unsigned S3 URLs.

        - Note that HTML listings of a collection's children will always link
          blob assets to their Archive API download URLs, regardless of
          `--prefer-s3-redirects`, as these listings are only returned for
          `GET` requests to a collection, which WebDAV clients don't do.
          (These listings link blob assets and Zarr entries directly to
          download URLs rather than `dandidav` resource URLs because doing the
          latter would result in subsequent requests to `dandidav` that just
          duplicate work.)

        - Note that this does not apply to download URLs for Zarr entries, as
          their S3 URLs are assumed to always have the same filename as the
          entry, and thus signing to add a `Content-Disposition` is
          unnecessary.

- [`DandiDav::get_resource()`][] and
  [`DandiDav::get_resource_with_children()`][] perform method calls on a
  `DandiClient` or `ZarrManClient` instance (as appropriate) belonging to the
  `DandiDav` instance in order to fetch information about the specified
  resource(s).


`DandiClient`
-------------

- The [`DandiClient`][] type is used to retrieve information about resources on
  an Archive instance (the one specified via the `--api-url` option on program
  invocation) via a combination of the Archive's API and (for entries & folders
  inside Zarrs) requests to an S3 bucket.  It is the data source for the
  `/dandisets/` hierarchy served by `dandidav`.

    - Information about child resources of Zarrs needs to be fetched via S3
      directly rather than using the Archive API because the latter currently
      does not provide an efficient way to list folders within a Zarr.

- The `DandiClient` API is modeled somewhat after
  [`octocrab`](https://github.com/XAMPPRocky/octocrab): To operate on an API
  resource, an endpoint object is requested from the `DandiClient` (possibly
  indirectly via a super-endpoint), and the endpoint's methods are called to
  obtain one or more objects that provide information about the resource or its
  children, but these resource objects are distinct from the endpoint objects
  and do not have any methods for making API calls themselves.

- The code assumes Zarr assets have a `contentUrl` that points to the Zarr's
  entries on an S3 bucket; specifically, the first element of `contentUrl` that
  can be parsed by [`S3Location::parse_url()`][s3loc-parse] into a bucket,
  optional region, and key prefix is used as such.  It is then assumed that the
  Zarr's entries are laid out under the given key prefix (after appending a
  trailing slash if one is not already present) on the given bucket with the
  same names & directory structure as the actual Zarr.

    - If a given Zarr asset lacks an S3 `contentUrl`, any requests to a path
      under that Zarr will result in a 502 response.

    - If an S3 URL does not specify the bucket region, it is determined via a
      `HEAD` request to the domain.  The S3 client caching (see below) means
      that this will almost certainly be done at most once per `dandidav`
      process.

    - The S3 client used to fetch information about Zarr entries from a given
      bucket is cached in the `DandiClient`.  While it is almost certain that
      all Zarrs in the same Archive instance will use the same bucket and thus
      the same client, the possibility of multiple buckets is guarded against
      by using a cache of up to [`S3CLIENT_CACHE_SIZE`][] clients.


`ZarrManClient`
---------------

The [`ZarrManClient`][] type is used to retrieve information about Zarr entries
by fetching *Zarr manifests* from <https://github.com/dandi/zarr-manifests> via
a mirror at <https://datasets.datalad.org/dandi/zarr-manifests/> (the *manifest
tree*); see [`doc/zarrman.md`](zarrman.md) for information on the Zarr manifest
format and server API.  The client is the data source for the `/zarrs/`
hierarchy served by `dandidav`.

`ZarrManClient` has three public methods exposed for use by `DandiDav`:

- [`get_top_level_dirs()`][zm-top-level] — for listing the folders at the top
  of the `/zarrs/` hierarchy
- [`get_resource()`][zm-res] — for obtaining details on a resource at a given
  path underneath `/zarrs/`
- [`get_resource_with_children()`][zm-res-with-child] — for obtaining details
  on a resource and its children at a path underneath `/zarrs/`

When listing the contents of a directory resource that directly corresponds to
a directory in the manifest tree (i.e., a directory directly or indirectly
containing Zarrs/Zarr manifests), an HTTP request is made to the manifest tree
server to obtain the listing.

When listing the contents of a Zarr (either the top level or a descendant
subdirectory), the Zarr's manifest is consulted to discover the resources at
the given location to return.  Zarr manifests are initially obtained via an
HTTP request to the manifest tree server and are afterwards cached until
implementation-defined expiry criteria are met.


[service-fn]: https://github.com/dandi/dandidav/blob/00d0714a88c28737f2d648a5dd57d37568ac0f0a/src/main.rs#L116-L122
[extract-depth]: https://github.com/dandi/dandidav/blob/9b9b04872065b8132657b878bad324b2dff68a97/src/dav/util.rs#L99-L111

[`DandiDav`]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/dav/mod.rs#L37
[handle-request]: https://github.com/dandi/dandidav/blob/d0401d96a45bd381b86bdf2e31d6d80898ccf737/src/dav/mod.rs#L70
[`DandiDav::get()`]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/dav/mod.rs#L129
[`DandiDav::propfind()`]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/dav/mod.rs#L165
[`DandiDav::get_resource()`]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/dav/mod.rs#L216
[`DandiDav::get_resource_with_children()`]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/dav/mod.rs#L272

[`DavPath`]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/dav/path.rs#L8

[`DandiClient`]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/dandi/mod.rs#L27
[s3loc-parse]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/s3/mod.rs#L176
[`S3CLIENT_CACHE_SIZE`]: https://github.com/dandi/dandidav/blob/8d058fe0e561e56ecd3d4c5cd49ca9403b0d196a/src/consts.rs#L24-L25

[`ZarrManClient`]: https://github.com/dandi/dandidav/blob/e9be2dd15ba95d760912344cd09c2a1a08da89b2/src/zarrman/mod.rs#L51
[zm-res]: https://github.com/dandi/dandidav/blob/28d4b5b8a6ad3adca4ae8771480143ac9bcb7c89/src/zarrman/mod.rs#L109
[zm-res-with-child]: https://github.com/dandi/dandidav/blob/28d4b5b8a6ad3adca4ae8771480143ac9bcb7c89/src/zarrman/mod.rs#L159
[zm-top-level]: https://github.com/dandi/dandidav/blob/28d4b5b8a6ad3adca4ae8771480143ac9bcb7c89/src/zarrman/mod.rs#L100
