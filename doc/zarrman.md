Requirements for the Zarr Manifest API
======================================

*This document is up-to-date as of 2024 July 13.*

The implementation of the `/zarrs/` hierarchy served by `dandidav` works by
fetching documents called *Zarr manifests* from a URL hierarchy, hereafter
referred to as the *manifest tree*, the base URL of which is hereafter referred
to as the *manifest root*.  Currently, the manifest root is hardcoded to
<https://datasets.datalad.org/dandi/zarr-manifests/zarr-manifests-v2-sorted/>,
a subdirectory of a mirror of <https://github.com/dandi/zarr-manifests> (but
see [issue #161](https://github.com/dandi/dandidav/issues/161)).  The manifest
tree and the Zarr manifests are expected to meet the following requirements:

- An HTTP `GET` request to any extant directory in the manifest tree (including
  the manifest root) must return a JSON object containing the following keys:

    - `"files"` — an array of strings listing the names (i.e., final path
      components) of the files (if any) available immediately within the
      directory

    - `"directories"` — an array of strings listing the names (i.e., final path
      components) of the subdirectories (if any) available immediately within
      the directory

- Each Zarr manifest describes a single Zarr in a DANDI Archive instance at a
  certain point in time.  The URL for each Zarr manifest must be of the form
  `{manifest_root}/{prefix1}/{prefix2}/{zarr_id}/{checksum}.json`, where:

    - `{prefix1}` is the first three characters of `{zarr_id}`
    - `{prefix2}` is the next three characters of `{zarr_id}`
    - `{zarr_id}` is the Zarr ID of the Zarr on the Archive instance
    - `{checksum}` is the [Zarr checksum][] of the Zarr's contents at the point
      in time that the manifest represents

- The manifest tree should not contain any files that are not Zarr manifests
  nor any directories that are not a parent directory of a Zarr manifest.
  `dandidav`'s behavior should it encounter any such "extra" resources is
  currently an implementation detail and may change in the future.

- A Zarr manifest is a JSON object containing an `"entries"` key whose value is
  a tree of objects mirroring the directory & entry structure of the Zarr.

    - Each entry in the Zarr is represented as an array of the following four
      elements, in order, describing the entry as of the point in time
      represented by the Zarr manifest:

        - The S3 version ID (as a string) of the then-current version of the S3
          object in which the entry is stored in the Archive instance's S3
          bucket

        - The `LastModified` timestamp of the entry's S3 object as a string of
          the form `"YYYY-MM-DDTHH:MM:SS±HH:MM"`

        - The size in bytes of the entry as an integer

        - The `ETag` of the entry's S3 object as a string with leading &
          trailing double quotation marks (U+0022) removed (not counting the
          double quotation marks used by the JSON serialization)

    - Each directory in the Zarr is represented as an object in which each key
      is the name of an entry or subdirectory inside the directory and the
      corresponding value is either an entry array or a directory object.

    - The `entries` object itself represents the top level directory of the
      Zarr.

    For example, a Zarr with the following structure:

    ```text
    .
    ├── .zgroup
    ├── foo/
    │   ├── .zgroup
    │   ├── bar/
    │   │   ├── .zarray
    │   │   └── 0
    │   └── baz/
    │       ├── .zarray
    │       └── 0
    └── quux/
        ├── .zarray
        └── 0
    ```

    would have an `entries` field as follows (with elements of the entry arrays
    omitted):

    ```json
    {
        ".zgroup": [ ... ],
        "foo": {
            ".zgroup": [ ... ],
            "bar": {
                ".zarray": [ ... ],
                "0": [ ... ]
            },
            "baz": {
                ".zarray": [ ... ],
                "0": [ ... ]
            }
        },
        "quux": {
            ".zarray": [ ... ],
            "0": [ ... ]
        }
    }
    ```

- For a Zarr with Zarr ID `zarr_id` and an entry therein at path `entry_path`,
  the download URL for the entry is expected to be
  `{base_url}/{zarr_id}/{entry_path}`, where `base_url` is currently hardcoded
  for all entries to <https://dandiarchive.s3.amazonaws.com/zarr/>.

[Zarr checksum]: https://github.com/dandi/dandi-archive/blob/master/doc/design/zarr-support-3.md#zarr-entry-checksum-format
