#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///
from __future__ import annotations
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import random
import string
from typing import IO
from uuid import uuid4

SCHEMA_VERSION = "0.6.3"

CONTEXT = f"https://raw.githubusercontent.com/dandi/schema/master/releases/{SCHEMA_VERSION}/context.json"

DIGEST_CHARS = "0123456789abcdef"

MIN_DATETIME = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
MAX_DATETIME = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

ZARR_PROBABILITY = 0.1

BLOB_TYPES = [
    (".nwb", "application/x-nwb"),
    (".json", "application/json"),
    (".tsv", "text/tab-separated-values"),
]

BLOB_TYPE_WEIGHTS = [0.8, 0.1, 0.1]

BLOB_SIZE_LAMBDA = 1.0 / 10000.0

ZARR_FILE_QTY_LAMBDA = 1.0 / 1000.0

ZARR_AVG_FILE_SIZE_LAMBDA = 1.0 / 10000.0

FILENAME_LENGTH_MU = 5
FILENAME_LENGTH_SIGMA = 2

FILES_PER_DIR_MU = 5
FILES_PER_DIR_SIGMA = 2

DIRS_PER_DIR_MU = 3
DIRS_PER_DIR_SIGMA = 1


@dataclass
class Asset:
    asset_id: str
    blob: str | None
    zarr: str | None
    path: str
    size: int
    created: datetime
    modified: datetime
    metadata: Metadata

    def dump(self, fp: IO[str]) -> None:
        print("- asset_id:", qqrepr(self.asset_id), file=fp)
        print("  blob:", qqrepr(self.blob), file=fp)
        print("  zarr:", qqrepr(self.zarr), file=fp)
        print("  path:", qqrepr(self.path), file=fp)
        print(f"  size: {self.size}", file=fp)
        print("  created:", qqrepr(self.created.isoformat()), file=fp)
        print("  modified:", qqrepr(self.modified.isoformat()), file=fp)
        print("  metadata:", file=fp)
        self.metadata.dump(fp)


@dataclass
class Metadata:
    identifier: str
    path: str
    encodingFormat: str
    dateModified: datetime
    blobDateModified: datetime
    contentUrl: list[str]
    contentSize: int
    digest: dict[str, str]

    def dump(self, fp: IO[str]) -> None:
        print('    "@context":', qqrepr(CONTEXT), file=fp)
        print("    schemaVersion:", qqrepr(SCHEMA_VERSION), file=fp)
        print('    schemaKey: "Asset"', file=fp)
        print("    id:", qqrepr(f"dandiasset:{self.identifier}"), file=fp)
        print("    identifier:", qqrepr(self.identifier), file=fp)
        print("    path:", qqrepr(self.path), file=fp)
        print("    encodingFormat:", qqrepr(self.encodingFormat), file=fp)
        print("    dateModified:", qqrepr(self.dateModified.isoformat()), file=fp)
        print(
            "    blobDateModified:", qqrepr(self.blobDateModified.isoformat()), file=fp
        )
        print("    contentUrl:", file=fp)
        for url in self.contentUrl:
            print("      - " + qqrepr(url), file=fp)
        print("    contentSize:", self.contentSize, file=fp)
        print("    digest:", file=fp)
        for k, v in self.digest.items():
            print(" " * 6 + k + ": " + qqrepr(v), file=fp)


@dataclass
class DatetimeSet:
    created: datetime
    modified: datetime
    metadata_modified: datetime
    blob_date_modified: datetime

    @classmethod
    def generate(cls) -> DatetimeSet:
        created = random_datetime()
        metadata_modified = random_datetime(min=created)
        blob_date_modified = random_datetime(min=metadata_modified)
        modified = random_datetime(min=blob_date_modified)
        return cls(
            created=created,
            modified=modified,
            metadata_modified=metadata_modified,
            blob_date_modified=blob_date_modified,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--depth", type=int, default=3)
    parser.add_argument("-o", "--outfile", type=argparse.FileType("w"), default="-")
    args = parser.parse_args()
    first = True
    for path in random_tree(args.depth):
        if random.random() <= ZARR_PROBABILITY:
            asset = random_zarr(path)
        else:
            asset = random_blob(path)
        if first:
            first = False
        else:
            print(file=args.outfile)
        asset.dump(args.outfile)


def random_tree(depth: int) -> list[str]:
    dirs = [""]
    files = []
    # Add 1 so we get a last layer of files `depth` directories deep:
    for _ in range(depth + 1):
        newdirs = []
        for d in dirs:
            if d:
                d += "/"
            for _ in range(round(random.gauss(DIRS_PER_DIR_MU, DIRS_PER_DIR_SIGMA))):
                name = random_filename()
                newdirs.append(d + name)
            for _ in range(round(random.gauss(FILES_PER_DIR_MU, FILES_PER_DIR_SIGMA))):
                name = random_filename()
                files.append(d + name)
        dirs = newdirs
    return files


def random_blob(path: str) -> Asset:
    asset_id = str(uuid4())
    blob = str(uuid4())
    dates = DatetimeSet.generate()
    [(ext, content_type)] = random.choices(BLOB_TYPES, BLOB_TYPE_WEIGHTS, k=1)
    path += ext
    size = round(random.expovariate(BLOB_SIZE_LAMBDA))
    contentUrl = [
        f"https://api.dandiarchive.org/api/assets/{asset_id}/download/",
        f"https://dandiarchive.s3.amazonaws.com/blobs/{blob[:3]}/{blob[3:6]}/{blob}",
    ]
    digest = {
        "dandi:dandi-etag": f"{random_digest(32)}-{etag_part_qty(size)}",
        "dandi:sha2-256": random_digest(64),
    }
    return Asset(
        asset_id=asset_id,
        blob=blob,
        zarr=None,
        path=path,
        size=size,
        created=dates.created,
        modified=dates.modified,
        metadata=Metadata(
            identifier=asset_id,
            path=path,
            encodingFormat=content_type,
            dateModified=dates.metadata_modified,
            blobDateModified=dates.blob_date_modified,
            contentUrl=contentUrl,
            contentSize=size,
            digest=digest,
        ),
    )


def random_zarr(path: str) -> Asset:
    asset_id = str(uuid4())
    zarr = str(uuid4())
    dates = DatetimeSet.generate()
    path += random.choice([".zarr", ".ngff"])
    file_qty = round(random.expovariate(ZARR_FILE_QTY_LAMBDA))
    size = round(file_qty * random.expovariate(ZARR_AVG_FILE_SIZE_LAMBDA))
    contentUrl = [
        f"https://api.dandiarchive.org/api/assets/{asset_id}/download/",
        f"https://dandiarchive.s3.amazonaws.com/zarr/{zarr}/",
    ]
    digest = {
        "dandi:dandi-zarr-checksum": f"{random_digest(32)}-{file_qty}--{size}",
    }
    return Asset(
        asset_id=asset_id,
        blob=None,
        zarr=zarr,
        path=path,
        size=size,
        created=dates.created,
        modified=dates.modified,
        metadata=Metadata(
            identifier=asset_id,
            path=path,
            encodingFormat="application/x-zarr",
            dateModified=dates.metadata_modified,
            blobDateModified=dates.blob_date_modified,
            contentUrl=contentUrl,
            contentSize=size,
            digest=digest,
        ),
    )


def random_filename() -> str:
    length = max(1, round(random.gauss(FILENAME_LENGTH_MU, FILENAME_LENGTH_SIGMA)))
    return "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(length)
    )


def random_datetime(
    min: datetime = MIN_DATETIME, max: datetime = MAX_DATETIME
) -> datetime:
    ts = random.uniform(min.timestamp(), max.timestamp())
    return datetime.fromtimestamp(ts, timezone.utc)


def random_digest(length: int) -> str:
    return "".join(random.choice(DIGEST_CHARS) for _ in range(length))


def qqrepr(s: str | None) -> str:
    return json.dumps(s)


def etag_part_qty(file_size: int) -> int:
    MAX_PARTS = 10_000
    MIN_PART_SIZE = 5 * (1 << 20)
    MAX_PART_SIZE = 5 * (1 << 30)
    DEFAULT_PART_SIZE = 64 * (1 << 20)
    if file_size == 0:
        return 0
    part_size = DEFAULT_PART_SIZE
    if file_size > 5 * (1 << 40):
        raise ValueError("File is larger than the S3 maximum object size.")
    if idiv_ceil(file_size, part_size) >= MAX_PARTS:
        part_size = idiv_ceil(file_size, MAX_PARTS)
    assert MIN_PART_SIZE <= part_size <= MAX_PART_SIZE
    part_qty, final_part_size = divmod(file_size, part_size)
    if final_part_size != 0:
        part_qty += 1
    return part_qty


def idiv_ceil(dividend: int, divisor: int) -> int:
    return (dividend + divisor - 1) // divisor


if __name__ == "__main__":
    main()
