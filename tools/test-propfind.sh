#!/bin/bash
# Make a number of test PROPFIND queries to dandidav and save the output
set -eux

# dandidav server root:
endpoint=http://127.0.0.1:8080
#endpoint=https://webdav.dandiarchive.org

# Path to a collection resource to query:
collection=dandisets/000108/draft
#collection=zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr

# Path to a non-collection resource to query:
item=dandisets/000027/draft/sub-RAT123/sub-RAT123.nwb

# Directory in which to store responses:
outdir=propfind

mkdir -p "$outdir"

curl -fsSL -X PROPFIND -H "Depth: 0" "$endpoint" > "$outdir"/root-depth0.xml
curl -fsSL -X PROPFIND -H "Depth: 1" "$endpoint" > "$outdir"/root-depth1.xml

curl -fsSL -X PROPFIND -H "Depth: 0" "$endpoint/$collection" > "$outdir"/collection-depth0.xml
curl -fsSL -X PROPFIND -H "Depth: 1" "$endpoint/$collection" > "$outdir"/collection-depth1.xml

curl -fsSL \
    -X PROPFIND \
    -H "Depth: 1" \
    -d '<propfind xmlns="DAV:"><propname/></propfind>' \
    "$endpoint/$collection" > "$outdir"/propname.xml

curl -fsSL \
    -X PROPFIND \
    -H "Depth: 1" \
    -d '<propfind xmlns="DAV:"><prop><resourcetype/></prop></propfind>' \
    "$endpoint/$collection" > "$outdir"/resourcetype.xml

curl -fsSL \
    -X PROPFIND \
    -H "Depth: 0" \
    -d '
        <propfind xmlns="DAV:">
          <prop>
            <getcontentlength/>
            <resourcetype/>
            <unknown/>
            <extension xmlns="https://dav.example.com"/>
            <displayname/>
          </prop>
        </propfind>
    ' "$endpoint/$item" > "$outdir"/prop.xml

# No -f:
curl -sSL -X PROPFIND -H "Depth: 1" "$endpoint/does-not-exist" > "$outdir"/nonexistent.txt
