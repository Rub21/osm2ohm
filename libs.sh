#!/bin/bash
set -e

# Install only what we actually need at the cluster's default python3.
# - boto3: read/write S3 from Python (rules.json, GeoJSON exporter, etc.)
# Pure-Python install, takes ~5s per node. Avoid shapely/geopandas here:
# they need GEOS/GDAL and pip can hang for 20+ min trying to compile them.
sudo python3 -m pip install --quiet boto3
