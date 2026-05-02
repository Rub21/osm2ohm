#!/bin/bash
set -e

PKGS="numpy pandas shapely geopandas pyarrow"

FLAGS=""
if sudo python3 -m pip install --help 2>/dev/null | grep -q "break-system-packages"; then
  FLAGS="--break-system-packages"
fi

sudo python3 -m pip install $FLAGS $PKGS
