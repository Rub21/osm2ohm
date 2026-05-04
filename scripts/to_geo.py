"""
to_geo.py — Convert candidate Parquet output to GeoParquet + GeoJSON.

Reads candidates written by extract_ohm_candidates.py (or the enriched
output from enrich_with_changesets.py) and produces visualization files.

Output layout:
  * Nodes: Point geometry from (last_lon, last_lat).
  * Ways:  geometry = null. They reference node IDs in `last_nds` but
    we don't have coords here. Will still appear in the GeoJSON properties
    table; map viewers will list them but not draw them. Build proper
    LineString/Polygon geometry with a follow-up enrichment job.

Properties included (when present):
  * id, type, num_versions, distinct_users, lifetime_days, age_at_deletion_days
  * deleted_at, last_changeset, last_good_version
  * tags (last visible version's tags, dict)
  * cs_comment, cs_created_by, cs_user, cs_source, comment_signal
  * marker-color / marker-symbol  (simplestyle-spec for geojson.io etc.)

Usage:
    python scripts/to_geo.py s3://osm2ohm-rub21/output/sample_001_enriched/ ./out/sample_001
"""

import json
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


# Style config keyed by comment_signal — simplestyle spec used by
# geojson.io, GitHub gist GeoJSON, and many other viewers.
STYLE = {
    "strong":  {"marker-color": "#1a9850", "marker-symbol": "star",   "marker-size": "medium"},
    "neutral": {"marker-color": "#999999", "marker-symbol": "circle", "marker-size": "small"},
    "exclude": {"marker-color": "#d73027", "marker-symbol": "cross",  "marker-size": "small"},
}
DEFAULT_STYLE = {"marker-color": "#666666", "marker-symbol": "circle", "marker-size": "small"}


def _to_dict(tags):
    """
    Spark MAP columns surface as list[{'key','value'}] OR list[(k, v)] OR
    dict depending on pyarrow version. Normalize to a plain dict.
    """
    if tags is None:
        return {}
    if isinstance(tags, dict):
        return tags
    out = {}
    for t in tags:
        if isinstance(t, dict):
            out[t.get("key")] = t.get("value")
        elif isinstance(t, (tuple, list)) and len(t) >= 2:
            out[t[0]] = t[1]
    out.pop(None, None)
    return out


def _read_partition(input_dir, type_value):
    path = f"{input_dir.rstrip('/')}/type={type_value}/"
    try:
        df = pd.read_parquet(path)
        df["type"] = type_value
        return df
    except (FileNotFoundError, OSError):
        return pd.DataFrame()


def _row_to_properties(row):
    props = {}
    for col, val in row.items():
        if col in ("geometry", "last_lat", "last_lon", "last_nds"):
            continue
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        if col == "good_tags":
            props["tags"] = _to_dict(val)
            continue
        if hasattr(val, "isoformat"):
            props[col] = val.isoformat()
        elif hasattr(val, "item"):
            props[col] = val.item()
        else:
            props[col] = val

    style = STYLE.get(props.get("comment_signal"), DEFAULT_STYLE)
    props.update(style)
    return props


def _df_to_features(df, geom_fn):
    features = []
    for _, row in df.iterrows():
        geom = geom_fn(row)
        features.append({
            "type": "Feature",
            "geometry": (
                {"type": "Point", "coordinates": [geom.x, geom.y]}
                if geom is not None else None
            ),
            "properties": _row_to_properties(row),
        })
    return features


def main(input_dir, output_base):
    nodes = _read_partition(input_dir, "node")
    ways  = _read_partition(input_dir, "way")
    print(f"[to_geo] nodes: {len(nodes)}")
    print(f"[to_geo] ways:  {len(ways)}")

    def node_geom(row):
        lat, lon = row.get("last_lat"), row.get("last_lon")
        if pd.isna(lat) or pd.isna(lon):
            return None
        return Point(float(lon), float(lat))

    def way_geom(_row):
        return None

    node_features = _df_to_features(nodes, node_geom) if len(nodes) else []
    way_features  = _df_to_features(ways,  way_geom)  if len(ways)  else []
    all_features  = node_features + way_features

    output_base = Path(output_base)
    output_base.parent.mkdir(parents=True, exist_ok=True)
    geojson_path    = output_base.with_suffix(".geojson")
    geoparquet_path = output_base.with_suffix(".geoparquet")

    geojson = {"type": "FeatureCollection", "features": all_features}
    geojson_path.write_text(json.dumps(geojson, ensure_ascii=False))

    if node_features:
        gdf = gpd.GeoDataFrame(
            [
                {**f["properties"],
                 "geometry": Point(f["geometry"]["coordinates"])}
                for f in node_features if f["geometry"] is not None
            ],
            geometry="geometry",
            crs="EPSG:4326",
        )
        if "tags" in gdf.columns:
            gdf["tags"] = gdf["tags"].apply(json.dumps)
        gdf.to_parquet(geoparquet_path, index=False)
        print(f"[to_geo] {len(gdf)} node features → {geoparquet_path}")

    print(f"[to_geo] {len(all_features)} total features → {geojson_path}")
    print(f"  ({len(node_features)} nodes with geometry, {len(way_features)} ways without)")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
