"""
enrich_with_changesets.py — Add changeset comment/editor + signal to candidates.

For each candidate, fetches the LAST changeset from the OSM API (the one that
deleted the object) and classifies the comment as:
  - "strong":  positive demolition signal (high confidence OHM candidate)
  - "exclude": vandalism/import/test/revert (skip)
  - "neutral": empty or generic comment (rely on other heuristics)

Runs locally; no Spark needed. OSM API allows ~1 req/sec per IP, so ~1000
unique changesets take ~17 minutes. A disk cache avoids re-fetching across runs.

Usage:
    python scripts/enrich_with_changesets.py \\
        s3://osm2ohm-rub21/output/sample_001/ \\
        ./out/sample_001_enriched.parquet
"""

import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

OSM_API = "https://api.openstreetmap.org/api/0.6"
USER_AGENT = "osm2ohm-research/0.1 (rub2106@gmail.com)"
CACHE_DIR = Path("/tmp/osm_changeset_cache")
CACHE_DIR.mkdir(exist_ok=True)
SLEEP_SEC = 1.0  # respect OSM rate limit

POSITIVE = re.compile(
    r"(?i)("
    r"demolish|demolido|demolida|demolicion|demolição|"
    r"destroy|destruct|destruido|destruída|destruída|"
    r"raze|razed|"
    r"torn\s*down|burned?\s*down|"
    r"no\s*longer\s*exists?|no\s*existe|"
    r"ya\s*no\s*(existe|esta|está)|"
    r"não\s*existe\s*mais|"
    r"removed|removido|removida|"
    r"derribad[oa]|tumbad[oa]|"
    r"eliminad[oa]"
    r")"
)

NEGATIVE = re.compile(
    r"(?i)("
    r"revert|reverted|reverting|"
    r"vandal|vandalism|vandalismo|"
    r"undo|undone|deshacer|"
    r"\btest\b|prueba|"
    r"import|imported|importacion|"
    r"\btiger\b|"
    r"duplicate|duplicado|duplicada|"
    r"mistake|by\s*mistake|por\s*error|"
    r"\bwrong\b|incorrecto|"
    r"\bbot\b|automated"
    r")"
)


def classify(comment):
    if not comment:
        return "neutral"
    if NEGATIVE.search(comment):
        return "exclude"
    if POSITIVE.search(comment):
        return "strong"
    return "neutral"


def _cache_path(cs_id):
    return CACHE_DIR / f"{cs_id}.json"


def fetch_changeset(cs_id, session):
    cache_file = _cache_path(cs_id)
    if cache_file.exists():
        return json.loads(cache_file.read_text())

    url = f"{OSM_API}/changeset/{cs_id}"
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        r = session.get(url, headers=headers, timeout=20)
        if r.status_code == 200:
            cs = r.json()["changeset"]
            tags = {t["k"]: t["v"] for t in cs.get("tags", [])}
            result = {
                "comment": tags.get("comment"),
                "created_by": tags.get("created_by"),
                "user": cs.get("user"),
                "uid": cs.get("uid"),
                "source": tags.get("source"),
            }
        elif r.status_code == 404:
            result = {"_status": 404}
        elif r.status_code == 429:
            print("  ⚠️  rate limited, sleeping 60s...")
            time.sleep(60)
            return fetch_changeset(cs_id, session)
        else:
            result = {"_status": r.status_code}
    except requests.RequestException as e:
        print(f"  error fetching cs {cs_id}: {e}")
        result = {"_error": str(e)}

    cache_file.write_text(json.dumps(result))
    time.sleep(SLEEP_SEC)
    return result


def _read_partition(input_dir, type_value):
    path = f"{input_dir.rstrip('/')}/type={type_value}/"
    try:
        df = pd.read_parquet(path)
        df["type"] = type_value
        return df
    except (FileNotFoundError, OSError):
        return pd.DataFrame()


def main(input_dir, output_path):
    print(f"[enrich] reading {input_dir}")
    df = pd.concat(
        [_read_partition(input_dir, "node"), _read_partition(input_dir, "way")],
        ignore_index=True,
    )
    print(f"[enrich] total candidates: {len(df)}")

    unique_cs = sorted(set(int(x) for x in df["last_changeset"].dropna()))
    print(f"[enrich] unique changesets to fetch: {len(unique_cs)}")

    session = requests.Session()
    cache = {}
    for i, cs_id in enumerate(unique_cs, 1):
        cache[cs_id] = fetch_changeset(cs_id, session)
        if i % 50 == 0 or i == len(unique_cs):
            print(f"  fetched {i}/{len(unique_cs)}")

    df["cs_comment"]    = df["last_changeset"].map(lambda x: cache.get(int(x), {}).get("comment") if pd.notna(x) else None)
    df["cs_created_by"] = df["last_changeset"].map(lambda x: cache.get(int(x), {}).get("created_by") if pd.notna(x) else None)
    df["cs_user"]       = df["last_changeset"].map(lambda x: cache.get(int(x), {}).get("user") if pd.notna(x) else None)
    df["cs_source"]     = df["last_changeset"].map(lambda x: cache.get(int(x), {}).get("source") if pd.notna(x) else None)
    df["comment_signal"] = df["cs_comment"].apply(classify)

    print()
    print("[enrich] comment_signal distribution:")
    print(df["comment_signal"].value_counts())
    print()
    print("[enrich] top editors:")
    print(df["cs_created_by"].value_counts().head(10))

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path)
    print(f"[enrich] written to {output_path}")
    print(f"[enrich] cache dir: {CACHE_DIR} ({len(list(CACHE_DIR.iterdir()))} files)")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
