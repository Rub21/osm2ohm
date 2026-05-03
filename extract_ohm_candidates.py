"""
extract_ohm_candidates.py

Reads the OSM history file (ORC at s3://osm-pds/planet-history/) and
produces a dataset of *deleted* objects that are good candidates for
import into OpenHistoricalMap (OHM), applying the rules declared in
rules.json.

Usage (spark-submit on EMR):

  spark-submit \\
    --deploy-mode cluster \\
    s3://osm2ohm-rub21/scripts/extract_ohm_candidates.py \\
    --history_uri  s3a://osm-pds/planet-history/history-latest.orc \\
    --rules_uri    s3://osm2ohm-rub21/scripts/rules.json \\
    --output_uri   s3://osm2ohm-rub21/output/ohm_candidates/
"""

import argparse
import json
import re

import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def _split_keys(keys):
    """Split a list of keys into (exact, prefixes). A trailing '*' marks a prefix."""
    exact = [k for k in keys if not k.endswith("*")]
    prefixes = [k[:-1] for k in keys if k.endswith("*")]
    return exact, prefixes


def _has_any(tag_keys_col, keys):
    """True if any tag key matches an exact key OR starts with any prefix."""
    exact, prefixes = _split_keys(keys)
    conds = []
    if exact:
        conds.append(F.arrays_overlap(tag_keys_col, F.array(*[F.lit(k) for k in exact])))
    if prefixes:
        pattern = "|".join(f"^{re.escape(p)}" for p in prefixes)
        conds.append(F.exists(tag_keys_col, lambda k: k.rlike(pattern)))
    if not conds:
        return F.lit(True)
    out = conds[0]
    for c in conds[1:]:
        out = out | c
    return out


def _read_json(uri: str) -> dict:
    """Read a small JSON file from s3:// or a local path. boto3 is provided
    by the bootstrap (libs.sh) and authenticates via the EC2 instance profile."""
    if uri.startswith("s3://"):
        _, _, rest = uri.partition("s3://")
        bucket, _, key = rest.partition("/")
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body)
    with open(uri) as fh:
        return json.load(fh)


def load_rules(rules_uri: str) -> dict:
    return _read_json(rules_uri)


def build_pipeline(spark: SparkSession, history_uri: str, rules: dict,
                   sample_rate: float = 1.0):
    df = spark.read.orc(history_uri)

    # ------------------------------------------------------------------
    # 0) Optional sampling for fast iteration. We hash by (type, id) so
    # ALL versions of a sampled object stay together — otherwise the
    # window/aggregations downstream would see incomplete history.
    # ------------------------------------------------------------------
    if sample_rate < 1.0:
        bucket = max(int(round(1.0 / sample_rate)), 2)
        df = df.filter(F.abs(F.hash("type", "id")) % bucket == 0)
        print(f"[osm2ohm] sampling 1/{bucket} of objects (~{sample_rate*100:.3f}%)")

    # ------------------------------------------------------------------
    # 1) Filter by type only.
    # ------------------------------------------------------------------
    df = df.filter(F.col("type").isin(rules["object_types"]))

    # ------------------------------------------------------------------
    # 2) Group by (type, id): each row is a version.
    # ------------------------------------------------------------------
    w = Window.partitionBy("type", "id").orderBy(F.col("version").desc())
    last_v = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1")

    w_first = Window.partitionBy("type", "id").orderBy(F.col("version").asc())
    first_v = df.withColumn("rn", F.row_number().over(w_first)).filter("rn = 1")

    # Last visible version (the one shipped to OHM if it passes the filter)
    last_visible = (
        df.filter(F.col("visible") == True)
          .withColumn("rn", F.row_number().over(w))
          .filter("rn = 1")
    )

    stats = (
        df.groupBy("type", "id")
          .agg(
              F.max("version").alias("num_versions"),
              F.countDistinct("uid").alias("distinct_users"),
              F.min("timestamp").alias("created_at"),
              F.max("timestamp").alias("last_edit_at"),
          )
    )

    base = (
        stats
        .join(last_v.select(
                "type", "id",
                F.col("visible").alias("last_visible"),
                F.col("uid").alias("last_uid"),
                F.col("timestamp").alias("deleted_at"),
            ), ["type", "id"])
        .join(first_v.select(
                "type", "id",
                F.col("uid").alias("creator_uid"),
            ), ["type", "id"])
        .join(last_visible.select(
                "type", "id",
                F.col("tags").alias("good_tags"),
                F.col("lat").alias("last_lat"),
                F.col("lon").alias("last_lon"),
                F.col("nds").alias("last_nds"),
                F.col("changeset").alias("last_changeset"),
                F.col("version").alias("last_good_version"),
            ), ["type", "id"], "left")
    )

    # ------------------------------------------------------------------
    # 3) Apply the rules.
    # ------------------------------------------------------------------
    cond = F.lit(True)

    if rules.get("must_be_deleted"):
        cond &= F.col("last_visible") == False

    cond &= F.col("num_versions") >= rules["min_versions"]
    cond &= F.col("distinct_users") >= rules["min_distinct_users"]

    lifetime_days = F.datediff(F.col("last_edit_at"), F.col("created_at"))
    cond &= lifetime_days >= rules["min_lifetime_days"]

    age_at_delete_days = F.datediff(F.col("deleted_at"), F.col("created_at"))
    cond &= age_at_delete_days >= rules["min_age_at_deletion_days"]

    if rules.get("exclude_deletions_by_creator"):
        cond &= F.col("creator_uid") != F.col("last_uid")

    # tags: map<string,string> in osm-pds ORC -> extract the set of keys
    tag_keys = F.map_keys(F.col("good_tags"))

    required_any = rules.get("required_tags_any") or []
    if required_any:
        cond &= _has_any(tag_keys, required_any)

    excluded = rules.get("exclude_tags_keys") or []
    if excluded:
        cond &= ~_has_any(tag_keys, excluded)

    cond &= F.size(F.col("good_tags")) >= rules["min_tag_count_last_visible"]

    # ways: minimum number of nodes in the last valid version
    way_min_nodes = rules.get("way_min_nodes", 0)
    if way_min_nodes:
        ok_geom = (F.col("type") != "way") | (F.size(F.col("last_nds")) >= way_min_nodes)
        cond &= ok_geom

    candidates = (
        base.filter(cond)
            .withColumn("lifetime_days", lifetime_days)
            .withColumn("age_at_deletion_days", age_at_delete_days)
            .select(
                "type", "id",
                "num_versions", "distinct_users",
                "created_at", "last_edit_at", "deleted_at",
                "lifetime_days", "age_at_deletion_days",
                "creator_uid", "last_uid",
                "last_changeset", "last_good_version",
                "last_lat", "last_lon", "last_nds",
                "good_tags",
            )
    )
    return candidates


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--history_uri", required=True,
                        help="ORC with the OSM history (s3a://osm-pds/planet-history/history-latest.orc)")
    parser.add_argument("--rules_uri", required=True,
                        help="rules.json (local or s3://...)")
    parser.add_argument("--output_uri", required=True,
                        help="Parquet destination for the candidates")
    parser.add_argument("--sample_rate", type=float, default=1.0,
                        help="Fraction of objects to keep (1/N hash sampling). "
                             "1.0 = full dataset; 0.001 = ~0.1%% (smoke test).")
    args = parser.parse_args()

    rules = load_rules(args.rules_uri)

    spark = (SparkSession.builder
             .appName("osm2ohm-extract-candidates")
             .getOrCreate())

    candidates = build_pipeline(
        spark, args.history_uri, rules,
        sample_rate=args.sample_rate,
    )

    (candidates
        .write.mode("overwrite")
        .partitionBy("type")
        .parquet(args.output_uri))

    total = candidates.count()
    print(f"[osm2ohm] candidates written to {args.output_uri}: {total}")

    spark.stop()


if __name__ == "__main__":
    main()
