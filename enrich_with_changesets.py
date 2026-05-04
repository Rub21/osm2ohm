"""
enrich_with_changesets.py — Annotate candidates with last-changeset metadata.

Reads candidates from a previous extract run and joins with the osm-pds
changesets dataset to add `cs_comment`, `cs_created_by`, `cs_user`,
`cs_source`, and a derived `comment_signal` column:

  - "strong":  positive demolition signal (high-confidence OHM candidate)
  - "exclude": vandalism / import / test / revert (skip)
  - "neutral": empty or generic comment (rely on other rules)

Usage (spark-submit on EMR):

  spark-submit \\
    --deploy-mode client \\
    s3://osm2ohm-rub21/scripts/enrich_with_changesets.py \\
    --candidates_uri s3://osm2ohm-rub21/output/sample_001/ \\
    --changesets_uri s3a://osm-pds/changesets/changesets-latest.orc \\
    --output_uri     s3://osm2ohm-rub21/output/sample_001_enriched/
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


POSITIVE_PATTERN = (
    r"(?i)("
    # ─── English ──────────────────────────────────────────────────
    r"demolish\w*|destroy\w*|destruct\w*|"
    r"raze[ds]?|"
    r"torn\s+down|knock\w*\s+down|tear\w*\s+down|pulled\s+down|"
    r"burned?\s+down|burnt\s+down|"
    r"no\s+longer\s+exists?|"
    r"\bremoved\b|\bgone\b|ceased|dismantled|leveled|vacated|"
    # ─── Spanish ──────────────────────────────────────────────────
    r"demolid[oa]s?|demolici[oó]n|"
    r"destruid[oa]s?|"
    r"derribad[oa]s?|tumbad[oa]s?|"
    r"removid[oa]s?|eliminad[oa]s?|"
    r"ya\s+no\s+(existe|esta|está)|"
    r"desaparecid[oa]s?|escombros|"
    # ─── Portuguese ───────────────────────────────────────────────
    r"demolid[oa]s?|destru[ií]d[oa]s?|derrubad[oa]s?|"
    r"n[aã]o\s+existe\s+mais|desapareceu|demoli[çc][aã]o|"
    # ─── French ───────────────────────────────────────────────────
    r"d[ée]moli[es]?|d[ée]molition|"
    r"d[ée]truit[es]?|abattu[es]?|supprim[ée]s?|ras[ée]s?|"
    r"n[''’]existe\s+plus|disparu[es]?|"
    # ─── German ───────────────────────────────────────────────────
    r"abgerissen|zerst[öo]rt|demoliert|entfernt|"
    r"abgebaut|abgebrochen|niedergerissen|"
    r"existiert\s+nicht\s+mehr|verschwunden|"
    # ─── Italian ──────────────────────────────────────────────────
    r"demolit[oaie]|distrutt[oaie]|abbattut[oaie]|rimoss[oaie]|"
    r"demolizione|non\s+esiste\s+pi[ùu]|scompars[oaie]|"
    # ─── Polish ───────────────────────────────────────────────────
    r"zburzon[ya]|zniszczon[ya]|wyburzon[ya]|rozebran[ya]|"
    r"usuni[ęe]t[ya]|nie\s+istnieje|"
    # ─── Dutch ────────────────────────────────────────────────────
    r"gesloopt|vernietigd|verwijderd|bestaat\s+niet\s+meer|weggehaald|"
    # ─── Swedish / Norwegian / Danish ─────────────────────────────
    r"\brivet\b|\brevet\b|nedrivet|fjernet|"
    r"\b[öo]delagd\b|eksisterer\s+ikke|"
    # ─── Czech ────────────────────────────────────────────────────
    r"zbouran[oýa]|zni[čc]en[ýa]|odstran[ěe]n[ýa]|"
    # ─── Turkish ──────────────────────────────────────────────────
    r"y[ıi]k[ıi]ld[ıi]|y[ıi]k[ıi]lm[ıi][şs]|kald[ıi]r[ıi]ld[ıi]|"
    r"mevcut\s+de[ğg]il|"
    # ─── Russian (Cyrillic) ───────────────────────────────────────
    r"снес[её]н[аоы]?|разрушен[аоы]?|удал[её]н[аоы]?|"
    r"уничтожен[аоы]?|не\s+существует|снесли|разобрали|"
    # ─── Greek ────────────────────────────────────────────────────
    r"κατεδαφ[ιί]στηκε|καταστρ[άα]φηκε|αφαιρ[έε]θηκε|"
    # ─── Hebrew ───────────────────────────────────────────────────
    r"נהרס[ה]?|נהרסת|פורק|הוסר|"
    # ─── Arabic ───────────────────────────────────────────────────
    r"هدم|تم\s+هدم|دم[ّر]|أزيل|أزيلت|تمت\s+إزالة|"
    # ─── Persian (Farsi) ──────────────────────────────────────────
    r"تخریب|تخریب\s+شد|حذف\s+شد|"
    # ─── Hindi ────────────────────────────────────────────────────
    r"ध्वस्त|नष्ट|हटाया|"
    # ─── Chinese (Simplified & Traditional) ──────────────────────
    r"拆除|拆[毁卸燬]|摧[毁燬]|不存在|已拆|被拆|消失|清拆|"
    # ─── Japanese ─────────────────────────────────────────────────
    r"取り壊|解体|撤去|存在しない|消失|"
    # ─── Korean ───────────────────────────────────────────────────
    r"철거|파괴|제거됨|존재하지\s*않"
    r")"
)

NEGATIVE_PATTERN = (
    r"(?i)("
    # NOTE: only match imports/bots when they're clearly massive/automated.
    # A bare "import" (e.g. "fix import error") is NOT excluded.
    # ─── English ──────────────────────────────────────────────────
    r"\brevert\w*|"
    r"vandal\w*|"
    r"\bundo\b|undone|"
    r"\btest\b|"
    r"\btiger\b|"
    r"mass\s+import|automated\s+import|bulk\s+import|"
    r"automated|\bbot\s+(edit|import|fix)|"
    r"duplicate|deduplicate|"
    r"\bmistake\b|by\s+mistake|"
    r"\bwrong\b|incorrect|"
    # ─── Spanish ──────────────────────────────────────────────────
    r"vandalismo|prueba|"
    r"duplicad[oa]s?|por\s+error|incorrecto|"
    # ─── Portuguese ───────────────────────────────────────────────
    r"vandalismo|duplicad[oa]s?|por\s+erro|"
    # ─── French ───────────────────────────────────────────────────
    r"vandalisme|annul[ée]s?|doublon|par\s+erreur|"
    # ─── German ───────────────────────────────────────────────────
    r"vandalismus|doppelt|aus\s+versehen|"
    # ─── Italian ──────────────────────────────────────────────────
    r"vandalismo|duplicato|per\s+errore|"
    # ─── Russian ──────────────────────────────────────────────────
    r"вандализм|откат|отменено|дубликат|по\s+ошибке|"
    # ─── Chinese / Japanese ───────────────────────────────────────
    r"撤回|恢复|破坏|测试|重复|错误|"
    r"取り消し|破壊|テスト"
    r")"
)


def build_changeset_lookup(spark, changesets_uri, needed_ids_df):
    """
    Read the giant changesets ORC, project tag fields, and inner-join
    against the small set of changeset IDs we actually need.
    """
    changesets = spark.read.orc(changesets_uri)

    cs = changesets.select(
        F.col("id").alias("cs_id"),
        F.col("user").alias("cs_user"),
        F.col("uid").alias("cs_uid"),
        F.col("tags").alias("cs_tags"),
    )

    cs = (
        cs.withColumn("cs_comment",    F.col("cs_tags").getItem("comment"))
          .withColumn("cs_created_by", F.col("cs_tags").getItem("created_by"))
          .withColumn("cs_source",     F.col("cs_tags").getItem("source"))
          .drop("cs_tags")
    )

    return cs.join(F.broadcast(needed_ids_df), "cs_id", "inner")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates_uri", required=True,
                        help="Parquet output from extract_ohm_candidates.py")
    parser.add_argument("--changesets_uri", required=True,
                        help="osm-pds changesets ORC, e.g. s3a://osm-pds/changesets/changesets-latest.orc")
    parser.add_argument("--output_uri", required=True,
                        help="Parquet destination for enriched candidates")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("osm2ohm-enrich-changesets")
             .getOrCreate())

    candidates = spark.read.parquet(args.candidates_uri)
    print(f"[enrich] candidates loaded: {candidates.count()}")

    needed_ids = (candidates
                  .select(F.col("last_changeset").alias("cs_id"))
                  .filter(F.col("cs_id").isNotNull())
                  .distinct())
    n_changesets = needed_ids.count()
    print(f"[enrich] unique changesets to look up: {n_changesets}")

    cs_lookup = build_changeset_lookup(spark, args.changesets_uri, needed_ids)

    enriched = candidates.join(
        cs_lookup,
        candidates.last_changeset == cs_lookup.cs_id,
        "left",
    ).drop("cs_id")

    enriched = enriched.withColumn(
        "comment_signal",
        F.when(F.col("cs_comment").rlike(NEGATIVE_PATTERN), F.lit("exclude"))
         .when(F.col("cs_comment").rlike(POSITIVE_PATTERN), F.lit("strong"))
         .otherwise(F.lit("neutral"))
    )

    print()
    print("[enrich] comment_signal distribution (BEFORE filter):")
    enriched.groupBy("comment_signal").count().show()

    # Hard filter: drop only the explicit "exclude" matches (vandalism,
    # imports, reverts, tests, duplicates). Keep "strong" and "neutral":
    # the upstream rules (min_versions, lifetime, etc.) already make
    # neutral candidates trustworthy, and dropping them would lose ~90%
    # of valid OHM material because most OSM mappers don't write detailed
    # comments. The comment_signal column is preserved so downstream
    # consumers can prioritize "strong" candidates for auto-import while
    # routing "neutral" to manual review.
    kept = enriched.filter(F.col("comment_signal") != "exclude")
    n_kept = kept.count()
    print(f"[enrich] keeping {n_kept} candidates (signal in 'strong' or 'neutral')")

    (kept.write
        .mode("overwrite")
        .partitionBy("type")
        .parquet(args.output_uri))

    print(f"[enrich] written to {args.output_uri}")
    print()
    print("[enrich] kept distribution:")
    kept.groupBy("comment_signal").count().show()

    print("[enrich] top editors:")
    (kept.groupBy("cs_created_by")
         .count()
         .orderBy(F.col("count").desc())
         .show(15, truncate=False))

    print("[enrich] sample of 'strong' comments (high-confidence):")
    (kept.filter(F.col("comment_signal") == "strong")
         .select("cs_comment")
         .distinct()
         .show(20, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
