# osm2ohm

A **Big Data pipeline on Spark / EMR** that identifies objects
**deleted from OpenStreetMap** that are good candidates to be imported
into **OpenHistoricalMap (OHM)**.

The selection is not arbitrary: a set of declarative rules
(`rules.json`) is applied over the OSM full history to keep only
objects that had a real life on the map (multiple versions, multiple
editors, meaningful tags) before being deleted.

## Overview

1. Source: `s3://osm-pds/planet-history/history-latest.orc` (~213 GB,
   AWS Open Data public bucket, refreshed weekly).
2. Spark groups every object by `(type, id)` and computes metrics
   across all of its versions.
3. A job applies the rules from `rules.json` and keeps only the good
   candidates.
4. Output is a Parquet (and later GeoJSON) with the last visible
   version of the object plus metadata required to bring it into OHM.

## Rules (`rules.json`)

| Rule | Purpose |
|------|---------|
| `min_versions` | Drop ephemeral objects / vandalism. |
| `min_distinct_users` | Confirm more than one mapper validated it. |
| `min_lifetime_days` | The object existed long enough to be real. |
| `min_age_at_deletion_days` | Avoid quick deletions due to mistakes or tests. |
| `must_be_deleted` | Only keep deleted objects (the focus of OHM). |
| `exclude_deletions_by_creator` | Whoever created it must be different from whoever deleted it. |
| `required_tags_any` | Must carry at least one tag with historical / identity value (`name`, `historic`, `building`, etc.). |
| `min_tag_count_last_visible` | The last valid version had to be properly tagged. |
| `way_min_nodes` | Reasonable minimum geometry for ways. |
| `country` | ISO_A3 (lowercase) of the country file under `countries/` to crop the planet (e.g. `bol`). Set to `null` for whole planet. |

## Repo layout

```
osm2ohm/
├── README.md
├── deploy_emr.sh             # creates / destroys the EMR cluster via Terraform
├── libs.sh                   # cluster bootstrap
├── rules.json                # declarative rules
├── extract_ohm_candidates.py # main Spark job
├── countries/                # bbox definitions per country, named by ISO_A3 (e.g. bol.json)
└── terraform/
    └── main.tf               # EMR cluster + S3 bucket
```

## Bucket

Everything project-related lives in **`s3://osm2ohm-rub21`**:

```
s3://osm2ohm-rub21/
├── bootstrap/libs.sh
├── scripts/extract_ohm_candidates.py
├── scripts/rules.json
├── countries/<iso_a3>.json       <- bbox definitions per country
├── output/ohm_candidates/        <- Parquet output
└── logs/                         <- EMR logs
```

> The name is set in `terraform/main.tf` (`var.bucket_name`) and in
> `deploy_emr.sh` (`BUCKET=...`). S3 requires globally unique names.

## Local requirements

- Terraform ≥ 1.5
- AWS CLI v2 with valid credentials in `.env.aws`
- `jq`

`.env.aws` (not committed):

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

## Deploy

```bash
./deploy_emr.sh apply     # creates bucket + uploads scripts + spins up EMR
                          # AND auto-launches the Spark step
./deploy_emr.sh plan      # preview
./deploy_emr.sh destroy   # tears everything down
```

It prints the JupyterHub URL, the SSH command, the `.pem` path and
the debug UIs (YARN + Spark History).

The `apply` already submits the Spark job as an EMR step
(`extract-ohm-candidates`), so you don't need to SSH in to launch it.

### Lifecycle modes

Two terraform variables control how the cluster shuts down:

| Variable | Default | Effect |
|----------|---------|--------|
| `auto_terminate_on_completion` | `true` | Cluster dies as soon as the step finishes (success or fail). **Fire-and-forget**. |
| `idle_timeout_seconds` | `600` | Extra safety: cluster also dies after 10 min idle. |

For iterative development (run multiple steps without recreating the
cluster), apply with the flag flipped:

```bash
cd terraform
terraform apply -var=auto_terminate_on_completion=false
```

In that mode you can use `./deploy_emr.sh run` to add new steps after
editing scripts/rules.

### Estimated runtime (Bolivia)

With the default cluster (1 × m5.xlarge master + 3 × m5.2xlarge cores),
a Bolivia-bbox run typically takes **30–90 minutes**. The bbox filter
helps but the `Window.partitionBy("type", "id")` shuffles the planet
before filtering. Watch progress in YARN UI / Spark History or via
`./deploy_emr.sh logs`.

## Running the job

The cluster auto-runs the job on creation. To re-run after editing
`rules.json`, the script, or a country file:

```bash
./deploy_emr.sh run        # re-uploads scripts/ and adds a fresh step
./deploy_emr.sh status     # PENDING / RUNNING / COMPLETED / FAILED
./deploy_emr.sh logs       # tail the driver stderr live over SSH
```

`run` does NOT recreate the cluster — it only adds a new step to the
existing one. Edit `rules.json` (or `countries/<iso>.json`, or the
`.py`) locally, then:

```bash
./deploy_emr.sh run
./deploy_emr.sh logs
```

### Watching progress

- `./deploy_emr.sh logs` → smart: live `tail -F` over SSH while the
  cluster is alive; falls back to dumping the gzipped logs from S3
  once the cluster has auto-terminated.
- `./deploy_emr.sh errors` → greps `ERROR/Exception/FAILED/Caused by`
  from the S3 logs. Use this after a fire-and-forget run that died.
- `./deploy_emr.sh ls-logs` → lists every file under
  `s3://osm2ohm-rub21/logs/<cluster_id>/`.
- **YARN UI** at `http://<MASTER_DNS>:8088` → running app, executors,
  memory, stage progress.
- **Spark History** at `http://<MASTER_DNS>:18080` → finished jobs with
  full stage / task / shuffle detail.

EMR pushes logs to S3 every ~5 minutes while running and one final
push when the cluster terminates. The layout is:

```
s3://osm2ohm-rub21/logs/<cluster_id>/
  steps/<step_id>/
    controller.gz    <- step lifecycle (exit code, why it failed)
    stderr.gz        <- Spark driver stderr (real errors live here)
    stdout.gz        <- driver stdout
    syslog.gz        <- Hadoop / YARN
  containers/<app_id>/<container_id>/
                     <- per-executor logs (only useful for deep debugging)
  node/<instance_id>/
                     <- system / bootstrap logs
```

### Manual spark-submit (optional)

If you want to drive Spark by hand for ad-hoc tests:

```bash
ssh -i terraform/emr-key.pem hadoop@<MASTER_DNS>
spark-submit \
  --deploy-mode client \
  s3://osm2ohm-rub21/scripts/extract_ohm_candidates.py \
  --history_uri   s3a://osm-pds/planet-history/history-latest.orc \
  --rules_uri     s3://osm2ohm-rub21/scripts/rules.json \
  --countries_uri s3://osm2ohm-rub21/countries \
  --output_uri    s3://osm2ohm-rub21/output/ohm_candidates/
```

### Debug UIs

Open these in a browser (URLs are printed by `deploy_emr.sh apply`):

- `http://<MASTER_DNS>:8088`  — YARN ResourceManager (running jobs, executors, memory).
- `http://<MASTER_DNS>:18080` — Spark History Server (finished jobs, stages, tasks, shuffle).
- `https://<MASTER_DNS>:9443` — JupyterHub (interactive notebooks).

## Recommended workflow

1. **Development (fast & cheap)**: keep the Bolivia `bbox` in
   `rules.json` and run the job. The subset finishes in a few minutes.
2. **Validation**: review 30–50 candidates in JOSM or iD using
   `(type, id, last_good_version)`. Tune the rules.
3. **Scale**: set `bbox = null` in `rules.json` and re-run the job over
   the full planet.
4. **Export to OHM**: convert the Parquet to GeoJSON with `start_date` ←
   `created_at` and `end_date` ← `last_edit_at` to feed JOSM/OHM.

## Technical notes

- `s3://osm-pds` is **public anonymous**. The cluster reads it without
  credentials thanks to the `AnonymousAWSCredentialsProvider` configured
  in `spark-defaults` from Terraform.
- Keep the cluster in **us-east-1**: that is where the dataset lives,
  so transfer is free and fast.
- The ORC carries `nds` (node refs) for ways but **no** resolved
  geometries. The first iteration works on nodes with `name` (POIs),
  where geometry reconstruction is not needed. Resolving ways /
  relations requires a `join` against the matching version of the
  nodes — left as a follow-up.
