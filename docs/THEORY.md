# Design Notes & Theory

Deep-dive on the design decisions behind the News Intelligence pipeline —
moved out of the README to keep that skimmable. See [../README.md](../README.md)
for the overview and how to run it.

## Medallion architecture rationale

- **Bronze** (S3, raw JSON): landing zone, one file per keyword per DAG run.
  Kept as raw JSON (not immediately parsed) so a downstream schema bug can
  be fixed and replayed against Snowflake without re-hitting NewsAPI.
- **Silver** (`news_silver`): flattened, deduplicated, one row per unique
  article. This is the layer application code and analysts should query.
- **Gold** (`news_gold_sentiment_metrics`, `news_gold_source_metrics`):
  pre-aggregated, ready for a dashboard with no further joins/aggregation.

## Idempotency design

Each article is assigned `id = "{keyword}_{md5(url)}"` (see
[`include/scrapper.py`](../news_sentiment_analysis/include/scrapper.py)).
NewsAPI doesn't provide a stable article ID, but URLs are effectively
unique per article, so hashing the URL gives a deterministic key that's
the same across DAG re-runs. The keyword prefix means the same article
surfaced under two different search terms gets two Silver rows (by
design — sentiment/authority metrics are computed per-keyword).

The Silver `MERGE ... WHEN NOT MATCHED THEN INSERT` (no `UPDATE` branch)
means Silver rows are insert-only: once an article is loaded, later
re-fetches of the same URL are no-ops there. Bronze→raw is append-only,
so `raw_news_data` can accumulate multiple copies of the same article
over time; the `QUALIFY ROW_NUMBER() ... = 1` in `silver_transform`
keeps only the most recently loaded copy per `id` before the MERGE.

## Incremental Gold loading

`gold_sentiment_incremental` processes `ds - 1` (yesterday relative to
the DAG's logical run date), using Airflow's `macros.ds_add(ds, -1)`
Jinja macro, and does `DELETE ... WHERE news_date = ...` followed by
`INSERT`, rather than `MERGE`. This keeps re-runs for the same date
idempotent (a re-run cleanly replaces that day's numbers instead of
double-counting) while being simpler to reason about than a MERGE for a
metrics table.

## Sentiment classification

`SNOWFLAKE.CORTEX.SENTIMENT(title)` scores each headline in `[-1, 1]`.
Scores within `±SENTIMENT_NEUTRAL_BAND` (`configs/config.yaml`, default
`0.15`) of zero are classified `'Neutral'`; outside that band, `'Positive'`
or `'Negative'`. This band is a heuristic default, not empirically tuned
against labeled data — if you adopt this pipeline, validate it against a
labeled sample of your own headlines before trusting the tone breakdown.

## Known limitations

- **No schema evolution strategy.** If NewsAPI adds/renames a field, the
  Bronze JSON changes shape silently; nothing here validates it against
  the `news_silver` column contract until the Silver `raw_json:field`
  extraction quietly returns `NULL`.
- **Gold assumes no missed DAG runs.** A skipped day leaves a permanent
  gap in `news_gold_sentiment_metrics` for that date unless someone
  manually backfills by re-running with the right logical date.
- **Connections are hardcoded** (`aws_default`, `snowflake_conn`) rather
  than configurable — fine for a single-environment deployment, a real
  constraint for multi-environment (dev/staging/prod) setups.
- **No live-database test coverage.** Tests validate the Python logic and
  the *shape* of the generated SQL (string assertions), not execution
  against a real Snowflake warehouse — there's no CI step that actually
  runs the DDL/DML in `snowflake_setup.sql` or the DAG's SQL tasks.
- **Bronze isn't deduplicated at write time** — a same-day re-run of the
  DAG can write a second S3 object for the same keyword (filename
  includes `HHMM`), relying entirely on the Silver layer's
  `QUALIFY ROW_NUMBER()` step to collapse duplicates later.

## References

- NewsAPI — https://newsapi.org/docs
- Snowflake Cortex `SENTIMENT` function — https://docs.snowflake.com/en/sql-reference/functions/sentiment-snowflake-cortex
- Apache Airflow — https://airflow.apache.org/docs/
- Astronomer / Astro CLI — https://www.astronomer.io/docs/astro/cli/overview
