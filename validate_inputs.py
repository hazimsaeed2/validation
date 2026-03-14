"""
Assignment Engine — Fast Input Validation (Hash-Based)
======================================================
Validates that S3 and Databricks inputs contain identical data for
ONLY the columns actually used by the assignment pipeline.

Strategy for billion-row datasets:
  1. Select only the columns the pipeline actually uses
  2. Compute a row-level SHA256 hash across all used columns, then aggregate
     with SUM(conv(substring(hash,1,15),16,10)) — order-independent fingerprint
  3. If hashes differ, drill down per-column (count, nulls, distinct) to
     pinpoint which column diverges
  4. All comparisons are aggregate-only — NO row-level shuffles or collects

Usage:
  1. Copy into a Databricks notebook
  2. Run config/utils and config/variables cells first
  3. Update CONFIG section below
  4. Run — takes ~5-10 min even on billions of rows
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# =============================================================================
# CONFIG — UPDATE THESE FOR YOUR CAMPAIGN
# =============================================================================

# --- S3 Paths (from pe_memberdna_base config_template.yml) ---
S3_PRED_LIST      = "s3://memberanalytics-data-out-prod/MODELDATA/PREDICTIONS/CF_MODEL/prod_2026_02_22/20250221-20260221-combined/preds-20260224-combined/PARQUET/"
S3_PROPENSITY     = "s3://memberanalytics-data-out-prod/MODELDATA/PREDICTIONS/TRIP_SPEND_MODELS/prod_2026_02_22/trip_spend_predictions_2026-02-21.csv"
S3_ARTICLE_MAP    = "s3://memberanalytics-data-out-prod/pipelined_intermediates/master/item"
S3_RAW_MEMBER     = "s3://memberanalytics-data-out-prod/pipelined_intermediates/master/member_extended"
S3_CUBE           = "s3://memberanalytics-data-out-prod/CUBES/customer_cube_2026-03-10/"

# --- Databricks Table Names ---
CATALOG = "datascience_ea_dev"
SILVER_SCHEMA = "pe_slv"
PE_SCHEMA = "pe"

DB_CF_PREDICTION = f"{CATALOG}.{PE_SCHEMA}.cf_prediction"
DB_TRIP_SPEND    = f"{CATALOG}.{PE_SCHEMA}.trip_spend_prediction"
DB_ARTICLE_MAP   = f"{CATALOG}.{SILVER_SCHEMA}.master_item"
DB_RAW_MEMBER    = f"{CATALOG}.{SILVER_SCHEMA}.master_member_extended"
DB_CUBE          = f"{CATALOG}.{PE_SCHEMA}.fs_customer_cube_full"

# --- Assignment Config ---
ASSIGNMENT_DATE = "2026-03-12"

# --- Lambda values used in CF predictions (from construct JSONs) ---
CF_LAMBDAS = [15, 30]  # produces hs_ind_lambda15, hs_ind_lambda30


# =============================================================================
# COLUMNS ACTUALLY USED BY THE ASSIGNMENT PIPELINE
# =============================================================================

# RAW_MEMBER — only 4 columns used in core assignment
RAW_MEMBER_COLS = [
    "MBRSHP_SID",
    "MBRSHP_NBR",
    "MKT_CD",
    "MBRSHP_FEE_INC",
]

# CUBE — core assignment columns
# Set CUBE_ALL_COLUMNS = True below to validate every column (slower)
CUBE_CORE_COLS = [
    "MBRSHP_SID",
    "DAYS_SINCE_LAST_TRIP",
    "TENURE",
    "LAST_TWENTY-SIX_WEEK_SPEND",       # sampling_weeks_rev_columns
    "LFIFTY-TWOW_SPEND_IN_STORE",
    "LTWELVEW_SPEND_IN_STORE",
    "LFOURW_SPEND_IN_STORE",
    "LAST_FIFTY-TWO_WEEK_TRIPS",
    "EXP_DT",
    "MBRSHP_EXP_DT",
    "MBRSHP_RNWL_DT",
]
CUBE_ALL_COLUMNS = False

# CF PREDICTIONS — columns used after max(RUN_NAME) filter
CF_PRED_COLS = [
    "MBRSHP_SID",
    "CATEGORY_ID",
    "prediction",
    "prediction_v2",
    "CATEGORY_LVL",
    "CATEGORY_NAME",
    "RUN_NAME",
] + [f"hs_ind_lambda{x}" for x in CF_LAMBDAS]

# PROPENSITY — columns used after fiscal_week filter
PROPENSITY_COLS = [
    "MBRSHP_SID",
    "prediction",
    "avg_basket",
]

# ARTICLE MAP — columns used
ARTICLE_MAP_COLS = [
    "ARTICLE_NBR",
    "AH4_CD",
    "AH5_CD",
    "AH4_DESC",
    "AH5_DESC",
    "MCH3_DESC",
    "ARTICLE_DESC",
]


# =============================================================================
# FAST COMPARISON ENGINE
# =============================================================================

results = []


def log_result(check_name, status, details=""):
    emoji = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "INFO": "ℹ️"}[status]
    results.append({"check": check_name, "status": status, "details": details})
    print(f"  {emoji} [{status}] {check_name}")
    if details:
        print(f"     → {details}")


def _safe_select(df, cols):
    """Select only columns that exist in the DataFrame."""
    available = set(df.columns)
    found = [c for c in cols if c in available]
    missing = [c for c in cols if c not in available]
    return found, missing


def row_hash_fingerprint(df, cols):
    """
    Order-independent fingerprint of an entire DataFrame in ONE pass.

    Per row: SHA256(concat_ws("||", cast(col1 as string), ...))
    Aggregate: SUM(conv(first 15 hex chars → decimal))  +  COUNT(*)

    Same SUM + same COUNT ⟹ identical data (collision probability ≈ 0).
    """
    hash_expr = F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("__NULL__")) for c in cols]
        ),
        256,
    )
    row = df.select(
        F.count("*").alias("cnt"),
        F.sum(F.conv(F.substring(hash_expr, 1, 15), 16, 10)).alias("hsum"),
    ).collect()[0]
    return row["cnt"], row["hsum"]


def column_fingerprint(df, cols):
    """
    Per-column aggregates in ONE pass: count(non-null), count(null), countDistinct.
    """
    agg_exprs = []
    for c in cols:
        safe = f"`{c}`"
        agg_exprs.append(F.count(F.col(safe)).alias(f"{c}__cnt"))
        agg_exprs.append(
            F.sum(F.when(F.col(safe).isNull(), 1).otherwise(0)).alias(f"{c}__nul")
        )
        agg_exprs.append(F.countDistinct(F.col(safe)).alias(f"{c}__dst"))

    row = df.select([F.col(f"`{c}`") for c in cols]).agg(*agg_exprs).collect()[0]
    return {
        c: {
            "count": row[f"{c}__cnt"],
            "nulls": row[f"{c}__nul"],
            "distinct": row[f"{c}__dst"],
        }
        for c in cols
    }


def compare_inputs(name, s3_df, db_df, cols, key_col=None):
    """
    Full comparison of two DataFrames on specific columns.
    Fast: only aggregates, zero row-level shuffles or collects.
    """
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"{'='*70}")

    # 1. Select only needed columns (intersect with what exists)
    s3_found, s3_missing = _safe_select(s3_df, cols)
    db_found, db_missing = _safe_select(db_df, cols)

    if s3_missing:
        log_result(f"{name} — S3 Schema", "WARN", f"Missing columns: {s3_missing}")
    if db_missing:
        log_result(f"{name} — DB Schema", "WARN", f"Missing columns: {db_missing}")

    common_cols = sorted(set(s3_found) & set(db_found))
    if not common_cols:
        log_result(f"{name}", "FAIL", "No common columns found!")
        return

    s3_sel = s3_df.select([F.col(f"`{c}`") for c in common_cols])
    db_sel = db_df.select([F.col(f"`{c}`") for c in common_cols])

    print(f"  Validating {len(common_cols)} columns: {common_cols}\n")

    # 2. Row-level hash fingerprint — one pass per source
    print("  Computing hash fingerprints...")
    s3_cnt, s3_hash = row_hash_fingerprint(s3_sel, common_cols)
    db_cnt, db_hash = row_hash_fingerprint(db_sel, common_cols)

    print(f"  S3: {s3_cnt:>15,} rows  |  hash_sum = {s3_hash}")
    print(f"  DB: {db_cnt:>15,} rows  |  hash_sum = {db_hash}\n")

    # Count check
    if s3_cnt == db_cnt:
        log_result(f"{name} — Row Count", "PASS", f"Both: {s3_cnt:,}")
    else:
        log_result(f"{name} — Row Count", "FAIL",
                   f"S3={s3_cnt:,}  DB={db_cnt:,}  diff={abs(s3_cnt - db_cnt):,}")

    # Hash check
    if s3_hash == db_hash and s3_cnt == db_cnt:
        log_result(f"{name} — Data Hash", "PASS", "Identical (hash match)")
        return  # perfect match — done

    log_result(f"{name} — Data Hash", "FAIL", "Mismatch — drilling into columns...")

    # 3. Per-column drill-down (one more pass per source)
    print("\n  Per-column fingerprints:")
    s3_fp = column_fingerprint(s3_sel, common_cols)
    db_fp = column_fingerprint(db_sel, common_cols)

    for c in common_cols:
        s = s3_fp[c]
        d = db_fp[c]
        if s == d:
            print(f"    ✅ {c}: cnt={s['count']:,}  nulls={s['nulls']:,}  dist={s['distinct']:,}")
        else:
            print(f"    ❌ {c}:")
            print(f"       S3: cnt={s['count']:,}  nulls={s['nulls']:,}  dist={s['distinct']:,}")
            print(f"       DB: cnt={d['count']:,}  nulls={d['nulls']:,}  dist={d['distinct']:,}")
            log_result(f"{name} — [{c}]", "FAIL",
                       f"S3(cnt={s['count']},null={s['nulls']},dist={s['distinct']}) ≠ "
                       f"DB(cnt={d['count']},null={d['nulls']},dist={d['distinct']})")

    # 4. If key_col exists, grab a small sample of differing keys
    if key_col and key_col in common_cols:
        try:
            s3_only = s3_sel.subtract(db_sel).select(f"`{key_col}`").distinct().limit(5)
            sample = [str(r[0]) for r in s3_only.collect()]
            if sample:
                log_result(f"{name} — Sample S3-only keys", "INFO",
                           f"{key_col} values in S3 but not DB: {sample}")
        except Exception:
            pass  # subtract can be expensive; skip if it fails


# =============================================================================
# CHECK 1: RAW_MEMBER
# =============================================================================
try:
    s3_mbr = spark.read.parquet(S3_RAW_MEMBER)
    db_mbr = spark.table(DB_RAW_MEMBER)
    compare_inputs("RAW_MEMBER (member_extended)", s3_mbr, db_mbr,
                   RAW_MEMBER_COLS, key_col="MBRSHP_SID")
except Exception as e:
    log_result("RAW_MEMBER", "FAIL", f"Error: {e}")


# =============================================================================
# CHECK 2: CUBE
# =============================================================================
try:
    s3_cube = spark.read.parquet(S3_CUBE)
    db_cube = spark.table(DB_CUBE)

    # Filter DB by fiscal_week (same as pipeline)
    if "FISCAL_WEEK_END" in db_cube.columns:
        closest = (
            db_cube
            .withColumn("_d", F.abs(F.datediff(F.col("FISCAL_WEEK_END"), F.lit(ASSIGNMENT_DATE))))
            .orderBy("_d")
            .select("FISCAL_WEEK_END")
            .first()
        )
        if closest:
            fw = closest["FISCAL_WEEK_END"]
            print(f"  DB CUBE filtered to FISCAL_WEEK_END = {fw}")
            db_cube = db_cube.filter(F.col("FISCAL_WEEK_END") == fw)

    cube_cols = CUBE_CORE_COLS
    if CUBE_ALL_COLUMNS:
        cube_cols = sorted(set(s3_cube.columns) & set(db_cube.columns))

    compare_inputs("CUBE (customer_cube)", s3_cube, db_cube,
                   cube_cols, key_col="MBRSHP_SID")
except Exception as e:
    log_result("CUBE", "FAIL", f"Error: {e}")


# =============================================================================
# CHECK 3: CF PREDICTIONS
# =============================================================================
try:
    s3_cf = spark.read.parquet(S3_PRED_LIST)
    db_cf = spark.table(DB_CF_PREDICTION)

    # Pipeline filters to max(RUN_NAME)
    max_run = db_cf.select(F.max("RUN_NAME").alias("r")).collect()[0]["r"]
    print(f"  DB CF filtered to max(RUN_NAME) = {max_run}")
    db_cf = db_cf.filter(F.col("RUN_NAME") == max_run)

    compare_inputs("CF Predictions", s3_cf, db_cf,
                   CF_PRED_COLS, key_col="MBRSHP_SID")
except Exception as e:
    log_result("CF Predictions", "FAIL", f"Error: {e}")


# =============================================================================
# CHECK 4: PROPENSITY
# =============================================================================
try:
    s3_prop = spark.read.csv(S3_PROPENSITY, header=True, inferSchema=True)
    db_prop = spark.table(DB_TRIP_SPEND)

    # Pipeline filters by fiscal_week
    if "FISCAL_WEEK_END" in db_prop.columns:
        closest = (
            db_prop
            .withColumn("_d", F.abs(F.datediff(F.col("FISCAL_WEEK_END"), F.lit(ASSIGNMENT_DATE))))
            .orderBy("_d")
            .select("FISCAL_WEEK_END")
            .first()
        )
        if closest:
            fw = closest["FISCAL_WEEK_END"]
            print(f"  DB Propensity filtered to FISCAL_WEEK_END = {fw}")
            db_prop = db_prop.filter(F.col("FISCAL_WEEK_END") == fw)

    compare_inputs("Propensity (trip_spend)", s3_prop, db_prop,
                   PROPENSITY_COLS, key_col="MBRSHP_SID")
except Exception as e:
    log_result("Propensity", "FAIL", f"Error: {e}")


# =============================================================================
# CHECK 5: ARTICLE MAP
# =============================================================================
try:
    s3_art = spark.read.parquet(S3_ARTICLE_MAP)
    db_art = spark.table(DB_ARTICLE_MAP)

    compare_inputs("Article Map (master_item)", s3_art, db_art,
                   ARTICLE_MAP_COLS, key_col="ARTICLE_NBR")

    # Extra: type safety check for cast('long')
    art_type = dict(db_art.dtypes).get("ARTICLE_NBR", "unknown")
    if art_type in ("bigint", "long", "int", "integer"):
        log_result("Article Map — ARTICLE_NBR type", "PASS", f"Type={art_type}")
    elif art_type == "string":
        bad = db_art.filter(~F.col("ARTICLE_NBR").rlike("^[0-9]+$")).count()
        if bad == 0:
            log_result("Article Map — ARTICLE_NBR type", "PASS",
                       "String but all numeric — cast('long') safe")
        else:
            log_result("Article Map — ARTICLE_NBR type", "FAIL",
                       f"{bad:,} non-numeric values will become NULL after cast!")
except Exception as e:
    log_result("Article Map", "FAIL", f"Error: {e}")


# =============================================================================
# SUMMARY
# =============================================================================
print(f"\n\n{'='*70}")
print(f"  VALIDATION SUMMARY")
print(f"{'='*70}\n")

pass_n = sum(1 for r in results if r["status"] == "PASS")
fail_n = sum(1 for r in results if r["status"] == "FAIL")
warn_n = sum(1 for r in results if r["status"] == "WARN")

for r in results:
    emoji = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "INFO": "ℹ️"}.get(r["status"], "")
    line = f"  {emoji} {r['check']}: {r['status']}"
    if r["details"] and r["status"] != "PASS":
        line += f"  —  {r['details']}"
    print(line)

print(f"\n  Totals: {pass_n} PASS  |  {warn_n} WARN  |  {fail_n} FAIL\n")

if fail_n > 0:
    print("  ❌ VALIDATION FAILED — fix inputs before running assignment")
elif warn_n > 0:
    print("  ⚠️  PASSED WITH WARNINGS — review before running")
else:
    print("  ✅ ALL CHECKS PASSED — inputs match, safe to run assignment")
