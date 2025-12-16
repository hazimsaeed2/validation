"""Function bank for QC.

TODO:
    - add support for matching CF prediction ranks for each example table
"""
import datetime
import itertools

import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from lib_assignment.assn_utils import (
    calc_overlapping_cols,
    cap_top_percentile,
    deterministic_sample,
)
from lib_assignment.campaign import Campaign
from lib_assignment.ingest import join_category_agnostic
from lib.spark_util import (
    count_nulls,
    crosstab_pct,
    grouped_percentiles,
    pct_flagged,
    safe_join,
    union_with_mismatched_columns,
)

spark = SparkSession.builder.getOrCreate()

RENEWAL_STATS = [
    "LATEST_MBRSHP_FEE_INC",
    "LATEST_MBRSHP_ENR_DT",
    "LATEST_MBRSHP_EXP_DT",
    "LATEST_MBRSHP_RNWL_DT",
    "TENURE",
]

DESCRIPTIVE_CDS = [
    "LATEST_RWDS_MBR_IND",
    "LATEST_MBRSHP_TYPE_ID",
    "LATEST_MBRSHP_SUB_TYPE",
    "cpn_channel",
]

NOMAIL_STATS = [
    "LTWELVEW_SPEND_IN_STORE",
    "LFIFTY_TWOW_SPEND_IN_STORE",
    "LAST_FIFTY_TWO_WEEK_TRIPS",
    "DAYS_SINCE_LAST_TRIP",
    "BBM_DECILE",
    "BBM_PROP_SCORE",
]

NOMAIL_CDS = ["cpn_channel"]

PERSONALIZED_CONTENT_CDS = ["LATEST_AUTO_RNWL_IND"]

PERSONALIZED_CONTENT_STATS = [
    "L26W_ATC_CPN_RED",
    "L_FIFTY-TWOW_GAS_TRIPS",
    "L_TWELVEW_GAS_TRIPS",
    "LFIFTY-TWOW_SPEND_IN_STORE",
    "LTWELVEW_SPEND_IN_STORE",
]

SAVINGS_OVERVIEW = [
    "LATEST_MBRSHP_FEE_INC",
    "LATEST_MFI_TIER",
    "LFIFTY-TWOW_SAVINGS_W_CLPLSS",
    "SAVINGS_content",
    "LAST_FIFTY-TWO_WEEK_TRIPS",
    "TRIPS_content",
    "L_FIFTY-TWOW_GAS_TRIPS",
    "GAS_TRIPS_content",
    "TENURE",
    "TENURE_content",
]

SAVINGS_KEYED = [
    "LATEST_MBRSHP_FEE_INC",
    "TENURE_content",
    "SAVINGS_content",
    "TRIPS_content",
    "GAS_TRIPS_content",
]

SAVINGS_DUMP = [
    "LATEST_MBRSHP_NBR",
    "MBRSHP_SID",
    "LATEST_MFI_TIER",
    "L_FIFTY-TWOW_FIRST_MOST_SHOPPED_CATEGORY",
    "L_FIFTY-TWOW_SECOND_MOST_SHOPPED_CATEGORY",
    "LATEST_MBRSHP_EXP_DT",
]

# ============================================================================#
# General Helpers
# ============================================================================#


def group_count_mbrs(df, group_cols=None):
    """Group dataframe by arbitrary columns and count distinct members.

    Parameters:
        df (pyspark.sql.DataFrame): dataframe to operate on. Must containg group columns
        group_cols (str, [str] ; opt): columns to group by
    Returns:
        grouped (pyspark.sql.DataFrame): grouped and counted dataframe
    """
    if group_cols:
        if isinstance(group_cols, str):
            grouped = df.groupBy(group_cols)
        else:
            grouped = df.groupBy(*group_cols)
    else:
        grouped = df.groupBy()

    grouped = grouped.agg(
        sqlf.countDistinct("MBRSHP_SID").alias("DISTINCT_MBRS")
    )
    grouped = grouped.orderBy(sqlf.desc("DISTINCT_MBRS"))

    return grouped


def _version_from_cpn(job, df):
    """
    For coupon numbers that are really versions, copy value to VERSION column.
    """
    versions = job.data.tables["version_map"].select("VERSION")
    versions = [row.VERSION for row in versions.collect()]
    if "cpn_nbr" in df.columns:
        df = df.withColumnRenamed("cpn_nbr", "CPN_NBR")
    df = df.withColumn(
        "VERSION",
        sqlf.when(df.CPN_NBR.isin(versions), df.CPN_NBR).otherwise(df.VERSION),
    )
    return df


def add_mbr_count(df_grouped, df_raw, key=None):
    """
    Add new column, DISTINCT_MBRS, to df_grouped, as calculated via grouping
    df_raw on key.

    Parameters:
        df_grouped (pyspark.sql.DataFrame): dataframe to append to
        df_raw (pyspark.sql.DataFrame): datefrom to group and get count from
        key (str;opt): key to group on

    Returns:
        (pyspark.sql.DataFrame): df with additional column (DISTINCT_MBRS)
    """
    if key:
        subselect = df_raw.select(key, "MBRSHP_SID")
    else:
        subselect = df_raw.select("MBRSHP_SID")
    members = group_count_mbrs(subselect, key)
    if key:
        df_grouped = safe_join(df_grouped, members, "left", key)
    else:
        mbrs = members.select("DISTINCT_MBRS").collect()[0][0]
        df_grouped = df_grouped.withColumn("DISTINCT_MBRS", sqlf.lit(mbrs))
    return df_grouped


def add_key_detail(job, df, cols, key):
    """
    Add key details to df and update the columns to reflect the added detail
    column(s). For example, if key = "CELL_ID", add detail of "CELL_DESC".

    Parameters:
        job (JobManager): Job to operate on.
        df (pyspark.sql.DataFrame): dataframe to copy and add details to
        cols ([str]): column list to append with new detail columns
        key (str): key to potentially retrieve more details around
    Generates:
        Appends cols
    Returns:
        (pyspark.sql.DataFrame): df with additional columns
    """
    if key.lower() == "cell_id":
        cols.append("CELL_DESC")
        cell = job.data.tables["cell"].select(
            "cell_id", "cell_desc", "experiment_id"
        )
        cell = cell.filter(
            cell.experiment_id == job.config.params["experiment"]
        )
        cell = cell.drop("experiment_id")
        cell = cell.toDF(*[c.upper() for c in cell.columns])
        df = df.join(cell, how="left", on="CELL_ID")
    elif key.lower() == "cpn_nbr":
        cols.append("CPN_DESC")
        desc = job.data.tables["coups"].select("cpn_nbr", "cpn_desc")
        desc = desc.toDF(*[c.upper() for c in desc.columns])
        df = df.join(desc, how="left", on="CPN_NBR")

    return df, cols


def filter_slots(job, df, tab):
    """
    Filter a custom table for the slots of interest.

    Parameters:
        job (JobManager): Job to operate on.
        df (pyspark.sql.DataFrame): dataset with slots
        tab (str): name of custom tab
    Returns:
        dat (pyspark.sql.DataFrame): filtered dataframe
    """
    filtered_slots = job.config.params["pre_configured"][tab].get("slot_nbr")
    if filtered_slots:
        df = df.where(df.slot_nbr.isin(filtered_slots))
    return df


def summary_stats(job, df, cols, key=None):
    """
    Generate summary stats on cols by the keyed value. If key is not provided,
    aggregates are performed and a dummy key column of "_" is added with value
    "All".

    The summary stats generated are:
        - nulls
        - 1st percentile
        - mean
        - 99th percentile
        - DISTINCT_MBRS

    Parameters:
        job (JobManager): Job to operate on.
        df (pyspark.sql.DataFrame): dataset with slots
        cols ([str]): columns to compute summary stats for
    Returns:
        df (pyspark.sql.DataFrame): filtered dataframe
    """
    # allow user config to be case insensitive
    dat = df.toDF(*[c.upper() for c in df.columns])

    if not key:
        key = "_"
        dat = dat.withColumn(key, sqlf.lit("All"))
        key = key.upper()

    # join on null key value doesn't work, so convert null to non-empty string
    dat = dat.withColumn(
        key,
        sqlf.when(sqlf.col(key).isNull(), sqlf.lit("NULL")).otherwise(
            sqlf.col(key)
        ),
    )

    percentiles = [0.01, 0.99]
    stats = None
    for col in cols:
        is_date = isinstance(dat.schema[col.upper()].dataType, sqlt.DateType)
        if is_date:  # inputs to summary stats need to be numeric, not date
            d = dat.withColumn(col, sqlf.unix_timestamp(col))
        else:
            d = dat

        nulls = count_nulls(d, col, key).withColumnRenamed(
            "nulls", "{}_nulls".format(col)
        )

        if stats:
            stats = safe_join(stats, nulls, "left", key)
        else:
            stats = nulls

        grouped = d.groupBy(key)
        nms = []

        mean_alias = "{}_mean".format(col)
        nms.append(mean_alias)
        mean = grouped.agg(sqlf.mean(col).alias(mean_alias))

        stats = safe_join(stats, mean, "left", key)

        pcts = grouped_percentiles(d, key, col, percentiles)
        for pct_col in pcts.columns:
            if pct_col != key:
                nm = "{}_{}".format(col, pct_col)
                pcts = pcts.withColumnRenamed(pct_col, nm)
                nms.append(nm)

        stats = safe_join(stats, pcts, "left", key)

        if is_date:
            for nm in nms:
                stats = stats.withColumn(
                    nm, sqlf.from_unixtime(nm, "yyyy-MM-dd")
                )

    stat_order = ("nulls", "pct_1", "mean", "pct_99")
    out_order = [key]
    if key:
        stats, out_order = add_key_detail(job, stats, out_order, key)

    stats = add_mbr_count(stats, d, key)
    out_order.append("DISTINCT_MBRS")

    for col in cols[1:]:
        for stat in stat_order:
            out_order.append("{}_{}".format(col, stat))
    stats = stats.select(*out_order)
    if key:
        stats = stats.orderBy(key)

    return stats


def extend_table(job, df, cols):
    """
    Return an extended version of dat with any missing cols, as joined from DNA

    Parameters:
        job (JobManager): Job to operate on.
        df (pyspark.sql.DataFrame): dataset to extend
        cols ([str]): columns to append to df if in DNA
    Returns:
        df (pyspark.sql.DataFrame): df with additional cols
    """
    dna = job.data.tables["dna"]

    existing_cols = set(df.columns)
    desired_cols = set(cols)
    dna_cols = set(dna.columns)
    missing = (desired_cols - existing_cols).intersection(dna_cols)
    missing = list(missing)
    missing.append("MBRSHP_SID")

    dna = dna.select(missing)
    df = df.join(dna, "MBRSHP_SID", "left")
    return df


# ============================================================================#
# Tab Generators
# ============================================================================#


def generate_group_count_dataset(job, name, cols, sort_cols=None):
    """Generates a dataset based on counting MEMBERS by <Cols> in base dataset.

    Parameters:
        job (JobManager): Job to operate on. Requres tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
                Schema can be found in docs for qc_assignments.generate_full_basedata
        name (str): name of dataset to generate
        cols (list[str]): name of columns to aggregate BY
        sort_cols (list[str]): Optional outgoing sort order

    Returns:
        new dataset with <NAME>:
            <COLS>
            MEMBERS (float) -- member count by cols
    """
    basedata = job.data.tables["base"]
    by_cols = group_count_mbrs(basedata, cols)

    if sort_cols:
        by_cols = by_cols.orderBy(sort_cols)

    job.data.add(name, by_cols)
    job.data.tables[name].persist(StorageLevel.DISK_ONLY)


def generate_coupon_dataset(job):
    """
    Generate coupon dataset for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requres tables:
            basedata (pyspark.sql.DataFrame): "base" dataset

    Returns:
        coupon (pyspark.sql.DataFrame): "coupon" dataset
            CPN_NBR (str)
            CPN_TYPE (str)
            CPN_DESC (str)
            IMPRESSIONS (int)
    """
    #job.log.info(
    print("Generating coupon impressions")

    mailed_pop = (
        job.data.tables["base"]
        .filter("mail_flag == 1")
        .union(job.data.tables["coupon_dummy"])
    )

    cpn_articles = job.data.tables["cpn_articles"]

    # subtract duplicate from coupon_dummy if it's there
    adjustment = sqlf.lit(1)
    if job.data.tables["coupon_dummy"].count() == 0:
        adjustment = sqlf.lit(0)

    coupon = (
        mailed_pop.groupBy("CPN_NBR")
        .count()
        .withColumnRenamed("count", "IMPRESSIONS")
        .withColumn("IMPRESSIONS", sqlf.col("IMPRESSIONS") - adjustment)
        .join(cpn_articles, "cpn_nbr", "left")
        .orderBy(sqlf.desc("IMPRESSIONS"))
    )

    input_coupons = []
    cpg_coupon = job.data.tables.get("cpg_coupon", None)
    if cpg_coupon:
        mapping = dict(
            [
                ("Promo #", "article Promo #"),
                ("Promo Description", "article Promo Description"),
                ("PMR Offer ID", "cpn_nbr"),
                ("Promo Type", "article Promo Type"),
                ("Valid From", "cpn_start"),
                ("Valid To", "cpn_end"),
                ("Quantity", "article quantity"),
                ("Discount Value", "article Discount Value"),
                ("Eligible for MMPC", "article Eligible for MMPC"),
            ]
        )
        cpg_coupon = cpg_coupon.select(
            [sqlf.col(c).alias(mapping.get(c, c)) for c in cpg_coupon.columns]
        )
        input_coupons.append(cpg_coupon)

    cat_coupon = job.data.tables.get("category_coupon", None)
    if cat_coupon:
        mapping = dict(
            [
                ("cpn_ah4_cd", "category_cpn_ah4_cd"),
                ("cpn_ah5_cd", "category_cpn_ah5_cd"),
            ]
        )
        cat_coupon = cat_coupon.select(
            [sqlf.col(c).alias(mapping.get(c, c)) for c in cat_coupon.columns]
        )
        input_coupons.append(cat_coupon)

    bkt_coupon = job.data.tables.get("basket_coupon", None)
    if bkt_coupon:
        bkt_coupon = job.data.tables["basket_coupon"]
        input_coupons.append(bkt_coupon)

    if input_coupons:
        l = len(input_coupons)
        if l == 1:
            cbc = input_coupons[0]
        elif l == 2:
            cbc = union_with_mismatched_columns(
                input_coupons[0], input_coupons[1]
            )
        else:
            cb = union_with_mismatched_columns(
                input_coupons[0], input_coupons[1]
            )
            cbc = union_with_mismatched_columns(cb, input_coupons[2])
        cbc = cbc.withColumnRenamed("cpn_nbr", "CPN_NBR")

        coupon = cbc.join(coupon, "CPN_NBR", "left").orderBy(
            ["IMPRESSIONS"], ascending=[0]
        )

    coupon = coupon.drop("cpn_desc")
    coupon_bank = (
        job.data.tables["coupon_bank"]
        .select(["cpn_nbr", "cpn_desc"])
        .withColumnRenamed("cpn_nbr", "CPN_NBR")
    )
    coupon = coupon.join(coupon_bank, "CPN_NBR", "left")

    leading_columns = [
        "CPN_NBR",
        "cpn_desc",
        "IMPRESSIONS",
        "NUM_ARTICLES",
        "SAMPLE_ARTICLE",
    ]
    coupon = coupon.select(
        leading_columns
        + [i for i in coupon.columns if i not in leading_columns]
    )

    join_keys = [
        "mbrshp_sid",
        "experiment_id",
        "cell_id",
        "construct",
        "cpn_nbr",
    ]
    constructs = job.data.tables["constructs"]
    assgn_full = job.data.tables["input_assignment"]
    assgn_full = assgn_full.join(constructs, on=join_keys, how="left").select(
        ["cpn_nbr", "is_backfill"]
    )

    assgn_full = (
        assgn_full.withColumn(
            "is_backfill", assgn_full.is_backfill.cast("int")
        )
        .groupBy("cpn_nbr")
        .agg(
            (sqlf.sum("is_backfill") / sqlf.count("is_backfill")).alias(
                "%Backfill"
            )
        )
        .withColumnRenamed("cpn_nbr", "CPN_NBR")
    )
    coupon = coupon.join(assgn_full, "CPN_NBR", "left")

    mailed_pop = (
        mailed_pop.select(["CPN_NBR", "slot_nbr"])
        .filter("MBRSHP_SID != -1")
        .groupBy("CPN_NBR")
        .agg(sqlf.mean("slot_nbr").alias("average slot number"))
    )
    coupon = coupon.join(mailed_pop, "CPN_NBR", "left").orderBy(
        ["IMPRESSIONS"], ascending=[0]
    )

    job.data.add("coupon", coupon)
    job.data.tables["coupon"].persist(StorageLevel.DISK_ONLY)


def generate_compare_dataset(job, key):
    """Generate cell dataset/compare_<key> tabs for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requres tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
                Schema can be found in docs for qc_assignments.generate_full_basedata
            cell_dtl (pyspark.sql.DataFrame): cell detail CDSA table
                cell_id (int)
                cell_desc (str)
                ctrl_flag (int)
                test_flag (int)
        key (str/[str]): column to group by. Currently supports only cols in basedata
    Generates:
        (pyspark.sql.DataFrame): job.tables["compare_<key>"]
            <key> (str)
            <key>_DESC (str if key contains "cell_id" or "cpn_nbr")
            DISTINCT_MBRS (int)
            IMPRESSIONS (int)
            IMPR_per_MEMB (float)
            NO_MAIL_MEMBERS (int)
            AVG_TRIPS (float)
            BASKET (float)
            AVG_SPEND_4 (float)
            AVG_SPEND_12 (float)
            AVG_SPEND_52 (float)
            STDEV_SPEND_4 (float)
            STDEV_SPEND_12 (float)
    """
    #job.log.info(
    print("Generating cell summary by {}".format(key))
    dupe_keys = ["MBRSHP_SID"]
    if isinstance(key, str):
        key = key.upper()
        selectors = [key]
    elif isinstance(key, list):
        key = list(map(str.upper, key))
        selectors = key

    basedata = job.data.tables["base"]
    if key == "CPN_NBR":
        basedata = filter_slots(job, basedata, "compare")
    cell_dtl = job.data.tables["cell"]

    # allow user config to be case insensitive
    basedata = basedata.toDF(*[c.upper() for c in basedata.columns])
    
    # join on null key value doesn't work, so convert null to non-empty string
    for selector in selectors:
    
        basedata = basedata.withColumn(
            selector,
            sqlf.when(sqlf.col(selector).isNull(), sqlf.lit("-1")).otherwise(
                sqlf.col(selector)
            ),
        )

    mail_raw = basedata.filter(
        (sqlf.col("mail_flag")  == 1)
        | (sqlf.col("cell_id").isin(job.config.params["force_out_cells"])) 
    )

    imps = mail_raw.groupBy(key).agg(
        sqlf.count("MBRSHP_SID").alias("IMPRESSIONS")
    )

    dupe_keys.extend(selectors)
    basedata = basedata.dropDuplicates(dupe_keys)
    mail_raw = mail_raw.dropDuplicates(dupe_keys)

    dat = mail_raw.groupBy(key).agg(
        sqlf.countDistinct("MBRSHP_SID").alias("MEMBERS"),
        sqlf.mean("LAST_FIFTY-TWO_WEEK_TRIPS").alias("AVG_TRIPS"),
        sqlf.mean("LFIFTY-TWOW_SPEND_IN_STORE").alias("AVG_SPEND_52"),
        sqlf.mean("LFOURW_SPEND_IN_STORE").alias("AVG_SPEND_4"),
        sqlf.mean("LTWELVEW_SPEND_IN_STORE").alias("AVG_SPEND_12"),
        sqlf.stddev("LFIFTY-TWOW_SPEND_IN_STORE").alias("STDEV_SPEND_52"),
        sqlf.stddev("LFOURW_SPEND_IN_STORE").alias("STDEV_SPEND_4"),
        sqlf.stddev("LTWELVEW_SPEND_IN_STORE").alias("STDEV_SPEND_12"),
        sqlf.countDistinct("CPN_NBR").alias("DISTNCT_CPN"),
    )
    dat = dat.join(imps, key, "inner")

    no_mail = (
        basedata.filter("mail_flag == 0")
        .groupBy(key)
        .agg(sqlf.countDistinct("MBRSHP_SID").alias("NO_MAIL_MEMBERS"))
    )

    dat = (
        dat.withColumn("IMPR_per_MEMB", dat.IMPRESSIONS / dat.MEMBERS)
        .join(no_mail, key, "full_outer")
        .na.fill(0)
    )
    if "CELL_ID" in key:
        cell_dtl = cell_dtl.select(
            "cell_id", "ctrl_flag", "test_flag", "estimated_size"
        ).where(sqlf.col("experiment_id") == job.config.params["experiment"])
        cell_dtl = cell_dtl.withColumnRenamed("cell_id", "CELL_ID")
        cell_dtl = cell_dtl.withColumnRenamed(
            "estimated_size", "ESTIMATED_TOTAL"
        )

        dat = dat.join(cell_dtl, "CELL_ID", "left")

    dat = dat.withColumn("BASKET", dat.AVG_SPEND_52 / dat.AVG_TRIPS)
    full_dat_cols = [
        "MEMBERS",
        "IMPRESSIONS",
        "IMPR_per_MEMB",
        "NO_MAIL_MEMBERS",
        "AVG_TRIPS",
        "BASKET",
        "AVG_SPEND_4",
        "STDEV_SPEND_4",
        "AVG_SPEND_12",
        "STDEV_SPEND_12",
        "AVG_SPEND_52",
        "STDEV_SPEND_52",
    ]

    if "CELL_ID" in key:
        full_dat_cols.insert(1, "ESTIMATED_TOTAL")

    for selector in selectors:
        dat, key_cols = add_key_detail(job, dat, [selector], selector)
        key_cols.extend(full_dat_cols)
        full_dat_cols = key_cols

    dat = dat.select(*full_dat_cols).orderBy(key)
    if isinstance(key, list):
        nm = "compare_{}".format("_".join(map(str.lower, key)))
    elif isinstance(key, str):
        nm = "compare_{}".format(key.lower())
    job.data.add(nm, dat)
    job.data.tables[nm].persist(StorageLevel.DISK_ONLY)


def generate_decile_by_mail_dataset(job):
    """Generate decile_by_mail dataset for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requres tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
            mail_list (pyspark.sql.DataFrame): "mail_list" dataset
            raw_member (pyspark.sql.DataFrame): "raw_member" dataset
            dna (pyspark.sql.DataFrame): "dna" dataset

    Generates:
        (pyspark.sql.DataFrame) job.data.tables["decile_mail"]
    """
    #job.log.info(
    print("Generating decile mail split")

    basedata = get_basedata_with_new_150(job)

    cell = basedata.groupBy(["decile", "mail_flag"]).agg(
        sqlf.countDistinct("MBRSHP_SID").alias("MEMBERS"),
        sqlf.count("MBRSHP_SID").alias("IMPRESSIONS"),
        sqlf.mean("LAST_FIFTY-TWO_WEEK_TRIPS").alias("AVG_TRIPS"),
        sqlf.mean("LFIFTY-TWOW_SPEND_IN_STORE").alias("AVG_SPEND_52"),
        sqlf.mean("LFOURW_SPEND_IN_STORE").alias("AVG_SPEND_4"),
        sqlf.mean("LTWELVEW_SPEND_IN_STORE").alias("AVG_SPEND_12"),
        sqlf.stddev("LFIFTY-TWOW_SPEND_IN_STORE").alias("STDEV_SPEND_52"),
        sqlf.stddev("LFOURW_SPEND_IN_STORE").alias("STDEV_SPEND_4"),
        sqlf.stddev("LTWELVEW_SPEND_IN_STORE").alias("STDEV_SPEND_12"),
        sqlf.countDistinct("CPN_NBR").alias("DISTNCT_CPN"),
        sqlf.min("EXP_DT").alias("Min(EXP_DT)"),
        sqlf.max("EXP_DT").alias("Max(EXP_DT)"),
        sqlf.min("MBRSHP_EXP_DT").alias("Min(MBRSHP_EXP_DT)"),
        sqlf.max("MBRSHP_EXP_DT").alias("Max(MBRSHP_EXP_DT)"),
        sqlf.min("MBRSHP_RNWL_DT").alias("Min(MBRSHP_RNWL_DT)"),
        sqlf.max("MBRSHP_RNWL_DT").alias("Max(MBRSHP_RNWL_DT)"),
    )

    cell = cell.withColumn(
        "IMPR_per_MEMB", cell.IMPRESSIONS / cell.MEMBERS
    ).na.fill(0)

    cell = cell.withColumn("BASKET", cell.AVG_SPEND_52 / cell.AVG_TRIPS)

    full_cell_cols = [
        "mail_flag",
        "decile",
        "MEMBERS",
        "IMPRESSIONS",
        "IMPR_per_MEMB",
        "AVG_TRIPS",
        "BASKET",
        "AVG_SPEND_4",
        "STDEV_SPEND_4",
        "AVG_SPEND_12",
        "STDEV_SPEND_12",
        "AVG_SPEND_52",
        "STDEV_SPEND_52",
        "Min(EXP_DT)",
        "Max(EXP_DT)",
        "Min(MBRSHP_EXP_DT)",
        "Max(MBRSHP_EXP_DT)",
        "Min(MBRSHP_RNWL_DT)",
        "Max(MBRSHP_RNWL_DT)",
    ]

    cell = cell.select(*full_cell_cols).orderBy(
        sqlf.concat(
            sqlf.when(
                sqlf.col("decile") == sqlf.lit("new_member"), 0
            ).otherwise(sqlf.col("decile").cast("int")),
            sqlf.col("mail_flag"),
        )
    )

    job.data.add("decile_mail", cell)
    job.data.tables["decile_mail"].persist(StorageLevel.DISK_ONLY)


def generate_renewal_dataset(job, key):
    """Generate renewal dataset for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requires tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
            cell_dtl (pyspark.sql.DataFrame) (optional): cell detail CDSA table
                table required if key="CELL_ID"
        key (str): column to group by. Currently supports only cols in basedata
    Generates:
        (pyspark.sql.DataFrame): job.tables["renewal_stats"]
    """
    #job.log.info(
    print("Generating renewal summary by {}".format(key))
    key = key.upper()
    base = job.data.tables["base"].dropDuplicates(["MBRSHP_SID", key])
    if key == "CPN_NBR":
        base = filter_slots(job, base, "renewal")

    cols = ["MBRSHP_SID"] + RENEWAL_STATS
    dna = job.data.tables["dna"].select(*cols)
    base = base.join(dna, "MBRSHP_SID", "left")

    stats = summary_stats(job, base, cols, key)
    nm = "renewal_{}".format(key.lower())
    job.data.add(nm, stats)
    job.data.tables[nm].persist(StorageLevel.DISK_ONLY)


def generate_personalized_dataset(job, key):
    """Generate personalized content dataset for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requires tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
            cell_dtl (pyspark.sql.DataFrame) (optional): cell detail CDSA table
                table required if key="CELL_ID"
        key (str): column to group by. Currently supports only cols in basedata
    Generates:
        (pyspark.sql.DataFrame): job.tables["personalized_content_<key>"]
    """
    #job.log.info(
    print("Generating personalized content by {}".format(key))
    key = key.upper()
    base = job.data.tables["base"].dropDuplicates(["MBRSHP_SID", key])
    if key == "CPN_NBR":
        base = filter_slots(job, base, "personalized_content")

    dna = job.data.tables["dna"]
    dna_new_cols = ["MBRSHP_SID"] + [
        c for c in dna.columns if c not in base.columns
    ]
    dna = dna.select(*dna_new_cols)
    base = base.join(dna, "MBRSHP_SID", "left")

    cols = ["MBRSHP_SID"]
    cols.extend(PERSONALIZED_CONTENT_STATS)

    stats = summary_stats(job, base, cols, key)

    inhome_date = get_inhome_date(job)
    job.data.read("sd_zip_codes", "SAME_DAY_ZIPCODES", filetype="csv")
    zip_codes = [
        row[0]
        for row in job.data.tables["sd_zip_codes"]
        .select("postalcode")
        .collect()
    ]
    stats = stats.join(pct_app_persocontent(base, key), key, "left")
    stats = stats.join(
        pct_ez_persocontent(base, inhome_date, key), key, "left"
    )
    stats = stats.join(pct_rewards_persocontent(base, key), key, "left")
    stats = stats.join(pct_credit_persocontent(base, key), key, "left")
    stats = stats.join(
        pct_same_day_persocontent(base, zip_codes, key), key, "left"
    )
    stats = stats.join(pct_has_quotient_id(base, key), key, "left")

    for var in PERSONALIZED_CONTENT_CDS:
        s = crosstab_pct(base, var, "MBRSHP_SID", key)

        if var == "LATEST_AUTO_RNWL_IND":
            s = s.select(key, "N", "O", "Q")

        for cd in s.columns:
            if cd.startswith(key):
                nm = key
            else:
                nm = "{}_{}".format(var, cd)
            s = s.withColumnRenamed(cd, nm)
        stats = stats.join(s, key, "left")

    nm = "personalized_content_{}".format(key.lower())
    job.data.add(nm, stats)
    job.data.tables[nm].persist(StorageLevel.DISK_ONLY)


def generate_descriptive_dataset(job, key):
    """Generate descriptive dataset for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requires tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
            cell_dtl (pyspark.sql.DataFrame) (optional): cell detail CDSA table
                table required if key="CELL_ID"
        key (str): column to group by. Currently supports only cols in basedata
    Generates:
        (pyspark.sql.DataFrame): job.tables["descriptive_stats"]
    """
    #job.log.info(
    print("Generating descriptive summary by {}".format(key))
    key = key.upper()
    dna = job.data.tables["dna"]
    base = job.data.tables["base"].dropDuplicates(["MBRSHP_SID", key])
    if key == "CPN_NBR":
        base = filter_slots(job, base, "descriptive")
    base = base.join(dna, "MBRSHP_SID", "left")

    # join on null key value doesn't work, so convert null to non-empty string
    base = base.withColumn(
        key,
        sqlf.when(sqlf.col(key).isNull(), sqlf.lit("NULL")).otherwise(
            sqlf.col(key)
        ),
    )
    

    stats = pct_team_mbr(base, key)
    stats = stats.join(pct_trial(job, key), key, "left")
    stats = stats.join(pct_am(base, key), key, "left")
    stats = stats.join(pct_gas52(base, key), key, "left")
    stats = stats.join(pct_gas_club(base, key), key, "left")
    stats = stats.join(pct_gas52_or_club(base, key), key, "left")
    stats = stats.join(pct_ez(base, key), key, "left")
    out_cols = [
        "pct_team_mbr",
        "pct_trial",
        "pct_AM_stat_cd",
        "pct_gas_52w",
        "pct_gas_club",
        "pct_gas52_or_club",
        "pct_ez_renewal",
    ]

    for var in DESCRIPTIVE_CDS:
        s = crosstab_pct(base, var, "MBRSHP_SID", key)

        for cd in s.columns:
            if cd.startswith(key):
                nm = key
            else:
                nm = "{}_{}".format(var, cd)
                out_cols.append(nm)
            s = s.withColumnRenamed(cd, nm)
        stats = stats.join(s, key, "left")

    out_order = [key]
    stats, out_order = add_key_detail(job, stats, out_order, key)
    stats = add_mbr_count(stats, base, key)
    out_order.append("DISTINCT_MBRS")
    out_order.extend(out_cols)
    stats = stats.select(*out_order).orderBy(key)

    nm = "descriptive_{}".format(key.lower())
    job.data.add(nm, stats)
    job.data.tables[nm].persist(StorageLevel.DISK_ONLY)


def generate_savings_dump(job):
    """
    Write an output file for savings when include in the
    config file is set to True.
    Parameters:
        job (JobManager): Job to operate on. Requires dna table.
    Generates:
        job.data.tables["savings_file"] (pySpark DataFrame)
        ../assn_output/..savings/csv/part.csv
    """
    #job.log.info(
    print("Creating output file for savings content.")

    savings = job.data.tables["mail_savings"]
    dna = (
        job.data.tables["dna"]
        .select(SAVINGS_DUMP)
        .dropDuplicates(["MBRSHP_SID"])
    )

    out = dna.join(savings, "MBRSHP_SID", "inner")
    out = out.select(
        "MBRSHP_SID",
        "LATEST_MFI_TIER",
        "LATEST_MBRSHP_EXP_DT",
        "tenure",
        "savings",
        "trips",
        "gas_trips",
        "top_category",
        "L_FIFTY-TWOW_FIRST_MOST_SHOPPED_CATEGORY",
        "L_FIFTY-TWOW_SECOND_MOST_SHOPPED_CATEGORY",
    )

    job.data.add("savings_file", out)
    job.data.write("savings_file", "SAVINGS", singlefile=True, ftype="csv")


def generate_savings_overview(job):
    """
    Generate savings aggregate tables for QC report.

    Parameters:
        job (JobManager): Job to operate on.

    Generates:
        (pyspark.sql.DataFrame): job.tables["savings_summary"]
        (pyspark.sql.DataFrame): job.tables["top_10"]

    """
    #job.log.info(
    print("Generating savings overview tables")

    base = job.data.tables["base"]
    base = extend_table(job, base, SAVINGS_OVERVIEW)
    savings = job.data.tables["mail_savings"]

    names = ["savings_tot", "savings_mfi", "savings_tenure"]
    keys = [None, "LATEST_MFI_TIER", "TENURE"]
    ss_tables = []
    for name, key in zip(names, keys):
        job.data.add(name, base)
        metrics = anniversary_stats(job, base, SAVINGS_OVERVIEW, name, key)
        ss_tables.append(metrics)

    summary = ss_tables[0]
    # insert empty rows prior to concatenating tables
    schema = []
    for field in summary.schema.fields:
        col, dtp = field.name, field.dataType
        schema.append(sqlt.StructField(col, dtp, True))
    schema = sqlt.StructType(schema)
    blank = spark.createDataFrame([[None] * len(summary.columns)], schema)
    for df in ss_tables[1:]:
        summary = summary.union(blank).union(df)

    tot = savings.agg(sqlf.countDistinct("MBRSHP_SID")).collect()[0][0]

    top10 = (
        savings.groupBy("top_category")
        .agg(sqlf.countDistinct("MBRSHP_SID").alias("count_mbrs"))
        .withColumn("pct_mbrs", sqlf.col("count_mbrs") / tot)
        .orderBy(sqlf.desc("count_mbrs"))
        .limit(10)
    )
    for nm, tbl in zip(("savings_summary", "top_10"), (summary, top10)):
        job.data.add(nm, tbl)
        job.data.tables[nm].persist(StorageLevel.DISK_ONLY)


def generate_savings_keyed(job, key):
    """
    Generate savings dataset by user specified keys for QC report.

    Parameters:
        job (JobManager): Job to operate on.
        key (str): column to group by. Currently supports only cols in basedata
    Generates:
        (pyspark.sql.DataFrame): job.tables["savings_content_" + key]
    """
    #job.log.info(
    print("Generating savings summary by {}".format(key))
    base = job.data.tables["base"]
    if key == "CPN_NBR":
        base = filter_slots(job, base, "savings")

    base = extend_table(job, base, SAVINGS_KEYED)

    nm = "savings_content_{}".format(key.lower())
    metrics = anniversary_stats(job, base, SAVINGS_KEYED, nm, key)

    job.data.add(nm, metrics)
    job.data.tables[nm].persist(StorageLevel.DISK_ONLY)


def generate_nomail_dataset(job):
    """Generate no mail dataset for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requires tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
    Generates:
        (pyspark.sql.DataFrame): job.tables["nomail_stats"]
    """
    #job.log.info(
    print("Generating no mail dataset")

    raw_paths = job.config.paths.get("RAW_MEMBER_LISTS")
    if raw_paths and raw_paths[0] is not None:
        pop = None
        for path in raw_paths:
            dat = job.spark.read.csv(path, header="true")
            dat = dat.toDF(*[c.upper().replace("-", "_") for c in dat.columns])
            for opt in ["SCORE", "DECILE"]:
                if opt not in dat.columns:
                    dat = dat.withColumn(opt, sqlf.lit("0"))
            dat = dat.select("MBRSHP_SID", "SCORE", "DECILE")
            if pop:
                pop = pop.union(dat)
            else:
                pop = dat
    else:
        pop = (
            job.data.tables["mail_list"]
            .select(["MBRSHP_SID", "score", "decile"])
            .withColumnRenamed("score", "SCORE")
            .withColumnRenamed("decile", "DECILE")
        )

    # keep record with highest scores
    pop = pop.orderBy(
        "MBRSHP_SID", sqlf.col("SCORE").desc(), sqlf.col("DECILE").desc()
    ).dropDuplicates(["MBRSHP_SID"])

    pop = pop.withColumnRenamed("SCORE", "BBM_PROP_SCORE").withColumnRenamed(
        "DECILE", "BBM_DECILE"
    )

    # Arbitrarily drop dupes in DNA.
    # Dupes should be few and unknown what causes/priority of them
    dna = job.data.tables["dna"].dropDuplicates(["MBRSHP_SID"])

    assgn_full = job.data.tables["input_assignment"]
    assgn_full = (
        assgn_full.withColumnRenamed("mbrshp_sid", "MBRSHP_SID")
        .select("MBRSHP_SID")
        .withColumn("assigned", sqlf.lit(1))
        .dropDuplicates()
    )

    assgn_mail = job.data.tables["assignment"]
    if "mail_flag" not in assgn_mail.columns:
        assgn_mail = assgn_mail.withColumn("mail_flag", sqlf.lit(1))
    assgn_mail = assgn_mail.select("MBRSHP_SID", "mail_flag").dropDuplicates()

    dat = (
        pop.join(dna, "MBRSHP_SID", "left")
        .join(assgn_full, "MBRSHP_SID", "left")
        .join(assgn_mail, "MBRSHP_SID", "left")
    )
    dat = dat.toDF(*[c.upper().replace("-", "_") for c in dat.columns])
    dat = dat.withColumn(
        "MAIL_GROUP",
        sqlf.when(dat.MAIL_FLAG == 1, "mailed").otherwise(
            sqlf.when(dat.MAIL_FLAG == 0, "holdout").otherwise(
                sqlf.when(dat.ASSIGNED == 1, "considered").otherwise(
                    "excluded"
                )
            )
        ),
    )

    stats = summary_stats(job, dat, NOMAIL_STATS, "MAIL_GROUP")

    for var in NOMAIL_CDS:
        s = crosstab_pct(dat, var, "MBRSHP_SID", "MAIL_GROUP")
        for cd in s.columns:
            if cd.startswith("MAIL_GROUP"):
                nm = "MAIL_GROUP"
            else:
                nm = "{}_{}".format(var, cd)
            s = s.withColumnRenamed(cd, nm)
        stats = stats.join(s, "MAIL_GROUP", "left")

    nm = "nomail_check"
    job.data.add(nm, stats)
    job.data.tables[nm].persist(StorageLevel.DISK_ONLY)


def generate_example_data(job, examples):
    """
    Generate example tables of specific members for QC report in addition to a member from
    every assigned cell

    Parameters:
        job (JobManager): Job to operate on. Requires tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
                Schema can be found in docs for qc_assignments.generate_full_basedata
        examples(dict): examples object for filling with data. Keys
            MBRSHP_SID

    Returns:
        examples(dict): examples object with data filled
    """
    #job.log.info(
    print("Generating Examples")

    
    basedata     = job.data.tables["base"]
    cpn_articles = job.data.tables["cpn_articles"]
    cpn_articles = cpn_articles 
    random_members = []

    # For each cell, deterministically, randomly take 1 person and add them to examples
    for cell_id_row in (
        basedata.select("cell_id").distinct().orderBy("cell_id").collect()
    ):
        cell_id = cell_id_row["cell_id"]
        full_cell_data = basedata.filter("cell_id == {}".format(cell_id))
        backfill_members = (
            full_cell_data.filter("is_backfill == 1")
            .select("mbrshp_sid")
            .distinct()
        )

        frontfill_only_cell_data = full_cell_data.join(
            backfill_members, "mbrshp_sid", "left_anti"
        )

        sample = deterministic_sample(
            frontfill_only_cell_data, 1, None, cell_id
        )
        if sample.count() > 0:

            id = sample.select("mbrshp_sid").collect()[0]["mbrshp_sid"]
            sample = {
                "name": "cell_id {} - no backfills.".format(cell_id),
                "id": id,
            }
        else:
            #job.log.warn(
            print(
                "Cell {} had no members without backfill".format(cell_id)
            )
            id = full_cell_data.select("mbrshp_sid").collect()[0]["mbrshp_sid"]
            sample = {
                "name": "cell_id {} - no 0 backfill members found.".format(
                    cell_id
                ),
                "id": id,
            }
        random_members.append(sample)

    full_examples = examples + random_members

    # For each example, pull their data
    for example in full_examples:
        #job.log.info(
        print("Generating example {}".format(example))

        table = basedata[basedata["MBRSHP_SID"] == str(example["id"])].join(
            cpn_articles, ["CPN_NBR"], "left"
        )
        table = table.select(
            "MBRSHP_SID",
            "CELL_ID",
            "SLOT_NBR",
            "CPN_TYPE",
            "CPN_DESC",
            "SAMPLE_ARTICLE",
            "ADJ_TRIPS_RANK",
            "ADJ_TRIPS",
            "TRIPS",
            "IS_BACKFILL",
            "MAIL_FLAG",
        )
        table = table.orderBy("SLOT_NBR")
        example["data"] = table.toPandas()

    return full_examples


def generate_coupon_articles(job):
    """

    Parameters:
        job (JobManager): Job to operate on. Requires tables:
            coup_map (pyspark.sql.DataFrame): coupon_map
            coups (pyspark.sql.DataFrame): coupon_bank
            item_master (pyspark.sql.DataFrame): item_master
    :param job:
    :return:
    """
    #job.log.info(
    print("Generating coupon articles")
    coups       = job.data.tables["coups"].select("CPN_NBR", "CPN_DESC", "CPN_TYPE")
    coup_map    = job.data.tables["coup_map"]
    item_master = job.data.tables["item_master"]
    coup_quals  = job.data.tables["quals"]

    experiment_id = job.config.params["experiment"]

    experiment_coups = (
        coup_quals.filter("experiment_id == {}".format(experiment_id))
        .select("cpn_nbr")
        .distinct()
    )

    full_coups = coups.join(coup_map, "CPN_NBR").join(
        experiment_coups, "CPN_NBR"
    )
    cols = ["CPN_TYPE", "CPN_DESC", "ARTICLE_DESC", "CPN_NBR", "ARTICLE_NBR"]

    # For article coupons, get all articles
    article_coups = (
        full_coups.join(item_master, "ARTICLE_NBR").select(cols).distinct()
    )

    ah5 = (
        item_master.select("ARTICLE_NBR", "AH5_CD", "ARTICLE_DESC")
        .withColumnRenamed("AH5_CD", "CATEGORY_ID")
        .select("CATEGORY_ID", "ARTICLE_NBR", "ARTICLE_DESC")
    )
    ah4 = (
        item_master.select("ARTICLE_NBR", "AH4_CD", "ARTICLE_DESC")
        .withColumnRenamed("AH4_CD", "CATEGORY_ID")
        .select("CATEGORY_ID", "ARTICLE_NBR", "ARTICLE_DESC")
    )

    category_coups = (
        full_coups.join(join_category_agnostic(coup_map), "CPN_NBR")
        .drop("ARTICLE_NBR")
        .join(ah5.union(ah4).withColumn('CATEGORY_ID',sqlf.col('CATEGORY_ID').try_cast('BIGINT')), "CATEGORY_ID")
        .select(cols)
        .distinct()
    )

    articles = article_coups.union(category_coups).orderBy(
        "CPN_TYPE", "CPN_DESC"
    )

    article_dna = job.data.tables["article_dna"].select(
        [
            "ARTICLE_NBR",
            "AH4_CD",
            "AH4_DESC",
            "AH5_CD",
            "AH5_DESC",
            "INCLUDE_OR_EXCLUDE",
            "EXCLUSION_TYPE",
            "EXCLUSION_SUBTYPE",
        ]
    )

    job.data.tables["articles"] = articles.join(
        article_dna, "ARTICLE_NBR", "left"
    )

    # For each coupon how mow many articles do we have?
    cpn_articles_dummy = (
        coups.withColumn("CPN_TYPE", sqlf.lit("dummy"))
        .withColumn("ARTICLE_DESC", sqlf.lit("dummy"))
        .withColumn("ARTICLE_NBR", sqlf.lit("dummy"))
        .select(cols)
        .distinct()
    )

    sample_articles = articles.groupBy("cpn_nbr").agg(
        sqlf.first("ARTICLE_DESC").alias("SAMPLE_ARTICLE")
    )

    cpn_articles = (
        articles.union(cpn_articles_dummy)
        .groupBy("cpn_nbr")
        .agg(sqlf.count("*").alias("NUM_ARTICLES"))
        .withColumn("NUM_ARTICLES", sqlf.col("NUM_ARTICLES") - sqlf.lit(1))
        .join(sample_articles, "cpn_nbr")
    )

    job.data.tables["cpn_articles"] = cpn_articles


def generate_configured_qc(job):
    """
    This function utilizes the qc: section of our configuration yaml to
    dynamically produce specified qc tables.  See config_template for details
    on configuration.

    :param job:
    :return:
    """
    #job.log.info(
    print("Generating configured qc")
    base = job.data.tables["base"]

    tab_list = []

    generate_compare_dataset(job, "cell_id")

    tabs = ["{}_{}".format("compare", "cell_id")]

    if job.config.params["pre_configured"]["savings_content"]["include"]:
        generate_savings_overview(job)
        generate_savings_dump(job)
        tabs.append("savings_summary")

    for tab_ in tabs:
        tab_list.append(tab_)

    for tab, settings in job.config.params["pre_configured"].items():

        if tab != "compare" and not settings["include"]:
            continue
        allowed = [c.lower() for c in base.columns]

        for grp in settings["group_by"]:
            if (isinstance(grp, str) and grp.lower() not in allowed) or (
                isinstance(grp, list)
                and not all(x in allowed for x in map(str.lower, grp))
            ):
                #job.log.warn(
                print(
                    "Skipping {} by {} tab. Key not allowed.".format(tab, grp)
                )
                continue

            if tab == "renewal":
                generate_renewal_dataset(job, grp)
            elif tab == "descriptive":
                generate_descriptive_dataset(job, grp)
            elif tab == "personalized_content":
                generate_personalized_dataset(job, grp)
            elif tab == "compare" and grp != "cell_id":
                # cell_id is a mandatory parameter and will be forced in afterwards
                generate_compare_dataset(
                    job, grp
                )  # requires full base and cell
            elif tab == "savings_content":
                generate_savings_keyed(job, grp)

            if isinstance(grp, str):
                tab_list.append("{}_{}".format(tab, grp.lower()))
            elif isinstance(grp, list):
                tab_list.append("{}_{}".format(tab, "_".join(grp)))

    for extra_info in [
        x for x in job.config.paths if x.startswith("EXTRA_INFO_")
    ]:
        job.data.read(extra_info, extra_info, filetype="csv")
        job.data.tables[extra_info] = job.data.tables[
            extra_info
        ].dropDuplicates(subset=["cpn_nbr"])
        extra_info_df = job.data.tables[extra_info]
        #job.log.info(
        print("Adding {} to the qc.".format(extra_info))
        base = base.join(
            extra_info_df, calc_overlapping_cols(base, extra_info_df), "left"
        ).fillna("0")


    # do not check coupons which are allowed to have duplicates
    # such as NULL and pool type universal
    #-- here all datatypes are string
    check_for_duplicates = base.filter(base["cpn_nbr"].isNotNull()).filter(
        base["pool_type"] == Campaign.PoolType.LAYOUT
    )

    invalid_duplicates = (
        check_for_duplicates.groupBy("mbrshp_sid", "cpn_nbr")
        .count()
        .select("count")
        .distinct()
        .count()
        != 1
    )

    if invalid_duplicates:
        raise Exception(
            "Coupons have duplicates and are not marked as allowed"
            " to have duplicates."
        )

    if job.config.params["aggregation_params"]:
        aggregation_params = job.config.params["aggregation_params"]
        for dummy in aggregation_params:
            # This is clunky in code, but it makes the yml more readable
            aggregation_param = dummy[list(dummy.keys())[0]]

            for i in range(0, len(aggregation_param["group_by"])):
                group_by = aggregation_param["group_by"][i]
                tab_name = aggregation_param["tab_names"][i]
                #job.log.info(
                print("Grouping by {}.".format(group_by))
                dict_aggs = {}
                tab_list.append(tab_name)

                for agg in aggregation_param["aggs"]:
                    dict_aggs[agg[0]] = agg[1]

                tab_table = (
                    base.fillna(0)
                    .groupBy(group_by)
                    .agg(dict_aggs)
                    .orderBy(group_by)
                )

                if "sqlf.count(1)" in tab_table.columns:
                    tab_table = tab_table.withColumnRenamed(
                        "sqlf.count(1)", "count"
                    )

                job.data.tables[tab_name] = tab_table.cache()
                tab_table.count()

    return tab_list


# =============================================================================#
# Anniversary Content Helpers
# =============================================================================#


def mailfile_to_savings(job):
    """
    Relabel the mailfile to match savings content and subset for only members
    receiving savings content version.
    Parameters:
        job (JobManager): Job to operate on.
    Returns:
        dat (pyspark.sql.DataFrame): content data for savings tabs
    """
    if not job.config.params["pre_configured"]["savings_content"]["include"]:
        return
    dat = job.data.tables["final_mailhouse"]
    savings_conf = job.config.params["pre_configured"]["savings_content"]

    slot_content_map = savings_conf["slot_content_map"]
    for field, slot in list(slot_content_map.items()):
        dat = dat.withColumnRenamed("CPN" + str(slot), field.upper())

    savings_versions = savings_conf["version_letter"]
    dat = dat.filter(dat.VERSION_SLOT.isin(savings_versions))
    dat = dat.select(
        ["MBRSHP_SID", "MBRSHP_NBR"] + list(slot_content_map.keys())
    )

    job.data.add("mail_savings", dat)


def anniversary_stats(job, df, cols, tab_name, key=None):
    """
    Calculate stats for anniversary overviews.

    Parameters:
        job (JobManager): Job to operate on.
        df (pyspark.sql.DataFrame): "base" dataset
        cols (list[str]): list of columns for which to compute the stats
        tab_name (str): pre_configured tab name per config
        key (str, optional): column to group by. Currently supports only cols
            in basedata
    return:
        stats (pyspark.sql.DataFrame): table of the stats
    """
    slot_content_map = job.config.params["pre_configured"]["savings_content"][
        "slot_content_map"
    ]
    slot_map_keys = [col.upper() for col in slot_content_map]

    savings = job.data.tables["mail_savings"]
    for col in slot_map_keys:
        savings = savings.withColumnRenamed(col, "{}_content".format(col))

    base = df.dropDuplicates(["MBRSHP_SID"])
    base = base.join(savings, "MBRSHP_SID", "inner")
    base = extend_table(job, base, cols)

    if tab_name == "savings_tenure":
        base = base.withColumn(
            "yearly_tenure",
            sqlf.when(base.TENURE < 366, sqlf.lit("<= 1 Year")).otherwise(
                sqlf.lit("> 1 Year")
            ),
        )
        key = "yearly_tenure"

    return summary_stats(job, base, cols, key)


def generate_dates(job):
    """
    Return start and end dates used to compute last 52 week statistics, per DNA.

    Parameters:
        job (JobManager): Job to operate on.
    return:
        start, end (str, str)
    """
    job.data.read("dna", "CUBE", filetype="parquet")
    dna = job.data.tables["dna"]
    start = dna.agg(
        sqlf.date_format(
            sqlf.date_sub(sqlf.max("FISCAL_L52W_END"), 6), "M/d/yyyy"
        )
    ).collect()[0][0]
    end = dna.agg(
        sqlf.date_format(sqlf.max("FISCAL_WEEK_END"), "M/d/yyyy")
    ).collect()[0][0]
    return start, end


# =============================================================================#
# Personalized Content Helpers
# =============================================================================#
def get_inhome_date(job):
    inhome_date = (
        job.data.tables["cell"]
        .select("inhome_date")
        .where(sqlf.col("experiment_id") == job.config.params["experiment"])
        .collect()[0][0]
    )
    # change the format of inhome date to YYYY-MM-DD so date_diff can be calculcated downstream
    inhome_date = datetime.datetime.strptime(inhome_date, "%m/%d/%Y").strftime(
        "%Y-%m-%d"
    )

    return inhome_date


def pct_team_mbr(df, key=None):
    """
    Return the percent of observations in df that are team members, per key
    """
    dat = df.withColumn(
        "mbr",
        sqlf.when(
            (df.LATEST_SIC_CD.isin("9991", "9992", "1055"))
            | (
                df.LATEST_GRP_AFFIL_ID.isin(9993, 9994, 9997, 9995, 1055, 9992)
            ),
            1,
        ).otherwise(0),
    )
    return pct_flagged(dat, "mbr", "pct_team_mbr", key)


def pct_trial(job, key=None):
    """
    Return the percent of observations in mail_list that are trial members,
    per key
    """

    def has_trial(job):
        """
        Return True if mail_list includes trial members, False otherwise.

        Trial flag is not in DNA; it comes from Unica and is included in the
        input_mail_list file.

        To avoid requiring additional custom configs, assume a fixed format for
        input trial data. This is a safe assumption as the format is generally
        consistent. In the case where it is not, the user will be warned.
        """
        mail = job.data.tables["mail_list"]

        if "CellName" not in mail.columns:
            #job.log.warn(
            print("Trial flag 'CellName' not present in mail list.")
            return False

        present = mail.select("CellName").dropDuplicates()
        present = set(
            [
                row.CellName
                for row in present.collect()
                if "trial" in row.CellName.lower()
            ]
        )
        expected = set(["Trial FHH", "Trial No FHH"])
        if len(present.intersection(expected)) != 2:
            #job.log.warn(
            print(    "Trial values not found matching standard: {}. Only trial related values found were: {}".format(
                    expected, present
                )
            )
            return False

        return True

    mail = job.data.tables["mail_list"]
    base = job.data.tables["base"]
    dat = base.join(mail, on="MBRSHP_SID", how="left")
    if not has_trial(job):
        empty = (
            dat.select(key)
            .dropDuplicates()
            .withColumn("pct_trial", sqlf.lit(None))
        )
        return empty

    dat = dat.withColumn(
        "trial",
        sqlf.when(
            dat.CellName.isin(["Trial FHH", "Trial No FHH"]), 1
        ).otherwise(0),
    )
    return pct_flagged(dat, "trial", "pct_trial", key)


def pct_am(df, key=None):
    """
    Return the percent of members in the df with stat_cd of "AM", per key
    """
    dat = df.withColumn(
        "am", sqlf.when(df.MBRSHP_STAT_CD == "AM", 1).otherwise(0)
    )
    return pct_flagged(dat, "am", "pct_AM_stat_cd", key)


def pct_app_persocontent(df, key=None):
    """
    Return the percent of members who should be targeted to use the BJs app,
    per key

    REMINDER: If you change the definition used to flag a member in this function,
    update the definition accordingly in excelreport.py
    """
    dat = df.withColumn(
        "app_pc",
        sqlf.when(
            ((df.HAS_QUOTIENT_ID == 0) & (df.L26W_ATC_CPN_RED == 0)), 1
        ).otherwise(0),
    )
    return pct_flagged(
        dat,
        "app_pc",
        "Percentage of Members Qualifying for Targeting_BJ's App",
        key,
    )


def pct_credit_persocontent(df, key=None):
    """
    Return the percent of members who should be targeted to use a credit card

    REMINDER: If you change the definition used to flag a member in this
    function, update the definition accordingly in excelreport.py
    """
    dat = df.withColumn(
        "credit_pc",
        sqlf.when(
            (
                (df.RWDS_MBR_IND.isin(["Y", "N"]))
                & (
                    (sqlf.col("LFIFTY-TWOW_SPEND_IN_STORE") >= 1500)
                    | (sqlf.col("LTWELVEW_SPEND_IN_STORE") >= 400)
                    | (sqlf.col("L_FIFTY-TWOW_GAS_TRIPS") >= 10)
                    | (sqlf.col("L_TWELVEW_GAS_TRIPS") > 2)
                )
            ),
            1,
        ).otherwise(0),
    )
    return pct_flagged(
        dat,
        "credit_pc",
        "Percentage of Members Qualifying for Targeting_Credit Card",
        key,
    )


def pct_ez(df, key=None):
    """
    Return the percent of members in the df enrolled in EZ renewal, per key
    """
    dat = df.withColumn(
        "ez",
        sqlf.when(
            df.LATEST_AUTO_RNWL_IND.isin(["P", "A", "C", "D"]), 1
        ).otherwise(0),
    )
    return pct_flagged(dat, "ez", "pct_ez_renewal", key)


def pct_ez_persocontent(df, inhome_date, key=None):
    """
    Return the percent of members who should be targeted with EZ renewal
    content, per key
    Parameters:
    df (pyspark.sql.DataFrame): dataframe to perform the calculations
    inhome_date (string): the inhome date for current experiment
    key (str): key from list in the qc personal_content section of the config

    REMINDER: If you change the definition used to flag a member in this
    function, update the definition accordingly in excelreport.py
    """

    dat = df.withColumn(
        "ez_pc",
        sqlf.when(
            (df.LATEST_AUTO_RNWL_IND.isin(["N", "O", "Q"]))
            & (
                sqlf.datediff(
                    sqlf.to_date(sqlf.col("LATEST_MBRSHP_EXP_DT")),
                    sqlf.to_date(sqlf.lit(inhome_date)),
                )
                > 90
            ),
            1,
        ).otherwise(0),
    )
    return pct_flagged(
        dat,
        "ez_pc",
        "Percentage of Members Qualifying for Targeting_EZ Renewal",
        key,
    )


def pct_gas52(df, key=None):
    """
    Return the percent of members in df who bought gas within last 52 weeks,
    per key
    """
    dat = df.withColumn(
        "gas52",
        sqlf.when(sqlf.col("L_FIFTY-TWOW_GAS_TRIPS") >= 1, 1).otherwise(0),
    )
    return pct_flagged(dat, "gas52", "pct_gas_52w", key)


def pct_gas_club(df, key=None):
    """
    Return the percent of members in df whose primary club has gas, per key
    """
    return pct_flagged(df, "PREFERRED_CLUB_HAS_GAS", "pct_gas_club", key)


def pct_gas52_or_club(df, key=None):
    """
    Return the percent of members in df who bought gas within the last 52
    weeks or whose primary club has gas, per key
    """
    dat = df.withColumn(
        "gas",
        sqlf.when(
            (sqlf.col("L_FIFTY-TWOW_GAS_TRIPS") >= 1)
            | (df.PREFERRED_CLUB_HAS_GAS == 1),
            1,
        ).otherwise(0),
    )
    return pct_flagged(dat, "gas", "pct_gas52_or_club", key)


def pct_rewards_persocontent(df, key=None):
    """
    Return the percent of members who should be targeted for
    Rewards Upgrade

    REMINDER: If you change the definition used to flag a member in this function,
    update the definition accordingly in excelreport.py
    """
    dat = df.withColumn(
        "rewards_pc",
        sqlf.when(
            (df.RWDS_MBR_IND.isin(["N", "P"]))
            & (
                (sqlf.col("LFIFTY-TWOW_SPEND_IN_STORE").between(1500, 3500))
                | (df.LTWELVEW_SPEND_IN_STORE.between(400, 900))
            ),
            1,
        ).otherwise(0),
    )
    return pct_flagged(
        dat,
        "rewards_pc",
        "Percentage of Members Qualifying for Targeting_Rewards Upgrade",
        key,
    )


def pct_same_day_persocontent(df, zip_codes, key=None):
    """
    Return the percent of members who should be targeted for
    Same Day Delivery

    REMINDER: If you change the definition used to flag a member in this function,
    update the definition accordingly in excelreport.py

    zip_codes (list[str]): list of zip_codes
    """
    dat = df.withColumn(
        "same_day_pc",
        sqlf.when(
            (
                (df.LATEST_HOME_ZIP_CD.isin(zip_codes))
                & (df.HAS_QUOTIENT_ID == 1)
            ),
            1,
        ).otherwise(0),
    )
    return pct_flagged(
        dat,
        "same_day_pc",
        "Percentage of Members Qualifying for Targeting_Same Day Delivery",
        key,
    )


def pct_has_quotient_id(df, key=None):
    """
    Return the percent of members that have quotient id
    REMINDER: If you change the definition used to flag a member in this function,
    update the definition accordingly in excelreport.py
    """
    dat = df.withColumn(
        "quotient", sqlf.when(df.HAS_QUOTIENT_ID == 1, 1).otherwise(0)
    )

    return pct_flagged(dat, "quotient", "prct_has_quotient_id", key)


# =============================================================================#
# Helper Tables
# =============================================================================#


def generate_full_basedata(job, TYPE):
    """Generates a full "base" dataset for use in QC reporting.

    Parameters:
        job (JobManager): Job to operate on. Requires tables:
            assignment (pyspark.sql.DataFrame): Mail Population Assignment
            constructs (pyspark.sql.DataFrame): all_constructs
            coups (pyspark.sql.DataFrame): coupon_bank
            memtrips (pyspark.sql.DataFrame): member_trips_coupon
            coup_quals (pyspark.sql.DataFrame): coupon_quals
            coup_map (pyspark.sql.DataFrame): coupon_map
            dna (pyspark.sql.DataFrame): member dna dataset
            cf: All CF scores
            version_map: For BBM

        TYPE (int): integer reflecting what type of campaign it is.
            1 - bbm
            2 - mmpc


    Returns:
        basedata: Full "base" dataset adds (to raw base):
             |-- MBRSHP_SID: string (nullable = true)
             |-- CPN_NBR: string (nullable = true)
             |-- CONSTRUCT: string (nullable = true)
             |-- EXPERIMENT_ID: string (nullable = true)
             |-- cell_id: integer (nullable = true)
             |-- slot_nbr: integer (nullable = true)
             |-- mail_flag: string (nullable = true)
             |-- CPN_TYPE: string (nullable = true)
             |-- CPN_DESC: string (nullable = true)
             |-- is_backfill: long (nullable = true)
             |-- ADJ_TRIPS: double (nullable = true)
             |-- ADJ_TRIPS_RANK: integer (nullable = true)
             |-- LAST_FIFTY-TWO_WEEK_TRIPS: double (nullable = true)
             |-- LFIFTY-TWOW_SPEND_IN_STORE: double (nullable = true)
             |-- LFOURW_SPEND_IN_STORE: double (nullable = true)
             |-- LTWELVEW_SPEND_IN_STORE: double (nullable = true)
             |-- VERSION: string (nullable = true)
        coupon_dummy: same as basedata, but with 1 dummy row per coupon

        cf: All CF scores pertaining to this assignment
             |-- CATEGORY_ID: long (nullable = true)
             |-- MBRSHP_SID: integer (nullable = true)
             |-- prediction: double (nullable = true)
             |-- CATEGORY_LVL: string (nullable = true)
             |-- CATEGORY_NAME: string (nullable = true)
             |-- CPN_NBR: string (nullable = true)
             |-- cpn_nbr: string (nullable = true)
             |-- CF_RANK: integer (nullable = true)
             |-- hs_ind_lambdaX: string (nullable = true)
                This column will come with various values for X

        trips: cpn trip information
             |-- MBRSHP_SID: long (nullable = true)
             |-- ADJ_TRIPS: double (nullable = true)
             |-- TRIPS: double (nullable = true)
             |-- CPN_NBR: string (nullable = true)
             |-- ADJ_TRIPS_RANK: integer (nullable = true)

    """
    #job.log.info(
    print("Generating base dataset")
    experiment_id = job.config.params["experiment"]
    assignment = (
        job.data.tables["assignment"]
        .withColumn("slot_nbr", sqlf.col("slot_nbr").cast("int"))
        .withColumn("cell_id", sqlf.col("cell_id").cast("int"))
    )
    if TYPE == 1 and job.config.paths.get("VERSION_MAP") is not None:
        #job.log.info(
        print("Reading version map for BBM.")
        vmap = job.data.tables["version_map"]
        vmap = vmap.withColumnRenamed("CPN1", "CPN_NBR")
        assignment = assignment.join(vmap, "CPN_NBR", "left")
        assignment = _version_from_cpn(job, assignment)

    constructs = (
        job.data.tables["constructs"]
        .select("mbrshp_sid", "construct", "cpn_nbr", "is_backfill")
        .distinct()
    )
    coups = job.data.tables["coups"]
    coup_quals = job.data.tables["quals"]
    coup_map = job.data.tables["coup_map"]
    memtrips = job.data.tables["memtrips"]
    dna = job.data.tables["dna"]
    cf_raw = job.data.tables["cf"]
    # the next 2 lines where added as the source we are using in databricks has the results for all dates and additional columns
    max_cf_run  = cf_raw.agg(sqlf.max("RUN_NAME").alias('max_dt')).first()['max_dt']
    cf     = cf_raw.filter(sqlf.col('RUN_NAME')==max_cf_run).drop('RUN_NAME', 'CATEORY_CD', 'START_DATE', 'END_DATE')



    # add coupon types
    experiment_coups = (
        coup_quals.filter("experiment_id == {}".format(experiment_id))
        .select("cpn_nbr")
        .distinct()
    )
    couptypes = (
        coups.select("cpn_nbr", "cpn_type", "cpn_desc")
        .join(experiment_coups, "CPN_NBR")
        .dropDuplicates(subset=["cpn_nbr"])
    )
    couptypes = couptypes.withColumnRenamed("cpn_nbr", "CPN_NBR")
    couptypes = couptypes.withColumnRenamed("cpn_type", "CPN_TYPE")
    couptypes = couptypes.withColumnRenamed("cpn_desc", "CPN_DESC")
    cat_coups = join_category_agnostic(coup_map).select(
        "CPN_NBR", "CATEGORY_ID"
    )

    # add adjusted trips and trips rank
    trips = memtrips.select(
        "MBRSHP_SID", "adjusted_trips", "TRIPS", "CPN_NBR"
    ).withColumnRenamed("adjusted_trips", "ADJ_TRIPS")

    window = Window.partitionBy(trips.MBRSHP_SID).orderBy(
        trips.ADJ_TRIPS.desc()
    )
    trips = trips.withColumn("ADJ_TRIPS_RANK", sqlf.row_number().over(window))

    # get cf scores for members
    window = Window.partitionBy(sqlf.col("MBRSHP_SID")).orderBy(
        sqlf.col("prediction").desc()
    )
    ranked_cf = (
        cf.dropDuplicates(["MBRSHP_SID", "CATEGORY_ID"])
        .join(cat_coups, "CATEGORY_ID")
        .join(assignment.select("mbrshp_sid").distinct(), "mbrshp_sid")
        .join(cat_coups, "CATEGORY_ID")
        .withColumn("CF_RANK", sqlf.row_number().over(window))
    )

    # add dna-based spends
    dna = dna.select(
        "MBRSHP_SID",
        "LAST_FIFTY-TWO_WEEK_TRIPS",
        "LFIFTY-TWOW_SPEND_IN_STORE",
        "LFOURW_SPEND_IN_STORE",
        "LTWELVEW_SPEND_IN_STORE",
    ).distinct()

    dna = cap_top_percentile(dna, "LAST_FIFTY-TWO_WEEK_TRIPS")
    dna = cap_top_percentile(dna, "LFIFTY-TWOW_SPEND_IN_STORE")
    dna = cap_top_percentile(dna, "LFOURW_SPEND_IN_STORE")
    dna = cap_top_percentile(dna, "LTWELVEW_SPEND_IN_STORE")

    if "mail_flag" not in [x.lower() for x in assignment.columns]:
        assignment = assignment.withColumn("mail_flag", sqlf.lit("1"))

    count_check = assignment.count()
    #job.log.info(
    print("Assignment Count: {}".format(count_check))
    basedata = assignment.join(couptypes, "CPN_NBR", "left")
    #job.log.info(
    print("Post Coupon Join: {}".format(basedata.count()))
    basedata = basedata.join(
        constructs, ["MBRSHP_SID", "CPN_NBR", "CONSTRUCT"], "left"
    )
    #job.log.info(
    print("Post Construct Join: {}".format(basedata.count()))
    basedata = basedata.join(trips, ["MBRSHP_SID", "CPN_NBR"], "left")
    #job.log.info(
    print("Post Member Trips join: {}".format(basedata.count()))
    basedata = basedata.join(dna, ["MBRSHP_SID"], "left").persist(  StorageLevel.MEMORY_ONLY    )
    #job.log.info(
    print("Post DNA join: {}".format(basedata.count()))
    assert basedata.count() == count_check

    coupon_dummy = (
        basedata.limit(1)
        .drop("MBRSHP_SID", "CPN_NBR", "CPN_TYPE", "CPN_DESC")
        .crossJoin(couptypes.select(["CPN_NBR", "CPN_TYPE", "CPN_DESC"]))
        .withColumn("MBRSHP_SID", sqlf.lit("-1"))
        .select(basedata.columns)
    )

    job.data.add("base", basedata)
    job.data.add("trips", trips)
    job.data.add("coupon_dummy", coupon_dummy)
    job.data.add("cf_original", cf)
    job.data.add("cf", ranked_cf)


def get_basedata_with_new_150(job):
    """
    Parameters:
        job (JobManager): Job to operate on. Requres tables:
            basedata (pyspark.sql.DataFrame): "base" dataset
            mail_list (pyspark.sql.DataFrame): "mail_list" dataset
            raw_member (pyspark.sql.DataFrame): "raw_member" dataset
            dna (pyspark.sql.DataFrame): "dna" dataset

    Returns:
        basedata with left joined F150 column
    """

    basedata = job.data.tables["base"]
    bbmprop = (
        job.data.tables["mail_list"]
        .withColumnRenamed("mbrshp_nbr", "MBRSHP_NBR")
        .select(["MBRSHP_NBR", "decile"])
    )

    mbr = job.data.tables["raw_member"].select(
        "MBRSHP_SID", "MBRSHP_NBR", "MKT_CD", "MBRSHP_FEE_INC"
    )
    cube = job.data.tables["dna"]
    cube = cube.dropDuplicates(subset=["MBRSHP_SID"])
    cube = cube.select(
        "MBRSHP_SID", "TENURE", "EXP_DT", "MBRSHP_EXP_DT", "MBRSHP_RNWL_DT"
    )
    cube = cube.withColumn(
        "IS_TENURE_NULL", sqlf.when(cube.TENURE.isNull(), 1).otherwise(0)
    )
    cube = cube.join(mbr, "MBRSHP_SID", "inner")
    cube = cube.join(bbmprop, "MBRSHP_NBR", "inner")
    new150 = sqlf.col("TENURE") < 150
    nofee = sqlf.col("MBRSHP_FEE_INC") == 0
    nulltenure = sqlf.col("IS_TENURE_NULL") == 1
    trialmarket = sqlf.col("MKT_CD").like("Z%")
    trial = nofee & trialmarket
    new150_forcein = new150 & ~trial & ~nulltenure

    cube = cube.withColumn(
        "NEW_150", sqlf.when(new150_forcein, 1).otherwise(0)
    )
    basedata = (
        basedata.join(cube, "MBRSHP_SID", "left")
        .fillna(1, subset=["NEW_150"])
        .withColumn(
            "NEW_150",
            sqlf.when(sqlf.col("decile").isNull(), 1).otherwise(
                sqlf.col("NEW_150")
            ),
        )
    )

    basedata = basedata.withColumn(
        "decile",
        sqlf.when(sqlf.col("NEW_150") == 1, sqlf.lit("new_member")).otherwise(
            sqlf.col("decile")
        ),
    )

    basedata.cache()

    return basedata


def generate_member_dataset(job):
    """Generate member dataset for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requres tables:
            basedata (pyspark.sql.DataFrame): "base" dataset

    Returns:
        mbr (pyspark.sql.DataFrame): "member" dataset
    """
    #job.log.info(
    print("Generating member level trips and spend")
    basedata = job.data.tables["base"]
    member = basedata.groupBy("MBRSHP_SID").agg(
        sqlf.countDistinct("CPN_NBR").alias("DSTNCT_CPN"),
        sqlf.first("LAST_FIFTY-TWO_WEEK_TRIPS").alias("TRIPS"),
        sqlf.first("LFIFTY-TWOW_SPEND_IN_STORE").alias("SPEND"),
    )
    job.data.add("member", member)
    job.data.tables["member"].persist(StorageLevel.DISK_ONLY)


def generate_longitudinal_dataset(job):
    """Generate member dataset for QC report.

    Parameters:
        job (JobManager): Job to operate on. Requres tables:
            cell_dtl (pyspark.sql.DataFrame): cell detail CDSA table
            basedata (pyspark.sql.DataFrame): "base" dataset

    Returns:
        longitudinal (pyspark.sql.DataFrame): "longitudinal" dataset (added to
            job.data)
        (Boolean): If or not the campaign has longitudinal test
    """
    #job.log.info(
    print("Generating longitudinal qc dataset")
    cell_dtl = job.data.tables["cell"].toPandas()
    campaign = job.data.tables["campaign"]
    assignment = job.data.tables["cdsa_assgn"]
    curr_assn = job.data.tables["final_mailhouse"]

    curr_cell_dtl = cell_dtl[
        (cell_dtl.experiment_id == job.config.params["experiment"])
        & cell_dtl.longitudinal_id.notnull()
    ]

    if curr_cell_dtl.empty:
        return False

    longitudinal = curr_cell_dtl[
        ["longitudinal_id", "cell_id", "ctrl_flag", "test_flag"]
    ].rename(columns={"ctrl_flag": "CONTROL", "test_flag": "TEST"})

    longitudinal["test_control"] = longitudinal[["TEST", "CONTROL"]].idxmax(
        axis=1
    )
    longitudinal = longitudinal.drop(columns=["TEST", "CONTROL"])

    long_ids = longitudinal.longitudinal_id.astype("int").tolist()
    long_stats = []
    for long_id in long_ids:
        long_cells = cell_dtl[cell_dtl.longitudinal_id == long_id]

        curr_cell_id = long_cells.loc[
            long_cells["experiment_id"] == job.config.params["experiment"],
            "cell_id",
        ].iloc[0]

        curr_cell_desc = long_cells.loc[
            long_cells["experiment_id"] == job.config.params["experiment"],
            "cell_desc",
        ].iloc[0]

        prev_cell_ids = long_cells[
            long_cells.experiment_id != job.config.params["experiment"]
        ].cell_id.tolist()

        prev_cell_ids = set(
            assignment.select("cell_id").distinct().toPandas().cell_id.tolist()
        ).intersection(prev_cell_ids)

        long_stat = {}
        long_stat["longitudinal_id"] = long_id
        long_stat["touches"] = len(prev_cell_ids) + 1

        curr_long_assn = curr_assn.filter(curr_assn.CELL_ID == curr_cell_id)
        long_stat["current_members"] = (
            curr_long_assn.select("mbrshp_sid").distinct().count()
        )

        if len(prev_cell_ids) != 0:
            prev_cell_ids = sorted(prev_cell_ids, reverse=True)
            prev_cell_id = max(prev_cell_ids)
            prev_experiment_id = long_cells.loc[
                long_cells["cell_id"] == prev_cell_id, "experiment_id"
            ].iloc[0]

            prev_assn = assignment.filter(assignment.cell_id == prev_cell_id)

            long_stat["previous_members"] = (
                prev_assn.select("mbrshp_sid").distinct().count()
            )
            long_stat["overlap_members"] = (
                prev_assn.join(curr_long_assn, on="mbrshp_sid", how="inner")
                .select("mbrshp_sid")
                .distinct()
                .count()
            )
            long_stat["dropped_members"] = (
                prev_assn.join(
                    curr_long_assn, on="mbrshp_sid", how="left_anti"
                )
                .select("mbrshp_sid")
                .distinct()
                .count()
            )
            long_stat["new_members"] = (
                curr_long_assn.join(
                    prev_assn, on="mbrshp_sid", how="left_anti"
                )
                .select("mbrshp_sid")
                .distinct()
                .count()
            )

            prev_cell_desc = long_cells.loc[
                long_cells["cell_id"] == prev_cell_id, "cell_desc"
            ].iloc[0]

            if curr_cell_desc.strip() == prev_cell_desc.strip():
                long_stat["cell_desc_check"] = "Matches"
            else:
                long_stat["cell_desc_check"] = "Does Not Match"
                long_stat["current_cell_desc"] = curr_cell_desc
                long_stat["previous_cell_desc"] = prev_cell_desc

            long_stat["previous_campaign"] = (
                campaign.filter(campaign.experiment_id == prev_experiment_id)
                .select("experiment_desc")
                .collect()[0]["experiment_desc"]
            )

        else:
            if (
                curr_cell_desc.strip()
                in cell_dtl[
                    (cell_dtl.experiment_id != job.config.params["experiment"])
                    & cell_dtl.longitudinal_id.notnull()
                ]["cell_desc"].values
            ):
                long_stat["cell_desc_check"] = "Not Unique"
            else:
                long_stat["cell_desc_check"] = "Unique"

        long_stats.append(long_stat)

    long_stats_df = spark.createDataFrame(long_stats)
    longitudinal[["longitudinal_id", "cell_id"]] = longitudinal[
        ["longitudinal_id", "cell_id"]
    ].astype(int)
    schema = sqlt.StructType(
        [
            sqlt.StructField("longitudinal_id", sqlt.IntegerType(), True),
            sqlt.StructField("cell_id", sqlt.IntegerType(), True),
            sqlt.StructField("test_control", sqlt.StringType(), True),
        ]
    )
    longitudinal = spark.createDataFrame(longitudinal, schema).join(
        long_stats_df, how="left", on="longitudinal_id"
    )

    job.data.add("longitudinal", longitudinal)
    job.data.tables["longitudinal"].persist(StorageLevel.DISK_ONLY)
    return True
