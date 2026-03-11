"""Function bank for Coupon Creation File. """
import os
import warnings
from datetime import datetime

import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
from pyspark.sql.functions import (
    col,
    concat,
    current_date,
    datediff,
    desc,
    explode,
    format_number,
    lit,
    lower,
)
from pyspark.sql.functions import min as fmin
from pyspark.sql.functions import regexp_replace, row_number, split
from pyspark.sql.functions import sum as fsum
from pyspark.sql.functions import trim, when
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


from lib_assignment.assn_utils import (
    deterministic_df,
    update_categories,
    env_path
)
# # from lib.spark_util import get_logger
from lib.utils import top_n, trips_only
from urllib.parse import urlparse


spark = SparkSession.builder.getOrCreate()
try:
    dbutils  
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)  


# log = get_logger("coupon_utils")
DEBUG_ASSIGNMENT = os.getenv("ASSIGNMENT_DEBUG", "1").lower() in (
    "1",
    "true",
    "yes",
    "y",
)


def _debug_log(msg):
    if DEBUG_ASSIGNMENT:
        print(f"[coupon_utils][debug] {msg}")


def _debug_column_sample(df, column, label):
    if (not DEBUG_ASSIGNMENT) or (column not in df.columns):
        return
    null_count = df.filter(col(column).isNull()).count()
    sample = [r[column] for r in df.select(column).limit(10).collect()]
    _debug_log(
        "{} column={} null_count={} sample={}".format(
            label, column, null_count, sample
        )
    )

# ---- Helpers --- #

def safe_cast_by_schema(df, schema):
    """
    Given a DataFrame and a target StructType schema, cast each column
    using try_cast so that invalid values become NULL instead of raising
    errors or silently corrupting data.
    """
    out = df
    for field in schema.fields:
        col_name = field.name
        if col_name not in out.columns:
            # skip columns that aren't in the dataframe
            continue

        col_escaped = col_name.replace("`", "``")
        sql_type = field.dataType.simpleString()   # e.g. "string", "int", "double", "date"

        if sql_type == "string":
            # Clean string: remove '�' and trim both sides
            cleaned_col = sqlf.regexp_replace(sqlf.col(col_name).cast("string"), "�", "")
            cleaned_col = sqlf.regexp_replace(cleaned_col, " ", "")
            cleaned_col = sqlf.ltrim(sqlf.rtrim(cleaned_col))  # or sqlf.trim(cleaned_col)
            out = out.withColumn(col_name, cleaned_col)
        else:
            # Use try_cast for non-string types
            expr = f"try_cast(`{col_escaped}` AS {sql_type})"
            out = out.withColumn(col_name, sqlf.expr(expr))

    return out

def append_non_duplicates(spark, existing, df, path, ftype, dedupe_col=None, output_vol=None, env="dev"):
    """Append non-duplicate items to a dataframe.
    Parameters:
        path (str): path of the existing dataframe
        existing (pyspark.sql.DataFrame): Existing file as a DataFrame
        df (pyspark.sql.DataFrame): dataframe to append
            NOTE: df must have identical columns to existing file!
        ftype (str): filetype of file at path
        dedupe_col (str/list): column(s) in df that the function will compare
            to remove duplicatses
    Returns
        Nothing!
    """
    cols = existing.columns

    if dedupe_col:
        dedupe_val = df.select(dedupe_col).distinct()
        existing = existing.join(dedupe_val, dedupe_col, "leftanti")
        existing = existing.select(*cols)  # maintain existing column order

    for c in cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))
    combined = existing.union(df.select(*cols))
    deduped = combined.dropDuplicates()
    deduped = deduped.filter(deduped.cpn_nbr.isNotNull())
    # spark cannot read from and write to the same path, the solution is persisting in the middle
    # deduped.persist()
    deduped.count()
    if ftype.lower() == "parquet":
        deduped.coalesce(1).write.parquet(env_path(path, output_vol, env), mode="overwrite")
    else:
        deduped.coalesce(1).write.csv(env_path(path, output_vol, env), mode="overwrite", header=True)
    # deduped.unpersist()


def append_new_campaign(job, new, path_id, ftype, cpn_quals, del_keys, df_schema):
    """Append by overwriting data for the campaign and store deleted data
    Parameters:
        new (pyspark.sql.DataFrame): dataframe to append
            NOTE: df must have identical columns to existing file!
        path_id (str): key to the path in job.config.paths
        ftype (str): filetype of the file at path (csv/parquet)
        cpn_quals (pyspark.sql.DataFrame): dataframe of qualified coupons
        del_keys (list[str]): column(s) to check for duplicates in the deleted
            file
    """

    job.data.read("existing", path_id, filetype=ftype)
    job.data.tables["existing"] = safe_cast_by_schema(job.data.tables["existing"], df_schema)
    existing = job.data.tables["existing"]

    job.config.paths["{}_TEMP".format(path_id)] = "{}_temp".format(
        job.config.paths[path_id]
    )

    if ftype.lower() == "parquet":
        existing.coalesce(1).write.parquet(
            env_path(job.config.paths["{}_TEMP".format(path_id)], job.vol_base, job.env), mode="overwrite"
        )
    else:
        existing.coalesce(1).write.csv(
            env_path(job.config.paths["{}_TEMP".format(path_id)], job.vol_base, job.env),
            mode="overwrite",
            header=True,
        )

    cols = existing.columns
    modified = (
        cpn_quals.filter(
            col("experiment_id") != job.config.params["experiment"]
        )
        .join(existing, on="cpn_nbr", how="left")
        .select(*cols)
    )
    append_non_duplicates(
        job.spark, modified, new, job.config.paths[path_id], ftype, "cpn_nbr", output_vol=job.vol_base, env=job.env
    )

    job.data.read("updated", path_id, filetype=ftype)
    updated = job.data.tables["updated"]
    job.data.read("existing", "{}_TEMP".format(path_id), filetype=ftype)
    existing = job.data.tables["existing"]
    deleted = existing.subtract(updated)
    deleted = (
        deleted.withColumn(
            "date", lit(str(datetime.now().strftime("%Y-%m-%d")))
        )
        .withColumn("run_name", lit(job.config.params["run_name"]))
        .withColumn("experiment_id", lit(job.config.params["experiment"]))
    )

    job.config.paths["{}_DELETED".format(path_id)] = "{}_deleted".format(
        job.config.paths[path_id]
    )
    job.data.read(
        "existing_deleted", "{}_DELETED".format(path_id), filetype=ftype
    )
    existing_deleted = job.data.tables["existing_deleted"]

    append_non_duplicates(
        job.spark,
        existing_deleted,
        deleted,
        job.config.paths["{}_DELETED".format(path_id)],
        ftype,
        del_keys,
        output_vol=job.vol_base, 
        env=job.env
    )


def cast(df, cols=None):
    """Make standard type casts.

    Parameters:
        df (pyspark.sql.DataFrame): spark dataframe to apply operation to
        cols (list[str], opt): columns to limit dataframe to

    Returns:
        df (pyspark.sql.DataFrame): data frame with type caserts
    """
    for column in cols:
        if column in [
            "ARTICLE_NBR",
            "Eligible for MMPC",
            "article_nbr",
            "Quantity Threshold",
            "MBRSHP_NBR",
            "MBRSHP_SID",
            "TRIPS",
            "cpn_nbr",
        ]:
            _debug_column_sample(df, column, "before cast(long/try_cast)")
            df = df.withColumn(column, col(column).try_cast("long"))
            _debug_column_sample(df, column, "after cast(long/try_cast)")
        if column in ["EXTENDED_PRC_AMT", "prediction for MMPC"]:
            _debug_column_sample(df, column, "before cast(float)")
            df = df.withColumn(column, col(column).cast("float"))
            _debug_column_sample(df, column, "after cast(float)")
    return df


def calculate_weight(df, cat_dna, month):
    """Adds a column with calculated weight to dataframe.

    Given the category dna and a data, calculate the combined weight of the scaling factors
    Parameters:
        cat_dna (dataframe): input dataframe
        coupon_month(int): date the assigment is valid for, used for seasonality

    Returns:
        cat_dna (dataframe): dataframe with additional "weight" column
    """
    rel_weighting = {"seasonality": 0.8, "cycle": 0.2}
    col_name = "SCALED_MONTH_" + str(month) + "_SEASONALITY"

    dna = cat_dna.select("category", "SCALED_PURCHASE_CYCLE", col_name)
    df = df.join(dna, "category", "left")

    # create weight column
    df = df.withColumn(
        "weight",
        (
            (df[col_name] * rel_weighting["seasonality"])
            + (df.SCALED_PURCHASE_CYCLE * rel_weighting["cycle"])
        ),
    )
    df = df.fillna(1, subset=["weight"])
    df = df.drop("SCALED_PURCHASE_CYCLE", col_name)
    return df


# ---- Coupon Routines --- #


def generate_coupon_member_trips(coupons, mbr_trips, cat_dna, rel_month):
    """Generate coupon-member-trip aggregations for coupon set.

    Aggregates trips by rolling up from the category/article level
    for each relevant category/article per-coupon. This will have the
    side effect of upweighting any coupon with multiple categories/articles
    in instances where multiple relevant items are bought together.

    Parameters:
        coupons (pyspark.sql.DataFrame): coupon data to generate trips for
            REQUIRES: cpn_ah4_cd (int) - relevant ah4 coupon codes (or Null)
                      cpn_ah5_cd (int) - relevant ah5 coupon codes (or Null)
                      article_nbr (int) - relevant artcile numbers (or Null)
        mbr_trips (pyspark.sql.DataFrame): relevant transaction data with aggregate trips
            REQUIRES: mbrshp_sid (int) - membership sid
                      category (int) - category code of relevant category (any level)
                      trips (int) - count of trips at category level (any level)
                      days_since_last (int) - number of days between day ran and last purchase (any level)
        cat_dna (pyspark.sql.DataFrame): relevant category dna for all cats on coupons
            REQUIRES: category (int) - category code of relevant category (any level)
                      SCALED_PURCHASE_CYCLE (float) - scaled purchase cycle weight of category
                      SCALED_MONTH_X_SEASONALITY (float) - scaled seasonality for month X
        rel_month (int): relevant month for seasonality factor (corresponds to X above)

    Returns:
        coups (pyspark.sql.DataFrame): trips per member coupon
            CONTAINS: cpn_npr (int) - coupon number
                      mbrshp_sid (int) - membership sid
                      trips (float) - relevant quantity adjusted trips
                      adjusted_trips (float) - seasonality/purch. cycle and quant. adjusted trips
                      days_since_last (int) - date difference of current date and last purchase
    """
    cat_colnames = ["cpn_ah4_cd", "cpn_ah5_cd", "article_nbr"]
    cat_cols = [col("cpn_ah4_cd"), col("cpn_ah5_cd"), col("article_nbr")]

    #  1. Join on raw trips per coupon per category per member
    coups = coupons.fillna(0, subset=cat_colnames)
    for cols in cat_colnames:
        coups = coups.withColumn(
            cols, when(coups[cols].isNull(), 0).otherwise(coups[cols])
        )
    coups = coups.withColumn("category", sum(cat_cols))
    coups = coups.join(mbr_trips, "category", "left")

    #  2. Multiply raw trips by calculated weight to get adjusted trips
    coups = calculate_weight(coups, cat_dna, rel_month)
    # we do an explicit cast here because adjusted_trips can end up having
    # different decimals for the same computation
    coups = coups.withColumn(
        "adjusted_trips",
        (coups.trips * coups.weight).cast(
            sqlt.DecimalType(precision=38, scale=18)
        ),
    )
    coups = coups.drop("weight")

    #  3. Calculate trips by grouping total trips per coupon per member (sum)
    #  4. Calculate adjusted trips by grouping total adjusted trips per coupon per member (sum)
    #  5. Take minimum of days since last purchase per coupon (METHODOLOGY COULD BE ADJUSTED)
    coups = coups.groupBy("cpn_nbr", "mbrshp_sid", "cpn_qty_threshold").agg(
        fsum("trips").alias("trips"),
        fsum("adjusted_trips").alias("adjusted_trips"),
        fmin("days_since_last").alias("days_since_last"),
    )

    #  6. Divide trips by quantity threshold to get output trips
    #  7. Divide adjusted by quantity threshold to get output trips
    # force fill threshold (just in case)
    fill_condition = (col("cpn_qty_threshold") == 0) | (
        col("cpn_qty_threshold").isNull()
    )
    coups = coups.withColumn(
        "cpn_qty_threshold",
        when(fill_condition, 1).otherwise(coups.cpn_qty_threshold),
    )
    coups = coups.withColumn("trips", coups.trips / coups.cpn_qty_threshold)
    coups = coups.withColumn(
        "adjusted_trips", coups.adjusted_trips / coups.cpn_qty_threshold
    )
    coups = coups.drop("cpn_qty_threshold")

    return coups


def calculate_member_trips(transactions, item, start, end):
    """Calculate member-trip aggregations.

    Calculates trips per member at all aggregation levels,
    including article, AH4 and AH5.

    Parameters:
        transactions (pyspark.sql.DataFrame): transactions data to use
            REQUIRES: PURCH_DT (date): purchase date
                      SALES_CTGRY_CD - Sales category code, filter limits to merchandise sales
                      MC_CD - Merch category code, filter removes gift cards and cafe
                      SALES_QTY - quantity bought, must be above zero
                      MBRSHP_SID - membership SID
        item (pyspark.sql.DataFrame): item master data to use
        start (datetime.datetime): start date of relevant transactions
        end (datetime.datetime): end date of relevant transactions

    Returns:
        mbr_trips (pyspark.sql.DataFrame): aggregated transactions
            CONTAINS: mbrshp_sid (int) - membership SID
                      category (int) - category code of relevant category
                      trips (int) - sum of trips in relevant category
                      days_since_last (int) - days since last purchase of relevant category
    """
    # filter to trips only in range
    transactions = transactions[transactions.PURCH_DT >= start]
    transactions = transactions[transactions.PURCH_DT <= end]
    transactions = trips_only(transactions)
    transactions = update_categories(transactions, item)

    # we keep the last purchase in a deterministic way
    w = Window.partitionBy("ARTICLE_NBR", "PURCH_HDR_ID").orderBy(
        col("PURCH_DT").desc_nulls_last(),
        col("AH5_CD").desc_nulls_last(),
        col("AH4_CD").desc_nulls_last(),
    )
    transactions = transactions.withColumn("rank", row_number().over(w))
    transactions = transactions.filter(col("rank") == 1).drop("rank")

    transactions = transactions.withColumn("trips", lit(1))
    transactions = transactions.withColumn(
        "days_since_last", datediff(current_date(), "PURCH_DT")
    )
    transactions = transactions.withColumn(
        "mbrshp_sid", transactions.MBRSHP_SID.cast("long")
    )

    # roll-up to cover trips at all levels
    by_art = transactions.select(
        "mbrshp_sid", "ARTICLE_NBR", "trips", "days_since_last"
    )
    by_art = by_art.withColumnRenamed("ARTICLE_NBR", "category")
    by_ah5 = transactions.select(
        "mbrshp_sid", "AH5_CD", "trips", "days_since_last"
    )
    by_ah5 = by_ah5.withColumnRenamed("AH5_CD", "category")
    by_ah4 = transactions.select(
        "mbrshp_sid", "AH4_CD", "trips", "days_since_last"
    )
    by_ah4 = by_ah4.withColumnRenamed("AH4_CD", "category")
    mbr_trips = by_art.union(by_ah5).union(by_ah4)

    _debug_column_sample(
        mbr_trips, "category", "before category cast in calculate_member_trips"
    )
    mbr_trips = mbr_trips.withColumn(
        "category", mbr_trips.category.try_cast("long")
    )
    _debug_column_sample(
        mbr_trips, "category", "after category cast in calculate_member_trips"
    )

    mbr_trips = mbr_trips.dropna(subset=["mbrshp_sid", "category"])

    mbr_trips = mbr_trips.groupBy("mbrshp_sid", "category").agg(
        fsum("trips").alias("trips"),
        fmin("days_since_last").alias("days_since_last"),
    )

    return mbr_trips


def calculate_member_usage(
    transactions, item, cpn_quals, cpn_map, start, end, experiment_id
):
    """Generate file to identify fraction of past coupon qualifying purchases
    made with a coupon.

    Member usage is calculated over the span of the year preceeding the coupon
    start date. Member usage is calculated as:
       purchases: # of distinct PURCH_HEADER_ID in the year which contained
                  a qualifying article
       redemptions: # of distinct PURCH_HEADER_ID in the year which used a
                  coupon on a qualifying article
       fraction_purchases_with_coupon: redemptions / purchases
    """
    transactions = transactions[transactions.PURCH_DT >= start]
    transactions = transactions[transactions.PURCH_DT <= end]
    transactions = trips_only(transactions)
    transactions = update_categories(transactions, item)

    cpns = (
        cpn_quals.filter(cpn_quals.experiment_id == experiment_id)
        .select("cpn_nbr")
        .join(cpn_map, on="cpn_nbr", how="inner")
    ).select("cpn_nbr", "article_nbr")

    purchases = transactions.join(cpns, on="ARTICLE_NBR", how="inner")
    purchases = purchases.select("MBRSHP_SID", "PURCH_HDR_ID", "ARTICLE_NBR")
    redemptions = transactions.filter(
        transactions.DISCOUNT_TYPE_CD.isin("ZPAP", "ZCOU", "ZECM")
    ).select("PURCH_HDR_ID", "ARTICLE_NBR", "DISCOUNT_TYPE_CD")

    usage = purchases.join(
        redemptions, on=["PURCH_HDR_ID", "ARTICLE_NBR"], how="left"
    )

    usage = usage.join(cpns, on="ARTICLE_NBR", how="inner")
    usage = usage.withColumn(
        "used_coupon",
        sqlf.when(usage.DISCOUNT_TYPE_CD.isNotNull(), 1).otherwise(0),
    )
    usage = usage.groupBy("MBRSHP_SID", "cpn_nbr", "PURCH_HDR_ID").agg(
        sqlf.max("used_coupon").alias("used_coupon")
    )
    usage = usage.groupBy("MBRSHP_SID", "cpn_nbr").agg(
        sqlf.count("PURCH_HDR_ID").alias("purchases"),
        sqlf.sum("used_coupon").alias("redemptions"),
    )
    usage = usage.withColumn(
        "fraction_purchases_with_coupon", usage.redemptions / usage.purchases
    )

    usage = usage.select(
        "cpn_nbr",
        "mbrshp_sid",
        "purchases",
        "redemptions",
        "fraction_purchases_with_coupon",
    )
    return usage


def calculate_discount(cpn_prices):
    cpn_prices = cpn_prices.withColumn(
        "min_retail",
        sqlf.split(cpn_prices["Chain_Level_Retail_P"], "-")
        .getItem(0)
        .cast("float"),
    )
    cpn_prices = cpn_prices.withColumn(
        "fraction_discount",
        sqlf.round(sqlf.col("Discount_Value") / sqlf.col("min_retail"), 2),
    )

    cpn_prices = cpn_prices.withColumnRenamed("PMR_Offer_ID", "cpn_nbr")
    return cpn_prices.select("cpn_nbr", "fraction_discount")


def clean_cpg_coupon_file(coupons, coupon_types):
    """Clean CPG inputcoupon file

    Cast and subset columns. Renames columns for forward compatability.

    Parameters:
        coupons (spark.DataFrame): raw input file
        coupon_types (list[str]): list of eligibile coupon types

    Returns:
        coupons (spark.DataFrame): cleaned coupon file

    """
    # 1. process array article columns
    coupons = coupons.withColumn("articles", split(col("Article Number"), ","))
    coupons = (
        coupons.withColumn("articles", explode(col("articles")))
        .withColumn("articles", trim(col("articles")))
        .filter("articles != ''")
    )
    _debug_column_sample(
        coupons, "articles", "before article_nbr cast in clean_cpg_coupon_file"
    )
    coupons = coupons.withColumn("article_nbr", coupons.articles.try_cast("long"))
    _debug_column_sample(
        coupons,
        "article_nbr",
        "after article_nbr cast in clean_cpg_coupon_file",
    )

    # 2. subset and reformat data
    coupons = coupons.select(
        "Promo Description",
        "article_nbr",
        "Discount Value",
        "PMR Offer ID",
        "Quantity Threshold",
        "Eligible for MMPC",
        "Promo Type",
        "Valid From",
        "Valid To",
        "self_funded_flag",
    )
    coupons = coupons.withColumn(
        "cpn_dollar_off", regexp_replace(col("Discount Value"), "$", "")
    )
    # generate "coupon number" from 7 digit PMR ID when it occurs
    coupons = coupons.withColumnRenamed("PMR Offer ID", "cpn_nbr")
    coupons = cast(
        coupons,
        [
            "Eligible for MMPC",
            "article_nbr",
            "Quantity Threshold",
            "dollar_off",
            "cpn_nbr",
        ],
    )
    coupons = coupons.withColumnRenamed("Promo Description", "cpn_desc")
    coupons = coupons.withColumnRenamed(
        "Quantity Threshold", "cpn_qty_threshold"
    )
    coupons = coupons.withColumnRenamed("Valid From", "cpn_start")
    coupons = coupons.withColumnRenamed("Valid To", "cpn_end")
    coupons = coupons.withColumn("offer_id", lit(None))
    coupons = coupons.withColumn("cpn_class_id", lit(None))
    coupons = coupons.withColumn("cpn_dollar_threshold", lit(None))

    # 3. filter and drop extra cols
    coupons = coupons.filter(col("Promo Type").isin(coupon_types))
    coupons = coupons.filter(col("Eligible for MMPC") == 1)
    coupons = coupons.drop(
        "Discount Value", "Promo Type", "PMR Offer ID", "Eligible for MMPC"
    )

    return coupons


def remove_exclusions(
    coupons, cat_dna, article_ah5_ah4, exclusion_types, back_in_coupons
):
    """Find excluded coupons and remove.

    Parameters:
        coupons (pyspark.sql.DataFrame): spark dataframe to apply operation to
            REQUIRES: cpn_nbr (int): coupon number
                      category (int): category id, independent of level
        category_dna (pyspark.sql.DataFrame): category level information
            REQUIRES: EXCLUSION_TYPE (str): type of exclusion
                      INCLUDE_OR_EXCLUDE (int): exclusion marker
                      category (int): category id, independent of level
        article_ah5_ah4 (pyspark.sql.DataFrame): map between article ah5 and ah4
        exclusion_types (list[str]): list of string identifiers of exclusion types to exclude

    Returns:
        coupons (pyspark.sql.DataFrame): coupons with excluded coupons removed
        exclusions_log (pyspark.sql.DataFrame): excluded coupons with removing reasons logged
    """
    # 1. prep cat dna
    # NOTE no special treatment for seasonality, since exlusion is not done by club
    # club-nonspecific seasonal categories are few and generally have lower sales
    # so it is acceptable to remove them all (if excluded), regardless of month
    dna = cat_dna.withColumn("EXCLUSION_TYPE", lower(cat_dna.EXCLUSION_TYPE))
    dna = dna[dna.INCLUDE_OR_EXCLUDE == "exclude"]
    excl_types = [excl_type.lower() for excl_type in exclusion_types]
    dna = dna[dna.EXCLUSION_TYPE.isin(excl_types)]
    dna = dna.select(
        "category", "category_desc", "INCLUDE_OR_EXCLUDE", "EXCLUSION_TYPE"
    )
    dna = dna.join(
        article_ah5_ah4, [dna.category == article_ah5_ah4.article_nbr], "inner"
    )
    # 2. join to coupons to define excluded ones
    all_coup_joined = coupons.join(dna, "category", "left")


    exclusions = all_coup_joined[
        all_coup_joined.INCLUDE_OR_EXCLUDE == "exclude"
    ]
    exclusions_log = (
        exclusions.dropDuplicates(
            subset=[
                "cpn_nbr",
                "cpn_desc",
                "category",
                "category_desc",
                "EXCLUSION_TYPE",
            ]
        )
        .fillna("")
        .select(
            "cpn_nbr",
            "cpn_desc",
            "category",
            "category_desc",
            "ah5_desc",
            "ah4_desc",
            "EXCLUSION_TYPE",
        )
        .orderBy("cpn_nbr")
    )
    exclusions = exclusions.select("cpn_nbr").dropDuplicates()

    # 3. add back force back in coupons if existed

    if back_in_coupons:
        print(
            "Coupons that are forced back in: "
            + ", ".join(map(str, back_in_coupons))
        )
        exclusions = exclusions.filter(
            exclusions.cpn_nbr.isin(*back_in_coupons) == False
        )
        print(
            "Exclusion list after forced back in: \n"
            + ", ".join(
                map(
                    str,
                    exclusions.select(["cpn_nbr"])
                    .distinct()
                    .toPandas()["cpn_nbr"],
                )
            )
        )

    # 4. antijoin to base cupons -- REMOVES BASED ON "ANY" EXCLUDED ITEMS IN COUPON
    coupons = coupons.join(exclusions, "cpn_nbr", "leftanti")
    return (coupons, exclusions_log)


def format_coupons(
    coupons, member_trips, cat_dna, params, couptype, article_map=None
):
    """Format coupons to produce output tables.

    Parameters:
        coupons (pyspark.sql.DataFrame): input coupons dataframe
        member_trips (pyspark.sql.DataFrame): aggregated member-trips for all levels
        cat_dna (pyspark.sql.DataFrame): category dna for all levels
        params (dict): input parameters dictionary
        couptype (str): type of coupon in coupons dataframe
        article_map (pyspark.sql.DataFrame): input article map dataframe, only needed for articlec coupons

    Returns:
        coupons (pyspark.sql.DataFrame): coupon table output, 1 row per coupon
        coupmap (pyspark.sql.DataFrame): coupon map output, 1 row per coupon-category
        memtrips (pyuspark.sql.DataFrame): member-trip output, 1 row per coupon-member
    """
    # calculate relevant seaonality and coupon dates
    coup_month = datetime.strptime(
        params["coupon_inhome_date"], "%Y-%m-%d"
    ).month
    # handle different formatting by type
    #   1. Member trips
    #   2. Coupon Map
    #   3. Coupon Quals
    #   4. Coupon Bank
    if couptype.lower() == "basket":
        # member trips
        memtrips = None

        # coupon map
        coupons = coupons.withColumn("ah5_cd", lit(None))
        coupons = coupons.withColumn("ah4_cd", lit(None))
        coupons = coupons.withColumn("article_nbr", lit(None))
        coupons = coupons.withColumn("cpn_qty_threshold", lit(None))

    else:
        if couptype.lower() == "article":
            coupons = coupons.withColumn("cpn_ah4_cd", lit(None))
            coupons = coupons.withColumn("cpn_ah5_cd", lit(None))
        elif couptype.lower() == "category":
            coupons = coupons.withColumn("article_nbr", lit(None))
            coupons = coupons.withColumn("cpn_qty_threshold", lit(1))
        elif couptype.lower() == "special":
            coupons = coupons.withColumn("cpn_qty_threshold", lit(1))
            coupons = coupons.withColumnRenamed(
                "cpn_article_nbr", "article_nbr"
            )
        else:
            raise ValueError("invalid coupon type!")
        # member trips
        memtrips = generate_coupon_member_trips(
            coupons, member_trips, cat_dna, coup_month
        )
        # coupon map
        coupons = coupons.withColumnRenamed("cpn_ah5_cd", "ah5_cd")
        coupons = coupons.withColumnRenamed("cpn_ah4_cd", "ah4_cd")

    # save coupon map
    coupmap = coupons.select(
        "cpn_nbr", "ah4_cd", "ah5_cd", "article_nbr"
    ).dropDuplicates()

    # quals and bank
    coupons = coupons.drop("ah4_cd", "ah5_cd", "article_nbr")
    coupons = coupons.dropDuplicates()

    # calculate backfill eligible list
    cpn_excl = [
        excl
        for excl in params["backfill"]["cpn_exclusion"]
        if excl is not None
    ]
    if couptype.lower() == "article":
        substi_excl, cpn_excl_log = backfill_eligibility_substitutable(
            memtrips,
            coupmap,
            cpn_excl,
            article_map,
            coupons,
            int(params["experiment"]),
        )
    else:
        substi_excl = []
    print("Exclude coupons for backfilling{}...".format(cpn_excl))
    cpn_incl = [
        incl for incl in params["backfill"]["cpn_force_in"] if incl is not None
    ]
    cpn_excl = [
        cpn for cpn in cpn_excl + substi_excl if int(cpn) not in cpn_incl
    ]

    if len(cpn_excl) > 0:
        backfill_elig = ~(col("cpn_nbr").isin(cpn_excl))
    else:
        backfill_elig = col("cpn_nbr").isNotNull()  # always true
    hero_elig = col("cpn_nbr").isin(params["hero_coupons"])

    if couptype.lower() == "basket":
        # all basket coupons are hero eligible
        coupons = coupons.withColumn("hero_eligible", lit(1))
    elif couptype.lower() == "article":
        coupons = coupons.withColumn(
            "hero_eligible", when(hero_elig, 1).otherwise(0)
        )
    elif couptype.lower() == "category":
        coupons = coupons.withColumn(
            "hero_eligible", when(hero_elig, 1).otherwise(0)
        )
    elif couptype.lower() == "special":
        coupons = coupons.withColumn(
            "hero_eligible", when(hero_elig, 1).otherwise(0)
        )
    else:
        raise ValueError("invalid coupon type!")

    coupons = coupons.withColumn("cpn_type", lit(couptype))
    coupons = coupons.withColumn("experiment_id", lit(params["experiment"]))
    coupons = coupons.withColumn(
        "backfill_eligible", when(backfill_elig, 1).otherwise(0)
    )
    if couptype.lower() == "article":
        final_cpn_excl_log = coupons.select(
            "cpn_nbr", "cpn_desc", "backfill_eligible"
        ).distinct()
        final_cpn_excl_log = final_cpn_excl_log.join(
            cpn_excl_log, ["cpn_nbr"], "left"
        )
        final_cpn_excl_log = final_cpn_excl_log.withColumn(
            "comments",
            when(final_cpn_excl_log.cpn_nbr.isin(cpn_incl), "config forced_in")
            .when(
                final_cpn_excl_log.cpn_nbr.isin(substi_excl),
                final_cpn_excl_log.comments,
            )
            .when(final_cpn_excl_log.cpn_nbr.isin(cpn_excl), "config excluded")
            .when(final_cpn_excl_log.backfill_eligible == 1, "selected")
            .otherwise("weird"),
        )
        final_cpn_excl_log = final_cpn_excl_log.fillna(0).orderBy(
            desc("AH5_CD"), desc("adjusted_trips")
        )
    else:
        final_cpn_excl_log = None
    return (coupons, coupmap, memtrips, final_cpn_excl_log)


def add_coupons(base_coups, new_coups, subset=None):
    """Add coupons to existing dataset.

    Parameters:
        base_coups (pyspark.sql.DataFrame):  exising dataset to add to
        new_coups (pyspark.sql.DataFrame): new dataset to add
            NOTE: base and new datasets must have identcal columns
        subset (list(str), opt): subset of columns to consider from new_coups

    Returns:
        added (pyspark.sql.DataFrame): Combined dataset
    """
    if subset is not None:
        for c in subset:
            if c not in new_coups.columns:
                new_coups = new_coups.withColumn(c, lit(None))
        new_coups = new_coups.select(*subset)
    if set(base_coups.columns) != set(new_coups.columns):
        raise ValueError("datasets have different columns!")
    cols = base_coups.columns
    added = base_coups.select(*cols).union(new_coups.select(*cols))
    return added


def backfill_eligibility_substitutable(
    df, coupmap, excl_list, article_map, coupons, seed
):
    """Apply the substitutable category rule to coupon backfill eligibility.
       If multiple coupons under the same ah5 category, only pick the most popular coupon
       (defined by adjusted trips) into the backfill eligible list.
       If a coupon has article numbers belong to different ah5 categories, pick the ah5 category
       has the highest sales to represent the coupon.

    Parameters:
        df (pyspark.sql.DataFrame):  member coupon trips count
            requires: cpn_nbr, mbrshp_sid, adjusted_trips
        coupmap (pyspark.sql.DataFrame): coupon-article_nbr map
            requires: cpn_nbr, article_nbr
        excl_list (list(int)): a list of coupon number already being excluded
        article_map (pyspark.sql.DataFrame): artical-ah5-mch3-sales map that only under mch3 specified in config
            requires: article_nbr, ah5_cd
        coupons (pyspark.sql.DataFrame): coupons with description
        seed (int): a deterministic unique value to start from when creating an
            order
    Returns:
        substi_excl (list(int)): a list of coupon number being excluded from backfill
        df_excl_log (pyspark.sql.DataFrame): coupon excluded for backfilling with reasons logged
    """

    df = df.groupby("cpn_nbr").agg(
        fsum("adjusted_trips").alias("adjusted_trips")
    )
    df = df.join(
        coupmap.select(["cpn_nbr", "article_nbr"]).distinct(),
        ["cpn_nbr"],
        "left",
    )
    df = df.filter(~df.cpn_nbr.isin(excl_list))
    df_excl = df.join(article_map, ["article_nbr"], "left")
    df_excl = df_excl.filter(df_excl.AH5_CD.isNotNull())
    df_excl = df_excl.groupby(
        "cpn_nbr", "adjusted_trips", "AH5_CD", "AH5_DESC"
    ).agg(fsum("sales").alias("sales"))

    deterministic, order_by, _ = deterministic_df(
        df_excl,
        [col("sales").desc()],
        seed,
        random_only=True,
        extra_hash_columns=[col("AH5_CD")],
    )

    df_excl = top_n(deterministic, 1, order_by, "cpn_nbr", keep=False)

    df_excl = df_excl.withColumn("comments", lit("excluded"))
    df_excl = df_excl.join(coupons, ["cpn_nbr"], "left")
    # include the most popular coupons within each ah5 category into backfill
    # eligible list
    deterministic, order_by, _ = deterministic_df(
        df_excl, [col("adjusted_trips").desc()], seed, random_only=True
    )

    df_incl = top_n(deterministic, 1, order_by, "AH5_CD", keep=False)
    substi_incl = (
        df_incl.select("cpn_nbr").distinct().toPandas().cpn_nbr.tolist()
    )
    substi_excl = (
        df_excl.select("cpn_nbr").distinct().toPandas().cpn_nbr.tolist()
    )
    substi_excl = [cpn for cpn in substi_excl if cpn not in substi_incl]

    df_incl = df_incl.withColumnRenamed("cpn_nbr", "selected_cpn_nbr")
    df_incl = df_incl.withColumnRenamed("cpn_desc", "selected_cpn_desc")
    df_excl = df_excl.join(
        df_incl.select(["selected_cpn_nbr", "selected_cpn_desc", "AH5_CD"]),
        ["AH5_CD"],
        "left",
    )
    df_excl_log = df_excl.withColumn(
        "comments",
        when(
            df_excl.cpn_nbr == df_excl.selected_cpn_nbr, "selected"
        ).otherwise(
            concat(
                lit("excluded because substitutable by:"),
                df_excl.selected_cpn_desc,
            )
        ),
    )
    df_excl_log = df_excl_log.select(
        "cpn_nbr", "AH5_CD", "AH5_DESC", "comments", "adjusted_trips"
    )

    df_excl_log = df_excl_log.fillna("")
    return (substi_excl, df_excl_log)
