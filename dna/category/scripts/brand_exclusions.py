"""Ad hoc script to generate full brand exclusions from inhertiance rules.

TODO:
    - integrate this more fully with the rest of the category_square
"""


# ----- IMPORTS ----- #
import argparse
import os

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    when,
    row_number,
    desc,
    countDistinct,
    first,
    col,
    lit,
    coalesce,
)
from pyspark.sql.functions import sum as fsum
from pyspark.sql.types import StringType, StructField, LongType, StructType
import yaml


# ----- CONSTANTS ----- #


# RAW input schemas
BRAND_SCHEMA = StructType(
    [
        StructField("ARTICLE_NBR", LongType(), True),
        StructField("CASE_EXPRESSION", StringType(), True),
        StructField("BRAND", StringType(), True),
    ]
)

# output schemas
EXCLUSION_SCHEMA = StructType(
    [
        StructField("CATEGORY_TYPE", StringType(), False),
        StructField("CATEGORY_CD", LongType(), False),
        StructField("CATEGORY_DESCRIPTION", StringType(), False),
        StructField("INCLUDE_OR_EXCLUDE", StringType(), False),
        StructField("EXCLUSION_TYPE", StringType(), False),
        StructField("EXCLUSION_SUBTYPE", StringType(), False),
        StructField("SEASON_MONTH_1", LongType(), False),
        StructField("SEASON_MONTH_2", LongType(), False),
        StructField("SEASON_MONTH_3", LongType(), False),
        StructField("SEASON_MONTH_4", LongType(), False),
        StructField("SEASON_MONTH_5", LongType(), False),
        StructField("SEASON_MONTH_6", LongType(), False),
        StructField("SEASON_MONTH_7", LongType(), False),
        StructField("SEASON_MONTH_8", LongType(), False),
        StructField("SEASON_MONTH_9", LongType(), False),
        StructField("SEASON_MONTH_10", LongType(), False),
        StructField("SEASON_MONTH_11", LongType(), False),
        StructField("SEASON_MONTH_12", LongType(), False),
    ]
)


# ----- HELPERS ----- #


def load_config():
    """Read in configuration file.

    Reads in confuguration file given path
    and retuns dictionary of full config.

    Parameters:
        path (str): local path to config file

    Returns:
        cfg (dict): dictionary representation of config
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_path",
        type=str,
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../configs/brand_excl_conf.yaml",
        ),
        help=(
            """
            path to the config file
            """
        ),
    )
    args = parser.parse_args()
    cfg_path = args.config_path
    with open(cfg_path, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    return cfg


def generate_seasonal_colnames(prefix):
    """Generate list of seasonal column names with prefix.

    Parameters:
        prefix (str): prefix to seasonal column name

    Returns:
        snl_cols (list[str]): list of column names
    """
    month_ints = [str(i + 1) for i in range(12)]
    snl_cols = [prefix + month_int for month_int in month_ints]
    return snl_cols


def preprocess_transactions(trans):
    """Filter and reshape transactions.

    Convenience wrapper for transaction preprocessing to
    calculate in-club sales by article in fy17.

    Parameters:
        trans (pyspark.sql.DataFrame): transaction detail intermediates

    Returns:
        trans (pyspark.sql.DataFrame): sales by article

    """
    in_store = col("SALES_CTGRY_CD") == "03"
    non_return = col("EXTENDED_PRC_AMT") > 0
    fy17 = (col("FISCAL_WEEK_END") > lit("2017")) & (
        col("FISCAL_WEEK_END") < lit("2018")
    )
    valid_trans = in_store & non_return & fy17
    in_store_year = trans[valid_trans]
    by_art = in_store_year.groupBy("ARTICLE_NBR").agg(
        fsum("EXTENDED_PRC_AMT").alias("FY18_SALES")
    )
    return by_art


def get_top_ah5_cat(branddata):
    """Calculate most representative AH5 category.

    Calculates representative category by using sales-weighted
    count of articles in that category. This can be used in future
    calcuations to represent the brand.

    Parameters:
        branddata (pyspark.sql.DataFrame): dataframe (brand-article level) containing
            AH5_CD (int): AH5 category code
            INCLUDE_OR_EXCLUDE (str): inclusion indicator for AH5 category
            BRAND (str): name of brand for arcile
            ARTICLE_NBR (int): article number
            FY18_SALES (float): sales of that article in 1 year period
    Returns:
        branddate (pyspark.sql.DataFrame): dataframe with added top category info

    """
    group_col = "AH5_CD"
    incl_col = "INCLUDE_OR_EXCLUDE"

    # group and count
    sales_by_brand = brand.groupBy("BRAND").agg(
        fsum("FY18_SALES").alias("total_sales")
    )
    by_lvl = brand.groupBy("BRAND", group_col, incl_col).agg(
        countDistinct("ARTICLE_NBR").alias("count"),
        fsum("FY18_SALES").alias("sales"),
    )
    by_lvl = by_lvl.join(sales_by_brand, "BRAND", "left")
    by_lvl = by_lvl.withColumn("pct_sales", col("sales") / col("total_sales"))
    by_lvl = by_lvl.withColumn(
        "weighted_count", col("count") * col("pct_sales")
    )

    # window to get top brand
    w = Window.partitionBy("BRAND")
    by_lvl = by_lvl.withColumn(
        "rn", row_number().over(w.orderBy(desc("weighted_count")))
    )
    by_lvl = by_lvl[by_lvl.rn == 1]

    # clean up
    by_lvl = by_lvl.drop("rn")
    count_name = "TOP_CAT_ART_CT"
    by_lvl = by_lvl.withColumnRenamed("count", count_name)
    topcat_name = "TOP_CAT"
    by_lvl = by_lvl.withColumnRenamed(group_col, topcat_name)
    incl_name = "TOP_CAT_INCL"
    by_lvl = by_lvl.withColumnRenamed(incl_col, incl_name)
    by_lvl = by_lvl.drop("sales", "total_sales", "pct_sales", "weighted_count")

    # join
    branddata = branddata.join(by_lvl, "BRAND", "left")
    return branddata


def calc_stats_by_brand(branddata, cat_dna):
    """Calculate by-brand stats for inheritance.


    Calculate percentage of coverage by top category and by top
    inclusion/exclusion flag to help determine eligibility for
    inheritance.

    Parameters:
        branddata (pyspark.sql.DataFrame): brand-category dataframe with:
        cat_dna (pyspark.sql.DataFrame): category DNA containing AH5 level info

    Returns:
        by_brand (pyspark.sql.DataFrame): dataframe aggregated to brand level
    """
    # group data
    by_brand = branddata.groupBy("BRAND").agg(
        countDistinct("ARTICLE_NBR").alias("N_ART"),
        countDistinct("AH5_CD").alias("N_AH5"),
        countDistinct("INCLUDE_OR_EXCLUDE").alias("MIXED_EXCL"),
        fsum("FY18_SALES").alias("FY18_SALES"),
        first("TOP_CAT").alias("TOP_CAT"),
        first("TOP_CAT_INCL").alias("TOP_CAT_INCL"),
        first("TOP_CAT_ART_CT").alias("TOP_CAT_ART_CT"),
    )

    # Remove nulls brands (defensive)
    by_brand = by_brand[by_brand.BRAND.isNotNull()]

    # flag brands with mixed exclude/include flags
    mixed = col("MIXED_EXCL") == 2
    by_brand = by_brand.withColumn("MIXED_EXCL", when(mixed, 1).otherwise(0))

    # calculate article coverage of top category
    by_brand = by_brand.withColumn(
        "PCT_TOP_COVG", by_brand.TOP_CAT_ART_CT / by_brand.N_ART
    )

    #  join back on type and subtype for top category exclusions
    reasons = cat_dna.drop("INCLUDE_OR_EXCLUDE")
    reasons = reasons.withColumnRenamed("AH5_CD", "TOP_CAT")
    by_brand = by_brand.join(reasons, "TOP_CAT", "left")

    # calculatecoverage by inclusion type
    by_incl = (
        branddata.groupBy("BRAND")
        .pivot("INCLUDE_OR_EXCLUDE")
        .agg(countDistinct("ARTICLE_NBR").alias("count"))
    )
    by_incl = by_incl.fillna(0)
    if "exclude" in by_incl.columns and "include" in by_incl.columns:
        by_incl = by_incl.withColumn(
            "total",
            coalesce(col("exclude"), lit(0))
            + coalesce(col("include"), lit(0)),
        )
    elif "exclude" in by_incl.columns:
        by_incl = by_incl.withColumn(
            "total",
            coalesce(col("exclude"), lit(0)),
        )
    elif "include" in by_incl.columns:
        by_incl = by_incl.withColumn(
            "total",
            coalesce(col("include"), lit(0)),
        )
    else:
        by_incl = by_incl.withColumn("total", lit(0))

    by_brand = by_brand.join(by_incl, "BRAND", "left")
    is_excluded = col("TOP_CAT_INCL") == "exclude"
    excluded_pct = coalesce(col("exclude"), lit(0)) / col("total")
    included_pct = coalesce(col("include"), lit(0)) / col("total")
    by_brand = by_brand.withColumn(
        "INCL_COVG", when(is_excluded, excluded_pct).otherwise(included_pct)
    )
    by_brand = by_brand.drop("exclude", "include", "total")

    return by_brand


def include_brands(branddata, included_brands):
    """Mark brands to be included.

    Parameters:
        branddata (pyspark.sql.DataFrame): dataframe of UNKNOWN brand data
        included_brands (pyspark.sql.DataFrame): dataframe of ONLY brands to include

    Returns:
        branddata (pyspark.sql.DataFrame): dataframe with included brands removed
    """
    # handle possible alternate column naming
    if "BRAND" not in included_brands.columns:
        included_brands = included_brands.withColumnRenamed(
            "CATEGORY_DESCRIPTION", "BRAND"
        )
    branddata = branddata.join(included_brands, "BRAND", "left_anti")
    return branddata


def exclude_brands(branddata, exclusions, excluded_brands, reshape=True):
    """Mark brands to be excluded.

    Parameters:
        branddata (pyspark.sql.DataFrame): dataframe of UNKNOWN brand data
        exclusions (pyspark.sql.DataFrame): dataframe of EXCLUDED brands
        included_brands (pyspark.sql.DataFrame): dataframe of ONLY brands to exclude
        reshape (bool): whether or not to reshape data before creating output

    Returns:
        branddata (pyspark.sql.DataFrame): dataframe with excluded brands removed
        exclusions (pyspark.sql.DataFrame): exclusion dataframe with excluded brands added
    """
    # handle alternate column name (non-reshape only)
    if "BRAND" not in excluded_brands.columns:
        excluded_brands = excluded_brands.withColumn(
            "BRAND", excluded_brands.CATEGORY_DESCRIPTION
        )

    branddata = branddata.join(excluded_brands, "BRAND", "left_anti")

    if reshape:
        # reshape exclusions to match schema to add to exclusions
        col_prefix = "SEASON_MONTH_"
        snl_cols = generate_seasonal_colnames(col_prefix)
        base_cols = [
            "BRAND",
            "TOP_CAT_INCL",
            "EXCLUSION_TYPE",
            "EXCLUSION_SUBTYPE",
        ]
        select_cols = base_cols + snl_cols
        reshaped_excludes = excluded_brands.select(select_cols)

        reshaped_excludes = reshaped_excludes.withColumn(
            "CATEGORY_TYPE", lit("brand")
        )
        reshaped_excludes = reshaped_excludes.withColumn(
            "CATEGORY_DESCRIPTION", reshaped_excludes.BRAND
        )
        reshaped_excludes = reshaped_excludes.withColumnRenamed(
            "BRAND", "CATEGORY_CD"
        )

        # we use the top category as template for exclusion type and reasoning
        reshaped_excludes = reshaped_excludes.withColumnRenamed(
            "TOP_CAT_INCL", "INCLUDE_OR_EXCLUDE"
        )
        final_col_order = [
            "CATEGORY_TYPE",
            "CATEGORY_CD",
            "CATEGORY_DESCRIPTION",
            "INCLUDE_OR_EXCLUDE",
            "EXCLUSION_TYPE",
            "EXCLUSION_SUBTYPE",
        ]
        final_cols = final_col_order + snl_cols
        reshaped_excludes = reshaped_excludes.select(final_cols)

    else:
        reshaped_excludes = excluded_brands.drop("BRAND")

    # join to exclusions
    exclusions = exclusions.union(reshaped_excludes)
    return (branddata, exclusions)


# ----- SCRIPT ----- #


if __name__ == "__main__":

    # 1. set up spark session
    name = "BrandInheritance"
    conf = SparkConf().setAppName(name)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 2. parse config
    cnf = load_config()
    paths = cnf["paths"]

    format_dict = {
        "output_prefix": cnf.get("output_prefix", "default"),
    }

    for path in paths.keys():
        paths[path] = paths[path].format(**format_dict)
    params = cnf["params"]

    # 3. read and subset input data
    print("reading and preprocessing data..")

    brand_data = spark.read.parquet(paths["brand"])
    brand_data = brand_data.select("ARTICLE_NBR", "BRAND")

    item_mstr = spark.read.parquet(paths["item"])
    item_mstr = item_mstr.select("ARTICLE_NBR", "ARTICLE_DESC", "AH5_CD")

    cat_dna = spark.read.parquet(paths["cat_dna"])
    prefix = "SEASON_MONTH_"
    snl_cols = generate_seasonal_colnames(prefix)
    base_cols = [
        "AH5_CD",
        "AH5_DESC",
        "INCLUDE_OR_EXCLUDE",
        "EXCLUSION_TYPE",
        "EXCLUSION_SUBTYPE",
    ]
    selection_cols = base_cols + snl_cols
    cat_dna = cat_dna.select(*selection_cols)

    transactions = spark.read.parquet(paths["transactions"])
    transactions = transactions.select(
        "ARTICLE_NBR", "SALES_CTGRY_CD", "FISCAL_WEEK_END", "EXTENDED_PRC_AMT"
    )

    manually_mapped = spark.read.csv(
        paths["exclusions"], header="true", schema=EXCLUSION_SCHEMA
    )

    # 4. Preprocess data
    transactions = preprocess_transactions(transactions)
    brand_heirarchy = brand_data.join(item_mstr, "ARTICLE_NBR", "left")
    brand_withdna = brand_heirarchy.join(cat_dna, "AH5_CD", "left")
    brand_withtrans = brand_withdna.join(transactions, "ARTICLE_NBR", "left")
    brand = brand_withtrans.fillna("include", subset=["INCLUDE_OR_EXCLUDE"])
    brand = brand.fillna(0, subset=["FY18_SALES"])
    brand = get_top_ah5_cat(brand)
    by_brand = calc_stats_by_brand(brand, cat_dna)
    by_brand.persist(StorageLevel.DISK_ONLY)

    print("total brands: ", by_brand.count())

    # 5. determine brands to inherit
    exclusions = spark.createDataFrame([], schema=EXCLUSION_SCHEMA)

    # 5a. include own brands
    ownbrand = ["BERKLEY JENSEN", "WELLSLEY FARMS", "UNBRANDED"]
    own_brands = by_brand[by_brand.BRAND.isin(ownbrand)]
    own_brands.cache()
    by_brand = include_brands(by_brand, own_brands)
    by_brand.cache()
    print("own brands: ", own_brands.count())
    print("remaining brands: ", by_brand.count())
    print("")

    # 5b. include / exclude single rule brands
    single_rule = col("MIXED_EXCL") == 0
    included = col("TOP_CAT_INCL") == "include"
    excluded = col("TOP_CAT_INCL") == "exclude"
    single_rule_includes = by_brand[single_rule & included]
    single_rule_excludes = by_brand[single_rule & excluded]
    single_rule_includes.cache()
    single_rule_excludes.cache()
    by_brand = include_brands(by_brand, single_rule_includes)
    by_brand, exclusions = exclude_brands(
        by_brand, exclusions, single_rule_excludes
    )
    by_brand.cache()
    exclusions.cache()
    print("single rule inclusions: ", single_rule_includes.count())
    print("single rule exclusions: ", single_rule_excludes.count())
    print("excluded brands: ", exclusions.count())
    print("remaining brands: ", by_brand.count())
    print("")

    # 5c. include/exclude threshold rule brands
    mixed_rule = ~single_rule
    pure_mixed = (col("INCL_COVG") >= params["threshold"]) & mixed_rule
    pure_mixed_includes = by_brand[pure_mixed & included]
    pure_mixed_excludes = by_brand[pure_mixed & excluded]
    pure_mixed_includes.cache()
    pure_mixed_excludes.cache()
    by_brand = include_brands(by_brand, pure_mixed_includes)
    by_brand, exclusions = exclude_brands(
        by_brand, exclusions, pure_mixed_excludes
    )
    by_brand.cache()
    exclusions.cache()
    print("'pure' mixed-rule inclusions: ", pure_mixed_includes.count())
    print("'pure' mixed-rule exclusions: ", pure_mixed_excludes.count())
    print("excluded brands: ", exclusions.count())
    print("remaining brands: ", by_brand.count())
    print("")

    # 5d. handle true mixed excludes manually
    excl = col("INCLUDE_OR_EXCLUDE") == "exclude"
    incl = ~col("INCLUDE_OR_EXCLUDE").isNotNull()
    manual_includes = manually_mapped[incl]
    manual_excludes = manually_mapped[excl]
    by_brand = include_brands(by_brand, manual_includes)
    by_brand, exclusions = exclude_brands(
        by_brand, exclusions, manual_excludes, reshape=False
    )
    by_brand.cache()
    exclusions.cache()
    print("manual mapped inclusions: ", manual_includes.count())
    print("manual mapped exclusions: ", manual_excludes.count())
    print("excluded brands: ", exclusions.count())
    print("remaining brands: ", by_brand.count())
    print("")

    # 6. write output
    excl_path = paths["output"] + "/exclusions"
    exclusions = exclusions.repartition(1)
    exclusions.write.csv(excl_path, header=True, mode="overwrite")

    if by_brand.count() > 0:
        extra_path = paths["output"] + "/unmatched_brands"
        by_brand = by_brand.repartition(1)
        by_brand.write.csv(extra_path, header=True, mode="overwrite")
