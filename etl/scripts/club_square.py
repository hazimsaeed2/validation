import argparse
import datetime as dt
import logging
import os
import pprint

import pe_memberdna.lib.misc as misc
import pe_memberdna.etl.lib.validations_ETL as validations
import pyspark.sql.functions as F
from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.utility import *
from pyspark.sql.window import Window as W


def load_paths(job, data_paths):

    drop_cols = {
        "SITE_NAME_2",
        "ADDR_LINE_2",
        "CITY_NAME",
        "STATE_CD",
        "ZIP_CD",
        "ZN_NBR",
        "RGN_NBR",
        "SITE_TYPE",
        "COMP_STTS",
        "FIRST_FW_HAS_GAS",
    }

    detail = job.spark.read.load(data_paths["intermediate"]["detail_fiscal"])
    item_with_brand = job.spark.read.load(
        data_paths["intermediate"]["item_with_brand"]
    )
    detail = add_brand_to_detail(item_with_brand, detail)
    club = job.spark.read.load(data_paths["intermediate"]["club"]).drop(
        *drop_cols
    )

    return detail, club


def add_brand_to_detail(item_with_brand, detail):
    """
    Adds BRAND_CD and BRAND_DESC to detail table.
    May need to be removed is detail creation is modified.

    Args:
        item_with_brand: item table with brand info
        detail: detail table

    Returns:
        detail_with_brand: detail table with brand info
    """
    art_and_brand = item_with_brand.select(
        "ARTICLE_NBR", "BRAND_CD", "BRAND_DESC"
    )
    art_and_brand = art_and_brand.withColumn(
        "BRAND_CD", F.col("BRAND_CD").cast("string")
    )
    art_and_brand = art_and_brand.dropDuplicates(["ARTICLE_NBR"])
    detail = detail.join(art_and_brand, "ARTICLE_NBR", "left")
    return detail


def club_category_month(club_square, detail, categories, max_date, is_monthly):
    """
    Creates a club skeleton. Will create monthly club skeleton if it
    is required.

    Args:
        club (job.spark.sql.Dataframe): the club intermediate
        detail(job.spark.sql.Dataframe): the detail intermediate
        categories(job.spark.sql.Dataframe): the list of categories to compute has_categories for
        max_date (str): The maximum date the dataset contains
        is_monthly (boolean): Whether or not the output will have month granularity

    Returns:
        club_square (job.spark.sql.Dataframe): A skeleton of the club square
        detail_filtered (job.spark.sql.Dataframe): Detail intermediate filtered properly
    """

    # Filter for the most recent year
    filter_date = (
        dt.datetime.strptime(max_date, "%Y-%M-%d") - dt.timedelta(days=365)
    ).strftime("%Y-%M-%d")

    detail = (
        detail.filter(detail.FISCAL_WEEK_END >= filter_date)
        .withColumn("YEAR", F.year(detail.FISCAL_WEEK_END))
        .withColumn("MONTH", F.month(detail.FISCAL_WEEK_END))
    )

    # If monthly granluarity cross join years and months
    if is_monthly:
        months = detail.select("MONTH").distinct()
        years = detail.select("YEAR").distinct()
        club_square = club_square.crossJoin(years).crossJoin(months)

    # Recursively build the skeleton
    def category_frames(i, categories, frame):
        if i == len(categories):
            return frame

        cat = categories[i]["col"]
        cat_frame = (
            detail.select(cat)
            .filter(detail[cat].isNotNull() & (detail[cat] != " "))
            .distinct()
            .withColumnRenamed(cat, "CATEGORY")
            .withColumn("CATEGORY_LVL", F.lit(cat).cast("string"))
        )

        if frame == None:
            return category_frames(i + 1, categories, cat_frame)
        else:
            return category_frames(i + 1, categories, frame.union(cat_frame))

    cat_frame = category_frames(0, categories, None)

    return club_square.crossJoin(cat_frame), detail


def has_categories(detail, club_square, categories, is_monthly):
    """
    Calculates whether or not club sells a certain category of product
    based on total sales threshold for an entire year. Adds two types
    of features to the club square: HAS_{category}_{CD} and SALES_{category}_{CD}.
    HAS_{category}_{CD} is either a 1 or 0. 1 means that the club sells the
    category and 0 implies that they do not. SALES_{category}_{CD} is the total
    dollar amount of sales in that category for that club in the last 52 weeks.

    Args:
        detail (job.spark.sql.DataFrame): The detail intermediate
        club_square (job.spark.sql.DataFrame): The current club square
        categories (dict): Dictionary of all the categories has_categories needs to be computed for
        is_monthly (boolean): Indicates month granularity

    Returns:
        club_square (job.spark.sql.Dataframe): The club square with the has_category features
    """

    def category_aggregation(
        club_square, detail, cat_col, threshold, is_monthly
    ):
        """
        Determines whether or not club has a certain category based on
        predetermined threshold.
        """
        monthly_cols = {"SITE_NBR", "MONTH", "YEAR", "CATEGORY"}

        standard_cols = {"SITE_NBR", "CATEGORY"}

        cols = monthly_cols if is_monthly else standard_cols

        tot_purchased = (
            detail.withColumn("CATEGORY", detail[cat_col].cast("string"))
            .filter(F.col("CATEGORY").isNotNull() & (detail[cat_col] != " "))
            .groupby(*cols)
            .agg(F.sum("EXTENDED_PRC_AMT").alias("SALES"))
        )

        return tot_purchased

    def join_category_aggregations(club_square, is_month, tot_purchased):
        """ """
        monthly_cols = {"SITE_NBR", "MONTH", "YEAR", "CATEGORY"}

        standard_cols = {"SITE_NBR", "CATEGORY"}

        cols = monthly_cols if is_monthly else standard_cols

        return club_square.join(tot_purchased, list(cols), "left_outer")

    def build_when_otherwise(i, categories):
        """
        Recusively builds when otherwise statement.
        """
        category = categories[i]

        sales_col = F.col("SALES")
        category_lvl_col = F.col("CATEGORY_LVL")

        if i == len(categories) - 1:
            return F.when(
                (category_lvl_col == category["col"])
                & (sales_col > category["threshold"]),
                1,
            ).otherwise(0)

        return F.when(
            (category_lvl_col == category["col"])
            & (sales_col > category["threshold"]),
            1,
        ).otherwise(build_when_otherwise(i + 1, categories))

    has_category_cols = []

    tot_purchased = None

    for category in categories:
        if tot_purchased == None:
            tot_purchased = category_aggregation(
                club_square,
                detail,
                category["col"],
                category["threshold"],
                is_monthly,
            )
        else:
            tot_purchased_cat = category_aggregation(
                club_square,
                detail,
                category["col"],
                category["threshold"],
                is_monthly,
            )
            tot_purchased = tot_purchased.union(tot_purchased_cat)

    club_square = join_category_aggregations(
        club_square, is_monthly, tot_purchased
    )

    has_category_cond = build_when_otherwise(0, categories)

    club_square = club_square.withColumn("HAS_CATEGORY", has_category_cond)

    club_square = club_square.fillna(0, subset=["HAS_CATEGORY"]).filter(
        club_square.CATEGORY.isNotNull()
    )

    return club_square


def main(job, data_paths, club_square_config, config_validation):

    logging.info("Starting processing table club_square")

    # Parameters
    categories = club_square_config["categories"]

    max_date = club_square_config.get("max_date")

    if max_date is None:
        _, max_date = misc.get_last_fiscal_weekend()
        logging.info("max_date = " + max_date)

    is_monthly = club_square_config["is_monthly"]

    detail, club = load_paths(job, data_paths)

    club_square, detail_filtered = club_category_month(
        club, detail, categories, max_date, is_monthly
    )

    # Has Categories
    club_square = has_categories(
        detail_filtered, club_square, categories, is_monthly
    )
    club_square.dropDuplicates()
    # Write Club Square
    dest_path = data_paths["intermediate"]["club_square"]

    validations.validate_table(
        job.spark,
        "intermediate",
        "club_square",
        config_validation,
        club_square,
    )

    logging.info("Saving the intermediate file " + dest_path)
    club_square.repartition(1).write.parquet(dest_path, mode="overwrite")


job = JobManager("Club_square")
parser = argparse.ArgumentParser()

parser.add_argument(
    "--config_path",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../configs/config.yaml",
    ),
    help=(
        """
        path to the config file
        """
    ),
)
args = parser.parse_args()
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)

main(job, data_paths, club_square_config, config_validation)
