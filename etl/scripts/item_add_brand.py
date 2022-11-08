import argparse
import logging
import os

from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.utility import *
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def main(job, data_paths, config_validation):
    logging.info("Starting processing table item_with_brand")

    item_path = data_paths["intermediate"]["item"]
    brand_path = data_paths["intermediate"]["brand"]
    ah5_path = data_paths["intermediate"]["AH5"]
    dest_path = data_paths["intermediate"]["item_with_brand"]

    item = getDf(
        job.spark,
        item_path,
        config_validation,
        "item",
        "parquet",
        sep=None,
        header=None,
        quote=None,
    )
    brands = getDf(
        job.spark,
        brand_path,
        config_validation,
        "brand",
        "parquet",
        sep=None,
        header=None,
        quote=None,
    )
    ah5 = getDf(
        job.spark,
        ah5_path,
        config_validation,
        "AH5",
        "parquet",
        sep=None,
        header=None,
        quote=None,
    )

    repartition_val = 1

    window = Window.orderBy(col("BRAND_DESC").asc())

    distinct_brands = (
        brands.drop("CASE_EXPRESSION", "ARTICLE_NBR")
        .withColumnRenamed("BRAND", "BRAND_CD")
        .withColumn("BRAND_DESC", col("BRAND_CD"))
        .distinct()
    )

    brands_join = distinct_brands.drop("BRAND_CD").withColumn(
        "rn", row_number().over(window)
    )

    # could make dynamic to make sure it's always greater than, but the count just needs to always be greater than number of distinct brands
    ah5_join = ah5
    for i in range(1, 5):
        ah5_join = ah5_join.union(ah5_join)

    assert (
        ah5_join.count() > brands_join.select("brand_desc").distinct().count()
    )

    ah5_window = Window.orderBy(col("AH5_CD").asc())

    ah5_join = ah5_join.withColumn("rn", row_number().over(ah5_window))

    final = (
        brands_join.distinct()
        .join(ah5_join.drop("ah5_cd", "ah5_desc"), "rn")
        .select(
            ["rn", "BRAND_DESC"]
            + ah5_join.drop("ah5_cd", "ah5_desc", "rn").columns
        )
        .withColumnRenamed("rn", "BRAND_CD")
    )

    item_with_brand = (
        brands.drop("CASE_EXPRESSION")
        .join(brands_join, col("BRAND") == col("BRAND_DESC"))
        .withColumnRenamed("rn", "BRAND_CD")
        .drop("brand")
    )

    final_parquet_df = item.join(item_with_brand, "ARTICLE_NBR", "left")

    validations.validate_table(
        job.spark,
        "intermediate",
        "item_with_brand",
        config_validation,
        final_parquet_df,
    )

    logging.info("Saving the intermediate file " + dest_path)
    final_parquet_df.repartition(1).write.save(dest_path, mode="overwrite")

    ah5.unpersist()


job = JobManager("item_add_brand")
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
main(job, data_paths, config_validation)
