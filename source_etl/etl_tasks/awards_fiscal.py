import logging

from memberdna.source_etl.utils.utility import *


def main(spark, data_paths, config_validation):
    """
    Reads the intermediate awards table and converts and partitions it with
    respect to fiscal week

    Args:
        spark - spark session object
        data_paths - dict structure containing the source and intermediate paths
        config_validation - dict structure containing the DQ_check configuration
    Returns:
    """

    awards_intermediate_path = data_paths["intermediate"].get("awards")

    if not awards_intermediate_path:
        return

    logging.info("Starting processing table awards")

    fiscal_days = spark.read.parquet(data_paths["intermediate"]["fiscal_days"])
    fiscal_days.registerTempTable("fiscal_days")

    awards = spark.read.parquet(awards_intermediate_path)
    awards.registerTempTable("awards")

    sql = """
    select
        aw.*,
        f.FISCAL_WEEK_END as FISCAL_WEEK_END
    from
        awards as aw
    inner join
        fiscal_days f
    on
        aw.AWRD_CERT_ISSUE_DT = f.FISCAL_DAY
    """
    awards_fiscal = spark.sql(sql)

    awards_fiscal.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["awards_fiscal"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )
