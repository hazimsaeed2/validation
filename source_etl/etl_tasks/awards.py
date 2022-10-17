import logging

from memberdna.source_etl.utils.utility import *


def main(spark, data_paths, config_validation):
    """
    Reads the source csv awards table and converts it to the parquet format

    Args:
        spark - spark session object
        data_paths - dict structure containing the source and intermediate paths
        config_validation - dict structure containing the DQ_check configuration
    Returns:
    """

    source_path = data_paths["source"].get("awards")

    if not source_path:
        return

    logging.info("Starting processing table awards")

    cast_sql = """
    select
        CAST(MBRSHP_SID AS int) as MBRSHP_SID,
        AWRD_CERT_NBR,
        CAST(AWRD_CERT_AMT as double) as AWRD_CERT_AMT,
        CAST(AWRD_CERT_ISSUE_DT as date) as AWRD_CERT_ISSUE_DT,
        CAST(AWRD_CERT_EXP_DT as date) as AWRD_CERT_EXP_DT,
        AWRD_CERT_RDMPTN_CD,
        AWRD_PROMO_ID
    from df
    """

    dest_path = data_paths["intermediate"]["awards"]
    repartition_val = "AWRD_CERT_ISSUE_DT"

    createSchemaParquet(
        spark,
        source_path,
        config_validation,
        "awards",
        cast_sql,
        dest_path,
        repartition_val,
        sep=",",
        header=True,
    )
