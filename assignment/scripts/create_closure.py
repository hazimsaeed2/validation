"""
Creates the initial file containing members, coupons
and their eligability/ineligibility
"""


import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
from pemember_dna.pipelines.assignment.lib.assn_io import JobManager
from pemember_dna.pipelines.assignment.lib.assn_utils import update_categories
from pyspark.sql.window import Window

CAT_LIST = "closure_ah5_cd"
CAT_HRRCHY = "AH5_CD"


def read_required_tbls(job):
    """
    Read the tables required by this script

    Args:
    job (JobManager object)

    Returns:
    """

    job.data.read("transactions", "TRANSACTIONS_PATH", filetype="parquet")
    job.data.read("item", "ITEM_MASTER", filetype="parquet")
    job.data.read("closure_input", "CLOSURE_COUPON_PATH", filetype="csv")
    job.data.read("member_extended", "RAW_MEMBER", filetype="parquet")
    job.data.read("mail_list", "MAIL_LIST", filetype="csv")

    job.data.tables["mail_list"] = job.data.tables["mail_list"].select(
        "MBRSHP_NBR"
    )
    job.data.tables["member_extended"] = job.data.tables[
        "member_extended"
    ].select("MBRSHP_SID", "MBRSHP_NBR")
    job.data.tables["closure_input"] = job.data.tables["closure_input"].select(
        "PMR Offer ID", CAT_LIST, "closure_window"
    )
    job.data.tables["transactions"] = job.data.tables["transactions"].select(
        "PURCH_HDR_ID",
        "QTY_IN_UNITS",
        "PURCH_DT",
        "MBRSHP_SID",
        CAT_HRRCHY,
        "ARTICLE_NBR",
    )


def process_tbls(job):
    """
    Cross join mail_list on the closure_input table and
    calculate transactions that took place for every
    combination of category and memebrship SID

    Args:
    job (JobManager object)

    Returns:
    """

    closure_input = job.data.tables["closure_input"]
    detail = job.data.tables["transactions"]
    item = job.data.tables["item"]
    mail_list = job.data.tables["mail_list"]
    members = job.data.tables["member_extended"]

    closure_ahcd_list = set()
    for row in closure_input.select(CAT_LIST).collect():
        closure_ahcd_list.update(row[CAT_LIST].replace(" ", "").split(","))

    closure_input = (
        closure_input.withColumnRenamed("PMR Offer ID", "cpn_nbr")
        .withColumn(CAT_HRRCHY, sqlf.explode(sqlf.split(CAT_LIST, ",")))
        .withColumn(CAT_HRRCHY, sqlf.trim(sqlf.col(CAT_HRRCHY)))
        .withColumn(CAT_HRRCHY, sqlf.col(CAT_HRRCHY).cast(sqlt.LongType()))
    )

    mail_list = mail_list.join(members, "MBRSHP_NBR", "inner").drop(
        "MBRSHP_NBR"
    )

    mail_list_with_cls_offers = mail_list.crossJoin(closure_input)

    max_lookback_wks = closure_input.agg(sqlf.max("closure_window")).collect()[
        0
    ][0]
    lookback_days = 7 * int(max_lookback_wks)

    detail = detail.filter(
        (
            sqlf.datediff(
                sqlf.lit(job.config.params["assignment_date"]),
                sqlf.col("PURCH_DT"),
            )
            > 0
        )
        & (
            sqlf.datediff(
                sqlf.lit(job.config.params["assignment_date"]),
                sqlf.col("PURCH_DT"),
            )
            <= lookback_days
        )
    )

    item = item.filter(item[CAT_HRRCHY].isin(closure_ahcd_list))
    detail = update_categories(detail, item, CAT_HRRCHY)

    closure_coupon = mail_list_with_cls_offers.join(
        detail.select(
            "PURCH_HDR_ID",
            "QTY_IN_UNITS",
            "PURCH_DT",
            "MBRSHP_SID",
            CAT_HRRCHY,
        ),
        [CAT_HRRCHY, "MBRSHP_SID"],
        "left_outer",
    )

    closure_coupon = closure_coupon.filter(
        (
            sqlf.datediff(
                sqlf.lit(job.config.params["assignment_date"]),
                sqlf.col("PURCH_DT"),
            )
            <= 7 * sqlf.col("closure_window")
        )
        | sqlf.col("PURCH_DT").isNull()
    )

    # calculate units and trips per a5h
    closure_coupon = closure_coupon.groupBy(
        "MBRSHP_SID", "cpn_nbr", CAT_HRRCHY, CAT_LIST, "closure_window"
    ).agg(
        sqlf.sum("QTY_IN_UNITS").alias("units"),
        sqlf.countDistinct("PURCH_HDR_ID").alias("trips"),
    )

    closure_coupon = closure_coupon.fillna({"units": 0, "trips": 0})

    # calculate max units purchased for a coupon by ah5
    closure_coupon = (
        closure_coupon.groupBy(
            "MBRSHP_SID", "cpn_nbr", CAT_LIST, "closure_window"
        )
        .agg(
            sqlf.sum("units").alias("units"),
            sqlf.max("units").alias("max_units"),
            sqlf.sum("trips").alias("trips"),
        )
        .withColumnRenamed("closure_ah5_cd", CAT_HRRCHY)
    )

    # mark eligible only if the max_units is not greater than 0
    # which means that the coupon is eligible for all ah5
    closure_coupon = closure_coupon.withColumn(
        "closure_flag",
        sqlf.when(
            # note that when there is a purchase + return
            # units should be 0, "<=" is to deal with
            # potencial data issues
            (sqlf.col("max_units") <= 0),
            1,
        ).otherwise(0),
    )

    closure_coupon = closure_coupon.drop("max_units")

    job.data.tables["closure_coupon"] = closure_coupon


def write_tbls(job):
    """
    Change the column capitalization and order, then write
    the closure_coupon table to S3

    Args:
    job (JobManager object)

    Returns:
    """

    job.data.tables["closure_coupon"] = job.data.tables[
        "closure_coupon"
    ].select(
        sqlf.col("MBRSHP_SID").alias("mbrshp_sid"),
        sqlf.col("cpn_nbr").alias("cpn_nbr"),
        sqlf.col(CAT_HRRCHY).alias(CAT_HRRCHY.lower()),
        sqlf.col("closure_window").alias("closure_window"),
        sqlf.col("trips").alias("trips"),
        sqlf.col("units").alias("units"),
        sqlf.col("closure_flag").alias("closure_flag"),
    )

    job.data.write(
        "closure_coupon",
        "COUPON_CLOSURE",
        ftype="parquet",
        partitionby="cpn_nbr",
    )


def main(conf_path_in=None):
    """
    Unify reading, processing and writting of data

    Args:
    conf_path_in (str) - path to the config file

    Returns:
    """

    job = JobManager("create_closure", "CreateClosure", conf_path_in)

    job.log.info("1. Ingest Data")
    read_required_tbls(job)

    job.log.info("2. Create Closure Bank")
    process_tbls(job)

    job.log.info("3. Write Closure Bank")
    write_tbls(job)

    job.log.info("Done")


if __name__ == "__main__":

    main()
