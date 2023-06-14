"""
helper function for BBM propensity model (gather and transform data)
"""
import re
from datetime import timedelta

import boto3
import pyspark.sql.functions as sqlf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

from pe_memberdna.lib.misc import get_last_fiscal_weekend


def create_datasets(job):
    """
    Creates the training dataset for BBM propensity model

    Args:
        job (JobManager) - an active instance created from JobManager class

    Returns:
    """
    read_input_tables(job)

    weeks_to_sample = job.config.params["weeks_to_sample"]
    assignment_lead_time = job.config.params["assignment_lead_time"]
    fiscal_day_lookup = job.data.tables["fiscal_days_lookup"]
    bbm_history_lookup = job.data.tables["bbms_history_dates"]
    purch_detail_raw = job.data.tables["purch_detail_raw"]
    purch_detail = job.data.tables["purch_detail"]
    payment = job.data.tables["payment"]
    coups = job.data.tables["coups"]

    if job.config.params["train"]:

        job.data.tables["bbms_history_dates"] = get_bbm_assignment_dates(
            assignment_lead_time, fiscal_day_lookup, bbm_history_lookup
        )

        job.data.tables["bbms_history_dates"] = add_assignment_paths(job)

        job.data.tables["mbr_dna"] = filter_customer_cube_for_training(job)

        job.data.tables["mbr_dna"] = add_dependent_col(job, purch_detail)

        job.data.tables["mbr_dna"] = sample_and_remove_outliers(
            job, weeks_to_sample
        )

    else:

        job.data.tables["mbr_dna"] = filter_customer_cube_for_inference(job)

        job.data.tables["mbr_dna"] = add_independent_cols(
            job, purch_detail_raw, purch_detail, payment, coups
        )

        job.config.paths["ETL_output"]["path"] = create_ETL_output_path(job)

        job.data.write("ETL_output", df=job.data.tables["mbr_dna"])


def create_ETL_output_path(job):

    _, last_fiscal_week_end = get_last_fiscal_weekend()

    run_name = job.config.params["run_name"].format(
        last_fiscal_week_end=last_fiscal_week_end
    )

    if job.config.params["train"]:
        train_inference = "TRAIN_ETL"
    else:
        train_inference = "INFERENCE_ETL"

    ETL_output_path = job.config.paths["ETL_output"]["path"].format(
        bucket=job.config.params["bucket"],
        base_path=job.config.params["base_path"],
        train_inference=train_inference,
        run_name=run_name,
    )

    return ETL_output_path


def read_input_tables(job):
    """
    Read all tables specified in the config into DataManager

    Args:
        job (JobManager) - an active instance created from JobManager class

    Returns:
    """
    for table_name, table_config in job.config.paths.items():

        if table_name == "ETL_output":
            continue

        job.data.read(
            table_name,
            file_type=table_config["format"],
            columns=table_config["cols"],
        )


def get_bbm_assignment_dates(
    assignment_lead_time, fiscal_day_lookup, bbm_history_lookup
):
    """
    Select assignment dates for historical BBM campaigns

    Args:
        fiscal_day_lookup (SPARK dataframe) - dataframe store the relationship
            between fiscal week and fiscal day
        bbm_history_lookup (SPARK dataframe) - dataframe store historical BBMs dates

    Returns:
        (SPARK Dataframe) - bbm_history_lookup store assignment dates for each BBM
    """
    bbm_history_lookup = (
        bbm_history_lookup.join(
            fiscal_day_lookup,
            on=fiscal_day_lookup.FISCAL_DAY
            == bbm_history_lookup.min_start_date,
            how="left",
        )
        .withColumn(
            "bbms_assignment_dates",
            sqlf.date_sub(
                fiscal_day_lookup.FISCAL_WEEK_END,
                (assignment_lead_time + 1) * 7,
            ),
        )
        .orderBy("bbms_assignment_dates", ascending=False)
        .select("bbm_names", "bbms_assignment_dates")
    )

    return bbm_history_lookup


def add_assignment_paths(job):
    """
    Add BBMs mail file path to bbm_history_lookup table

    Args:
        job (JobManager) - an active instance created from JobManager class

    Returns:
    """
    paths = []

    bbm_history_lookup = job.data.tables["bbms_history_dates"]
    bbms_mailfile_bucket = job.config.params["bucket"]
    bbms_mailfile_prefix = job.config.params["assignment_prefix"]

    BBM_folder_search = "BBM\d{1,2}FY\d\d"
    final_mailhouse_search = "final_mailhouse"
    final_folder_search = "final_\d{4}-\d{2}-\d{2}/"

    paginator = boto3.client("s3").get_paginator("list_objects_v2")

    page_iterator = paginator.paginate(
        Bucket=bbms_mailfile_bucket, Prefix=bbms_mailfile_prefix
    ).search("Contents[?contains(Key, `BBM`)][]")

    for page in page_iterator:
        if (
            re.search(BBM_folder_search, page["Key"], re.I)
            and re.search(final_folder_search, page["Key"], re.I)
            and re.search(final_mailhouse_search, page["Key"], re.I)
            and page["Key"].rsplit("/", 1)[0] not in paths
        ):

            paths.append(page["Key"].rsplit("/", 1)[0])

    paths = job.spark.createDataFrame([(path,) for path in paths], ["paths"])

    bbm_history_lookup = bbm_history_lookup.join(
        paths, sqlf.col("paths").contains(sqlf.col("bbm_names")), how="left"
    )

    return bbm_history_lookup


def filter_customer_cube_for_training(job):
    """
    Filter members at specific fiscal week end based on
    few criterion

    Args:
        job (JobManager) - an active instance created from JobManager class

    Returns:
    """
    bucket = job.config.params["bucket"]
    mbr_dna = job.data.tables["mbr_dna"]
    bbm_history_lookup = job.data.tables["bbms_history_dates"]

    bbms_assignment_dates = [
        row["bbms_assignment_dates"] for row in bbm_history_lookup.collect()
    ]

    last_fiscal_week_for_training = bbms_assignment_dates[0]
    first_fiscal_week_for_training = last_fiscal_week_for_training - timedelta(
        days=365
    )

    mbr_dna = mbr_dna.filter(
        (
            sqlf.col("FISCAL_WEEK_END").between(
                first_fiscal_week_for_training, last_fiscal_week_for_training
            )
        )
        & (sqlf.col("FISCAL_WEEK_END").isin(bbms_assignment_dates))
        & (sqlf.col("TENURE") >= 0)
        & (sqlf.col("TENURE_GROUP") != "expired")
    )

    schema = StructType(
        [
            StructField("LATEST_MBRSHP_NBR", StringType(), True),
            StructField("MBRSHP_SID", StringType(), True),
            StructField("FISCAL_WEEK_END", StringType(), True),
        ]
    )
    assignments = job.spark.createDataFrame((), schema)

    for bbm_history_date in bbm_history_lookup.collect():

        if bbm_history_date["paths"]:

            assignment_path = f"s3://{bucket}/{bbm_history_date['paths']}"

            assignment = (
                job.spark.read.csv(assignment_path, header=True)
                .withColumn(
                    "FISCAL_WEEK_END",
                    sqlf.lit(bbm_history_date["bbms_assignment_dates"]),
                )
                .withColumnRenamed("MBRSHP_NBR", "LATEST_MBRSHP_NBR")
                .select("LATEST_MBRSHP_NBR", "MBRSHP_SID", "FISCAL_WEEK_END")
            )

            assignments = assignments.unionByName(assignment)

    mbr_dna = mbr_dna.join(
        assignments, on=["MBRSHP_SID", "FISCAL_WEEK_END", "LATEST_MBRSHP_NBR"]
    )

    return mbr_dna


def filter_customer_cube_for_inference(job):

    mbr_dna = job.data.tables["mbr_dna"]

    _, last_fiscal_week_end_dt = get_last_fiscal_weekend()

    mbr_dna = mbr_dna.filter(
        (mbr_dna.FISCAL_WEEK_END == last_fiscal_week_end_dt)
        & (mbr_dna.TENURE >= 0)
        & (mbr_dna.TENURE_GROUP != "expired")
    )

    return mbr_dna


def add_independent_cols(
    job,
    purch_detail_raw,
    purch_detail,
    payment,
    coups,
):
    """
    Add extra independent columns to mbr_dna

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end
        purch_detail_raw (SPARK dataframe) - purchase detail with only purch_hrd_id and sales_channel_id
        purch_detail (SPARK dataframe) - purchase detail without sales_channel_id
        payment (SPARK dataframe) - payment table
        coups (SPARK dataframe) - ad-hoc coups table

    Returns:
        (SPARK Dataframe) - mbr_dna with extended features
    """

    mbr_dna = job.data.tables["mbr_dna"]

    mbr_dna = add_independent_col_frequnecy(mbr_dna)

    mbr_dna = add_independent_col_basket(mbr_dna)

    mbr_dna = add_independent_col_seasonality(mbr_dna)

    mbr_dna = add_independent_col_shopped(mbr_dna)

    mbr_dna = add_independent_col_non_edible_trips(
        mbr_dna,
        purch_detail_raw,
        purch_detail,
    )

    mbr_dna = add_independent_col_other_cpn_trips(
        mbr_dna,
        payment,
        coups,
    )

    return mbr_dna


def add_independent_col_frequnecy(
    mbr_dna,
    low_frequency_visits_last_26_weeks_cutoff=0,
    high_frequency_visits_last_12_weeks_cutoff=12,
):
    """
    Add independent categorical feature "MEMBER_FREQUENCY_GROUP"
    based on last half year trips, i.e. HIGH, MEDIUM, LOW

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end
        low_frequency_visits_last_26_weeks_cutoff (int) - low bound to classify
            as LOW MEMBER_FREQUENCY_GROUP
        high_frequency_visits_last_12_weeks_cutoff (int) - high bound to classify
            as HIGH MEMBER_FREQUENCY_GROUP

    Returns:
        (SPARK Dataframe) - mbr_dna with extended feature, "MEMBER_FREQUENCY_GROUP"
    """
    mbr_dna = mbr_dna.withColumn(
        "MEMBER_FREQUENCY_GROUP",
        sqlf.when(
            mbr_dna["LAST_TWENTY-SIX_WEEK_TRIPS"]
            <= low_frequency_visits_last_26_weeks_cutoff,
            "LOW",
        )
        .when(
            mbr_dna["LAST_TWELVE_WEEK_TRIPS"]
            >= high_frequency_visits_last_12_weeks_cutoff,
            "HIGH",
        )
        .otherwise("MEDIUM"),
    )

    return mbr_dna


def add_independent_col_basket(mbr_dna):
    """
    Add independent numeric feature "SPEND_IN_STORE_BY_TRIPS_LAST_TWENTY-SIX_WEEKS"
    based on last half year spend in store and trips

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end

    Returns:
        (SPARK Dataframe) - mbr_dna with extended feature,
            "SPEND_IN_STORE_BY_TRIPS_LAST_TWENTY-SIX_WEEKS"
    """
    mbr_dna = mbr_dna.withColumn(
        "SPEND_IN_STORE_BY_TRIPS_LAST_TWENTY-SIX_WEEKS",
        sqlf.when(mbr_dna["LAST_TWENTY-SIX_WEEK_TRIPS"] == 0, 0).otherwise(
            (mbr_dna["LAST_TWENTY-SIX_WEEK_SPEND"] * 1.1)
            / mbr_dna["LAST_TWENTY-SIX_WEEK_TRIPS"]
        ),
    )

    return mbr_dna


def add_independent_col_seasonality(mbr_dna):
    """
    Add independent categorical feature "WEEK_OF_YEAR" and "MONTH"
    based on "FISCAL_WEEK_END"

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end

    Returns:
        (SPARK Dataframe) - mbr_dna with extended feature,
            "WEEK_OF_YEAR" and "MONTH"
    """
    mbr_dna = mbr_dna.withColumn(
        "WEEK_OF_YEAR", sqlf.weekofyear(mbr_dna.FISCAL_WEEK_END)
    ).withColumn("MONTH", sqlf.month(mbr_dna.FISCAL_WEEK_END))

    return mbr_dna


def add_independent_col_shopped(mbr_dna):
    """
    Add independent categorical feature "Shopped_in_Last_3Month"

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end

    Returns:
        (SPARK Dataframe) - mbr_dna with extended feature, "Shopped_in_Last_3Month"
    """
    mbr_dna = mbr_dna.withColumn(
        "Shopped_in_Last_3Month",
        sqlf.when(sqlf.col("LAST_TWELVE_WEEK_TRIPS") > 0, 1).otherwise(0),
    )

    return mbr_dna


def add_independent_col_non_edible_trips(
    mbr_dna, purch_detail_raw, purch_detail, left_start_window=182
):
    """
    Add independent categorical feature "non_edible_trips",
    i.e. merchandise sales of sales category 03 and MCH3_CD of 300000000 on
    sales channle 10, 30, 40

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end
        purch_detail_raw (SPARK dataframe) - purchase detail with only purch_hrd_id and sales_channel_id
        purch_detail (SPARK dataframe) - purchase detail without sales_channel_id
        left_start_window (int) - starting point of a period to calculate KPIs from "FISCAL_WEEK_END"

    Returns:
        (SPARK Dataframe) - mbr_dna with extended feature, "non_edible_trips"
    """
    purch_detail_raw = (
        purch_detail_raw.withColumnRenamed("purch_hdr_id", "PURCH_HDR_ID")
        .withColumnRenamed("sales_channel_id", "SALES_CHANNEL_ID")
        .drop_duplicates()
    )

    purch_detail = purch_detail.join(
        purch_detail_raw, on="PURCH_HDR_ID", how="left"
    )

    mbr_dna_temp = mbr_dna.withColumn(
        "window_start",
        sqlf.date_sub(sqlf.col("FISCAL_WEEK_END"), left_start_window),
    )

    customers_non_edible = (
        mbr_dna_temp.join(
            purch_detail,
            on=[
                sqlf.col("PURCH_DT") >= sqlf.col("window_start"),
                sqlf.col("PURCH_DT") <= sqlf.col("FISCAL_WEEK_END"),
                mbr_dna_temp.MBRSHP_SID == purch_detail.MBRSHP_SID,
            ],
            how="left",
        )
        .filter(
            (sqlf.col("SALES_CTGRY_CD") == "03")
            & (sqlf.col("MCH3_CD") == "300000000")
            & (sqlf.col("SALES_CHANNEL_ID").isin([10, 30, 40]))
        )
        .groupBy(mbr_dna_temp.MBRSHP_SID, "FISCAL_WEEK_END")
        .agg(
            sqlf.countDistinct(
                sqlf.when(
                    sqlf.col("MCH3_CD") == "300000000",
                    sqlf.col("PURCH_HDR_ID"),
                )
            ).alias("non_edible_trips")
        )
    )

    mbr_dna = mbr_dna.join(
        customers_non_edible, on=["MBRSHP_SID", "FISCAL_WEEK_END"], how="left"
    ).fillna(0, subset=["non_edible_trips"])

    return mbr_dna


def add_independent_col_other_cpn_trips(
    mbr_dna,
    payment,
    coups,
    left_start_window=182,
    tender_type=[
        "PCUM",
        "PCUS",
        "PCUE",
        "PCUB",
        "PCUR",
        "CPN",
    ],
):
    """
    Add independent categorical feature "other_cpn_trips"

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end
        payment (SPARK dataframe) - payment table
        coups (SPARK dataframe) - ad-hoc coups table
        left_start_window (int) - starting point of a period to calculate KPIs from "FISCAL_WEEK_END"
        tender_type (list) - payment tender type define other cpn trips

    Returns:
        (SPARK Dataframe) - mbr_dna with extended feature, "other_cpn_trips"
    """
    mbr_dna_temp = mbr_dna.withColumn(
        "window_start",
        sqlf.date_sub(
            start=sqlf.col("FISCAL_WEEK_END"), days=left_start_window
        ),
    ).withColumnRenamed("MBRSHP_SID", "MBRSHP_SID_dup")

    mbr_dna_temp_2061 = mbr_dna_temp.join(
        payment,
        on=[
            sqlf.col("PURCH_DT") >= sqlf.col("window_start"),
            sqlf.col("PURCH_DT") <= sqlf.col("FISCAL_WEEK_END"),
            mbr_dna_temp.MBRSHP_SID_dup == payment.MBRSHP_SID,
        ],
        how="left",
    ).filter(sqlf.col("TENDER_TYPE_CD").isin(tender_type))

    mbr_dna_temp_2319 = (
        coups.filter(
            (sqlf.col("mailer_name").like("%BBM"))
            & (~sqlf.col("text_ver_coupon").isin(["", "0000000000000000"]))
        )
        .select("text_ver_coupon")
        .distinct()
    )

    text_ver_coupon_BBM = [
        row.text_ver_coupon for row in mbr_dna_temp_2319.collect()
    ]

    mbr_dna_temp_5562 = (
        mbr_dna_temp_2061.select(
            "PURCH_HDR_ID",
            "CPN_NBR",
            "SALES_PYMT_AMT",
            "MBRSHP_SID",
            "PURCH_DT",
            "FISCAL_WEEK_END",
            sqlf.when(
                condition=sqlf.col("CPN_NBR").isin(text_ver_coupon_BBM),
                value="Y",
            )
            .otherwise("N")
            .alias("BBM"),
        )
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(
            sqlf.countDistinct(
                sqlf.when(sqlf.col("BBM") == "N", sqlf.col("PURCH_HDR_ID"))
            ).alias("Other_CPN_Trips")
        )
    )

    mbr_dna = mbr_dna.join(
        mbr_dna_temp_5562, on=["MBRSHP_SID", "FISCAL_WEEK_END"], how="left"
    ).fillna(0, subset=["Other_CPN_Trips"])

    return mbr_dna


def add_dependent_col(
    job, purch_detail, coupons=["ZPAP"], left_end_days=35, right_end_days=63
):
    """
    Add dependent column, redeem paper coupon within BBMs valid window

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end
        purch_detail (SPARK dataframe) - purchase detail without sales_channel_id
        coupons (list) - qualified coupon type
        left_end_days (int) 35 days after assignemnt, estimate BBM inhome date
        right_end_days (int) 63 days after assignemnt, estimate BBM expire date

    Returns:
        (SPARK Dataframe) - mbr_dna with extended target feature
    """

    mbr_dna = job.data.tables["mbr_dna"]

    mbr_dna_temp = mbr_dna.withColumn(
        "window_end",
        sqlf.date_add(sqlf.col("FISCAL_WEEK_END"), right_end_days),
    ).withColumn(
        "window_start",
        sqlf.date_add(sqlf.col("FISCAL_WEEK_END"), left_end_days),
    )

    customers_redeem_paper_article_cpn = (
        mbr_dna_temp.join(
            purch_detail,
            on=[
                sqlf.col("PURCH_DT") >= sqlf.col("window_start"),
                sqlf.col("PURCH_DT") <= sqlf.col("window_end"),
                mbr_dna_temp.MBRSHP_SID == purch_detail.MBRSHP_SID,
            ],
            how="left",
        )
        .filter(sqlf.col("DISCOUNT_TYPE_CD").isin(coupons))
        .groupBy(
            mbr_dna_temp.MBRSHP_SID,
            "FISCAL_WEEK_END",
        )
        .agg(sqlf.count("PURCH_DT").alias("redeem_paper_article_cpn"))
        .fillna(0, subset=["redeem_paper_article_cpn"])
        .withColumn(
            "redeem_paper_article_cpn",
            sqlf.when(sqlf.col("redeem_paper_article_cpn") > 0, 1).otherwise(
                0
            ),
        )
    )

    mbr_dna = (
        mbr_dna.join(
            customers_redeem_paper_article_cpn,
            on=["MBRSHP_SID", "FISCAL_WEEK_END"],
            how="left",
        )
        .withColumnRenamed("redeem_paper_article_cpn", "redeem_paper_cpn")
        .fillna(0, subset=["redeem_paper_cpn"])
    )

    return mbr_dna


def sample_and_remove_outliers(
    job, weeks_to_sample, column="FW_SPEND_IN_STORE"
):
    """
    Remove bottom 5% and top 5% outliers based on specified column
    And randomly select weeks_to_sample for each member

    Args:
        mbr_dna (SPARK dataframe) - mbr_dna is the cube for each member at one fiscal week end
        weeks_to_sample (int) - number of observations selected for each member
        column (str) - remove outlier based on this column

    Returns:
        (SPARK Dataframe) - mbr_dna after removing outlier and sampling
    """

    mbr_dna = job.data.tables["mbr_dna"]

    quantile = mbr_dna.approxQuantile(column, [0.05, 0.95], 0.01)

    mbr_dna = mbr_dna.filter(
        (sqlf.col(column) >= quantile[0]) & (sqlf.col(column) <= quantile[1])
    )

    mbr_dna = (
        mbr_dna.withColumn("rnd_", sqlf.rand())
        .withColumn(
            "rn_",
            sqlf.row_number().over(
                Window.partitionBy("MBRSHP_SID").orderBy(sqlf.rand())
            ),
        )
        .where(sqlf.col("rn_") <= weeks_to_sample)
        .drop("rn_", "rnd_")
    )

    return mbr_dna


#edited spaces