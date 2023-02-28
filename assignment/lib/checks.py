import warnings
from datetime import datetime

import pandas as pd
import pyspark.sql.functions as sqlf

from pe_member_dna.pipelines.assignment.lib.assn_io import get_run_args
from pe_member_dna.pipelines.lib.iotools import (
    is_s3_file,
    is_s3_path,
    read_s3_to_local,
    split_path_bucket_key,
    write_local_to_s3,
)


def get_datetime_now():
    return datetime.now()


def check_prod_status(job):
    """Ensure this job won't cause a conflict with an existing running job.
    Given centralized coupon bank, coupon etl cannot allow prod run by multi user
    When one user runs coupon etl, it will block other's run by inserting a token into log
    """
    coupon_log = read_s3_to_local(job.config.paths["COUPON_LOG_FOLDER"])
    last_time = max(coupon_log.time)
    last_event = coupon_log.loc[coupon_log.time == str(last_time)].iloc[0][
        "log_event"
    ]
    last_campaign = coupon_log.loc[coupon_log.time == str(last_time)].iloc[0][
        "campaign"
    ]
    last_run_name = coupon_log.loc[coupon_log.time == str(last_time)].iloc[0][
        "run_name"
    ]
    last_time = datetime.strptime(last_time, "%Y-%m-%d-%H-%M-%S")
    current_time = get_datetime_now()
    minutes_lag = (current_time - last_time).total_seconds() / 60
    min_threshold = 120

    # scenario 1: last coupon etl job finished - pass
    if last_event == "END":
        pass
    # scenario 2: last coupon etl job did not finish but same campaign name and run name
    #             being attempted - pass
    elif (job.config.params["run_name"] == last_run_name) & (
        job.config.params["campaign"] == last_campaign
    ):
        pass
    # scenario 3: last coupon etl job did not finish but very likely last job failed in the middle - pass
    elif minutes_lag > min_threshold:
        warnings.warn(
            "WARNING: the previous coupon etl scripts started over {} minutes ago without"
            " reaching to the end".format(min_threshold)
        )
    # scenario 4: other coupon etl is running
    else:
        raise ValueError(
            "Collision!!! You are colliding with other jobs running coupon etl,"
            " please wait till they finish and retry!"
        )

    log_line = {
        "campaign": job.config.params["campaign"],
        "run_name": job.config.params["run_name"],
        "run_type": job.config.params["run_type"],
        "log_event": "START",
        "time": str(get_datetime_now().strftime("%Y-%m-%d-%H-%M-%S")),
    }

    write_local_to_s3(log_line, job.config.paths["COUPON_LOG"])


def check_execution_overwrite(paths_to_check):
    """
        Check used to stop execution if paths_to_check would risk to be
        overwrite.
    Parameters:
        paths_to_check (list): list of dir/file paths to check for existence
    Returns
        None
    """
    args = get_run_args()

    if args.force:
        return

    for path in paths_to_check:
        bucket, key = split_path_bucket_key(path)
        if is_s3_path(bucket, key) or is_s3_file(bucket, key):
            raise Exception(
                "There already has been a campaign executed with the same"
                " campaign and run name. Change the names in the config or"
                " use --force in order to overwrite the last execution."
            )


def check_loaded_long_cells(assignment, long_cells):
    """
    Check used to stop execution if the past longitudinal cells from cells.csv
    do not match the longitudinal cells from CDSA_ASSGN.

    Parameters:
        assignment (pyspark.sql.DataFrame): past longitudinal assignments
        long_cells (list(float)): past cells from cells.csv
    Returns
        None
    """
    loaded_long_cells = (
        assignment.select("cell_id").distinct().toPandas().cell_id.tolist()
    )

    if set(loaded_long_cells) != set(long_cells):
        raise Exception(
            "Loaded longitudinal cells do not match past longitudinal"
            " cells from cells.csv"
        )


def check_multi_long_tests(memberdata, cells):
    """
    Check used to stop execution if a members if eligible for multiple
    longitudinal tests as a past member.

    Parameters:
        memberdata (pyspark.sql.DataFrame): memberdata with segment
            filters applied
        cells (pandas.DataFrame): input cells.csv
    Returns
        None
    """
    memberdata = memberdata.withColumn("long_tests", sqlf.lit(0))
    for cell in cells:
        long_id = cell["longitudinal_id"]
        if pd.isna(long_id):
            continue

        colname = str(cell["segment_id"])
        memberdata = memberdata.withColumn(
            "long_tests",
            sqlf.when(
                (sqlf.col(colname) == 1)
                & (sqlf.col(f"l{long_id}_past_mbr") == 1),
                sqlf.col("long_tests") + sqlf.lit(1),
            ).otherwise(sqlf.col("long_tests")),
        )

    multi_long_tests = memberdata.filter(sqlf.col("long_tests") > 1).head()

    args = get_run_args()
    if multi_long_tests and not args.allow_multi_long_tests:
        raise Exception(
            "There are members eligible for multiple longitudinal"
            " tests. If this is part of the design use,"
            " --allow-multi-long-tests when running the script."
        )
