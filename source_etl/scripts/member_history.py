import argparse
import logging
import os

import memberdna.source_etl.utils.validations_ETL as validations
import pyspark.sql.functions as sqlf
from memberdna.lib.job_manager import JobManager
from memberdna.source_etl.utils.utility import *
from pyspark.sql.window import Window


def main(job, data_paths, config_validation):

    logging.info("Starting processing table member_history")

    logging.info(
        "Reading the input file " + data_paths["source"]["member_history"]
    )

    df = job.spark.read.csv(
        data_paths["source"]["member_history"], sep="|", header=True, quote="'"
    )

    validations.validate_table(
        job.spark, "source", "member_history", config_validation, df
    )

    df.registerTempTable("df")

    cast_sql = """
    select
        cast(MBRSHP_HIST_SID as int) as MBRSHP_HIST_SID
        ,cast(MBRSHP_SID as int) as MBRSHP_SID
        ,cast(EFF_DT as date) as EFF_DT
        ,cast(EXP_DT as date) as EXP_DT
        ,MBRSHP_STAT_CD
        ,cast(MBRSHP_EXP_DT as date) as MBRSHP_EXP_DT
        ,cast(MBRSHP_RNWL_DT as date) as MBRSHP_RNWL_DT
        ,cast(MBRSHP_FEE_INC as double) as MBRSHP_FEE_INC
        ,RWDS_MBR_IND
        ,cast(RWDS_MBR_ENR_DT as date) as RWDS_MBR_ENR_DT
        ,cast(CLUB_OF_FREQUENCY as int) as CLUB_OF_FREQUENCY
        ,TEAM_MBR_IND
    from df
    """

    df = job.spark.sql(cast_sql)

    w = Window.partitionBy("MBRSHP_SID", "EFF_DT").orderBy(
        sqlf.col("MBRSHP_HIST_SID").desc_nulls_last()
    )
    df = df.withColumn("rank", sqlf.row_number().over(w))
    df = df.filter(df.rank == 1).drop("rank").drop("MBRSHP_HIST_SID")

    df = df.withColumn("DIFF", sqlf.datediff(df.EXP_DT, df.EFF_DT))
    df = df.filter(df.DIFF > 0)

    weird = df.groupby("MBRSHP_SID", "EFF_DT").count()
    df = df.join(weird, ["MBRSHP_SID", "EFF_DT"], "left_outer")
    micro_cases = df.filter(
        (df["count"] > 1) & (df.CLUB_OF_FREQUENCY.isNotNull())
    )
    df = df.filter(df["count"] < 2)
    df = df.union(micro_cases)

    fiscal_days = job.spark.read.parquet(
        data_paths["intermediate"]["fiscal_days"]
    )
    df = df.join(
        fiscal_days.select("FISCAL_DAY", "FISCAL_WEEK_END"),
        df.EFF_DT == fiscal_days.FISCAL_DAY,
        "left_outer",
    ).drop("FISCAL_DAY", "DIFF", "count")
    df = df.dropDuplicates()

    dest_path = data_paths["intermediate"]["member_history"]

    validations.validate_table(
        job.spark, "intermediate", "member_history", config_validation, df
    )

    logging.info("Saving the intermediate file " + dest_path)

    df.repartition(100).write.parquet(dest_path, mode="overwrite")


job = JobManager("member_history")
parser = argparse.ArgumentParser()

parser.add_argument("--prod-mode", dest="prod_mode", action="store_true")
parser.add_argument(
    "config_path",
    nargs="?",
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../configs/config.yaml",
    ),
)
parser.set_defaults(prod_mode=True)
args = parser.parse_args()
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths, config_validation)
