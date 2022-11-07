"""Calculates covariate shifts between two bbm score inputs"""
import numpy as np
import os
import pandas as pd
import re

from pyspark import SparkContext, SparkConf
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pe_member_dna.pipelines.lib.iotools import (
    write_local_to_s3,
    split_path_bucket_key,
    copy_file_to_s3,
)
from pe_member_dna.pipelines.BBM_propensity_score.QC.lib.bbm_io import (
    load_config,
    get_matching_s3_keys,
    get_past_file_date,
    map_file_date,
)

CONF = SparkConf().setAppName("covariate_shift")
SC = SparkContext(conf=CONF)
SPARK = SparkSession.builder.getOrCreate()
SPARK.sparkContext.setLogLevel("WARN")

PARAMS = load_config()
PATHS = PARAMS["paths"]

BUCKET, KEY = split_path_bucket_key(PATHS["BBM_SCORE"])
LIST_OF_SCORE_FILE = get_matching_s3_keys(
    bucket=BUCKET, path="MODELDATA/BBM_PROPENSITY/BBM_SCORES/", contains="MU"
)
SCORE_PATH_DICT = map_file_date(LIST_OF_SCORE_FILE)
CURRENT_DATE = re.search(r"\d{4}-\d{2}-\d{2}", PATHS["BBM_SCORE"]).group()


def read_score_file(spark, score_path_dict, dt):
    """
    Reads a file.
    Parameters
        spark(Spark Session): a spark session
        score_path_dict(dict): dictionary of file paths where the key is the date
                        in the file name
        dt(date): current date
    Output:
        dat[spark dataframe]
    """
    path = score_path_dict[dt]
    dat = spark.read.csv(path, header=True)
    return dat


def get_summary_stats(df, date):
    """
    Summarizes dataframe.
    Parameters
        df(datafrmae): dataframe that we would like to summarize
        date(date): current date
    Output:
        df_agg[pandas dataframe]
    """
    df_agg = df.describe().toPandas().set_index("summary")
    df_agg["date"] = date
    df_agg["mbrs"] = df_agg.loc["count"]["mbrshp_nbr"]
    df_agg.drop(columns="mbrshp_nbr", inplace=True)
    return df_agg


def get_covariate_stats(
    spark=SPARK,
    score_path_dict=SCORE_PATH_DICT,
    list_of_files=LIST_OF_SCORE_FILE,
    current_date=CURRENT_DATE,
    n_weeks_apart=1,
):
    """
        Gets summary statistics for scores run x weeks appart
        the n number of weeks where n is determined by n_weeks_apart parameter.
        Parameters:
            spark(Spark Session) :  sparkSession
            score_path_dict(dict): dictionary of paths of bbm SCores where the KEYs are
                                the date the Score was ran
            list_of_files([str]): list of file paths
            current_date(date): the closest date we have a bbm score
            n_weeks_apart(int): the period lag in weeks between two runs that we are
                            interested in
    Output:
        shift_summary(dataframe): Summary statistics of the two base and curren data
        current_data_aggregate(dataframe): Summary statistics of current week data
        base_data_aggregate(dataframe): Summary statistics of base week data
    """
    INPUT_FEATURES = [
        "NON_EDIBLE_VISITS_SQRT",
        "OTHER_CPN_TRIPS_SQRT",
        "mbrshp_fee_inc",
        "shopped_in_last3mo",
    ]
    base_date = get_past_file_date(list_of_files, current_date, n_weeks_apart)
    base_data = read_score_file(spark, score_path_dict, base_date).select(
        "mbrshp_nbr", *INPUT_FEATURES
    )

    current_data = read_score_file(
        spark, score_path_dict, current_date
    ).select("mbrshp_nbr", *INPUT_FEATURES)

    base_data_agg = get_summary_stats(base_data, base_date)
    current_data_agg = get_summary_stats(current_data, current_date)

    shift_summary = pd.concat(
        [
            base_data_agg.loc[["mean"]].reset_index(drop=True),
            current_data_agg.loc[["mean"]].reset_index(drop=True),
        ]
    )

    shift_summary = shift_summary.set_index("date").astype("float")

    shift_summary.loc["prc_change"] = (
        (shift_summary.loc[base_date] - shift_summary.loc[current_date])
        / shift_summary.loc[base_date]
    ) * 100

    current_data_agg.drop(columns=["date", "mbrs"], inplace=True)
    base_data_agg.drop(columns=["date", "mbrs"], inplace=True)

    return shift_summary, current_data_agg, base_data_agg


def get_cross_tab(
    spark=SPARK,
    score_path_dict=SCORE_PATH_DICT,
    list_of_files=LIST_OF_SCORE_FILE,
    current_date=CURRENT_DATE,
    n_weeks_apart=1,
):
    """
    Gets cross tab of decile migration,i.e, number of people moved from one
    decile to another during
    the n number of weeks where n is determined by n_weeks_apart parameter.
    Parameters:
        spark(Spark Session) :  sparkSession
        score_path_dict(dict): dictionary of paths of bbm SCores where the KEYs are
                            the date the Score was ran
        list_of_files([str]): list of file paths
        current_date(date): the closest date for which we have bbm score
        n_weeks_apart(int): the period lag in weeks between two runs that we are
                        interested in
    Output:
        decile_shift[dataframe]    cross tab of the two deciles
    """
    base_date = get_past_file_date(list_of_files, current_date, n_weeks_apart)

    base_data = (
        read_score_file(spark, score_path_dict, base_date)
        .select("mbrshp_nbr", "decile")
        .withColumnRenamed("decile", "decile_base")
    )
    current_data = (
        read_score_file(spark, score_path_dict, current_date)
        .select("mbrshp_nbr", "decile")
        .withColumnRenamed("decile", "decile_current")
    )

    decile_shift = (
        (
            base_data.join(current_data, "mbrshp_nbr", "inner")
            .crosstab("decile_base", "decile_current")
            .withColumn(
                base_date, F.col("decile_base_decile_current").cast("int")
            )
            .orderBy(base_date)
            .select(
                base_date, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"
            )
            .toPandas()
        )
        .set_index(base_date)
        .rename_axis(current_date, axis="columns")
    )

    return decile_shift


for lag in PARAMS["covariate_shift_lag"]:
    shift_summary, current_summary, base_summary = get_covariate_stats(
        spark=SPARK,
        score_path_dict=SCORE_PATH_DICT,
        current_date=CURRENT_DATE,
        n_weeks_apart=lag,
    )
    decile_migration = get_cross_tab(
        spark=SPARK,
        score_path_dict=SCORE_PATH_DICT,
        current_date=CURRENT_DATE,
        n_weeks_apart=lag,
    )

    PERC_MBRS_ON_DIAG = (
        np.sum(np.diag(decile_migration).astype(float))
        / decile_migration.sum().sum()
    ) * 100

    BASE_DATE = get_past_file_date(LIST_OF_SCORE_FILE, CURRENT_DATE, lag)

    COVARIATE_SHIFT_OUTPUT = os.path.join(
        PATHS["BBM_SUMMARY_OUTPUT"],
        CURRENT_DATE,
        "lags",
        str(lag),
        "covariate_summary.xlsx",
    )

    writer = pd.ExcelWriter("temp_covariatShift.xlsx", engine="xlsxwriter")
    workbook = writer.book
    worksheet = workbook.add_worksheet("Summary")
    writer.sheets["Summary"] = worksheet
    ROW_NUMBER = 0

    for name, data_frame in (
        ("Covariate_shift", shift_summary),
        ("current_summary({})".format(CURRENT_DATE), current_summary),
        ("base_summary({})".format(BASE_DATE), base_summary),
        ("decile_migration", decile_migration),
    ):
        if name == "decile_migration":
            worksheet.write_string(ROW_NUMBER - 1, 0, string=name)
            worksheet.write_string(ROW_NUMBER, 4, string=CURRENT_DATE)
            worksheet.write_string(
                ROW_NUMBER + data_frame.shape[0] + 3,
                2,
                string="Percentage of mbrs who stayed in the same decile is {}".format(
                    round(PERC_MBRS_ON_DIAG, 2)
                ),
            )
        else:
            worksheet.write_string(ROW_NUMBER, 0, string=name)

        data_frame = data_frame.astype(float).round(3)
        data_frame.to_excel(
            writer,
            sheet_name="Summary",
            startrow=ROW_NUMBER + 1,
            startcol=0,
            index=True,
        )
        ROW_NUMBER += data_frame.shape[0] + 5

    writer.save()
    writer.close()

    copy_file_to_s3("temp_covariatShift.xlsx", COVARIATE_SHIFT_OUTPUT)

SPARK.stop()
