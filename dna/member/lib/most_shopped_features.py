"""
The script contains the function required to generate the most shopped category
features.
"""
import pyspark.sql.functions as sqlf
import pyspark.sql.window as W
import word2number.w2n as w2n
from pyspark import StorageLevel as SL

import pe_memberdna.dna.member.lib.utils as utils


def feature_most_shopped_category(job, dna, num_weeks):
    """
    Calculate the two most shopped member facing categories per member.

    Notes:
    - the aggregation happens on the member facing category level
    - null facing categories are excluded
    - if there is a tie between two categories in terms of number of purchased
        quantity the category that was purchased most recently is selected.
    - this class is very computationally intensive

    First calculate the sum of QTY_IN_UNITS and max LAST_DT over a rolling
    lookback interval of the length num_weeks for each fiscal week,
    each member and each member facing category.

    Then rank the resulting table by the resulting sum partitioning
    by member and fiscal week.

    For the fist most shopped category only select rows with rank=1
    and for the second shopped category only select the rows with rank=2.

    (only listing the most important steps)

    Parameters:
        job (object): Job Manager object based on the current config file
        dna (pyspark.sql.DataFrame): data containing the population of interest
        num_weeks (list(str)) - represents the lookback interval sizes in
            weeks as a list of words e.g. "FOUR", "EIGHT", "FIFTY-TWO", etc.

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new columns
    """

    AH5_custumer_facing_desc = job.data.tables["AH5_custumer_facing_desc"]

    counts_by_cat = (
        job.data.tables["detail"]
        .select(
            "MBRSHP_SID",
            "FISCAL_WEEK_END",
            "PURCH_DT",
            "AH5_CD",
            "QTY_IN_UNITS",
        )
        .join(
            sqlf.broadcast(
                AH5_custumer_facing_desc.where(
                    sqlf.col("MBR_FACING_CATEGORY").isNotNull()
                )
            ),
            "AH5_CD",
            "inner",
        )
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END", "MBR_FACING_CATEGORY")
        .agg(
            sqlf.sum("QTY_IN_UNITS").alias("SUM_QTY_IN_UNITS"),
            sqlf.max("PURCH_DT").alias("LAST_DT"),
        )
    )

    skeleton = job.data.tables["feature_population"]
    skeleton = skeleton.repartition("MBRSHP_SID", "FISCAL_WEEK_END")
    counts_by_cat = counts_by_cat.repartition("MBRSHP_SID", "FISCAL_WEEK_END")

    for weeks in num_weeks:
        lookback_weeks = w2n.word_to_num(weeks)
        rolling_counts_per_ah5_fw = (
            skeleton.alias("skel")
            .select(["MBRSHP_SID", "FISCAL_WEEK_END"])
            .join(
                counts_by_cat.alias("counts_by_categ"),
                (
                    sqlf.col("skel.MBRSHP_SID")
                    == sqlf.col("counts_by_categ.MBRSHP_SID")
                )
                & (
                    sqlf.datediff(
                        sqlf.col("skel.FISCAL_WEEK_END"),
                        sqlf.col("counts_by_categ.FISCAL_WEEK_END"),
                    )
                    < lookback_weeks * 7
                )
                & (
                    sqlf.datediff(
                        sqlf.col("skel.FISCAL_WEEK_END"),
                        sqlf.col("counts_by_categ.FISCAL_WEEK_END"),
                    )
                    >= 0
                ),
                "left_outer",
            )
            .groupBy(
                sqlf.col("skel.MBRSHP_SID"),
                sqlf.col("skel.FISCAL_WEEK_END"),
                "MBR_FACING_CATEGORY",
            )
            .agg(
                sqlf.sum("SUM_QTY_IN_UNITS").alias("ROLLING_SUM_PER_CAT"),
                sqlf.max("LAST_DT").alias("ROLLING_LAST_DT"),
            )
        )

        columns = [
            sqlf.col("skel.MBRSHP_SID").cast("string"),
            sqlf.col("skel.FISCAL_WEEK_END").cast("string"),
            sqlf.col("MBR_FACING_CATEGORY").cast("string"),
        ]

        rolling_counts_per_ah5_fw_limit_2 = rolling_counts_per_ah5_fw.where(
            (sqlf.col("ROLLING_SUM_PER_CAT") >= 2)
        ).withColumn(
            "deterministic_column", sqlf.sha2(sqlf.concat(*columns), 256)
        )

        ah5_ranks = rolling_counts_per_ah5_fw_limit_2.select(
            sqlf.col("skel.MBRSHP_SID").alias("MBRSHP_SID"),
            sqlf.col("skel.FISCAL_WEEK_END").alias("FISCAL_WEEK_END"),
            "MBR_FACING_CATEGORY",
            sqlf.row_number()
            .over(
                W.Window.partitionBy(
                    sqlf.col("skel.MBRSHP_SID"),
                    sqlf.col("skel.FISCAL_WEEK_END"),
                ).orderBy(
                    sqlf.desc("ROLLING_SUM_PER_CAT"),
                    sqlf.desc("ROLLING_LAST_DT"),
                    sqlf.col("deterministic_column"),
                )
            )
            .alias("MBR_FACING_CATEGORY_RANK"),
        ).drop("deterministic_column")

        # this persist call is important for the functions of the code
        # if removed row_number() will be different for different iterations
        # of the for loop below
        ah5_ranks.persist(SL.MEMORY_ONLY)
        utils.remove_spark_df(
            (rolling_counts_per_ah5_fw, rolling_counts_per_ah5_fw_limit_2)
        )

        col_name_list = [
            "L_" + weeks + "W_FIRST_MOST_SHOPPED_CATEGORY",
            "L_" + weeks + "W_SECOND_MOST_SHOPPED_CATEGORY",
        ]

        for rank_number, new_dna_col_name in enumerate(col_name_list, start=1):

            nth_most_shopped_ah5s = (
                ah5_ranks.where(
                    sqlf.col("MBR_FACING_CATEGORY_RANK") == rank_number
                )
                .withColumnRenamed("MBR_FACING_CATEGORY", new_dna_col_name)
                .select("MBRSHP_SID", "FISCAL_WEEK_END", new_dna_col_name)
            )

            dna = dna.repartition("MBRSHP_SID", "FISCAL_WEEK_END")
            nth_most_shopped_ah5s = nth_most_shopped_ah5s.repartition(
                "MBRSHP_SID", "FISCAL_WEEK_END"
            )

            dna = dna.join(
                nth_most_shopped_ah5s,
                ["MBRSHP_SID", "FISCAL_WEEK_END"],
                "left_outer",
            )

            utils.remove_spark_df(nth_most_shopped_ah5s)

    return dna
