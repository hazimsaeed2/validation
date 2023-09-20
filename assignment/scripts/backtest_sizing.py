"""Spark job to size the coupon CPN_RED by backtesting

Hardware Requirements:
- Recommend at least 6 r4 worker nodes to run
- Runtime: Approx.

TODO:
[] Add preprocessor class
"""

import sys
from datetime import datetime

import pyspark.sql.functions as sqlf
from pe_memberdna.pipelines.assignment.lib.assn_io import JobManager
from pe_memberdna.pipelines.assignment.lib.assn_utils import (
    explode_columns,
    has_coupons,
    read_subset_and_cast,
)
from pe_memberdna.pipelines.assignment.lib.checks import (
    check_execution_overwrite,
)
from pe_memberdna.pipelines.lib.utils import capitalize_col_names, trips_only


def _make_category_agnostic(df):
    """create category column that is agnostic to hierachy

    Parameters:
        df (pyspark.sql.DataFrame): data frame to be converted
            Requires: ARTICLE_NBR, AH5_CD, AH4_CD
    Returns:
        df (pyspark.sql.DataFrame): output dataframe with category id
    """
    cols_cat = ["ARTICLE_NBR", "AH4_CD", "AH5_CD"]
    df = df.fillna(0, subset=cols_cat)
    df = explode_columns(df, cols_cat, "CATEGORY_ID")
    df = df.filter(df.CATEGORY_ID > 0)
    return df


def _flag_qualifiers(df):
    """flag mbrs who could be qualified for redeeming targeted coupons
       during backtesting period

    Parameters:
        df (pyspark.sql.DataFrame): data frame to be flagged
    Returns:
        df (pyspark.sql.DataFrame): output dataframe with flagging column
    """
    df = df.groupby(
        "MBRSHP_SID", "CPN_NBR", "CPN_DOLLAR_THRESHOLD", "PURCH_HDR_ID"
    ).agg(sqlf.sum(df.EXTENDED_PRC_AMT).alias("SPEND"))
    df = df.groupby("MBRSHP_SID", "CPN_NBR", "CPN_DOLLAR_THRESHOLD").agg(
        sqlf.max(df.SPEND).alias("SPEND")
    )
    df = df.withColumn(
        "is_qualified",
        sqlf.when(df.SPEND > df.CPN_DOLLAR_THRESHOLD, 1).otherwise(0),
    )
    df = df.select("MBRSHP_SID", "CPN_NBR", "is_qualified")
    return df


def _attach_club(df):
    """identify members preferred club, if no preferred club, use registered club

    Parameters:
        df (pyspark.sql.DataFrame): data frame to attach club
            requires: MBRSHP_NBR, 'L52W_PREFERRED_CLUB_NBR'
    Returns:
        df (pyspark.sql.DataFrame): output dataframe with club id
    """
    df = df.withColumn("registered_club", df.MBRSHP_NBR[0:3].cast("integer"))
    df = df.select("MBRSHP_SID", "registered_club", "L52W_PREFERRED_CLUB_NBR")
    df = df.fillna(0, subset=["L52W_PREFERRED_CLUB_NBR"])
    df = df.withColumn(
        "club_id",
        sqlf.when(
            df.L52W_PREFERRED_CLUB_NBR == 0, df.registered_club
        ).otherwise(df.L52W_PREFERRED_CLUB_NBR),
    )
    df = df.select("MBRSHP_SID", "club_id")
    df = df.dropDuplicates(subset=["MBRSHP_SID"])
    return df


def _backfill_threshold(df):
    df = df.groupby("CPN_NBR", "CPN_DOLLAR_THRESHOLD", "CPN_TYPE").agg(
        sqlf.sum(df.CATEGORY_THRESHOLD).alias("CATEGORY_THRESHOLD")
    )
    df = df.withColumn(
        "CPN_DOLLAR_THRESHOLD",
        sqlf.when(
            (df.CPN_DOLLAR_THRESHOLD > 0) | (df.CPN_TYPE != "category"),
            df.CPN_DOLLAR_THRESHOLD,
        ).otherwise(df.CATEGORY_THRESHOLD),
    )
    df = df.select("CPN_NBR", "CPN_DOLLAR_THRESHOLD")
    df = df.dropDuplicates(subset=["CPN_NBR"])
    return df


def main(conf_path_in=None):

    name = "Sizing"
    job = JobManager("sizing", name, conf_path_in)

    if job.config.params["run_type"].lower() == "prod":
        check_execution_overwrite(
            paths_to_check=[
                job.config.paths["SIZING_IMP"],
                job.config.paths["SIZING_BUDGET"],
                job.config.paths["SIZING_CELL"],
                job.config.paths["SIZING_CLUB"],
            ]
        )

    if not has_coupons(job.config.params):
        job.log.info("No coupons. Skipping backtest sizing")
        job.log.info("done")
        return

    # ---- Read Data ---- #
    job.log.info("1. Reading Data...")

    # transacation data
    col_names = [
        "PURCH_DT",
        "PURCH_HDR_ID",
        "MBRSHP_SID",
        "AH4_CD",
        "AH5_CD",
        "ARTICLE_NBR",
        "EXTENDED_PRC_AMT",
        "SALES_CTGRY_CD",
        "MC_CD",
        "SALES_QTY",
    ]
    transaction = read_subset_and_cast(
        job.config.paths["TRANSACTIONS_PATH"], "parquet", col_names
    )

    # mbr DNA
    col_names = ["MBRSHP_SID", "L52W_PREFERRED_CLUB_NBR"]
    mbr_dna = read_subset_and_cast(
        job.config.paths["CUBE"],
        "parquet",
        col_names,
        "fiscal_week",
        job.config.params["assignment_date"],
    )
    mbr_dna = mbr_dna.dropDuplicates()

    # category DNA - ah4
    col_names = ["AH4_CD", "UNIT_RETAIL_PRICE", "UNITS_PER_TRIP"]
    ah4_dna = read_subset_and_cast(
        job.config.paths["AH4_DNA_PATH"], "parquet", col_names
    )

    # category DNA - ah5
    col_names = ["AH5_CD", "UNIT_RETAIL_PRICE", "UNITS_PER_TRIP"]
    ah5_dna = read_subset_and_cast(
        job.config.paths["AH5_DNA_PATH"], "parquet", col_names
    )

    # coupon quals
    col_names = ["cpn_nbr", "hero_eligible", "experiment_id"]
    coups_quals = read_subset_and_cast(
        job.config.paths["COUPON_QUALS"], "csv", col_names
    )
    coups_quals = capitalize_col_names(coups_quals)
    coups_quals = coups_quals.filter(
        coups_quals.EXPERIMENT_ID == job.config.params["experiment"]
    )

    # mail file
    mail_file = read_subset_and_cast(
        job.config.paths["MAIL_POPULATION_ASSIGNMENT"], "csv"
    )
    mail_file = capitalize_col_names(mail_file)
    if "MAIL_FLAG" not in [x.upper() for x in mail_file.columns]:
        mail_file = mail_file.withColumn("MAIL_FLAG", sqlf.lit(1))

    mail_file = mail_file.filter(mail_file.MAIL_FLAG == 1)

    mail_file = mail_file.join(coups_quals, "CPN_NBR", "inner")

    # backtest mail list
    backtest_mail_list = read_subset_and_cast(
        job.config.paths["BACKTEST_MAIL_LIST"], "csv"
    )
    backtest_mail_list = capitalize_col_names(backtest_mail_list)

    # coupon bank
    col_names = [
        "cpn_nbr",
        "offer_id",
        "cpn_class_id",
        "cpn_type",
        "cpn_desc",
        "cpn_dollar_off",
        "cpn_dollar_threshold",
        "self_funded_flag",
        "cpn_start",
        "cpn_end",
    ]
    coups_bank = read_subset_and_cast(
        job.config.paths["COUPON_BANK"], "csv", col_names
    )
    coups_bank = capitalize_col_names(coups_bank)

    # coupon map
    col_names = ["cpn_nbr", "ah4_cd", "ah5_cd", "article_nbr"]
    coups_map = read_subset_and_cast(
        job.config.paths["COUPON_MAP"], "csv", col_names
    )
    coups_map = capitalize_col_names(coups_map)

    # raw members
    cols = ["MBRSHP_NBR", "MBRSHP_SID"]
    mail_list = read_subset_and_cast(job.config.paths["MAIL_LIST"], "csv")

    if "MBRSHP_NBR" not in [x.upper() for x in mail_file.columns]:
        mbr_lkup = read_subset_and_cast(
            job.config.paths["RAW_MEMBER"], "parquet", cols
        )
        mail_file = mail_file.join(mbr_lkup, "MBRSHP_SID", "left")

    # ---- Transform Data ---- #

    job.log.info("2. Transforming Data...")

    # subset and filter transaction data
    transaction = transaction.withColumn(
        "SIZE_FOR_DT",
        sqlf.date_add(
            transaction.PURCH_DT, job.config.params["num_days_backwards"]
        ),
    )
    transaction = transaction.filter(
        (transaction.SIZE_FOR_DT >= job.config.params["min_cpn_start_date"])
        & (transaction.SIZE_FOR_DT <= job.config.params["max_cpn_end_date"])
    )
    transaction = trips_only(transaction)
    job.log.info(
        "{} transaction records for dates {} to {} ".format(
            transaction.count(),
            job.config.params["min_cpn_start_date"],
            job.config.params["max_cpn_end_date"],
        )
    )
    transaction_cat = _make_category_agnostic(transaction)

    # combine category DNA
    ah4_dna = ah4_dna.withColumnRenamed("AH4_CD", "CATEGORY_ID")
    ah5_dna = ah5_dna.withColumnRenamed("AH5_CD", "CATEGORY_ID")
    cat_dna = ah4_dna.union(ah5_dna.select(ah4_dna.columns))
    cat_dna = cat_dna.fillna(0)
    cat_dna = cat_dna.filter(cat_dna.CATEGORY_ID > 0)
    cat_dna = cat_dna.dropDuplicates()
    cat_dna = cat_dna.withColumn(
        "CATEGORY_THRESHOLD",
        cat_dna.UNIT_RETAIL_PRICE * cat_dna.UNITS_PER_TRIP,
    )
    cat_dna = cat_dna.select("CATEGORY_ID", "CATEGORY_THRESHOLD")

    # fill na coupon bank
    coups_bank = coups_bank.fillna(0)
    coups_bank = coups_bank.filter(
        coups_bank.CPN_TYPE.isin(["article", "basket", "category", "special"])
    )
    coups_bank = coups_bank.dropDuplicates(subset=["CPN_NBR"])
    # cast date type
    coups_bank = coups_bank.withColumn(
        "CPN_START",
        sqlf.regexp_replace(
            coups_bank.CPN_START, "(\d+)/(\d+)/(\d+)", "$3-$1-$2"
        ).cast("date"),
    )
    coups_bank = coups_bank.withColumn(
        "CPN_END",
        sqlf.regexp_replace(
            coups_bank.CPN_END, "(\d+)/(\d+)/(\d+)", "$3-$1-$2"
        ).cast("date"),
    )

    # transform coupon map
    coups_map = _make_category_agnostic(coups_map)
    coups_map = coups_map.dropDuplicates()

    # combine coupons data
    coups_all = coups_bank.join(coups_quals, "CPN_NBR", "inner")
    coups_all = coups_all.join(coups_map, "CPN_NBR", "left")
    coups_all = coups_all.dropDuplicates()

    # rescale redemption rate
    overlap_count = mail_file.join(
        backtest_mail_list, "MBRSHP_NBR", "inner"
    ).count()
    all_count = mail_file.count()
    scale_index = (
        float(overlap_count) / all_count if all_count > 0 else float(0)
    )

    # ---- Calculate qualified transaction for redeeming coupons ---- #
    job.log.info("3. Calculate transactions...")
    mbr_coups = mail_file.join(coups_all, "CPN_NBR", "inner")
    # article offer, calculate article level transaction
    mbr_coups_art = mbr_coups.filter(
        (mbr_coups.CPN_TYPE == "article") & (mbr_coups.SELF_FUNDED_FLAG == 1)
    )
    mbr_tran_art = mbr_coups_art.join(
        transaction, ["MBRSHP_SID", "ARTICLE_NBR"], "inner"
    )
    mbr_tran_art = mbr_tran_art.filter(
        (mbr_tran_art.SIZE_FOR_DT >= mbr_tran_art.CPN_START)
        & (mbr_tran_art.SIZE_FOR_DT <= mbr_tran_art.CPN_END)
    )
    mbr_tran_art = _flag_qualifiers(mbr_tran_art)
    # category offer, calculate category level transaction
    mbr_coups_cat = mbr_coups.filter(
        (mbr_coups.CPN_TYPE == "category") & (mbr_coups.SELF_FUNDED_FLAG == 1)
    )
    mbr_tran_cat = mbr_coups_cat.join(
        transaction_cat, ["MBRSHP_SID", "CATEGORY_ID"], "inner"
    )
    mbr_tran_cat = mbr_tran_cat.filter(
        (mbr_tran_cat.SIZE_FOR_DT >= mbr_tran_cat.CPN_START)
        & (mbr_tran_cat.SIZE_FOR_DT <= mbr_tran_cat.CPN_END)
    )
    mbr_tran_cat = _flag_qualifiers(mbr_tran_cat)
    # basket offer, calculate basket level transaction
    mbr_coups_bas = mbr_coups.filter(
        (mbr_coups.CPN_TYPE == "basket") & (mbr_coups.SELF_FUNDED_FLAG == 1)
    )
    mbr_tran_bas = mbr_coups_bas.join(transaction, "MBRSHP_SID", "inner")
    mbr_tran_bas = mbr_tran_bas.filter(
        (mbr_tran_bas.SIZE_FOR_DT >= mbr_tran_bas.CPN_START)
        & (mbr_tran_bas.SIZE_FOR_DT <= mbr_tran_bas.CPN_END)
    )
    mbr_tran_bas = _flag_qualifiers(mbr_tran_bas)
    mbr_tran = mbr_tran_art.union(
        mbr_tran_cat.select(mbr_tran_art.columns)
    ).union(mbr_tran_bas.select(mbr_tran_art.columns))

    # ---- Attach club id to mbr ---- #
    job.log.info("4. Calculate club id...")
    mbr_club = mail_file.join(mbr_dna, "MBRSHP_SID", "left")
    if "MBRSHP_NBR" not in [x.upper() for x in mbr_club.columns]:
        mbr_club = mbr_club.join(mbr_lkup, "MBRSHP_SID", "left")

    mbr_club = _attach_club(mbr_club)

    # ---- create dummy threshold for "$ off any" coupons----#
    job.log.info("5. Calculate threshold for any...")
    coups_thresh = coups_all.join(cat_dna, "CATEGORY_ID", "left")
    coups_thresh = _backfill_threshold(coups_thresh)

    # ---- Generate base table for calculation----#
    job.log.info("6. Generate base table...")
    df_base = mail_file.join(mbr_tran, ["MBRSHP_SID", "CPN_NBR"], "left")
    df_base = df_base.join(mbr_club, "MBRSHP_SID", "left")
    df_base = df_base.join(
        coups_bank.drop("CPN_DOLLAR_THRESHOLD"), "CPN_NBR", "left"
    )
    df_base = df_base.join(coups_thresh, "CPN_NBR", "left")

    # ---- Generate financial calculation table---#
    job.log.info("7. Generate calculation table...")
    group_cols = ["CELL_ID", "CLUB_ID"] + coups_bank.columns
    df_impr = df_base.groupby(*group_cols).agg(
        sqlf.countDistinct(df_base.MBRSHP_SID).alias("CPN_IMP")
    )

    df_impr = (
        job.sc.parallelize(df_impr.collect()).toDF(df_impr.schema).cache()
    )

    df_calc = df_base.filter(df_base.is_qualified.isNotNull())
    df_calc = df_calc.groupby(*group_cols).agg(
        sqlf.sum("is_qualified").alias("qualifiers")
    )

    df_calc = (
        job.sc.parallelize(df_calc.collect()).toDF(df_calc.schema).cache()
    )

    df_calc = df_impr.join(df_calc, group_cols, "left")
    df_calc = df_calc.filter(df_calc.SELF_FUNDED_FLAG == 1)
    df_calc = df_calc.fillna(0, subset=["qualifiers"])

    df_calc = df_calc.withColumn(
        "red_over_qual_rate",
        sqlf.when(
            df_calc.CPN_TYPE == "article",
            job.config.params["article_red_over_qual_rate"],
        )
        .when(
            df_calc.CPN_TYPE == "category",
            job.config.params["category_red_over_qual_rate"],
        )
        .when(
            df_calc.CPN_TYPE == "basket",
            job.config.params["basket_red_over_qual_rate"],
        )
        .otherwise(1),
    )
    df_calc = df_calc.withColumn(
        "cann_rate",
        sqlf.when(
            df_calc.CPN_TYPE == "article",
            job.config.params["article_cann_rate"],
        )
        .when(
            df_calc.CPN_TYPE == "category",
            job.config.params["category_cann_rate"],
        )
        .when(
            df_calc.CPN_TYPE == "basket", job.config.params["basket_cann_rate"]
        )
        .otherwise(0),
    )
    df_calc = df_calc.withColumn(
        "CPN_RED",
        (df_calc.qualifiers * df_calc.red_over_qual_rate) / scale_index,
    )
    df_calc = df_calc.withColumn(
        "CPN_COST", df_calc.CPN_RED * df_calc.CPN_DOLLAR_OFF
    )
    df_calc = df_calc.withColumn(
        "SALES_LIFT",
        sqlf.when(
            df_calc.CPN_TYPE == "basket",
            df_calc.CPN_IMP
            * job.config.params["basket_sales_lift_per_capita"],
        ).otherwise(
            df_calc.CPN_RED
            * df_calc.CPN_DOLLAR_THRESHOLD
            * (1 - df_calc.cann_rate)
        ),
    )
    df_calc = df_calc.drop("qualifiers")

    imp_group_by_cols = [
        "CPN_NBR",
        "OFFER_ID",
        "CPN_CLASS_ID",
        "CPN_TYPE",
        "CPN_DESC",
    ]
    imp_cols = ["MBRSHP_SID"] + imp_group_by_cols

    coups_bas = (
        coups_all.withColumn("MBRSHP_SID", sqlf.lit(-1))
        .select(imp_cols)
        .distinct()
    )
    df_base = df_base.cache()
    # ---- OUTPUT 1: impressions by coupons (for printer) ---- #
    job.log.info("output impressions...")
    df_output_1 = (
        df_base.select(imp_cols)
        .union(coups_bas)
        .groupby(imp_group_by_cols)
        .agg(
            sqlf.countDistinct(df_base.MBRSHP_SID).alias("CPN_IMP"),
            sqlf.count(df_base.MBRSHP_SID).alias("impressions_check"),
        )
        .withColumn("CPN_IMP", sqlf.col("CPN_IMP") - sqlf.lit(1))
        .withColumn(
            "impressions_check", sqlf.col("impressions_check") - sqlf.lit(1)
        )
    )

    job.data.add("cpn_impression", df_output_1)
    job.data.write(
        "cpn_impression",
        "SIZING_IMP",
        mode="overwrite",
        singlefile=True,
        ftype="csv",
    )

    # ---- OUTPUT 2: est. CPN_RED and cost by self-funded category coupons (for budgeting)---- #
    job.log.info("output coupon redemptions...")
    df_output_2 = df_calc.groupby(
        "CPN_NBR", "OFFER_ID", "CPN_CLASS_ID", "CPN_TYPE", "CPN_DESC"
    ).agg(
        sqlf.sum(df_calc.CPN_IMP).alias("CPN_IMP"),
        sqlf.sum(df_calc.CPN_RED).alias("CPN_RED"),
        sqlf.sum(df_calc.CPN_COST).alias("CPN_COST"),
        sqlf.sum(df_calc.SALES_LIFT).alias("SALES_LIFT"),
    )

    job.data.add("cpn_redemption", df_output_2)
    job.data.write(
        "cpn_redemption",
        "SIZING_BUDGET",
        mode="overwrite",
        singlefile=True,
        ftype="csv",
    )

    # ---- OUTPUT 3: cell level est. CPN_RED and cost by self funded category coupons (for analytic) ---- #
    job.log.info("output cell redemptions...")
    df_output_3 = df_calc.groupby(
        "CELL_ID",
        "OFFER_ID",
        "CPN_CLASS_ID",
        "CPN_NBR",
        "CPN_TYPE",
        "CPN_DESC",
    ).agg(
        sqlf.sum(df_calc.CPN_IMP).alias("CPN_IMP"),
        sqlf.sum(df_calc.CPN_RED).alias("CPN_RED"),
        sqlf.sum(df_calc.CPN_COST).alias("CPN_COST"),
    )

    job.data.add("cell_redemption", df_output_3)
    job.data.write(
        "cell_redemption",
        "SIZING_CELL",
        mode="overwrite",
        singlefile=True,
        ftype="csv",
    )

    # ---- OUTPUT 4: club level est. CPN_RED and cost by self funded category coupons (for merchants) ---- #
    job.log.info("output club redemptions")
    df_output_4 = df_calc.groupby(
        "CLUB_ID",
        "OFFER_ID",
        "CPN_CLASS_ID",
        "CPN_NBR",
        "CPN_TYPE",
        "CPN_DESC",
    ).agg(
        sqlf.sum(df_calc.CPN_IMP).alias("CPN_IMP"),
        sqlf.sum(df_calc.CPN_RED).alias("CPN_RED"),
        sqlf.sum(df_calc.CPN_COST).alias("CPN_COST"),
    )

    job.data.add("club_redemption", df_output_4)
    job.data.write(
        "club_redemption",
        "SIZING_CLUB",
        mode="overwrite",
        singlefile=True,
        ftype="csv",
    )

    job.log.info("done")


if __name__ == "__main__":

    main()
