"""
The script contains all the functions required to generate the transaction
related features.

Functions starting with the keyword "feature_" add features to the data,
whereas the ones starting with "__" are helper functions
"""

import numpy as np
import pe_memberdna.dna.member.lib.utils as utils
import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
import pyspark.sql.window as W
import word2number.w2n as w2n


def feature_grouped_tender_spend_nw(job, dna, lb_weeks=51):
    """
    Calculate spend by tender group.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        lb_weeks (int): weeks to consider
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    tender_group_spend, gen = __all_preprocessing(job, dna)

    final_output, new_features = __loop_through_groups(
        job, dna, gen, tender_group_spend, lb_weeks
    )

    joins = ["MBRSHP_SID", "FISCAL_WEEK_END"]
    dna = dna.join(final_output, joins, "left")

    dna = utils.set_default_value(dna, new_features, value=0)

    return dna


def feature_stdev(job, dna):
    """
    Aggregate trips and spend on n week chunks and computes standard deviation
    looking back k weeks with respect to those chunks
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with standard deviation of trips and
                                     spend
    """
    detail_isnr = job.data.tables["detail_isnr"]
    skeleton = job.data.tables["population"]

    stdev_lb = 51
    ts_lb = 3
    stdev_win_parts = ["MBRSHP_SID", "MODULO"]
    ts_win_parts = ["MBRSHP_SID"]

    trips_spend_df = __preprocess_df(detail_isnr, skeleton)

    nas = {"TRIPS": 0, "SPEND": 0}
    trips_spend_df = trips_spend_df.fillna(nas)

    mod_epoch_df = __epoch_and_modulo(trips_spend_df, ts_lb)

    lb_ts_df = __lb_trips_and_spend(mod_epoch_df, ts_win_parts, ts_lb)

    stdev_ts_df = __lb_stdev(lb_ts_df, stdev_win_parts, stdev_lb, ts_lb)

    stdev_df = __postprocess_df(stdev_ts_df)

    joins = ["MBRSHP_SID", "FISCAL_WEEK_END"]
    dna = dna.join(stdev_df, joins, "left")

    dna = utils.set_default_value(dna, stdev_df.columns, value=None)

    utils.remove_spark_df(
        (trips_spend_df, mod_epoch_df, lb_ts_df, stdev_ts_df, stdev_df)
    )

    return dna


def feature_member_category(
    job, dna, is_in, is_not_in, cols, category_name, weeks_list
):
    """
    Provide a fiscal week as well as a look back context to determine whether
    or not a customer has bought any items in a given category. The categories
    are custom, meaning that there is a custom word search filter on
    either the AH or MH columns.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        is_in (list(str)): existance filter for cols
        is_not_in (list(str)): absence filter for cols
        cols (list(str)): the columns to consider
        category_name (str): the feature name
        weeks_list (list(int)) number of weeks to consider
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    feature_name = "HAS_BOUGHT_{}".format(category_name)
    window_feature_name = utils.fiscal_week_feature_name(1, feature_name)

    df = __filter_finite(
        job.data.tables["detail_isnr"],
        window_feature_name,
        is_in,
        is_not_in,
        cols,
    )
    dna = dna.join(df, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer")
    dna = dna.fillna(0, subset=[window_feature_name])
    dna, new_features = __apply_weeks_back(
        dna, weeks_list, feature_name, window_feature_name
    )

    dna = utils.set_default_value(dna, new_features, value=0)

    return dna


def feature_trips(job, dna):
    """
    Calculate the weekly trips
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with weekly trips
    """
    detail_isnr = job.data.tables["detail_isnr"]

    trips_col = "WEEK_TRIPS"
    weeks_back = ["FOUR", "EIGHT", "TWELVE", "TWENTY-SIX", "FIFTY-TWO"]

    trips_df = __week_trips(detail_isnr, trips_col)

    dna = dna.join(trips_df, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer")
    columns = trips_df.columns

    dna, new_cols = __compute_trip_lookbacks(dna, weeks_back, trips_col)
    columns.extend(new_cols)

    dna = utils.set_default_value(dna, columns, value=0)

    utils.remove_spark_df((trips_df))

    return dna


def feature_member_basket_size(job, dna, lb_weeks=None):
    """
    Provide a fiscal week as well as a look back context to calculate the
    median basket size.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        lb_weeks (int): weeks to consider
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    metric_column = "EXTENDED_PRC_AMT"
    basket_name = "L" + str(lb_weeks + 1) + "W_MEDIAN_BASKETSIZE"

    rel_det = __aggregate_daily(job, dna, metric_column)

    epoched_df = __create_epoch(rel_det)

    window = utils.create_epoch_window(lb_weeks)

    windowed_df, new_features = __add_window(
        epoched_df, window, basket_name, metric_column
    )

    final_df = windowed_df.select(
        "MBRSHP_SID", "FISCAL_WEEK_END", basket_name
    ).dropDuplicates()

    joins = ["MBRSHP_SID", "FISCAL_WEEK_END"]

    dna = dna.join(final_df, joins, "left")

    dna = utils.set_default_value(dna, new_features, value=0)

    return dna


def feature_spend(job, dna):
    """
    Calculate the total spend per customer within a fiscal week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with weekly spend
    """
    header = job.data.tables["header"]
    detail = job.data.tables["detail"]

    relevant_amounts = header.select(
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        (header.TOT_SALES_AMT).alias("WEEK_SPEND"),
    )

    relevant_amounts = relevant_amounts.union(
        detail.select(
            "MBRSHP_SID",
            "FISCAL_WEEK_END",
            (detail.REDUCTION_AMT).alias("WEEK_SPEND"),
        ).filter(detail.DISCOUNT_TYPE_CD.isin(["ZCOU", "ZPAP"]))
    )

    sum_relevant_amounts = relevant_amounts.groupBy(
        "MBRSHP_SID", "FISCAL_WEEK_END"
    ).agg(sqlf.sum("WEEK_SPEND").alias("WEEK_SPEND"))

    dna = dna.join(
        sum_relevant_amounts, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, sum_relevant_amounts.columns, value=0)

    utils.remove_spark_df((sum_relevant_amounts, relevant_amounts))

    return dna


def feature_distinct_categories(job, dna):
    """
    Compute the number of distinct ah4_cds over the past 52 weeks.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    feature_name = "L52W_DISTINCT_CATEGORIES"
    new_features = [feature_name]

    sel = {"MBRSHP_SID", "FISCAL_WEEK_END", "AH4_CD"}
    ah4s = job.data.tables["detail_isnr"].select(*sel).dropDuplicates()

    ah4s = ah4s.withColumnRenamed("MBRSHP_SID", "_sid").withColumnRenamed(
        "FISCAL_WEEK_END", "_fwe"
    )

    feature_population = job.data.tables["feature_population"]
    eq = feature_population["MBRSHP_SID"] == ah4s["_sid"]
    ineq = ah4s["_fwe"].between(
        feature_population["FISCAL_L52W_END"],
        feature_population["FISCAL_WEEK_END"],
    )

    joined = feature_population.join(ah4s, eq & ineq, "left")

    grouped = joined.groupBy("MBRSHP_SID", "FISCAL_WEEK_END").agg(
        sqlf.countDistinct("AH4_CD").alias(feature_name)
    )

    dna = dna.join(grouped, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left")

    dna = utils.set_default_value(dna, new_features, value=0)

    del eq
    del ineq
    utils.remove_spark_df((joined, grouped, ah4s))

    return dna


def feature_spend_in_store(job, dna):
    """
    Calculate the total spend per customer within a fiscal week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with the amount spend in store
    """
    detail_isnr = job.data.tables["detail_isnr"]
    detail = job.data.tables["detail"]

    relevant_amounts = detail_isnr.select(
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        (detail_isnr.EXTENDED_PRC_AMT).alias("FW_SPEND_IN_STORE"),
    )

    relevant_amounts = relevant_amounts.union(
        detail.select(
            "MBRSHP_SID",
            "FISCAL_WEEK_END",
            (detail.REDUCTION_AMT).alias("FW_SPEND_IN_STORE"),
        ).filter(detail.DISCOUNT_TYPE_CD.isin(["ZCOU", "ZPAP"]))
    )

    sum_relevant_amounts = relevant_amounts.groupBy(
        "MBRSHP_SID", "FISCAL_WEEK_END"
    ).agg(sqlf.sum("FW_SPEND_IN_STORE").alias("FW_SPEND_IN_STORE"))

    dna = dna.join(
        sum_relevant_amounts, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, sum_relevant_amounts.columns, value=0)

    utils.remove_spark_df((sum_relevant_amounts, relevant_amounts))

    return dna


def feature_days_since_last_trip(job, dna):
    """
    Calculate number of days since last time a member had a trip to BJs.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    detail_isnr = job.data.tables["detail_isnr"]
    last_fiscal_trip = (
        detail_isnr.filter(
            ~detail_isnr.MC_CD.isin(["402030190", "402030191", "203010098"])
        )
        .groupBy(["MBRSHP_SID", "FISCAL_WEEK_END"])
        .agg(sqlf.max("PURCH_DT").alias("LAST_FISCAL_WEEK_TRIP"))
    )

    dna, new_features = __add_feature(dna, last_fiscal_trip, "TRIP")

    dna = utils.set_default_value(dna, new_features)

    utils.remove_spark_df(last_fiscal_trip)

    return dna


def feature_units(job, dna):
    """
    Calculate the total units per customer within a fiscal week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with weekly units purchased
    """
    detail = job.data.tables["detail"]

    detail_trans = (
        detail.select(
            "MBRSHP_SID",
            "FISCAL_WEEK_END",
            "SALES_QTY",
            "SALES_CTGRY_CD",
            "QTY_IN_UNITS",
        )
        .filter(
            (sqlf.col("SALES_QTY") > 0) & (sqlf.col("SALES_CTGRY_CD") == "03")
        )
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.sum("QTY_IN_UNITS").alias("WEEK_UNITS"))
    )

    dna = dna.join(
        detail_trans, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, detail_trans.columns, value=0)

    utils.remove_spark_df(detail_trans)

    return dna


def feature_trip_intervals(job, dna, weeks):
    """
    Build feature for min and max intervals.
    An interval is the number of days between a trip. The min and max intervals
    for the past {weeks} weeks is defined by the minimum and maximum interval
    between trips that occured within those weeks. NOTE: This does not account
    for the edge case where the first interval has a trip occuring outside the
    lookback window, this should be fixed in the future.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        weeks (int): number of weeks to consider
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    max_feature_name = utils.fiscal_week_feature_name(weeks, "MAX_INTERVAL")
    min_feature_name = utils.fiscal_week_feature_name(weeks, "MIN_INTERVAL")

    new_features = [max_feature_name, min_feature_name]

    trips = __trip_days(job.data.tables["detail_isnr"])

    calc_inter = __intervals(trips)

    min_max_inter = __min_max_intervals(calc_inter, weeks)

    min_max_inter = (
        job.data.tables["population"]
        .select("MBRSHP_SID", "FISCAL_WEEK_END")
        .join(min_max_inter, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer")
    )

    min_max_inter = __last_interval(
        min_max_inter, weeks, max_feature_name, min_feature_name
    )

    select_cols = {
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        max_feature_name,
        min_feature_name,
    }

    dna = dna.join(
        min_max_inter.select(*select_cols),
        ["MBRSHP_SID", "FISCAL_WEEK_END"],
        "left_outer",
    )

    dna = utils.set_default_value(dna, new_features)

    return dna


def feature_units_over_fifty(job, dna):
    """
    Calculate the total units that cost over $50 per customer within a fiscal
    week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with number of units that cost over
                                     $50
    """
    detail = job.data.tables["detail"]

    detail_trans = (
        detail.select(
            "MBRSHP_SID",
            "FISCAL_WEEK_END",
            "SALES_QTY",
            "SALES_CTGRY_CD",
            "QTY_IN_UNITS",
            "EXTENDED_UNIT_PRC_AMT",
        )
        .filter(
            (sqlf.col("SALES_QTY") > 0)
            & (sqlf.col("SALES_CTGRY_CD") == "03")
            & (sqlf.col("EXTENDED_UNIT_PRC_AMT") > 50.0)
        )
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.sum("QTY_IN_UNITS").alias("WEEK_UNITS_OVER_FIFTY"))
    )

    dna = dna.join(
        detail_trans, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, detail_trans.columns, value=0)

    utils.remove_spark_df(detail_trans)

    return dna


def feature_gas_trips(job, dna):
    """
    Calculate the number of gas transactions within a given fiscal week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with the number of gas trips
    """
    detail = job.data.tables["detail"]

    detail_trans = (
        detail.select(
            "MBRSHP_SID", "FISCAL_WEEK_END", "SALES_CTGRY_CD", "PURCH_HDR_ID"
        )
        .filter(sqlf.col("SALES_CTGRY_CD") == "01")
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.countDistinct("PURCH_HDR_ID").alias("FW_GAS_TRIPS"))
    )

    dna = dna.join(
        detail_trans, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, detail_trans.columns, value=0)

    utils.remove_spark_df(detail_trans)

    return dna


def feature_gas_distinct_days(job, dna):
    """
    Calculate the number of gas transactions within a given fiscal week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with distinct days where gas purchases
                                     were made
    """
    detail = job.data.tables["detail"]

    detail_trans = (
        detail.select(
            "MBRSHP_SID", "FISCAL_WEEK_END", "SALES_CTGRY_CD", "PURCH_DT"
        )
        .filter(sqlf.col("SALES_CTGRY_CD") == "01")
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.countDistinct("PURCH_DT").alias("FW_GAS_DISTINCT_DAYS"))
    )

    dna = dna.join(
        detail_trans, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, detail_trans.columns, value=0)

    utils.remove_spark_df(detail_trans)

    return dna


def feature_gas_spend(job, dna):
    """
    Calculate the total gas spend within a given fiscal week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with the gas spend
    """
    detail = job.data.tables["detail"]

    detail_trans = (
        detail.select(
            "MBRSHP_SID",
            "FISCAL_WEEK_END",
            "SALES_CTGRY_CD",
            "EXTENDED_PRC_AMT",
        )
        .filter(sqlf.col("SALES_CTGRY_CD") == "01")
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.sum("EXTENDED_PRC_AMT").alias("FW_GAS_SPEND"))
    )
    dna = dna.join(
        detail_trans, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, detail_trans.columns, value=0)

    utils.remove_spark_df(detail_trans)

    return dna


def feature_gas_and_store_distinct_days(job, dna):
    """
    Calculate the number of total trips including gas trips within a given
    fiscal week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with distinct days where gas or store
                                     purchases were made
    """
    detail = job.data.tables["detail"]

    detail_trans = (
        detail.select(
            "MBRSHP_SID",
            "FISCAL_WEEK_END",
            "SALES_QTY",
            "SALES_CTGRY_CD",
            "PURCH_DT",
        )
        .filter(
            (sqlf.col("SALES_QTY") > 0)
            & (sqlf.col("SALES_CTGRY_CD").isin(["01", "03"]))
        )
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END", "PURCH_DT")
        .agg(
            sqlf.collect_set(sqlf.col("SALES_CTGRY_CD")).alias(
                "SALES_CTGRY_SET"
            )
        )
    )

    detail_trans = detail_trans.withColumn(
        "GAS_AND_STORE",
        sqlf.when(sqlf.size(detail_trans.SALES_CTGRY_SET) > 1, 1).otherwise(0),
    )

    detail_trans = detail_trans.groupBy("MBRSHP_SID", "FISCAL_WEEK_END").agg(
        sqlf.sum("GAS_AND_STORE").alias("FW_GAS_AND_STORE_DISTINCT_DAYS")
    )

    dna = dna.join(
        detail_trans, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, detail_trans.columns, value=0)

    utils.remove_spark_df(detail_trans)

    return dna


def feature_ecommerce_metric(job, dna):
    """
    Calculate the number of total ecommerce trips and spend within a given
    fiscal week
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with ecommerce spend and trips
    """
    detail = job.data.tables["detail"]

    spend = sqlf.sum("EXTENDED_PRC_AMT").alias("FW_ECOMMERCE_SPEND")
    trip = sqlf.countDistinct("PURCH_HDR_ID").alias("FW_ECOMMERCE_TRIPS")
    mc_cd_exclusions = ["402030190", "402030191", "203010098", "B01010003"]
    detail_trans = (
        detail.select(
            "MBRSHP_SID",
            "FISCAL_WEEK_END",
            "SITE_NBR",
            "EXTENDED_PRC_AMT",
            "PURCH_HDR_ID",
        )
        .filter(
            (sqlf.col("SITE_NBR") == 549)
            & (~detail.MC_CD.isin(*mc_cd_exclusions))
        )
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(spend, trip)
    )

    dna = dna.join(
        detail_trans, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, detail_trans.columns, value=0)

    utils.remove_spark_df(detail_trans)

    return dna


def feature_distinct_days(job, dna):
    """
    Compute the number of distinct days a customer visited a  client store
    location. Distinct days is defined as a count distinct on PURCH_DT.
    Also compute lookbacks on distinct days, i.e how many distinct days did the
    customer visit in the past 4 weeks? 8 weeks? or even fifty-two weeks? The
    number of lookbacks is configured in the constructors "weeks_back"
    parameter.
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with distinct days where purchases
                                     were made
    """
    header = job.data.tables["header"]

    distinct_days_col = "WEEK_DISTINCT_DAYS"

    weeks_back = ["FOUR", "EIGHT", "TWELVE", "TWENTY-SIX", "FIFTY-TWO"]

    distinct_days = __week_distinct_days(header, distinct_days_col)

    dna = dna.join(
        distinct_days, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )
    columns = distinct_days.columns

    dna, new_cols = __compute_distinct_days_lookbacks(
        dna, weeks_back, distinct_days_col
    )
    columns.extend(new_cols)

    dna = utils.set_default_value(dna, columns, value=0)

    utils.remove_spark_df((distinct_days))

    return dna


def feature_agg_spend(job, dna, num_weeks):
    """
    Compute the aggregate spend per customer {weeks} fiscal weeks back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        spend_name = "LAST_" + weeks + "_WEEK_SPEND"
        dna = dna.withColumn(spend_name, sqlf.sum("WEEK_SPEND").over(window))
        new_cols.append(spend_name)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_agg_spend_in_store(job, dna, num_weeks):
    """
    Compute the aggregate customer spend in store for {weeks} fiscal weeks
    back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        spend_in_store_name = "L" + weeks + "W_SPEND_IN_STORE"
        dna = dna.withColumn(
            spend_in_store_name, sqlf.sum("FW_SPEND_IN_STORE").over(window)
        )
        new_cols.append(spend_in_store_name)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_agg_units(job, dna, num_weeks):
    """
    Compute the aggregate units a cusomter bought {weeks} fiscal weeks back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        category_units = "LAST_" + weeks + "_WEEK_UNITS"
        dna = dna.withColumn(
            category_units, sqlf.sum("WEEK_UNITS").over(window)
        )
        new_cols.append(category_units)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_agg_units_over_fifty(job, dna, num_weeks):
    """
    Compute the aggregate units over $50 a cusomter bought {weeks} fiscal
    weeks back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        category_units = "LAST_" + weeks + "_WEEK_UNITS_OVER_FIFTY"
        dna = dna.withColumn(
            category_units, sqlf.sum("WEEK_UNITS_OVER_FIFTY").over(window)
        )
        new_cols.append(category_units)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_agg_gas_trips(job, dna, num_weeks):
    """
    Compute the aggregate gas trips (defined by client) a cusomter took
    {weeks} fiscal weeks back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        gas_trips_name = "L_" + weeks + "W_GAS_TRIPS"
        dna = dna.withColumn(
            gas_trips_name, sqlf.sum("FW_GAS_TRIPS").over(window)
        )
        new_cols.append(gas_trips_name)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_agg_gas_distinct_days(job, dna, num_weeks):
    """
    Compute the aggregate gas transactions {weeks} fiscal weeks back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        gas_distinct_days_name = "L_" + weeks + "W_GAS_DISTINCT_DAYS"
        dna = dna.withColumn(
            gas_distinct_days_name,
            sqlf.sum("FW_GAS_DISTINCT_DAYS").over(window),
        )
        new_cols.append(gas_distinct_days_name)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_agg_gas_spend(job, dna, num_weeks):
    """
    Compute the aggregate gas spend {weeks} fiscal weeks back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        gas_spend_name = "L_" + weeks + "W_GAS_SPEND"
        dna = dna.withColumn(
            gas_spend_name, sqlf.sum("FW_GAS_SPEND").over(window)
        )
        new_cols.append(gas_spend_name)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_agg_gas_and_store_distinct_days(job, dna, num_weeks):
    """
    Compute the aggregate gas and store transactions {weeks} fiscal weeks
    back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        gas_spend_name = "L_" + weeks + "W_GAS_AND_STORE_DISTINCT_DAYS"
        dna = dna.withColumn(
            gas_spend_name,
            sqlf.sum("FW_GAS_AND_STORE_DISTINCT_DAYS").over(window),
        )
        new_cols.append(gas_spend_name)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_agg_ecommerce_metric(job, dna, num_weeks, metric):
    """
    Compute the aggregate ecommerce trips/spend {weeks} fiscal weeks
    back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (list(str)): List of string names of the weeks to compute back
        metric (str): trips/spend

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)

        if metric == "spend":
            ecommerce_spend_name = "L_" + weeks + "W_ECOMMERCE_SPEND"
            dna = dna.withColumn(
                ecommerce_spend_name,
                sqlf.sum("FW_ECOMMERCE_SPEND").over(window),
            )
            dna = utils.set_default_value(dna, [ecommerce_spend_name], value=0)
            new_cols.append(ecommerce_spend_name)

        elif metric == "trips":
            ecommerce_trips_name = "L_" + weeks + "W_ECOMMERCE_TRIPS"
            dna = dna.withColumn(
                ecommerce_trips_name,
                sqlf.sum("FW_ECOMMERCE_TRIPS").over(window),
            )
            dna = utils.set_default_value(dna, [ecommerce_trips_name], value=0)
            new_cols.append(ecommerce_trips_name)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def feature_transactions(job, dna):
    """
    Calculate the total transactions per customer within a fiscal week.

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with weekly transactions
    """
    header = job.data.tables["header"]
    transaction_col = "WEEK_TRANSACTIONS"

    transaction_counts = header.groupby("MBRSHP_SID", "FISCAL_WEEK_END").agg(
        sqlf.countDistinct("PURCH_HDR_ID").alias(transaction_col)
    )

    dna = dna.join(
        transaction_counts, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left"
    )

    dna = utils.set_default_value(dna, [transaction_col], value=0)

    utils.remove_spark_df(transaction_counts)

    return dna


def feature_agg_transactions(job, dna, num_weeks):
    """
    Compute the aggregate transactions per customer {weeks} fiscal weeks back

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        num_weeks (list(str)): List of string names of the weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new columns
    """

    new_cols = []
    for weeks in num_weeks:
        window = __get_window(weeks)
        col_name = f"LAST_{weeks}_WEEK_TRANSACTIONS"
        dna = dna.withColumn(
            col_name, sqlf.sum("WEEK_TRANSACTIONS").over(window)
        )
        new_cols.append(col_name)

    dna = utils.set_default_value(dna, new_cols, value=0)

    return dna


def __to_epoch(date_col):
    """
    Convert the date column to an epoch (cast as long)
    Parameters:
        date_col (pyspark.sql.column): column containing readable dates

    Returns:
        (pyspark.sql.column): column containing corresponding epoch values
    """
    return date_col.cast("timestamp").cast("long")


def __modulo(column, mod):
    """
    Columnar operation to reduce modulo 'mod'
    Parameters:
        column (pyspark.sql.column): column with the dividend values
        mod (int): divisor

    Returns:
        (pyspark.sql.column): column with the remainder values
    """
    return column % sqlf.lit(mod)


def __epoch_to_weeks(epoch):
    """
    Convert an epoch column to a weeks column
    Parameters:
        epoch (pyspark.sql.column): column containing epoch values

    Returns:
        (pyspark.sql.column): column with the weeks
    """
    days_per_week = 7
    hours_per_day = 24
    minutes_per_hour = 60
    seconds_per_minute = 60

    final_scalar = (
        days_per_week * hours_per_day * minutes_per_hour * seconds_per_minute
    )

    return epoch / sqlf.lit(final_scalar)


def __fwe_to_thurs(fwe):
    """
    Since Unix Epoch Time starts on a Thursday, to get the number of weeks, we
    need to convert all our Fiscal Week End Saturdays to Thursdays. We need a
    number that is 0 mod 7
    Parameters:
        fwe (pyspark.sql.column): column with fiscal weekends on Saturday

    Return:
        (pyspark.sql.column): column with fiscal weekends on Thursday
    """
    return sqlf.date_add(fwe, -2)


def __weeks_to_seconds(weeks):
    """
    Produce a single number NOT a column  for use in window function
    Parameters:
        weeks (int): Number of weeks

    Returns:
        (float): Number of weeks converted to seconds
    """
    days_per_week = 7
    hours_per_day = 24
    minutes_per_hour = 60
    seconds_per_minute = 60

    final_scalar = (
        days_per_week * hours_per_day * minutes_per_hour * seconds_per_minute
    )

    return final_scalar * weeks


def __preprocess_df(detail_isnr, skeleton):
    """
    Compute spend and trips by member and fiscal week end from detail_isnr.
    Replace nulls with 0s so standard deviation calc knows what to do
    Parameters:
        details_isnr (pyspark.sql.DataFrame): details_isnr dataframe
        skeleton (pyspark.sql.DataFrame): population dataframe

    Returns:
        (pyspark.sql.DataFrame): as explained above
    """

    trips_agg = sqlf.countDistinct("PURCH_DT").alias("TRIPS")
    spend_agg = sqlf.sum("EXTENDED_PRC_AMT").alias("SPEND")

    trips_spend_df = detail_isnr.groupBy("MBRSHP_SID", "FISCAL_WEEK_END").agg(
        trips_agg, spend_agg
    )

    joins = ["MBRSHP_SID", "FISCAL_WEEK_END"]

    trips_spend_df = trips_spend_df.join(
        skeleton.select(*joins), joins, "right"
    )

    return trips_spend_df


def __epoch_and_modulo(trips_spend_df, ts_lb):
    """
    We convert the week column to the nearest thursday then find its epoch,
    then count the number of weeks that translates to.  This way they can be
    broken into n week chunks in a natural way by reducing modulo n.
    Parameters:
        trips_spend_df (pyspark.sql.DataFrame): output of __prerpocess_df
        ts_lb (int): number of weeks

    Returns:
        (pyspark.sql.DataFrame): as explained above
    """

    fwe = trips_spend_df["FISCAL_WEEK_END"]

    epoch_col = __to_epoch(fwe)

    mod = ts_lb + 1
    group_col = __modulo(
        __epoch_to_weeks(__to_epoch(__fwe_to_thurs(fwe))), mod
    )

    mod_epoch_df = trips_spend_df.withColumn("EPOCH", epoch_col).withColumn(
        "MODULO", group_col
    )

    return mod_epoch_df


def __lb_trips_and_spend(mod_epoch_df, ts_win_parts, ts_lb):
    """
    Get the sum of spend and trips for the previous n weeks for each week
    Parameters:
        mod_epoch_df (pyspark.sql.DataFrame): output of __epoch_and_modulo
        ts_win_parts (int): number of window partitions
        ts_lb (int): number of weeks

    Returns:
        (pyspark.sql.DataFrame): as explained above
    """
    ts_win = utils.create_epoch_window(ts_lb, cols=ts_win_parts)

    lb_ts_df = mod_epoch_df.withColumn(
        "LB_TRIPS", sqlf.sum("TRIPS").over(ts_win)
    ).withColumn("LB_SPEND", sqlf.sum("SPEND").over(ts_win))

    return lb_ts_df


def __lb_stdev(lb_ts_df, stdev_win_parts, stdev_lb, ts_lb):
    """
    Now that we have n week aggregations for trips and spend and each week has
    been assigned a group based on their n week lookback chain, we can compute
    the stdev with respect to those groups
    Parameters:
        lb_ts_df (pyspark.sql.DataFrame): output of __lb_trips_and_spend
        stdev_win_parts (int): number of window partitions
        stdev_lb (int): number of weeks

    Returns:
        (pyspark.sql.DataFrame): as explained above
    """
    stdev_win = utils.create_epoch_window(stdev_lb, cols=stdev_win_parts)

    prefix = "L" + str(stdev_lb + 1) + "W_" + "G" + str(ts_lb + 1) + "W_STDEV_"

    trips_name = prefix + "TRIPS"
    spend_name = prefix + "SPEND"

    stdev_ts_df = lb_ts_df.withColumn(
        trips_name, sqlf.stddev("LB_TRIPS").over(stdev_win)
    ).withColumn(spend_name, sqlf.stddev("LB_SPEND").over(stdev_win))

    return stdev_ts_df


def __postprocess_df(stdev_trips_df):
    """
    Remove unnecessary columns constructed to facilitate computation
    Parameters:
        stdev_trips_df (pyspark.sql.DataFrame): output of __lb_stdev

    Returns:
        (pyspark.sql.DataFrame): with deleted columns
    """

    kill_cols = {"TRIPS", "SPEND", "LB_TRIPS", "LB_SPEND", "EPOCH", "MODULO"}

    return stdev_trips_df.drop(*kill_cols)


def __week_trips(detail_isnr, trips_col):
    """
    Count trips in each fiscal week per member. Trips, as defined by the client
    are unique PURCH_HDR_ID where the member was purchasing items in the store,
    did not return items and did not purchase from a specific set of MC_CDs for
    things like cafe etc.

    Parameters:
        detail_isnr (pyspark.sql.Dataframe): The detail_isnr intermediate
                                           dataframe
        trips_col (list): list of column names

    Returns:
        (pyspark.sql.Dataframe): Dataframe with member, fiscal_week_end and
                                     trips
    """
    groupby_cols = {"MBRSHP_SID", "FISCAL_WEEK_END"}

    mc_cd_exclusions = ["402030190", "402030191", "203010098"]

    return (
        detail_isnr.filter(~detail_isnr.MC_CD.isin(*mc_cd_exclusions))
        .groupby(*groupby_cols)
        .agg(sqlf.countDistinct(detail_isnr.PURCH_HDR_ID).alias(trips_col))
    )


def __compute_trip_lookbacks(dna, weeks_back, trips_col):
    """
    Aggregate trips over the select number of windows.
    weeks_back is the windows to compute over.

    Parameters:
        dna (pyspark.sql.Dataframe): data containing the population of interest
        weeks_back (list): list of week intervals where the aggregate needs to
                           be calculated
        trips_col (list): list of all trips columns

    Returns:
        (pyspark.sql.Dataframe): as explained above
    """
    feat_list = []
    for weeks in weeks_back:
        feat = "LAST_{}_WEEK_TRIPS".format(weeks)
        w = __get_window(weeks)
        dna = dna.withColumn(feat, sqlf.sum(trips_col).over(w))
        feat_list.append(feat)

    return dna, feat_list


def __week_distinct_days(header, distinct_days_col):
    """
    Calculate the number of distinct days in a week per
    member. Transactions are distinct number of PURCH_DTs.

    Parameters:
        header (pyspark.sql.Dataframe): The header intermediate dataframe
        distinct_days_col (list): list of columns

    Returns:
        (pyspark.sql.Dataframe): Dataframe with member, fiscal_week_end and
                                 distinct_days
    """
    groupby_cols = {"MBRSHP_SID", "FISCAL_WEEK_END"}

    return header.groupby(*groupby_cols).agg(
        sqlf.countDistinct(header.PURCH_DT).alias(distinct_days_col)
    )


def __compute_distinct_days_lookbacks(dna, weeks_back, distinct_days_col):
    """
    Aggregate distinct_days over the select number of windows.
    weeks_back is the windows to compute over.

    Parameters:
        dna (pyspark.sql.Dataframe): data containing the population of interest
        weeks_back (list): list of week intervals where the aggregate needs to
                           be calculated
        distinct_days_col (list): list of columns

    Returns:
        (pyspark.sql.Dataframe): as explained above
    """
    feat_list = []
    for weeks in weeks_back:
        feat = "LAST_{}_WEEK_DISTINCT_DAYS".format(weeks)
        w = __get_window(weeks)
        dna = dna.withColumn(feat, sqlf.sum(distinct_days_col).over(w))
        feat_list.append(feat)

    return dna, feat_list


def __get_window(weeks):
    """
    Defines window in order to compute columns

    Args:
        weeks (str): String name of the weeks to compute back

    Returns:
        (pyspark.sql.window.Window): window to use to compute the column
    """
    rows_back = -(w2n.word_to_num(weeks) - 1)
    window = (
        W.Window.partitionBy("MBRSHP_SID")
        .orderBy("FISCAL_WEEK_END")
        .rowsBetween(rows_back, 0)
    )
    return window


def __all_preprocessing(job, dna):
    """
    Aggregate payment by payment type.
    Join to tender_map and rolls up by GROUPED_TENDER_TYPES.

    Parameters:
        job (object): Job Manager object based on the current config file
        dna (pyspark.sql.DataFrame): the dna object to append feature to

    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    agg_payment = __preprocess_payment(job.data.tables["payment"])

    tender_map = __preprocess_tender_map(job.data.tables["tender_map"])

    gen = __tender_group_iterator(tender_map)

    tender_group_spend = __roll_up_tender_type(agg_payment, tender_map)

    return tender_group_spend, gen


def __preprocess_payment(payment):
    """
    Aggregate payment file by tender type
    Parameters:
        payment (pyspark.sql.DataFrame): source_etl payment

    Returns:
        (pyspark.sql.DataFrame): spend by tender type
    """

    # select necessary columns from payment
    sel = payment.select(
        "MBRSHP_SID", "FISCAL_WEEK_END", "TENDER_TYPE_CD", "SALES_PYMT_AMT"
    )

    group = {"MBRSHP_SID", "FISCAL_WEEK_END", "TENDER_TYPE_CD"}
    aggs = sqlf.sum("SALES_PYMT_AMT").alias("SPEND")

    agg_payment = sel.groupBy(*group).agg(aggs)

    return agg_payment


def __preprocess_tender_map(tender_map):
    """
    Convert the GROUPED_TENDER_TYPE column to uppercase and filter out type
    'OTHER'.

    Parameters:
        tender_map (pyspark.sql.DataFrame): raw tender_map

    Returns:
        (pyspark.sql.DataFrame): uniform tender map
    """
    tm_cols = {"TENDER_TYPE_CD", "GROUPED_TENDER_TYPE"}
    tender_map = tender_map.select(*tm_cols)
    tender_map = __upper_case(tender_map, "GROUPED_TENDER_TYPE")
    tender_map = tender_map.filter(
        tender_map["GROUPED_TENDER_TYPE"] != "OTHER"
    )

    return tender_map


def __upper_case(df, col_name):
    """
    Creates a temp version of the column in upper case,
    drops the original column and renames the temp column.
    It's enough mundane steps and this avoids the clutter

    Parameters:
        df (pyspark.sql.DataFrame): dataframe to add an upper case column
        col_name (str): column to transform to upper case

    Returns:
        (pyspark.sql.DataFrame): uniform tender map
    """
    upper = sqlf.upper(df[col_name])

    df = df.withColumn("_TEMP", upper)

    df = df.drop(col_name)

    df = df.withColumnRenamed("_TEMP", col_name)

    return df


def __tender_group_iterator(tender_map):
    """
    We need to loop through each GROUPED_TENDER_TYPE, so we create
    an iterator of those unique values here.

    Parameters:
        tender_map (pyspark.sql.DataFrame): raw tender_map

    Returns:
        (pyspark.sql.DataFrame): iterator over tender group types
    """

    isolated = tender_map.select("GROUPED_TENDER_TYPE").dropDuplicates()

    gen = (row["GROUPED_TENDER_TYPE"] for row in isolated.collect())

    return gen


def __roll_up_tender_type(agg_payment, tender_map):
    """
    There is a bcg-originated map which rolls up TENDER_TYPE_CD.
    Our output will be at the GROUPED_TENDER_TYPED granularity, so
    we bring that in here.

    Parameters:
        agg_payment (pyspark.sql.DataFrame): spend by tender type
        tender_map (pyspark.sql.DataFrame): uniform tender map

    Returns:
        (pyspark.sql.DataFrame): spend by tender type
    """
    bc_tender_map = sqlf.broadcast(tender_map)

    joined = agg_payment.join(bc_tender_map, "TENDER_TYPE_CD", "inner")

    group = {"MBRSHP_SID", "FISCAL_WEEK_END", "GROUPED_TENDER_TYPE"}
    aggs = sqlf.sum("SPEND").alias("SPEND")
    tender_group_spend = joined.groupBy(*group).agg(aggs)

    return tender_group_spend


def __loop_through_groups(job, dna, gen, tender_group_spend, lb_weeks):
    """
    Create a feature for each tender group.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        gen (iterator): iterator over all groups
        tender_group_spend (pyspark.sql.DataFrame): spend by tender type
        lb_weeks (int): weeks to look back

    Returns:
        (pyspark.sql.DataFrame, list(str)): dataframe with new feature,
            new columns

    """
    final_output = job.data.tables["feature_population"]

    window = utils.create_epoch_window(lb_weeks)
    new_features = []

    for gtt in gen:
        filt = tender_group_spend["GROUPED_TENDER_TYPE"] == gtt

        filtered = tender_group_spend.filter(filt)
        lb_df, new_columns = __do_lookback(
            job.data.tables["population"], filtered, gtt, window, lb_weeks
        )
        new_features.extend(new_columns)

        final_output = __population_join(final_output, lb_df)

    return final_output, new_features


def __do_lookback(population, df, tender_group, window, lb_weeks):
    """
    We do spend aggregations for the last 52 weeks for the relevant
    tender_group

    Parameters:
        population (pyspark.sql.DataFrame): the skeleton to append features to
        df (pyspark.sql.DataFrame): dataframe for 1 group
        tender_group (str): group name
        window (pyspark.window.Window): time window to partition member on
        lb_weeks (int): weeks to look back
    Returns:
        (pyspark.sql.DataFrame, list(str)): dataframe with new feature,
            new columns

    """

    filled_in = __population_join(population, df)

    epoch_col = df["FISCAL_WEEK_END"].cast("timestamp").cast("long")
    epoched_df = df.withColumn("EPOCH", epoch_col)

    new_col_name = "L" + str(lb_weeks + 1) + "W_" + tender_group
    summed = sqlf.sum(epoched_df["SPEND"]).over(window)
    lb_df = epoched_df.withColumn(new_col_name, summed)

    new_columns = [new_col_name]

    lb_df = lb_df.drop("GROUPED_TENDER_TYPE", "EPOCH", "SPEND")

    return lb_df, new_columns


def __population_join(population, right_df):
    """
    Join two dataframes which might have population columns that lead to
    duplicate columns.

    Parameters:
        population (pyspark.sql.DataFrame): a dataframe with member specific
            columns
        right_df (pyspark.sql.DataFrame): a dataframe with at least
         'MBRSHP_SID', 'FISCAL_WEEK_END'
    Returns:
        (pyspark.sql.DataFrame): the union
    """
    population = population.drop(
        "FISCAL_L8W_END",
        "FISCAL_L4W_END",
        "FISCAL_L26W_END",
        "FISCAL_L12W_END",
        "FISCAL_L52W_END",
        "FISCAL_WEEK_START",
    )

    join_fields = ["MBRSHP_SID", "FISCAL_WEEK_END"]

    return population.join(right_df, join_fields, "left")


def __filter_finite(df, window_feature_name, is_in, is_not_in, cols):
    """
    Filter the dataframe based on the regex. Applies a column with all ones.
    Reduces to fiscal week end granularity.

    Args:
        df (spark.sql.DataFrame): The dataframe to be filtered
        cols (spark.sql.Column): Applies filters on those columns specifically

    Returns
        (spark.sql.DataFrame): Three column DataFrame at Member, Fiscal Week
            End granularity. Feature column is 'FW_HAS_BOUGHT_{category}'
    """
    df = (
        df.filter(__filter_builder(is_in, is_not_in, cols))
        .withColumn(window_feature_name, sqlf.lit(1))
        .groupby("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.max(window_feature_name).alias(window_feature_name))
    )

    return df


def __filter_builder(is_in, is_not_in, cols):
    """
    Builds a long filter query to reduce the detail_isnr DataFrame for
    appropriate rows.
    The goal is to find a time in which a member bought an item in a defined
     category.
    The final filter will be in this form:
        "{SOME COL} like x_1 OR {SOME COL} like x_2 ...
         AND {SOME COL} not like y_1 AND ..."

    Parameters:
        is_in (list(str)): existance filter for cols
        is_not_in (list(str)): absence filter for cols
        cols (list(str)): the columns to consider

    Returns:
        (str): The sql query string to filter the detail_isnr DataFrame
    """
    like_cols = ["{} like".format(col) for col in cols]
    not_like_cols = ["{} not like".format(col) for col in cols]
    fuzzy_ors = __word_fuzzy_match_variations(is_in)
    fuzzy_and_nots = __word_fuzzy_match_variations(is_not_in)
    ors = __individual_filter_queries(like_cols, fuzzy_ors)
    and_nots = __individual_filter_queries(not_like_cols, fuzzy_and_nots)

    return "{} AND {}".format(" OR ".join(ors), " AND ".join(and_nots))


def __word_fuzzy_match_variations(words):
    """
    Builds all word fuzzy matches required to filter column for a list of
    words.
    e.g "'{SOME WORD} %'" or "'% {SOME WORD} %'"

    Parameters:
        words (list(str)): List of words to build the fuzzy matches with.

    Returns:
        (list(str)): List of all needed fuzzy match strings.
    """
    return [fuzz_word for word in words for fuzz_word in __word_function(word)]


def __word_function(word):
    """
    Builds all word fuzzy matches required to filter column for a word.
    e.g "'{SOME WORD} %'" or "'% {SOME WORD} %'"

    Args:
        word (str): A word to fuzzy match for

    Returns:
        fuzzies (str): Returns three fuzzy match strings
    """
    return (
        "'{} %'".format(word),
        "'% {} %'".format(word),
        "'% {}'".format(word),
    )


def __individual_filter_queries(cols, fuzzies):
    """
    Builds singular sql filter queries.
    e.g "{SOME COLUMN} is like '{SOME WORD} %'"
    Creates a list of all necessary queries and returns.

    Parameters:
        cols (list(str)): The cols to build the queries on
        fuzzies (list(str)): The word fuzzy matches

    Returns:
        (list(str)): List of all the singular sql filter queries
    """
    return ["{} {}".format(col, fuzzy) for col in cols for fuzzy in fuzzies]


def __apply_weeks_back(dna, weeks_list, feature_name, window_feature_name):
    """
    Computes new features based on the weeks back list. Determines if customer
    has bought and item within the category {weeks} weeks back.

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        weeks_list (list(int)) : List of integers determining what features to
            build
        feature_name (str): template for cumulative weeks feature
        window_feature_name (str): overall feature name

    Returns:
        (pyspark.sql.DataFrame, list(str)): dna with new features, new columns
    """

    new_columns = []
    for weeks in weeks_list:
        w = utils.create_window(weeks)
        new_feature_name = utils.fiscal_week_feature_name(weeks, feature_name)
        dna = dna.withColumn(
            new_feature_name, sqlf.max(window_feature_name).over(w)
        )
        new_columns.append(new_feature_name)

    return dna, new_columns


def __aggregate_daily(job, dna, metric_column):
    """
    Aggregate the detail_isnr file by member, fiscal week start and
    end, purchase date, and aggregate the dollar field with respect to those
    columns. We also have to add the amount redeemed as vector coupons to get
    the gross amount.
    Additional step is taken here to join with member-week skeleton.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        metric_column (str): aggregate name
    Results:
        (pyspark.sql.DataFrame): aggregated detail_isnr
    """

    rel_det = job.data.tables["detail_isnr"].select(
        "MBRSHP_SID",
        "FISCAL_WEEK_START",
        "FISCAL_WEEK_END",
        "PURCH_DT",
        metric_column,
        sqlf.lit(0).alias("REDUCTION_AMT"),
    )

    rel_det = rel_det.union(
        job.data.tables["detail"].select(
            "MBRSHP_SID",
            "FISCAL_WEEK_START",
            "FISCAL_WEEK_END",
            "PURCH_DT",
            sqlf.lit(0).alias(metric_column),
            "REDUCTION_AMT",
        )
    )

    rel_det = rel_det.withColumn(
        "REDUCTION_AMT",
        sqlf.when(sqlf.col("REDUCTION_AMT").isNull(), 0).otherwise(
            sqlf.col("REDUCTION_AMT")
        ),
    )

    date_exc = rel_det["MBRSHP_SID"].isNotNull()

    rel_det = rel_det.filter(date_exc)

    rel_det = rel_det.groupBy(
        "MBRSHP_SID", "FISCAL_WEEK_START", "FISCAL_WEEK_END", "PURCH_DT"
    ).agg(
        (sqlf.sum(metric_column) + sqlf.sum("REDUCTION_AMT")).alias(
            metric_column
        )
    )

    mbrweek_skeleton = (
        job.data.tables["population"]
        .select("MBRSHP_SID", "FISCAL_WEEK_END")
        .drop_duplicates()
    )
    joins = ["MBRSHP_SID", "FISCAL_WEEK_END"]

    rel_det_final = mbrweek_skeleton.join(rel_det, joins, "left")
    return rel_det_final


def __create_epoch(filled_df):
    """
    Cast fiscal week end as a timestamp and then as a long here.  It will give
    the unix epoch time (number of seconds since january 1, 1970).
    We need this because the windowing function for the lookback can't yet
    handle actual dates.  So we measure our 52 week lookback in seconds.

    Parameters:
        filled_df (pyspark.sql.DataFrame): dataframe to cast fiscal weekend to
            timestamp => EPOCH column
    Results:
        (pyspark.sql.DataFrame): dataframe with EPOCH column
    """

    epoch = filled_df["FISCAL_WEEK_END"].cast("timestamp").cast("long")

    # epoched_df
    return filled_df.withColumn("EPOCH", epoch)


def __add_window(epoched_df, window, basket_name, metric_column):
    """
    Compute the median 52 weeks back for each week in the table, again grouped
    by member id

    Parameters:
        epoched_df (pyspark.sql.DataFrame): dataframe with EPOCH column
        window (pyspark.window.Window): period to look into
        basket_name (str): feature name
        metric_column (pyspark.sql.Column): aggregated column
    Returns:
        (pyspark.sql.DataFrame, list(str)): dataframe with basket feature,
            feature list
    """

    median_udf = sqlf.udf(lambda x: float(np.median(x)), sqlt.FloatType())

    df_with_basket = (
        epoched_df.withColumn(
            "list", sqlf.collect_list(metric_column).over(window)
        )
        .drop_duplicates()
        .withColumn(basket_name, median_udf("list"))
    )

    new_columns = [basket_name]

    return df_with_basket, new_columns


def __add_feature(dna, df, feature_name):
    """
    Add `DAY_SINCE_LAST_` + `feature_name` column to dna.

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        df (pyspark.sql.DataFrame): dataframe contains new feature information
        feature_name (str): the name on which the feature will be based on
    Returns:
        (pyspark.sql.DataFrame, list(str)): dna object with new features,
            new columns
    """
    dna = dna.join(df, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer")
    window = (
        W.Window.partitionBy("MBRSHP_SID")
        .orderBy("FISCAL_WEEK_END")
        .rowsBetween(W.Window.unboundedPreceding, 0)
    )
    new_columns = [
        "LAST_{}".format(feature_name),
        "DAYS_SINCE_LAST_{}".format(feature_name),
    ]
    dna = dna.withColumn(
        new_columns[0],
        sqlf.max("LAST_FISCAL_WEEK_{}".format(feature_name)).over(window),
    )
    dna = dna.withColumn(
        new_columns[1],
        sqlf.datediff(
            dna.FISCAL_WEEK_END, sqlf.col("LAST_{}".format(feature_name))
        ),
    )

    return dna, new_columns


def __trip_days(df):
    """
    Filter Detail (in store no return) for trip dates. Trips are as defined by
    the client.

    Parameters:
        df (pyspark.sql.DataFrame): detail_isnr from source_etl
    Returns:
        (pyspark.sql.DataFrame): filtered detail_isnr
    """
    return (
        df.filter(~df.MC_CD.isin(["402030190", "402030191", "203010098"]))
        .select("MBRSHP_SID", "FISCAL_WEEK_END", "PURCH_DT")
        .dropDuplicates()
    )


def __intervals(df):
    """
    Calculate the intervals between purchase dates for each member
    Parameters:
        df (pyspark.sql.DataFrame): filtered detail_isnr from source_etl
    Returns:
        (pyspark.sql.DataFrame): dataframe with purchase intervals
    """
    w = W.Window.partitionBy("MBRSHP_SID").orderBy("PURCH_DT")
    df = df.withColumn("PRIOR_PURCH_DT", sqlf.lag(df.PURCH_DT).over(w))
    df = df.withColumn(
        "EPOCH", df.FISCAL_WEEK_END.cast("timestamp").cast("long")
    )
    df = df.withColumn("PRIOR_EPOCH", sqlf.lag(df.EPOCH).over(w))
    df = df.withColumn(
        "INTERVAL", sqlf.datediff(df.PURCH_DT, df.PRIOR_PURCH_DT)
    )
    return df


def __min_max_intervals(df, weeks):
    """
    Calculate minimum and maximum intervals within the past {weeks} weeks

    Parameters:
        df (pyspark.sql.DataFrame): dataframe with intervals
        weeks (int): weeks to consider
    Returns:
        (pyspark.sql.DataFrame): dataframe with min and max intervals
    """
    w = utils.create_epoch_window(weeks)

    df = df.withColumn("MAX_INT", sqlf.max(df.INTERVAL).over(w)).withColumn(
        "MIN_INT", sqlf.min(df.INTERVAL).over(w)
    )

    df = df.groupby("MBRSHP_SID", "FISCAL_WEEK_END").agg(
        sqlf.max(df.MAX_INT).alias("MAX_INTERVAL"),
        sqlf.min(df.MIN_INT).alias("MIN_INTERVAL"),
    )

    return df


def __last_interval(df, weeks, max_feature_name, min_feature_name):
    """
    Fill nulls with last known min and max interval within the past {weeks}
    weeks.

    Parameters:
        df (pyspark.sql.DataFrame): dataframe with min max intervals
        weeks (int): number of weeks to consider
        max_feature_name (str): column name for max feature
        min_feature_name (str): column name for min feature
    Returns:
        (pyspark.sql.DataFrame): dataframe with last min/max intervals
    """
    w = (
        W.Window.partitionBy("MBRSHP_SID")
        .orderBy("FISCAL_WEEK_END")
        .rowsBetween(-(weeks - 1), 0)
    )

    df = df.withColumn(
        max_feature_name, sqlf.last(df.MAX_INTERVAL).over(w)
    ).withColumn(min_feature_name, sqlf.last(df.MIN_INTERVAL).over(w))

    return df
