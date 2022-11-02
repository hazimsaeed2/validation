"""
All coupon and digital features are generated using the functions from this
module.

Feature functions starte with the keyword "feature_" while helper function
start with "__".
"""
import datetime
import word2number.w2n as w2n

import pyspark.sql.functions as sqlf
import pyspark.sql.window as W

import pe_memberdna.dna.member.lib.utils as utils


def feature_atc(job, dna, weeks_back):
    """
    Add-To-Card coupons are coupons that were specifically
    redeemed from the client app. There are two different
    types of coupons that a customer can redeem. BJs offers
    coupons internally, we call these BJs coupons. Vendors
    offer coupons nationally as well, we call these national
    coupons. Both types of coupons can be redeemed from the
    client app. The detail table is internal data, BJs
    Add-To-Card redemptions appear in this table. The detail
    table however does not contain information about national
    coupon redemptions (how could it, it's internal).
    Payment table is not internal, it therefore holds the
    data for the national add-to-card coupons but does not
    contain information about the BJs internal coupons
    (how could it, it's not internal). Therefore we have to
    use both tables to calculate Add-To-Card coupon
    redemptions. In the detail table BJ's Add-To-Card coupon
    redemptions have the DISCOUNT_TYPE_CD 'ZCOU'. In the
    payment table, national Add-To-Card coupon redemptions
    have the TENDER_TYPE_CD 'PCUE'. We add the two
    redemptions together to get total Add-To-Card (ATC)
    redemptions.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        weeks_back(list(int)):
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    feature_columns = []

    dna, new_columns = __client_atc_red(dna, job.data.tables["detail"])
    feature_columns.extend(new_columns)

    dna, new_columns = __nat_atc_red(dna, job.data.tables["payment"])
    feature_columns.extend(new_columns)

    dna, new_columns = __combine_atc_red(dna)
    feature_columns.extend(new_columns)

    dna, new_columns = __compute_atc_red_lookbacks(dna, weeks_back)
    feature_columns.extend(new_columns)

    dna = utils.set_default_value(dna, feature_columns, value=0)

    return dna


def feature_coupon_clipped(job, dna, weeks_back):
    """
    Calculate the number of coupons clipped looking {weeks_back} back
    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        weeks_back(list(int)): list of number of weeks to compute back
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    feature_columns = []

    dna, features = __num_coupon_clipped(dna, job.data.tables["coupon_clip"])
    feature_columns.extend(features)

    dna, features = __compute_cpn_clp_lookbacks(dna, weeks_back)
    feature_columns.extend(features)

    dna = utils.set_default_value(dna, feature_columns, value=0)

    return dna


def feature_fiscal_coupon_general(
    job, dna, tender_type_cds, discount_type_cds, aggregate, alias
):
    """
    Aggregate over TENDER_TYPE_CD and DISCOUNT_TYPE_CD.

    Serve for counting coupon redemptions or fiscal amount saved

    Store the lists of TENDER_TYPE_CDs and DISCOUNT_TYPE_CDs that will
    be used in filtering. Also store aggregate that will be used in
    aggregation.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        tender_type_cds (list of str): list of TENDER_TYPE_CDs to filter for
        discount_type_cds (list of str): list of DISCOUNT_TYPE_CDs to filter
                                        for
        aggregate (pyspark.sql.Column): aggregate to use
        alias (str): name for the aggregate, also the name for the new feature

    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    aggregate = aggregate.alias(alias)

    relevant_coup_purchases = (
        job.data.tables["payment"]
        .filter(sqlf.col("TENDER_TYPE_CD").isin(*tender_type_cds))
        .select(*["MBRSHP_SID", "FISCAL_WEEK_END", "SALES_PYMT_AMT"])
        .withColumnRenamed("SALES_PYMT_AMT", "savings_temp")
    )

    relevant_coup_purchases = relevant_coup_purchases.union(
        job.data.tables["detail"]
        .filter(sqlf.col("DISCOUNT_TYPE_CD").isin(*discount_type_cds))
        .select(*["MBRSHP_SID", "FISCAL_WEEK_END", "REDUCTION_AMT"])
        .withColumnRenamed("REDUCTION_AMT", "savings_temp")
    )

    coup_purchase_cnts = relevant_coup_purchases.groupby(
        "MBRSHP_SID", "FISCAL_WEEK_END"
    ).agg(aggregate)

    dna = dna.join(
        coup_purchase_cnts, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, [alias], value=0)

    utils.remove_spark_df((relevant_coup_purchases, coup_purchase_cnts))

    return dna


def feature_days_since_last_coupon_redeemed(job, dna):
    """
    Calculate the number of days since last time a member redeemed a coupon

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to

    Returns:
        (pyspark.sql.DataFrame): dna object with new features
    """
    relevant_coup_purchases = (
        job.data.tables["payment"]
        .filter(
            (
                sqlf.col("TENDER_TYPE_CD").isin(
                    ["CPN", "PCUM", "PCUS", "PCUE", "PCUB", "PCUR"]
                )
            )
            & (sqlf.col("SALES_PYMT_AMT") > 0)
        )
        .select(*["MBRSHP_SID", "FISCAL_WEEK_END", "PURCH_DT"])
    )

    relevant_coup_purchases = relevant_coup_purchases.union(
        job.data.tables["detail"]
        .filter(sqlf.col("DISCOUNT_TYPE_CD").isin(["ZCOU", "ZPAP"]))
        .select(*["MBRSHP_SID", "FISCAL_WEEK_END", "PURCH_DT"])
    )

    last_coup_purchases = relevant_coup_purchases.groupBy(
        ["MBRSHP_SID", "FISCAL_WEEK_END"]
    ).agg(sqlf.max("PURCH_DT").alias("LAST_FISCAL_WEEK_COUPON_REDEEMED"))

    dna, new_features = __add_feature(
        dna, last_coup_purchases, "COUPON_REDEEMED"
    )

    dna = utils.set_default_value(dna, new_features)

    utils.remove_spark_df((relevant_coup_purchases, last_coup_purchases))

    return dna


def feature_days_since_last_email_open(job, dna):
    """
    Calculates number of days since a member opened BJs' email last time

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to

    Returns:
        (pyspark.sql.DataFrame): dna object with new features
    """
    email_fiscal = (
        job.data.tables["email_fiscal"]
        .groupBy(["MBRSHP_SID", "FISCAL_WEEK_END"])
        .agg(
            sqlf.max("FIRST_OPEN_DATE").alias("LAST_FISCAL_WEEK_EMAIL_OPENED")
        )
    )

    dna, new_features = __add_feature(dna, email_fiscal, "EMAIL_OPENED")

    dna = utils.set_default_value(dna, new_features)

    return dna


def feature_days_since_last_atc_clipped(job, dna):
    """
    Calculates number of days since a member last clipped add-to-card coupon

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to

    Returns:
        (pyspark.sql.DataFrame): dna object with new features
    """
    coupon_clip = (
        job.data.tables["coupon_clip"]
        .where(sqlf.col("EVENTTYPE") == "Activation")
        .groupBy(["MBRSHP_SID", "FISCAL_WEEK_END"])
        .agg(sqlf.max("EVENTDATETIME").alias("LAST_FISCAL_WEEK_ATC_CLIPPED"))
    )

    dna, new_features = __add_feature(dna, coupon_clip, "ATC_CLIPPED")

    dna = utils.set_default_value(dna, new_features)

    utils.remove_spark_df(coupon_clip)

    return dna


def feature_email_open_rate(job, dna):
    """
    Calculates email open rate

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to

    Returns:
        (pyspark.sql.DataFrame): dna object with new features
    """
    new_features = []
    member_email, features = __get_member_email_L52_weeks(
        dna, job.data.tables["email"], job.data.tables["population"]
    )
    new_features.extend(features)

    member_email, features = __get_open_rate_L52_weeks(member_email)
    new_features.extend(features)

    dna = dna.join(
        member_email, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer"
    )

    dna = utils.set_default_value(dna, new_features)

    return dna


def feature_fiscal_savings_with_clipless(job, dna):
    """
    Calculate the savings amount intended for presentation to the user

    This number is calculated as a sum of:
        surcharge savings - the overall amount spent with BJS time 0.2 where
                            0.2
        represents the fact that BJS is 20% cheaper than competition
        cpn1_savings - amount spent in the form of vector + clipless coupons
        cpn2_savings - amount spent in the form of regural coupons
        awards_savings - amount awarded
        gas_savings - overall number of gallons purchased multiplied by 0.1
        representing the fact that our memebrs save 10 cents per every gallon

    This function aggregates this number of fiscal week level.

    The result of this function intends to mimic the number that memebrs
    already receive via email.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to

    Returns:
        (pyspark.sql.DataFrame): dna object with new features
    """
    exclude_mc_cd = [
        "101010008",
        "101010009",
        "101010010",
        "101010011",
        "203020099",
        "302010131",
        "304010159",
        "304020160",
        "304030161",
        "304030162",
        "402030188",
        "402030190",
        "402030191",
        "402030242",
        "A01010001",
        "B01010003",
        "B01010239",
        "C01010004",
        "C01010005",
        "C01010006",
        "C01010235",
        "C01010240",
        "Z01010237",
        "Z01010238",
        "Z01010241",
        "ZDUM01010",
    ]

    surcharge_savings = (
        job.data.tables["header"]
        .alias("header")
        .join(
            job.data.tables["detail"].alias("detail"),
            ["PURCH_HDR_ID"],
            "inner",
        )
        .filter(sqlf.col("detail.SALES_CTGRY_CD").isin("03"))
        .filter(sqlf.col("header.SALES_CHANNEL_ID").isin("10", "40", "30"))
        .filter(~(sqlf.col("detail.MC_CD").isin(exclude_mc_cd)))
        .withColumn(
            "spend",
            sqlf.when(
                sqlf.col("detail.DISCOUNT_TYPE_CD") == "ZPAP",
                sqlf.col("detail.REDUCTION_AMT"),
            ).otherwise(sqlf.col("detail.EXTENDED_PRC_AMT")),
        )
        .groupBy(
            sqlf.col("header.MBRSHP_SID").alias("MBRSHP_SID"),
            sqlf.col("detail.FISCAL_WEEK_END"),
        )
        .agg(sqlf.sum(sqlf.col("spend")).alias("spend"))
        .withColumn("savings", sqlf.col("spend") * 0.20)
    ).select("MBRSHP_SID", "FISCAL_WEEK_END", "savings")

    cpn1_savings = (
        job.data.tables["detail"]
        .filter(sqlf.isnull(sqlf.col("VOIDED_FLAG")))
        .filter(sqlf.col("DISCOUNT_TYPE_CD").isin(["ZCOU", "ZCLP", "ZPAP"]))
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.sum(sqlf.col("REDUCTION_AMT")).alias("savings"))
    ).select("MBRSHP_SID", "FISCAL_WEEK_END", "savings")

    cpn2_savings = (
        job.data.tables["payment"]
        .alias("payment")
        .join(
            job.data.tables["header"].alias("header"),
            ["PURCH_HDR_ID", "MBRSHP_SID"],
            "inner",
        )
        .filter(
            sqlf.col("payment.TENDER_TYPE_CD").isin(
                ["PCUM", "PCUS", "PCUB", "PCUR", "CPN", "PCUE"]
            )
        )
        .groupBy(
            sqlf.col("payment.MBRSHP_SID").alias("MBRSHP_SID"),
            sqlf.col("payment.FISCAL_WEEK_END").alias("FISCAL_WEEK_END"),
        )
        .agg(sqlf.sum(sqlf.col("payment.SALES_PYMT_AMT")).alias("savings"))
    ).select("MBRSHP_SID", "FISCAL_WEEK_END", "savings")

    gas_savings = (
        job.data.tables["detail"]
        .alias("detail")
        .join(
            job.data.tables["member_history"].alias("mem_hist"),
            ["MBRSHP_SID", "EFF_DT"],
            "inner",
        )
        .filter(sqlf.col("detail.MC_CD").isin("A01010001"))
        .filter(sqlf.col("detail.SALES_UOM").isin("GLL"))
        .filter(sqlf.col("mem_hist.RWDS_MBR_IND").isin("E", "P"))
        .groupBy(
            sqlf.col("detail.MBRSHP_SID").alias("MBRSHP_SID"),
            sqlf.col("detail.FISCAL_WEEK_END").alias("FISCAL_WEEK_END"),
        )
        .agg(sqlf.sum(sqlf.col("detail.SALES_QTY")).alias("gallons"))
        .withColumn("savings", sqlf.col("gallons") * 0.1)
    ).select("MBRSHP_SID", "FISCAL_WEEK_END", "savings")

    awards_savings = (
        job.data.tables["awards"]
        .groupBy("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.sum("AWRD_CERT_AMT").alias("savings"))
    ).select("MBRSHP_SID", "FISCAL_WEEK_END", "savings")

    savings = (
        surcharge_savings.union(cpn1_savings)
        .union(cpn2_savings)
        .union(gas_savings)
        .union(awards_savings)
    )

    dna = dna.join(
        savings.groupBy("MBRSHP_SID", "FISCAL_WEEK_END").agg(
            sqlf.sum("savings").alias("FW_SAVINGS_W_CLPLSS")
        ),
        ["MBRSHP_SID", "FISCAL_WEEK_END"],
        "left_outer",
    )

    dna = utils.set_default_value(dna, ["FW_SAVINGS_W_CLPLSS"], value=0)

    utils.remove_spark_df(
        (
            savings,
            awards_savings,
            cpn1_savings,
            cpn2_savings,
            gas_savings,
            surcharge_savings,
        )
    )

    return dna


def feature_aggregate_per_member_weeks(job, dna, weeks, fw_column_name):
    """
    Compute the aggregate fiscal amount redeemed in coupons + clipless
    per customer {weeks} fiscal weeks back

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        weeks (str): string name of the weeks to compute back
        fw_column_name (str): name on which the feature is based on

    Returns:
        (pyspark.sql.DataFrame): dna object with new features
    """
    window = __window_for_column(weeks)
    new_col_name = "L" + weeks + fw_column_name[1:]
    dna = dna.withColumn(new_col_name, sqlf.sum(fw_column_name).over(window))
    dna = utils.set_default_value(dna, [new_col_name], value=0)

    return dna


def feature_cpn_channel(job, dna):
    """
    Compute the preferred coupon channel (digital, paper, dual, none)

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to

    Returns:
        (pyspark.sql.DataFrame): dna object with new feature
    """

    paper_cd = "ZPAP"
    digital_cd = "ZCOU"
    threshold = 0.9
    lookback_weeks = 25

    detail = job.data.tables["detail"]

    detail = detail.filter(
        detail.DISCOUNT_TYPE_CD.isin([paper_cd, digital_cd])
    )

    start_date, _, _ = utils.window_dates(job)
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    lookback_start_date = start_date - datetime.timedelta(weeks=lookback_weeks)
    detail = detail.filter(detail.FISCAL_WEEK_END >= lookback_start_date)

    detail = detail.withColumn(
        "paper_cpn",
        sqlf.when(detail.DISCOUNT_TYPE_CD == paper_cd, detail.VECTOR_OFFER_ID),
    ).withColumn(
        "digital_cpn",
        sqlf.when(
            detail.DISCOUNT_TYPE_CD == digital_cd, detail.VECTOR_OFFER_ID
        ),
    )

    dna_mbrs = dna.select("MBRSHP_SID", "FISCAL_WEEK_END").dropDuplicates()
    dna_mbrs = dna_mbrs.filter(dna_mbrs.FISCAL_WEEK_END >= start_date)
    detail = dna_mbrs.join(
        detail, on=["MBRSHP_SID", "FISCAL_WEEK_END"], how="outer"
    )

    epoch_col = utils.to_epoch("FISCAL_WEEK_END")
    detail = detail.withColumn("EPOCH", epoch_col)
    window = utils.create_epoch_window(lookback_weeks)

    detail = detail.withColumn(
        "cpns", sqlf.size(sqlf.collect_set("VECTOR_OFFER_ID").over(window))
    )
    detail = detail.withColumn(
        "frac_paper",
        sqlf.size(sqlf.collect_set("paper_cpn").over(window))
        / sqlf.col("cpns"),
    )
    detail = detail.withColumn(
        "frac_digital",
        sqlf.size(sqlf.collect_set("digital_cpn").over(window))
        / sqlf.col("cpns"),
    )

    cpns = detail.select(
        "MBRSHP_SID", "FISCAL_WEEK_END", "cpns", "frac_paper", "frac_digital"
    )
    # filter to calculate preference for relevant DNA weeks
    cpns = cpns.filter(cpns.FISCAL_WEEK_END >= start_date)
    cpns = cpns.dropDuplicates(subset=["MBRSHP_SID", "FISCAL_WEEK_END"])
    cpns = cpns.withColumn(
        "cpn_channel",
        sqlf.when(cpns.frac_paper > threshold, "paper")
        .when(cpns.frac_digital > threshold, "digital")
        .when(cpns.cpns == 0, "no_cpns")
        .otherwise("dual"),
    )

    cpns = cpns.select("MBRSHP_SID", "FISCAL_WEEK_END", "cpn_channel")
    dna = dna.join(cpns, on=["MBRSHP_SID", "FISCAL_WEEK_END"], how="left")

    utils.remove_spark_df((detail, cpns))

    return dna


def __client_atc_red(dna, detail):
    """
    Calculates the number of BJ coupon ATC redemptions. That is
    ATC coupon redemptions for coupons BJs only offers. This is
    defined as any item purchase where the DISCOUNT_TYPE_CD is
    'ZCOU'.

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        detail (pyspark.sql.Dataframe): The detail intermediate table
    Returns:
        (pyspark.DataFrame, list): dna with new features, new columns
    """
    client_atc_red = (
        detail.filter(detail.DISCOUNT_TYPE_CD == "ZCOU")
        .groupby("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(
            sqlf.count("PURCH_HDR_ID").alias("BJ_ATC_RED"),
            sqlf.sum("REDUCTION_AMT").alias("BJ_ATC_SPEND"),
        )
    )

    return (
        dna.join(client_atc_red, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left"),
        [],
    )


def __nat_atc_red(dna, payment):
    """
    Calcualtes the number of national (Non-BJs) ATC coupon redemptions.
    That is ATC coupon redempions for coupons offered by other vendors.
    This is defined as any item purchase where the TENDER_TYPE_CD is
    'PCUE'.

    Parameters:
        dna (pyspark.DataFrame): the dna object to append feature to
        payment (pyspark.sql.Dataframe): The payment intermediate table
    Returns:
        (pyspark.sql.DataFrame, list): dna with new features, new columns
    """
    nat_atc_red = (
        payment.filter(payment.TENDER_TYPE_CD == "PCUE")
        .groupby("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(
            sqlf.count("PURCH_HDR_ID").alias("NAT_ATC_RED"),
            sqlf.sum("SALES_PYMT_AMT").alias("NAT_ATC_SPEND"),
        )
    )

    return (
        dna.join(nat_atc_red, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left"),
        [],
    )


def __combine_atc_red(dna):
    """
    In order to get all ATC coupon redemptions we need to add
    BJs ATC coupon redemptions to national ATC coupon redemptions.
    The result gives us ATC coupon redemptions at member,
    fiscal week level.

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame, list): The dna object, new columns
    """
    cols = ["BJ_ATC_RED", "NAT_ATC_RED", "NAT_ATC_SPEND", "BJ_ATC_SPEND"]
    new_columns = ["FW_ATC_CPN_RED", "FW_ATC_CPN_SPEND"]
    dna = dna.fillna(0, cols)

    dna = dna.withColumn(
        new_columns[0], sqlf.col("BJ_ATC_RED") + sqlf.col("NAT_ATC_RED")
    ).withColumn(
        new_columns[1], sqlf.col("NAT_ATC_SPEND") + sqlf.col("BJ_ATC_SPEND")
    )

    dna = dna.drop(*cols)

    return dna, new_columns


def __compute_atc_red_lookbacks(dna, weeks_back):
    """
    Aggregates trips over the select number of windows.
    weeks_back is the windows to compute over.

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        weeks (int): number of weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): dna object with new features, new columns
    """

    def name(weeks, metric=None):
        return "L{}W_ATC_CPN_{}".format(weeks, metric)

    new_columns = ["FW_ATC_CPN_RED", "FW_ATC_CPN_SPEND"]

    for weeks in weeks_back:
        red_feat = name(weeks, metric="RED")
        spend_feat = name(weeks, metric="SPEND")
        w = __window_for_column(weeks - 1)
        dna = dna.withColumn(
            red_feat, sqlf.sum(new_columns[0]).over(w)
        ).withColumn(spend_feat, sqlf.sum(new_columns[1]).over(w))

        new_columns.append(red_feat)
        new_columns.append(spend_feat)

    return dna, new_columns


def __num_coupon_clipped(dna, coupon_clip):
    """
    Count the coupons clipped

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        coupon_clip (pyspark.sql.DataFrame): intermediate data for clipped
                                            coupons

    Returns:
        (pyspark.sql.DataFrame): dna object with new features, new columns
    """
    new_columns = ["FW_CPN_CLP_COUNT"]
    coupon_clip_act = (
        coupon_clip.filter(coupon_clip.EVENTTYPE == "Activation")
        .groupby("MBRSHP_SID", "FISCAL_WEEK_END")
        .agg(sqlf.count("*").alias(new_columns[0]))
    )

    dna = dna.join(coupon_clip_act, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left")

    return dna, new_columns


def __window_for_column(weeks):
    """
    Defines window in order to compute columns

    Parameters:
        weeks (int): Number of weeks to compute back

    Returns:
        (pyspark.sql.window.Window): Window to use to compute the column
    """
    if isinstance(weeks, str):
        rows_back = -(w2n.word_to_num(weeks) - 1)
    else:
        rows_back = -weeks

    window = (
        W.Window.partitionBy("MBRSHP_SID")
        .orderBy("FISCAL_WEEK_END")
        .rowsBetween(rows_back, 0)
    )
    return window


def __compute_cpn_clp_lookbacks(dna, weeks_back):
    """
    Aggregates number of coupon clipped over the select number of windows.
    weeks_back is the windows to compute over.

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        weeks (int): Number of weeks to compute back

    Returns:
        (pyspark.sql.DataFrame): dna object with new features, new columns
    """
    new_columns = []
    for weeks in weeks_back:
        feat = "L{}W_CPN_CLP_COUNT".format(weeks)
        w = __window_for_column(weeks - 1)
        dna = dna.withColumn(feat, sqlf.sum("FW_CPN_CLP_COUNT").over(w))
        new_columns.append(feat)

    return dna, new_columns


def __add_feature(dna, df, feature_name):
    """
    Add `DAY_SINCE_LAST_` + `feature_name` column to dna.

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        df (pyspark.sql.DataFrame): dataframe contains new feature information
        feature_name (str): the name on which the feature will be based on
    Returns:
        (pyspark.sql.DataFrame): dna object with new features, new columns
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


def __get_member_email_L52_weeks(dna, email, population):
    """
    This function takes email data, group by each member and
    calculate number of emails each member received and opened
    in last 52 weeks.

    Parameters:
        dna (pyspark.sql.DataFrame): the dna object to append feature to
        email (pyspark.sql.DataFrame): intermediates email data
        population (pyspark.sql.DataFrame): generated population with window
                                            partition

    Returns:
        (pyspark.sql.DataFrame): Table with `MBRSHP_SID`,
                            # emails the member received in last 52 weeks,
                            # emails the member opened in last 52 weeks.
    """

    select_cols_in = {"MBRSHP_SID", "FIRST_SEND_DATE", "OPEN_TOTAL", "ID"}

    email = email.select(*select_cols_in)
    email = email.withColumnRenamed("MBRSHP_SID", "SID")

    theta_join_cond = (
        (sqlf.col("MBRSHP_SID") == email.SID)
        & (sqlf.col("FISCAL_WEEK_END") >= email.FIRST_SEND_DATE)
        & (sqlf.col("FISCAL_L52W_END") < email.FIRST_SEND_DATE)
    )

    # Theta join to cube skeleton

    all_email = population.join(email, theta_join_cond).drop("SID")

    groupby_cols = {"MBRSHP_SID", "FISCAL_WEEK_END"}

    num_email = sqlf.count("ID").alias("NUM_EMAIL")
    num_open = sqlf.count(sqlf.when(sqlf.col("OPEN_TOTAL") > 0, True)).alias(
        "NUM_OPEN"
    )

    member_email = all_email.groupby(*groupby_cols).agg(num_email, num_open)

    return member_email, []


def __get_open_rate_L52_weeks(member_email):
    """
    Calculate the email open rate = email opened / email sent

    When a member received email from BJs but never opened:
    email open rate = 0

    Parameters:
        (pyspark.sql.DataFrame): Table with `MBRSHP_SID`,
                              # emails the member received in last 52 weeks,
                              # emails the member opened in last 52 weeks.
    Returns:
        (pyspark.sql.DataFrame): dataframe with email open rate

    """
    select_cols_out = {"MBRSHP_SID", "EMAIL_OPEN_RATE", "FISCAL_WEEK_END"}

    new_columns = ["EMAIL_OPEN_RATE"]
    member_email = member_email.withColumn(
        new_columns[0], sqlf.col("NUM_OPEN") / sqlf.col("NUM_EMAIL")
    )

    member_email = member_email.select(*select_cols_out).fillna(
        0, subset=["EMAIL_OPEN_RATE"]
    )

    return member_email, new_columns
