"""Code for setting up and running backtests."""

from pyspark.sql.functions import sum, mean

from pe_member_dna.pipelines.lib.utils import top_n


def calc_fresh(cat_lookup):
    """Determine 'fresh' categories.

    Currently based on categories that have 'fresh' in the name

    Parameters:
        cat_lookup (pyspark.sql.DataFrame): Lookup table tying category
            ids to category names

    Returns:
        fresh_list (list): list of fresh category ids
    """
    fresh = cat_lookup[cat_lookup.CATEGORY_NAME.contains("FRESH")]
    fresh_list = fresh.select("CATEGORY_ID").toPandas()["CATEGORY_ID"].tolist()
    return fresh_list


def prep_backtest_metrics(
    memberdata, cat_lookup, past_data, future_data, start_date, end_date
):
    """Calculate future metric for evaluating backtest.

    Currently supports future sales ($), future fresh units, future trips,
    future fresh sales ($), and past fresh sales ($). Evaluates over given
    future timeperiod

    Parameters:
        memberdata (pyspark.sql.DataFrame): member cube data that covers future period. Requires:
            FISCAL_WEEK_END (date): Fiscal week identifier of cube entry
            SPEND_IN_STORE (float): total sales dollars in week
            WEEK_TRIPS (float): total trips in week
            MBRSHP_SID (int): Membership id of members in dataset
        cat_lookup (pyspark.sql.DataFrame): lookup table that ties category id and name
        future_data (pyspark.sql.DataFrame): matrix data over future period
        past_data (pyspark.sql.DataFrame): matrix data over past period
        start_date (str): start date of backtest period in 'YYYY-MM-DD' format
        end_date (str): end date of backtest period in 'YYYY-MM-DD' format

    Returns:
        future_metrics (pyspark.sql.DataFrame): dataframe containing calculated metrics by member
            MBRSHP_SID (int): Membership id of members in dataset
            future_weeksales (float): sales ($) during backtest period
            fresh_units (float): sales units during backtest period
            past_fresh_units (float): sales units during prior period
            future_trips (float): trips during backtest period
            future_freshsales (float): sales ($) in 'fresh'categories during backtest period
            past_freshsales (float): sales ($) in 'fresh' categories during prior period to backtest
            fresh_delta (float): difference in 'fresh' sales between post and pre backtest periods
    """
    future_memb = memberdata[memberdata.FISCAL_WEEK_END >= start_date]
    future_memb = future_memb[future_memb.FISCAL_WEEK_END <= end_date]
    future_memb = future_memb.fillna(0)
    future_memb = future_memb.groupBy("MBRSHP_SID").agg(
        sum("FW_SPEND_IN_STORE").alias("future_weeksales"),
        sum("WEEK_TRIPS").alias("future_trips"),
    )
    future_memb = future_memb.repartition(400, "MBRSHP_SID")
    fresh_list = calc_fresh(cat_lookup)
    future_data = future_data[future_data.CATEGORY_ID.isin(fresh_list)].fillna(
        0
    )
    future_data = future_data.groupBy("MBRSHP_SID").agg(
        sum("SALES_AMT").alias("future_freshsales"),
        sum("SALES_UNITS").alias("fresh_units"),
    )
    future_data = future_data.repartition(400, "MBRSHP_SID")
    past_data = past_data[past_data.CATEGORY_ID.isin(fresh_list)].fillna(0)
    past_data = past_data.groupBy("MBRSHP_SID").agg(
        sum("SALES_AMT").alias("past_freshsales"),
        sum("SALES_UNITS").alias("past_fresh_units"),
    )
    past_data = past_data.repartition(400, "MBRSHP_SID")
    future_metrics = future_memb.select(
        "MBRSHP_SID", "future_weeksales", "future_trips"
    )
    future_metrics = future_metrics.join(future_data, "MBRSHP_SID", "left")
    future_metrics = future_metrics.join(past_data, "MBRSHP_SID", "left")
    calc = future_metrics.future_freshsales - future_metrics.past_freshsales
    future_metrics = future_metrics.withColumn("fresh_delta", calc)
    future_metrics = future_metrics.fillna(0)
    future_metrics = future_metrics.repartition(400, "MBRSHP_SID")
    return future_metrics


def prep_bbm14_campaign(sparksession):
    """Combine and prep bbm14 backtest campaign data.

    Parameters:
        sparksession (pyspark.SparkSession): spark session to pull data in

    Returns:
        bbm (pyspark.sql.DataFrame): dataframe containing bbm14 data
            MBRSHP_NBR (int): Membership number of member involved in campaign
            CellName (str): name of cell member is in
            CellCode (str): Id of cell member is in
            control (bool): whether or not cell is control or treatment
            member_type (str): tenured/non-tenured flag for member
            member_group (str): Member group description (e.g. 'unrealiable grocery')
            coupon (str): description of coupon associated with cell
            offer_type (str): classification of offer (e.g. 'category, basket')
            MBRSHP_SID (str): Membership ID of member involved in campaign
    """
    member_cell_path = "s3://memberanalytics-data-in/CM2467V_IS.dat"
    offer_lookup_path = "s3://memberanalytics-data-in/campaign/lookup_files/BBM14_offer_lookup.csv"
    cellname_lookup_path = "s3://memberanalytics-data-in/campaign/lookup_files/BBM14_cellname_lookup.csv"
    conversion_path = (
        "s3://memberanalytics-data-in/extended_mbr_table_032318.csv"
    )
    sid_lkup = sparksession.read.csv(conversion_path, header=True).select(
        "MBRSHP_NBR", "MBRSHP_SID"
    )
    member_cell = sparksession.read.csv(member_cell_path, header=True)
    cell_lkup = sparksession.read.csv(cellname_lookup_path, header=True)
    cell_lkup = cell_lkup.withColumnRenamed(
        "cell_name", "CellName"
    ).withColumnRenamed("cell_code", "CellCode")
    offer_lkup = sparksession.read.csv(
        offer_lookup_path, header=True
    ).withColumnRenamed("cell_name", "CellName")
    # join all offer data together
    bbm = member_cell.join(cell_lkup, ["CellName", "CellCode"], "left")
    bbm = bbm.join(offer_lkup, ["CellName"], "left")
    bbm = bbm.join(sid_lkup, ["MBRSHP_NBR"], "left")
    bbm = bbm.na.drop(subset="control").dropDuplicates()
    # controls have unknown tenure and we want to keep them (for now)
    bbm = bbm.fillna("Tenured", subset=["member_type"])
    # limit to relevant offers and controls
    category_controls = [
        "Set CO 2xMSB.Base_A",
        "Set CO UG.Base_A",
        "Set CO MEN.Base_A",
        "Set CO QBB.Base_A",
        "Set CO ISB.Base_A",
        "Set CO LVGM.Base_A",
    ]
    bbm = bbm[
        (
            (
                (bbm.offer_type == "Category")
                | (bbm.CellName.isin(category_controls))
            )
            & (bbm.member_type == "Tenured")
        )
    ]
    return bbm


def map_to_ah4(sparksession, df, cat_lookup, level="AH5"):
    """Map categories to ah4 for comparison.

    Aggregates lower level category data up to AH4
    categories for comparison.

    Parameters:
        sparksession (pyspark.SparkSession): spark session to pull data in
        df (pyspark.sql.DataFrame): dataframe to aggregate Requires
            CATEGORY_ID (column): column containing category ids at current level
        cat_lookup(pyspark.sql.DataFrame): dataframe containing mapping between cat levels
        level(str): name of current aggregation level

    Returns:
        df (pyspark.sql.DataFrame): dataframe to map categories for
    """
    level_id = level + "_CD"
    cat_level = cat_lookup.select("AH4_CD", level_id).withColumnRenamed(
        level_id, "CATEGORY_ID"
    )
    df = df.repartition(400, "CATEGORY_ID")
    df = df.join(cat_level, "CATEGORY_ID", "left")
    df = df.withColumnRenamed("CATEGORY_ID", "DROP")
    df = df.withColumnRenamed("AH4_CD", "CATEGORY_ID")
    df = df.drop("DROP")
    return df


def prep_backtest(
    sparksession,
    predictions,
    cat_lookup,
    members,
    past_data,
    future_data,
    cutoff=0,
    cats_to_test=1,
    campaign="bbm14",
    level="AH4",
):
    """Prep full backtest dataset.

    Convenience function wrapping other behaviors to allow for simple execution
    of backtests. Currently supports only BBM14 but will extend to other backtests
    as data becomes available.

    Parameters:
        sparksession (pyspark.SparkSession): spark session to pull data in
        predictions (pyspark.sql.DataFrame): prediction dataframe, as produced my models.predict_pf
        cat_lookup (pyspark.sql.DataFrame): lookup table tying category names and ids
        members (pyspark.sql.DataFrame): member cube data that covers future period. Requires:
            FISCAL_WEEK_END (date): Fiscal week identifier of cube entry
            SPEND_IN_STORE (float): total sales dollars in week
            WEEK_TRIPS (float): total trips in week
            MBRSHP_SID (int): Membership id of members in dataset
        future_data (pyspark.sql.DataFrame): matrix data over future period
        past_datas (pyspark.sql.DataFrame): matrix data over past period
        cutoff (float, optional): max % of prior trips allowed for a prediction to be made
        cats_to_test (int, optional): number of categories to use in 'top categories' for testing
        campaign (str, optional): name of campaign to backtest against. Default BBM14

    Returns:
        campaign_data (pyspark.sql.DataFrame): all relevant backtest data
            MBRSHP_NBR (int): Membership number of member involved in campaign
            CellName (str): name of cell member is in
            CellCode (str): Id of cell member is in
            control (bool): whether or not cell is control or treatment
            member_type (str): tenured/non-tenured flag for member
            member_group (str): Member group description (e.g. 'unrealiable grocery')
            coupon (str): description of coupon associated with cell
            offer_type (str): classification of offer (e.g. 'category, basket')
            MBRSHP_SID (str): Membership ID of member involved in campaign
            future_weeksales (float): sales ($) during backtest period
            fresh_units (float): sales units during backtest period
            future_trips (float): trips during backtest period
            future_freshsales (float): sales ($) in 'fresh'categories during backtest period
            past_freshsales (float): sales ($) in 'fresh' categories during prior period to backtest
            fresh_delta (float): difference in 'fresh' sales between post and pre backtest periods
            CATEGORY_ID (int): Category id of prediction
            prediction (float): Score of prediction
            FUTURE_TRIPS (int): number of future trips from prediction data
            PAST_TRIPS (int): number of past trips from prediction data
            CATEGORY_NAME (str): name of category of prediction
    """
    if campaign.lower() == "bbm14":
        bbm = prep_bbm14_campaign(sparksession)
        in_home_date = "2017-10-10"
        test_end_date = "2017-11-07"
    else:
        raise ValueError("that campaign is not currently supported!")
    if level != "AH4":
        cat_master = "s3://memberanalytics-data-in/Master_Data/kantar_itemmaster_hierarchy_data_SAP_20180307_headers_v2.txt"
        cat_master = sparksession.read.csv(cat_master, header=True, sep="|")
        # pre-subset predictions before mapping to AH4
        predictions = top_n(
            predictions, cats_to_test, "prediction", "MBRSHP_SID"
        )
        predictions = map_to_ah4(
            sparksession, predictions, cat_master, level=level
        )
        # pre-subset categories before mapping to AH4
        level_code = level + "_CD"
        pre_mapper = cat_master.select("AH4_DESC", level_code).distinct()
        pre_mapper = pre_mapper.withColumnRenamed("AH4_DESC", "CATEGORY_NAME")
        pre_mapper = pre_mapper.withColumnRenamed(level_code, "CATEGORY_ID")
        ah4_cats = calc_fresh(pre_mapper)
        past_data = past_data[past_data.CATEGORY_ID.isin(ah4_cats)]
        future_data = future_data[future_data.CATEGORY_ID.isin(ah4_cats)]
        past_data = map_to_ah4(
            sparksession, past_data, cat_master, level=level
        )
        future_data = map_to_ah4(
            sparksession, future_data, cat_master, level=level
        )
    top_per_memb = top_n(predictions, cats_to_test, "prediction", "MBRSHP_SID")
    top_per_memb = top_per_memb.repartition(400, "MBRSHP_SID")
    # calculate comparsion metrics
    future_metrics = prep_backtest_metrics(
        members,
        cat_lookup,
        past_data,
        future_data,
        in_home_date,
        test_end_date,
    )
    # prep campaign
    # join data together
    campaign_data = top_per_memb.join(bbm, ["MBRSHP_SID"], "inner")
    campaign_data = campaign_data.join(future_metrics, ["MBRSHP_SID"], "inner")
    return campaign_data


def run_backtest(
    campaign_data, cat_lookup, metric="past_freshsales", campaign="bbm14"
):
    """Run backtest on campaign_data.

    Run backtest on campaign dataset for a given comparison metric.
    Currently only supports BBM14 campaign for backtesting

    Parameters:
        campaign_data (pyspark.sql.DataFrame): all relevant backtest data
            MBRSHP_NBR (int): Membership number of member involved in campaign
            CellName (str): name of cell member is in
            CellCode (str): Id of cell member is in
            control (bool): whether or not cell is control or treatment
            member_type (str): tenured/non-tenured flag for member
            member_group (str): Member group description (e.g. 'unrealiable grocery')
            coupon (str): description of coupon associated with cell
            offer_type (str): classification of offer (e.g. 'category, basket')
            MBRSHP_SID (str): Membership ID of member involved in campaign
            future_weeksales (float): sales ($) during backtest period
            fresh_units (float): sales units during backtest period
            future_trips (float): trips during backtest period
            future_freshsales (float): sales ($) in 'fresh'categories during backtest period
            past_freshsales (float): sales ($) in 'fresh' categories during prior period to backtest
            fresh_delta (float): difference in 'fresh' sales between post and pre backtest periods
            CATEGORY_ID (int): Category id of prediction
            prediction (float): Score of prediction
            FUTURE_TRIPS (int): number of future trips from prediction data
            PAST_TRIPS (int): number of past trips from prediction data
            CATEGORY_NAME (str): name of category of prediction
        metric (str): the name of the metric to use for backtesting against
        campaign (str): the name of the campaign to use for backtesting, should match campaign data

    Returns:
        aggd (pandas.DataFrame): pandas dataframe containing results of backtest, treated
            vs. control, CF pred vs non.

    """
    import pandas as pd

    if campaign.lower() == "bbm14":
        category_tests = [
            "Set CO 2xMSB.V_F",
            "Set CO UG.V_F",
            "Set CO MEN.V_F",
            "Set CO QBB.V_F",
            "Set CO ISB.V_F",
            "Set CO LVGM.V_F",
        ]
        category_controls = [
            "Set CO 2xMSB.Base_A",
            "Set CO UG.Base_A",
            "Set CO MEN.Base_A",
            "Set CO QBB.Base_A",
            "Set CO ISB.Base_A",
            "Set CO LVGM.Base_A",
        ]
    else:
        raise ValueError("that campaign is not currently supported!")
    results = []
    campaign_data = campaign_data.cache()
    fresh_list = calc_fresh(cat_lookup)
    # loop through campaign and control groups and calculate per-group lifts
    for i, _ in enumerate(category_tests):
        pairs = ["fresh", "non"]
        for pair in pairs:
            treated = campaign_data[
                campaign_data.CellName == category_tests[i]
            ]
            ctrl = campaign_data[
                campaign_data.CellName == category_controls[i]
            ]
            if pair == "fresh":
                treated = treated[treated.CATEGORY_ID.isin(fresh_list)]
                ctrl = ctrl[ctrl.CATEGORY_ID.isin(fresh_list)]
            else:
                treated = treated[~treated.CATEGORY_ID.isin(fresh_list)]
                ctrl = ctrl[~ctrl.CATEGORY_ID.isin(fresh_list)]
            treat_metric = treated.select(
                mean(metric).alias("mean")
            ).toPandas()["mean"][0]
            ctrl_metric = ctrl.select(mean(metric).alias("mean")).toPandas()[
                "mean"
            ][0]
            treat_size = treated.count()
            ctrl_size = ctrl.count()
            obj = {
                "type": pair,
                "treat_value": treat_metric,
                "ctrl_value": ctrl_metric,
                "treat_size": treat_size,
                "ctrl_size": ctrl_size,
            }
            results.append(obj)

    lifts = pd.DataFrame(results)
    sizes = (
        lifts.groupby(["type"])
        .sum()[["treat_size", "ctrl_size"]]
        .reset_index()
    )
    sizes = sizes.rename(
        columns={
            "treat_size": "treat_groupsize",
            "ctrl_size": "ctrl_groupsize",
        }
    )
    lifts = lifts.merge(sizes, on=["type"], how="left")
    lifts["frac_treatsize"] = lifts["treat_size"] / lifts["ctrl_groupsize"]
    lifts["frac_ctrlsize"] = lifts["ctrl_size"] / lifts["ctrl_groupsize"]
    lifts["frac_treat_val"] = lifts["treat_value"] * lifts["frac_treatsize"]
    lifts["frac_ctrl_val"] = lifts["ctrl_value"] * lifts["frac_ctrlsize"]
    lifts["frac_lift"] = lifts["frac_treat_val"] - lifts["frac_ctrl_val"]
    lifts = lifts.fillna(0)
    aggd = lifts.groupby(["type"]).sum()[
        ["frac_treat_val", "frac_ctrl_val", "frac_lift"]
    ]
    campaign_data.unpersist()
    return aggd
