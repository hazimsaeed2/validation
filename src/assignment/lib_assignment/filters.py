"""Function bank for Filter Functions."""

import re
import warnings

import pyspark.sql.functions as sqlf
from pyspark.sql import SparkSession

from lib_assignment.assn_utils import (
    CONSTRUCT_COLUMN_EXT,
    CONSTRUCT_COLUMN_EXT_BACKFILL,
    calc_avg_basket,
    flag_rows,
    # get_logger,
    map_under_threshold,
    mapping,
)

# log = get_logger("filters")

spark = SparkSession.builder.getOrCreate()

filter_functions = {}


def make_available_to_filters(f):
    """Decorator for filter function eligibility."""
    filter_functions[f.__name__] = f
    return f


@make_available_to_filters
def sql(df, colname, sql_string="select * from df"):
    """
    A general function to expose lots of functionality without having to create a new filter function.
    With great power comes great responsibility, this function is not guaranteed to maintain the contract
    of a general slot function, that is, returning a df with colname as a column of value either 1 or 0.

    :param df: Dataframe that has all the member data necessary for this slot function.
    :param colname: column that has to be set to either 1 or 0
    :param sql_string: an sql string that wil be run against df
    :return: filtered dataframe
    """
    df.createOrReplaceTempView("df")
    sql_string = sql_string.format(colname)
    return df.sparkSession.sql(sql_string)


@make_available_to_filters
def special_club(df, colname, value=["378"]):
    """Members in special club

    Parameters:
        df (pyspark.sql.DataFrame): Data to checkcoupons = coupons.filter
        colname (str): name of new column
        value (array<string>, opt): list of club numbers

    Returns:
        df (pyspark.sql.DataFrame): Data with club check column
    """
    df = df.withColumn(
        colname, sqlf.when(df.MBRSHP_NBR[0:3].isin(value), 1).otherwise(0)
    )
    return df


@make_available_to_filters
def special_zipcode(df, colname, value=["02142"]):
    """Members in special zipcode

    Parameters:
        df (pyspark.sql.DataFrame): Data to checkcoupons = coupons.filter
        colname (str): name of new column
        value (array<string>, opt): list of zipcode numbers

    Returns:
        df (pyspark.sql.DataFrame): Data with zipcode check column
    """
    df = df.withColumn(
        colname, sqlf.when(df.LATEST_HOME_ZIP_CD.isin(value), 1).otherwise(0)
    )
    return df


@make_available_to_filters
def expired(df, colname, value="2016-01-01"):
    """Members that were expired in a given date

    Parameters:
        df (pyspark.sql.DataFrame): Data to checkcoupons = coupons.filter
        colname (str): name of new column
        value (string): date to check

    Returns:
        df (pyspark.sql.DataFrame): Data with expired check column
    """
    df = df.withColumn(
        colname, sqlf.when(df.LATEST_MBRSHP_EXP_DT <= value, 1).otherwise(0)
    )
    return df


@make_available_to_filters
def tenure(df, colname, value=150):
    """Member has given tenure.

    Parameters:
        df (pyspark.sql.DataFrame): Data to check
        colname (str): name of new column
        value (int, opt): minimum allowable tenure, specified in days

    Returns:
        df (pyspark.sql.DataFrame): Data with tenure check column
    """
    df = df.withColumn(
        colname, sqlf.when(df["TENURE"] >= value, 1).otherwise(0)
    )
    return df


@make_available_to_filters
def new(df, colname, value=150):
    """Member has given tenure.

    Parameters:
        df (pyspark.sql.DataFrame): Data to check
        colname (str): name of new column
        value (int, opt): minimum allowable tenure, specified in days

    Returns:
        df (pyspark.sql.DataFrame): Data with tenure check column
    """
    df = df.withColumn(
        colname, sqlf.when(df["TENURE"] < value, 1).otherwise(0)
    )
    return df


@make_available_to_filters
def trial(df, colname, val_colname, value=["Trial MBR"]):
    """Member in trial membership.

    Parameters:
        df (pyspark.sql.DataFrame): Data to check
        colname (str): name of new column
        val_colname (str): name of column for flagging trial member
        value (str list): value list for flagging trial member

    Returns:
        df (pyspark.sql.DataFrame): Data with trial flag column
    """
    df = df.withColumn(
        colname, sqlf.when(df[val_colname].isin(value), 1).otherwise(0)
    )
    return df


@make_available_to_filters
def gas(df, colname, val_colname, value=1):
    """Member has gas purchase/club.

    Parameters:
        df (pyspark.sql.DataFrame): Data to check
        colname (str): name of new column
        value (int, opt): value for flagging gas targeting

    Returns:
        df (pyspark.sql.DataFrame): Data with gas flag column
    """
    if len(val_colname) == 1:
        cond = df[val_colname[0]] >= value
    elif len(val_colname) == 2:
        cond = (df[val_colname[0]] >= value) | (df[val_colname[1]] >= value)
    else:
        warnings.warn(
            "\n WARNING: this function can not take over 2 conditions"
        )
    df = df.withColumn(colname, sqlf.when(cond, 1).otherwise(0))
    return df


@make_available_to_filters
def avg_basket(
    df,
    colname,
    spend_col="LFIFTY-TWOW_SPEND_IN_STORE",
    trips_col="LAST_FIFTY-TWO_WEEK_TRIPS",
    limit=400,
    propensity_threshold=0,
):
    """Member has has purchase/club.

    Parameters:
        df (pyspark.sql.DataFrame): Data to check
        colname (str): name of new column
        value (int, opt): value for flagging gas targeting

    Returns:
        df (pyspark.sql.DataFrame): Data with gas flag column
    """
    # 1. calculate the average basket size
    #    if no trips in the last # weeks, or if unlikely to visit from trip propensity score
    #    then assign 0 to basket size in order to match easiest offer
    df = calc_avg_basket(df, spend_col, trips_col, propensity_threshold)
    df = df.withColumn(
        colname, sqlf.when(df["avg_basket"] <= limit, 1).otherwise(0)
    )
    return df


@make_available_to_filters
def longitudinal(
    df, colname, longitudinal_ids, mbr_type, filters, filter_past_mbrs=False
):
    """
    Finds eligible members already in a longitudinal group and adds (as needed) new members.

    Parameters:
        df (pyspark.sql.DataFrame): Data to check
        colname (str): name of new column
        longitudinal_ids (list): list of longitudinal ids that use this segment
        mbr_type (str): past, new, or both regarding which type of mbrs to use
        filters (list): filters to use for new mbrs and/or to filter past mbrs
        filter_past_mbrs (boolean): whether to use filters to determine past
                                    mbr eligibility

    Returns:
            df (pyspark.sql.DataFrame): data with longitudinal flag column
    """
    use_new_mbrs = mbr_type in ("new", "both")
    use_past_mbrs = mbr_type in ("past", "both")

    if use_new_mbrs or filter_past_mbrs:
        df = run_sub_filters(df, "elig_mbr", filters)
    else:
        df = df.withColumn("elig_mbr", sqlf.lit(1))

    drop_cols = ["elig_mbr", "past_mbr"]

    if not use_past_mbrs:
        df = df.withColumn("past_mbr", sqlf.lit(0))

    else:
        past_mbr_cols = [
            sqlf.col(f"l{long_id}_past_mbr") for long_id in longitudinal_ids
        ]
        print(f"past mbr cols {past_mbr_cols}")
        print(f"long ids {longitudinal_ids}")
        if len(past_mbr_cols) > 1:
            df = df.withColumn("past_mbr", sqlf.greatest(*past_mbr_cols))
        else:
            df = df.withColumn("past_mbr", past_mbr_cols[0])

        if filter_past_mbrs:
            df = df.withColumn(
                "past_mbr",
                sqlf.when(
                    (sqlf.col(f"past_mbr") == 1) & (sqlf.col("elig_mbr") == 1),
                    1,
                ).otherwise(0),
            )

    if not use_new_mbrs:
        df = df.withColumnRenamed("past_mbr", colname)
        drop_cols.remove("past_mbr")

    else:
        df = df.withColumn(
            colname,
            sqlf.when(
                (sqlf.col("elig_mbr") == 1) | (sqlf.col("past_mbr") == 1), 1
            ).otherwise(0),
        )

    df = df.drop(*drop_cols)

    return df


def run_sub_filters(df, colname, filters):
    """
    Applies the specified filters to the given df and appends a column indicating whether (1) or not (0) all filters passed.

    Parameters:
        df (pyspark.sql.DataFrame): member data
        colname (str): name of new column
        filters: A list of filters that parsed from the segment.json file. Below is an example segment.
                 The filters section inside the longitudinal filter(2 general_filter, 1 sql filter) is what should be parsed and assigned to this perameter

                {
                "segment_id": 12,
                "data_sources": [
                    {
                    "name": "segment_touch_lookup",
                    "ftype": "csv",
                    "path": "SEGMENTATION_TOUCH_LOOKUP_PATH",
                    "cols": [
                        "MBRSHP_SID",
                        "touch",
                        "segment"
                    ]
                    }
                ],
                "filters":
                [
                    {
                    "filter_num": 0,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                    "s3_path" : "s3://memberanalytics-data-out-prod/TEST/Longitudinal/ASSIGNMENTS/cdsa/longitudinal_control/segment_id=12/",
                    "append": "True",
                    "filters": [
                        {
                        "filter_num": 0,
                        "filter_type": "general_filter",
                        "filter_parameters": {
                        "column": "touch",
                        "relation": "=",
                        "threshold": "0"
                        }
                        },
                        {
                        "filter_num": 1,
                        "filter_type": "general_filter",
                        "filter_parameters": {
                        "column": "segment",
                        "relation": "=",
                        "threshold": "0"
                        }
                        },
                        {
                        "filter_num": 2,
                        "filter_type": "sql",
                        "filter_parameters": {
                        "sql_string": "select * , if(rand() < .1, 1, 0) as {} from df"
                        }
                        }
                    ]
                    }
                    }
                ]
                }


    output:
        df: original df and a column indicating whether (1) or not (0) all filters passed.

    """
    check_column = []
    for filter in filters:
        name = filter["filter_type"] + "_" + str(filter["filter_num"])
        print("filter for construct by {}".format(name))
        df = filter_functions.get(filter["filter_type"])(
            df, name, **filter["filter_parameters"]
        )
        check_column.append(name)

    df = df.withColumn("combined", sqlf.concat(*df[check_column])).withColumn(
        colname,
        sqlf.when(sqlf.col("combined").rlike("^1+$"), sqlf.lit(1)).otherwise(
            sqlf.lit(0)
        ),
    )

    check_column.append("combined")
    df = df.drop(*check_column)

    return df


@make_available_to_filters
def ROI_positive(
    df,
    colname,
    basket_offer_discount="[50:5, 400:10]",
    basket_offer_threshold="[50:50, 400:100]",
    spend_col="LFIFTY-TWOW_SPEND_IN_STORE",
    trips_col="LAST_FIFTY-TWO_WEEK_TRIPS",
    trips_weeks=52,
    incr_weeks=3,
    margin_rate=0.175,
    cannibalization_rate=0.6,
    incr_margin_lb=-1,
    incr_sales_lb=0.1,
    propensity_threshold=0,
):
    """Calculate features for basket offer assignment for members

    relevant features are 1) average basket, 2) incremental sales if drive a trip in 3 weeks

    Parameters:
        df (pyspark.sql.DataFrame): Data to check
        colname (str): name of new column
        basket_offer_discount (dict): map the basket size threshold with dollar off
        spend_col (str): names of column $ total spend
        trips_col (str): names of column # of trips
        trips_weeks (int): number of weeks that trips_col account for
        incr_week (int): number of weeks that will account for incrementality
        margin_rate (double): sales margin, default is 17.5%
        CANNIBALIZATION_RATE (double): cannibalization rate to account for incrementality
        incr_margin_lb (double): lower bound threshold for incr margin to be considered as ROI "positive"
        incr_sales_lb (double): lower bound threshold for incr sales to be considered as ROI "positive"
        propensity_threshold (double): for whoever has propensity score lower than the threshold,
                                       will send the easiest offer

    Returns:
        df (pyspark.sql.DataFrame): Data with ROI positive flag column

    """
    # 1. calculate the average basket size
    #    if no trips in the last # weeks, or if unlikely to visit from trip propensity score
    #    then assign 0 to basket size in order to match easiest offer
    df = calc_avg_basket(df, spend_col, trips_col, propensity_threshold)
    # 2. map the cpn threshold and dollar off given the basket size
    threshold = sorted(eval(basket_offer_discount).keys())
    df = map_under_threshold(df, "avg_basket", "rounded_basket", threshold)
    df = mapping(
        df, "rounded_basket", "cpn_dollar_off", eval(basket_offer_discount)
    )
    df = mapping(
        df,
        "rounded_basket",
        "cpn_dollar_threshold",
        eval(basket_offer_threshold),
    )
    # 3. calculate delta trips as the incrementalilty if drive a trip
    df = df.withColumn(
        "expected_trips", incr_weeks * (df[trips_col] / trips_weeks)
    )
    df = df.fillna(0, subset=["expected_trips"])
    df = df.withColumn(
        "expected_trips",
        sqlf.when(df.expected_trips.isNull(), 0).otherwise(df.expected_trips),
    )
    df = df.withColumn("delta_trips", 1 - df.expected_trips)
    # 4. calculate incremental sales and margin
    df = df.withColumn(
        "incr_sales",
        df.delta_trips
        * df.cpn_dollar_threshold
        * df.probability_making_a_trip
        * (1 - cannibalization_rate),
    )
    df = df.fillna(0, subset=["incr_sales"])
    df = df.withColumn(
        "incr_sales",
        sqlf.when(df.incr_sales.isNull(), 0).otherwise(df.incr_sales),
    )
    df = df.withColumn("incr_sales_margin", df.incr_sales * margin_rate)
    df = df.withColumn(
        "incr_margin",
        df.incr_sales_margin
        - df.cpn_dollar_off * df.probability_making_a_trip,
    )
    # 5. flag the ROI positive members given the basket offer
    df = df.withColumn(
        colname,
        sqlf.when(
            (
                (df.incr_margin >= incr_margin_lb)
                & (df.incr_sales >= incr_sales_lb)
            ),
            1,
        ).otherwise(0),
    )
    return df


@make_available_to_filters
def construct_satisfied(df, colname, construct_id=1, slot_num=None):
    """Determine if construct has been satisfied.

    Parameters:
        df (pyspark.sql.DataFrame): Data to check
        colname (str): name of new column
        construct_id (int, opt): construct to check against
        slot_num (array(int)): slot number to check against

    Returns:
        df (pyspark.sql.DataFrame): Data with construct check column
    """
    # 1. find matched construct for the segment
    all_cols = df.columns
    const_cols = []

    column_map = dict()
    for column in all_cols:
        match = re.findall(CONSTRUCT_COLUMN_EXT, column)
        unmatch = re.findall(CONSTRUCT_COLUMN_EXT_BACKFILL, column)
        if (len(match) > 0) & (len(unmatch) == 0):
            parent_construct = match[0][3]
            if parent_construct == str(construct_id):
                slot = int(match[0][1])
                column_map[slot] = column

    for index, slot in enumerate(sorted(column_map)):
        if slot_num is None or index in slot_num:
            const_cols.append(column_map[slot])

    # 2. check if construct is filled
    if len(const_cols) == 0:
        warnings.warn(
            "\n WARNING: no matched constructid in assignment, please check the segment json"
        )

    df = df.withColumn(
        "satisfy",
        sum(
            [
                sqlf.when(df[column].isNull(), sqlf.lit(None)).otherwise(1)
                for column in const_cols
            ]
        ),
    )

    # 3. flag the eligibility
    df = df.withColumn(
        colname, sqlf.when(df.satisfy.isNotNull(), 1).otherwise(0)
    )
    df = df.drop("satisfy")
    return df


@make_available_to_filters
def general_filter(df, colname, column, relation, threshold):
    """Flag a dataframe based on conditions.

    Given a dataframe and filtering conditions, flag rows with 1 in the dataframe that meet
    the threshold criteria.

    Parameters:
        df (dataframe): input dataframe
        colname (string): column with flag
        column (string): filter column
        relation (string): operation relation, example: '<', '>'
        threshold (column.dtype): threshold value.

        for example:
            'column':'MBRSHP_SID'
            'relation': '='
            'threshold':7

    Returns:
        df: dataframe with filter flag column

    """
    df = flag_rows(df, colname, column, relation, threshold)
    return df
