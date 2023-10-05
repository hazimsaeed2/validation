"""Utilities and Helpers for assignment pipeline."""

import copy
import math
import operator
import warnings
from collections import defaultdict
from datetime import datetime
from random import sample, seed

import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
from dateutil import parser
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

import pe_memberdna.assignment.lib.checks as checks
from pe_memberdna.lib.iotools import read_s3_to_local
from pe_memberdna.lib.spark_util import get_logger
from pe_memberdna.lib.utils import convert_id_cols_to_str, next_fiscal_week_end

spark = SparkSession.builder.getOrCreate()
sparkContext = spark.sparkContext

log = get_logger("assn_utils")

CONSTRUCT_MATCH = r"c([\d_]+)"
SLOT_MATCH = r"s(\d+)"
GROUP_MATCH = r"g(\d+)"
PARENT_CONSTRUCT = r"p(\d+)"
BACKFILL_MATCH = r"b"

CONSTRUCT_COLUMN = CONSTRUCT_MATCH + SLOT_MATCH
CONSTRUCT_COLUMN_EXT = CONSTRUCT_COLUMN + GROUP_MATCH + PARENT_CONSTRUCT
CONSTRUCT_COLUMN_EXT_BACKFILL = CONSTRUCT_COLUMN_EXT + BACKFILL_MATCH

LAYOUT_ID_MATCH = r"c([\d_]+)__([\d_]+)"


def group_filters_by_segment(filters):
    """Group filters on simple segment_id
    Parameters:
        filters (list): list of unique filters
    Returns:
        grouped_filters(dict): a dict containing the filter groups
    """
    grouped_filters = defaultdict(list)

    for filt in filters:
        grouped_filters[filt["segment_id"]].append(filt)

    return grouped_filters


def at_least_one_filter(segment):
    """Check whether segment is a multi segment with an OR condition.
    Parameters:
        segment (str): represents a simple or multi segment
    Returns:
        (bool): True if multi segment with an OR condition
                False otherwise
    """
    return "|" in segment


def decompose_segment(segment):
    """Return a list of segment ids.
    Parameters:
        segment (str): represents a simple or multi segment
    Returns:
        segment (list): a list of segment ids resulted from the
            segment parameter
    """

    if "_" in segment:
        segments = [segment_id.strip() for segment_id in segment.split("_")]
    elif "|" in segment:
        segments = [segment_id.strip() for segment_id in segment.split("|")]
    else:
        try:
            segments = [str(int(float(segment)))]
        except ValueError:
            segments = [None]
    return segments


def decompose_construct(construct):
    """Return a list of construct ids.
    Parameters:
        construct (str): represents a simple or multi construct
    Returns:
        constructs (list): a list of construct ids resulted from the
            construct parameter
    """
    constructs = list()
    for c in construct.split("_"):
        try:
            constructs.append(str(int(float(c.strip()))))
        except ValueError:
            constructs = [None]
            break
    return constructs


def has_coupons(params):
    """Check whether this campaign has any coupons
    Parameters:
        params (dict): campaign parameters
    Returns:
        bool
    """
    if params.get("min_cpn_start_date") is None:
        return False
    return True


def determine_fiscal_week(df, rel_date):
    """Calculate the relevant fiscal week from dataframe.

    Given a dataframe and relevant date of interest, determine
    the fiscal week in the dataframe that most closely matches
    the date of interest.

    Importantly, this function ASSUMES a dense dataframe containing
    all fiscal weeks betwen the start and end dates of the dataframe
    For sparse dataframes, this function will not necessarily return the
    closes dataframe, and will only return the next or largest.

    Parameters:
        df (pyspark.sql.DatayFrame): spark dataframe to analyze
        rel_date (str): string representation of desired date in YYYY-MM-DD format

    Returns:
        fiscal_week (str): nearest available fiscal week in YYYY-MM-DD format
    """
    max_fiscal_week = (
        df.groupBy()
        .agg(sqlf.max("FISCAL_WEEK_END").alias("FISCAL_WEEK_END"))
        .first()["FISCAL_WEEK_END"]
    )
    if (isinstance(max_fiscal_week, str)) | (isinstance(max_fiscal_week, str)):
        max_fiscal_week = parser.parse(max_fiscal_week).date()
    if isinstance(max_fiscal_week, datetime):
        max_fiscal_week = max_fiscal_week.date()
    fiscal_week = next_fiscal_week_end(rel_date)
    if parser.parse(fiscal_week).date() > max_fiscal_week:
        fiscal_week = datetime.strftime(max_fiscal_week, "%Y-%m-%d")
    return fiscal_week


def get_all_of_key(dictionary, key="hs_ind_lambda"):
    """
    This function uses the get_key_values function to first get all the key/value pairs in the nested
    dictionary.  After it has that list, it filters to only the key specified and returns the
    corresponding values.

    :param dictionary: nested dictionary that you want to look through
    :param key: The key that you want to filter to
    :return: all individual key value pairs in the nested dictionary with the provided key
    """
    key_values = get_key_values(dictionary)
    lambda_key_values = [
        key_value for key_value in key_values if key_value[0].lower() == key
    ]
    lambdas = list([k_v[1] for k_v in lambda_key_values])
    return lambdas


def get_key_values(dictionary):
    """
    Given a dictionary of dictionaries/lists, it will recursively traverse the
    dictionary to find all key/value pairs in the dictionary where the value is not
    a dictionary or list itself.

    returns an empty generator if the input is not a dictionary

    :param dictionary: nested dictionary that you want to look through
    :return: all individual key value pairs in the nested dictionary
    """
    if isinstance(dictionary, dict):
        for key, value in list(dictionary.items()):
            if isinstance(value, dict):
                for key_value in get_key_values(value):
                    yield key_value
            elif isinstance(value, list):
                for item in value:
                    for key_value in get_key_values(item):
                        yield key_value
            else:
                yield (key, value)


def flag_rows(df, colname, column, relation, threshold):
    """Flag a dataframe based on conditions.

    Given a dataframe and flagging conditions, rows are flagged with 1 in
    the dataframe that meet the threshold criteria.

    Note the logic which is used for None values. For example, None != 2 will
    not evalue to True.

    Parameters:
        df (dataframe): input dataframe
        colname (string): column name with flag
        column (string): filter column
        relation (string): operation relation
                           (must be one of: '<', '>', '>=', '<=', '=' ,'!=')
        threshold (column.dtype): threshold value (can be None).

    Returns:
        df: dataframe with flag column

    """
    if threshold is None:
        if relation == "=":
            return df.withColumn(
                colname, sqlf.when(df[column].isNull(), 1).otherwise(0)
            )
        elif relation == "!=":
            return df.withColumn(
                colname, sqlf.when(df[column].isNotNull(), 1).otherwise(0)
            )
        else:
            msg = (
                "Unknown operator {} for use with {} value. "
                + "Please use '=' or '!='"
            ).format(relation, threshold)
            raise ValueError(msg)
    else:
        ops = {
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
            "=": operator.eq,
            "!=": operator.ne,
        }
        df = df.withColumn(
            colname,
            sqlf.when(ops[relation](df[column], threshold), 1).otherwise(0),
        )
        return df


def downsample_rows(df, column, value, ratio, seed=None):
    """
    This function takes a dataframe and randomly filter out rows when column has the value 'value'

    :param df: some spark dataframe
    :param column: the column that we want to downsample on
    :param value: the values for the column that we will downsample on
    :param ratio: the target ratio to downsample to
    :param seed: the value of a seed to pass in. Typically None but necessary in unit test
    :return: the downsampled dataframe
    """
    df.createOrReplaceTempView("df")
    query = "select * from df"
    if isinstance(value, str):
        where = " where {} != '{}'"
    else:
        where = " where {} != {}"
    if seed:
        rand = " or rand({}) < {}"
        return spark.sql(
            (query + where + rand).format(column, value, seed, ratio)
        )
    else:
        rand = " or rand() < {}"
        return spark.sql((query + where + rand).format(column, value, ratio))

    return


def filter_rows(df, filter_col):
    """Filter a dataframe based on conditions.

    Given a dataframe and filtering conditions, select rows in the dataframe that meet
    the threshold criteria. This function takes a dictonary input as filter_col.

    Parameters:
        df (dataframe): input dataframe
        filter_col(dict): dictionary contains filter column, relation and threshold value.

        for example,
            filter_col = {'column':'MBRSHP_SID', 'relation': '>', 'threshold':'7'}

    Returns:
        df_filter (dataframe): dataframe after filters applied

    """
    if isinstance(filter_col, dict):
        df = flag_rows(
            df,
            "flag",
            filter_col["column"],
            filter_col["relation"],
            filter_col["threshold"],
        )
        df_filter = df[df.flag == 1]
        return df_filter
    else:
        warnings.warn("WARNING: Make sure the filter_col is a dictionary")


def mapping(df, from_col, to_col, mapping):
    """map value in spark dataframe

    Parameters:
        df (pyspark.sql.DataFrame): data frame that need to map new value
        from_col (string): column name that will be mapped to new value
        to_col (string): column name that save after mapped new value
        mapping (dictionary): dictionary that maps the value to new value

    Returns:
        df (pyspark.sql.DataFrame): data frame after mapping
    """

    from pyspark.sql.functions import udf

    def inner(k):
        return mapping.get(k, mapping.get("DEFAULT", k))

    _fudf = udf(inner)

    df = df.withColumn(to_col, _fudf(from_col))

    return df


def check_cpn_nbr_or_version(df, check_col):
    """determine if a column is a version or a coupon number
    if the coupon number is a single letter return null as number, else return the number.

    Parameters:
        df (pyspark.sql.DataFrame): data frame that need to map new value
        check_col (string): column to check if it is a string or a coupon number

    Returns:
        df (pyspark.sql.DataFrame): data frame after check
    """
    df = df.withColumn("len", sqlf.length(df[check_col]))
    df = df.withColumn(
        check_col, sqlf.when(df.len <= 1, None).otherwise(df[check_col])
    )
    df = df.drop("len")
    return df


def map_under_threshold(df, from_col, to_col, threshold):
    """map the value to the closest ceiling threshold

    Parameters:
        df (pyspark.sql.DataFrame):data frame must contain from_col
        from_col (string): name of column that have value to be mapped
        to_col (string): name of column to save the mapped value
        threshold (sorted array<>): the sorted array that each value is the threshold for mapping

    Returns:
        df (pyspark.sql.DataFrame): data frame that have to_col

    """

    from pyspark.sql.functions import udf

    def under(k):
        if k is None:
            return k

        for i in threshold:
            if k < i:
                return i

        return None

    _fudf = udf(under)

    df = df.withColumn(to_col, _fudf(from_col))

    return df


def calc_avg_basket(df, spend_col, trips_col, propensity_threshold):
    """Calculate average basket size to match basket offers
       if no trips in the last # weeks, then assign 0 to basket size in order to match easiest offer
       if unlikely to visit, then assign 0 to basket size in order to match easiest offer

    Parameters:
        df (pyspark.sql.DataFrame): Data to calculate basket size
            Requires: spend_col, trips_col
        spend_col (str): names of column $ total spend
        trips_col (str): names of column # of trips
        propensity_threshold (double): for whoever has propensity score lower than the threshold,
                                       will send the easiest offer

    Returns:
        df (pyspark.sql.DataFrame): Data with avg_basket
    """
    # calculate the average basket size
    df = df.withColumn("avg_basket", df[spend_col] / df[trips_col])
    # if no trips in the last # weeks, then assign 0 to basket size in order to match easiest offer
    df = df.withColumn(
        "avg_basket",
        sqlf.when(df.avg_basket.isNull(), 0).otherwise(df.avg_basket),
    )
    # if unlikely to visit, then assign 0 to basket size in order to match easiest offer
    df = df.withColumn(
        "avg_basket",
        sqlf.when(
            df.probability_making_a_trip < propensity_threshold, sqlf.lit(0)
        ).otherwise(df.avg_basket),
    )
    return df


def fill_with_average(df, fill_col, na_map_to=None):
    """fill null and specified value with averge value of the rest data

    Parameters:
        df (pyspark.sql.DataFrame): data frame that will has the column to be filled. Must Contain:
            fill_col:column to fill with
        fill_col (str): column that has value to be filled with average value
        na_map_to (int): na will map to a specific value,
                        thus na and this specific value will be both filled,
                        default will be None value.
                        E.g. if you want to fill both NA and 0 with avgerage, then set na_map_to = 0
    Returns:
        df (pyspark.sql.DataFrame): output data frame
    """

    df_subset = df.filter((df[fill_col].isNotNull()))
    # fill na with the specified value
    if na_map_to is not None:
        df_subset = df_subset.filter(df_subset[fill_col] != na_map_to)
    average_fill = df_subset.select(
        sqlf.avg(df_subset[fill_col]).alias("average_fill")
    ).take(1)[0]["average_fill"]
    average_fill = 0 if average_fill is None else average_fill
    df = df.withColumn(
        fill_col,
        sqlf.when(
            (df[fill_col] == na_map_to) | (df[fill_col].isNull()), average_fill
        ).otherwise(df[fill_col]),
    )
    return df


def apply_offer_recency(
    assign, channel, coupon, cells, inhome_date, grp_by_col
):
    """Flag # days since last time members that have received the same offers.

    Parameters:
        assign (spark.sql.DataFrame): previous assignment log file
            required columns: "mbrshp_sid", "experiment_id", "cpn_nbr"
        channel (spark.sql.DataFrame): map between experiment id and channel
            required columns: 'experiment_id', 'channel'
        coupon (spark.sql.DataFrame): map between coupon number and offer id
            required columns: 'cpn_nbr', 'cpn_type', 'offer_id'
        cells (spark.sql.DataFrame):
            required columns: 'experiment_id', 'inhome_date'
        inhome_date (str): the inhome date for current experiment for calculating the recency
        grp_by_col (str): 'offer_id' or 'cpn_type'

    Returns:
        df_grp (spark.sql.DataFrame): Data with offer recency flagged
            columns: 'mbrshp_sid', grp_by_col, 'channel'
    """

    cells = cells.withColumn(
        "inhome_date", sqlf.to_date(cells.inhome_date, "MM/dd/yy")
    )
    inhome_date = datetime.strptime(inhome_date, "%m/%d/%Y").date()
    exp_inhome = cells.groupby("experiment_id").agg(
        sqlf.min("inhome_date").alias("inhome_date")
    )

    assign = assign.join(exp_inhome, "experiment_id", "inner")

    df = assign.join(coupon, "cpn_nbr", "left")
    df = df.join(channel, "experiment_id", "left")
    df = df.filter(df[grp_by_col].isNotNull())
    df = df.withColumn("current_inhome_date", sqlf.lit(inhome_date))

    df = df.withColumn(
        "distance_from_current",
        sqlf.abs(
            sqlf.datediff(
                sqlf.col("current_inhome_date"), sqlf.col("inhome_date")
            )
        ),
    )
    df_grp = df.groupBy(["mbrshp_sid", grp_by_col, "channel"]).agg(
        sqlf.min("distance_from_current").alias("days_to_recent_exposure")
    )
    df_grp = df_grp.withColumn("channel", sqlf.upper(df_grp.channel))
    pivot_col = df_grp.select("channel").distinct().toPandas().channel.tolist()
    pivot_col = [x.upper() for x in pivot_col]
    df_grp = (
        df_grp.groupby("mbrshp_sid", grp_by_col)
        .pivot("channel", pivot_col)
        .agg(sqlf.min("days_to_recent_exposure"))
    )
    return df_grp


def count_pcs_of_mail(df):
    """Count number of peices of mail represented by dataframe.

    Parameters:
        df (pyspark.sql.DataFrame): Dataframe to count
            REQUIRES:
                FHH_IND: indicator of free-householder (double count mail)
    Returns:
        mail_pcs (int): current count of mail pieces for dataframe
    """
    # create one for each member
    df = df.withColumn("count_mbr", sqlf.lit(1))
    # sum adds up to two for FHH and one for all other mailed members
    df = df.withColumn("mail_pcs", df.FHH_IND + df.count_mbr)
    mail_pcs = df.select(sqlf.sum("mail_pcs")).collect()[0][0]
    return mail_pcs


def set_mail_flag(base_data, subset, flag_value):
    """Set the mail flag in base dataset for subset members.

    Parameters:
        base_data (pyspark.sql.DataFrame): dataset to adjust flag in
            REQUIRES: MBRSHP_SID, mail_flag
        subset (pyspark.sql.DataFrame): subset of base data on which to make the change
            REQUIRES: MBRSHP_SID, mail_flag
        flag_value (int): desired value of flag
    Returns:
        flagged_data (pyspark.sql.DataFrame): base_data with flags adjusted
    """
    subset = subset.withColumn("new_mail_flag", sqlf.lit(flag_value))
    subset = subset.select("MBRSHP_SID", "new_mail_flag")
    joined = base_data.join(subset, "MBRSHP_SID", "left")
    changed_condition = joined.new_mail_flag == flag_value
    joined = joined.withColumn(
        "mail_flag",
        sqlf.when(changed_condition, flag_value).otherwise(joined.mail_flag),
    )
    flagged_data = joined.drop("new_mail_flag")
    return flagged_data


def calc_overlapping_cols(df1, df2):
    """Determine overlapping columns between two dataframe.

    Example use case: During data ingestion we want to join member
    data on either SID or NBR, but don't always know
    which exists where. This function returns the relevant
    column to join on. In general, we will want to join
    on all overlapping columns between the two frames

    Since Spark columsn are not case sensitive, and this function is
    for use in spark,  if there is a capitalization difference the
    capitalization from the first df will be used

    Parameters:
        df1 (pyspark.sql.DataFrame): first spark dataframe
        df2 (pyspark.sql.DataFrame): second spark dataframe

    Returns:
        df (list[str]): list of overlapping columns between two frames
    """
    log.debug("Overlapping column calculation on: \n{}\n{}".format(df1, df2))
    d1_map = dict([(x.upper(), x) for x in df1.columns])
    d2_map = dict([(x.upper(), x) for x in df2.columns])
    intersection = set(d1_map.keys()).intersection(set(d2_map.keys()))
    if len(intersection) != len(set(df1.columns).intersection(df2.columns)):
        log.warn(
            "These two dataframes have capitalization differences in columns:\n{}\n{}".format(
                df1, df2
            )
        )
    return [
        d1_map[column.upper()]
        for column in df1.columns
        if column.upper() in intersection
    ]


def subset_by_time(df, rel_date, partition):
    """Subset a time partitioned dataframe to the relevant member specific slice.

    Many of our data sources have a time-specific axis (i.e. DNA)
    and need to be subsetted to the relevant period for analysis.
    This function subsets by a given time partition, returning the
    partition closes to the relevant date. Currently only supports
    fiscal week.

    Parameters:
        df (pyspark.sql.DataFrame): spark dataframe to subset
        rel_date (str): string representation of desired date in YYYY-MM-DD format
        partition (str): string represntation of partition format

    Returns:
        df (pyspark.sql.DataFrame): subsetted dataframe containing only relevant partition
    """
    if partition == "fiscal_week":
        fiscal_week = determine_fiscal_week(df, rel_date)
        df = df[df.FISCAL_WEEK_END == fiscal_week]
    else:
        print("time partition not currently supported")
    return df


def read_subset_and_cast(
    file_path, file_type, subset_cols=None, time_part=None, time_part_val=None
):
    """Subset input file to relevant columns and make standard type casts.

    Parameters:
        sparkcontext(pyspark.SparkContext): The spark context to ingest into
        file_path(str): path of data file to be read in
        file_type(str): file type as parquet vs. csv
        subset_cols(array<str>): selected columns
        time_part(str): partition name
        time_part_val(str): time value to filter

    Returns:
        df (pyspark.sql.DataFrame): subsetted dataframe
    """
    if file_type == "parquet":
        df = spark.read.parquet(file_path)
    else:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    if time_part:
        df = subset_by_time(df, time_part_val, time_part)
    if subset_cols is not None:
        existing_cols = [
            col
            for col in subset_cols
            if col.upper() in list([x.upper() for x in df.columns])
        ]
        df = df.select(*existing_cols)
    if "prediction" in df.columns:
        df = df.withColumn("prediction", df.prediction.cast("float"))
        df = df[df.prediction.isNotNull()]
        df = df[df.prediction > 0]
    if "MBRSHP_SID" in df.columns:
        df = df.withColumn("MBRSHP_SID", df.MBRSHP_SID.cast("integer"))
    if "MBRSHP_NBR" in df.columns:
        df = df.withColumn("MBRSHP_NBR", df.MBRSHP_NBR.cast("string"))
        df = df.withColumn("MBRSHP_NBR", sqlf.lpad(df.MBRSHP_NBR, 11, "0"))
    if "cpn_nbr" in df.columns:
        df = df.withColumn("cpn_nbr", df.cpn_nbr.cast("string"))
    if "offer_id" in df.columns:
        df = df.withColumn("offer_id", df.offer_id.cast("integer"))
    if "EXTENDED_PRC_AMT" in df.columns:
        df = df.withColumn(
            "EXTENDED_PRC_AMT", df.EXTENDED_PRC_AMT.cast("double")
        )
    if "PURCH_HDR_ID" in df.columns:
        df = df.withColumn("PURCH_HDR_ID", df.PURCH_HDR_ID.cast("double"))
    if "cpn_dollar_threshold" in df.columns:
        df = df.withColumn(
            "cpn_dollar_threshold", df.cpn_dollar_threshold.cast("double")
        )
    if "cpn_class_id" in df.columns:
        df = df.withColumn("cpn_class_id", df.cpn_class_id.cast("integer"))

    return df


def explode_columns(df, col_list, col_name):
    df = df.withColumn("COMBINED", sqlf.array(*col_list))
    df = df.withColumn(col_name, sqlf.explode(df.COMBINED))
    df = df.drop("COMBINED")
    return df


def cap_value(df, cap_col, cap_lb=None, cap_ub=None):
    """cap the value with lower bound value and upper bound value

    Parameters:
        df(pyspark.sql.DataFrame): spark dataframe to cap
        cap_col(str): the column of value to be capped
        cap_lb(int): lower bound value
        cap_ub(int): upper bound value

    Returns:
        df (pyspark.sql.DataFrame): capped dataframe
    """
    if cap_lb:
        df = df.withColumn(
            cap_col,
            sqlf.when(df[cap_col] < cap_lb, cap_lb).otherwise(df[cap_col]),
        )
    if cap_ub:
        df = df.withColumn(
            cap_col,
            sqlf.when(df[cap_col] > cap_ub, cap_ub).otherwise(df[cap_col]),
        )
    return df


def create_equal_size_bucket(df, num_buckets, value_col_name, bucket_col_name):
    """cap the value with lower bound value and upper bound value

    Parameters:
        df(pyspark.sql.DataFrame): spark dataframe to be bucketed
        num_buckets(int): number of buckets
        value_col_name(str): column name to be bucketed
        bucket_col_name(str): column name that saves the buckets

    Returns:
        df (pyspark.sql.DataFrame): dataframe after being bucketed equal size
    """
    qds = QuantileDiscretizer(
        numBuckets=num_buckets,
        inputCol=value_col_name,
        outputCol=bucket_col_name,
        relativeError=0,
        handleInvalid="error",
    )
    bucketizer = qds.fit(df)
    df = bucketizer.transform(df)
    return df


def cap_top_percentile(df, column, percentile=0.99, error=0.0001):
    """
    This function caps values of a column to the percentile specified

    :param df (pyspark.sql.DataFrame): input dataframe
    :param column (str): The column you want to cap
    :param percentile (float): the percentile you want to make the new max
    :param error (float): a parameter to control how approximate it is
    :return:
    """
    upper_bound = df.approxQuantile(column, [percentile], error)[0]
    log.info("Upper Bound: {}".format(upper_bound))
    return cap_value(df, column, None, upper_bound)


def calc_sampling_value(df, spend_col):
    """calculate the value want to do the supervised sampling

    Parameters:
        df(pyspark.sql.DataFrame): spark dataframe to calculated
            REQUIRES: MBRSHP_SID, spend_col, DAYS_SINCE_LAST_TRIP
        spend_col(string): columns of Last X weeks revenue to mimic on

    Returns:
        df (pyspark.sql.DataFrame): dataframe after calculation
        Columns: MBRSHP_SID, sampling_value
    """
    df = df.withColumn(spend_col, df[spend_col].cast("double"))
    df = df.withColumn(
        "DAYS_SINCE_LAST_TRIP", df["DAYS_SINCE_LAST_TRIP"].cast("double")
    )
    df = df.fillna(0, subset=[spend_col, "DAYS_SINCE_LAST_TRIP"])
    # cap outliers for # weeks spend
    spend_LB = 0
    spend_UB = df.approxQuantile(spend_col, [0.99], 0.0001)[0]
    df = cap_value(df, spend_col, spend_LB, spend_UB)
    # cap outliers for days since last trip
    days_LB = 0
    days_UB = 365
    df = cap_value(df, "DAYS_SINCE_LAST_TRIP", days_LB, days_UB)
    # calculate final value for sampling
    df = df.withColumn("spend", sqlf.ceil(df[spend_col]))
    df = df.withColumn(
        "sampling_value", df["spend"] - df["DAYS_SINCE_LAST_TRIP"] / days_UB
    )
    df = df.select("MBRSHP_SID", "sampling_value")
    return df


def calc_sampling_seg(df):
    """calculate the value want to do the supervised sampling

    Parameters:
        df(pyspark.sql.DataFrame): spark dataframe to calculated
            REQUIRES: MBRSHP_SID, 'decile', 'TENURE'

    Returns:
        df (pyspark.sql.DataFrame): dataframe after calculation
            Columns: MBRSHP_SID, sampling_seg
    """
    df = df.withColumn("TENURE", df.TENURE.cast("double"))
    df = df.fillna(0, subset="TENURE")
    df = df.withColumn(
        "is_tenured", sqlf.when(df.TENURE < 150, 0).otherwise(1)
    )
    df = df.withColumn("decile", sqlf.col("DECILE").cast("double"))
    df = df.fillna(10, subset="decile")
    df = df.withColumn("sampling_seg", df["IS_TENURED"] * sqlf.col("DECILE"))
    df = df.select("MBRSHP_SID", "sampling_seg")
    return df


def rdd_rank_by_col(
    df, sort_col_asc, sort_col_desc, new_col_name, hash_col="MBRSHP_SID"
):
    """rank the full dataframe in scalable way

    Parameters:
        df(pyspark.sql.DataFrame): spark dataframe to be bucketed
        sort_col_asc(str): column name to be sorted ascending
        sort_col_desc(str): column name to be sorted descending
        new_col_name(str): column name that saves the rank
        hash_col(str): hash column

    Returns:
        df(pyspark.sql.DataFrame): dataframe after ranked
    """
    hash_num = df.count()
    df = df.withColumn("hash_col", sqlf.hash(sqlf.col(hash_col) + hash_num))
    df_sorted = df.orderBy(sort_col_asc, sqlf.desc(sort_col_desc), "hash_col")
    df_ranked = df_sorted.rdd.zipWithIndex()
    new_schema = (
        sqlt.StructType()
        .add("data", df_sorted.schema)
        .add(new_col_name, sqlt.LongType())
    )
    columns = ["data." + i for i in df_sorted.columns] + [new_col_name]
    df_ranked = spark.createDataFrame(df_ranked, new_schema).select(columns)
    return df_ranked


def palindrome_rank_group(df, num_groups, rank_col_name, new_col_name):
    """palindrome rank

    Parameters:
        df(pyspark.sql.DataFrame): spark dataframe to be ranks
        num_groups(int): num of groups
        rank_col_name(str): column to be grouped
        new_col_name(str): column name that saves the group number

    Returns:
        df(pyspark.sql.DataFrame): dataframe after ranked
    """
    original_columns = df.columns
    df = df.withColumn("rank_col_name", df[rank_col_name] - 1)
    df = df.withColumn(
        "row_number", sqlf.floor(df[rank_col_name] / num_groups)
    )
    df = df.withColumn("asc_order", df[rank_col_name] % num_groups + 1)
    df = df.withColumn("is_reverse", df.row_number % 2)
    df = df.withColumn(
        "col_number", sqlf.abs(df.is_reverse * (num_groups + 1) - df.asc_order)
    )
    df = df.withColumn(new_col_name, df.col_number)
    df = df.select(original_columns + [new_col_name])
    return df


def palindrome_sample(df, sample_size, group_num=100):
    """palindrome sample

    Parameters:
        df(pyspark.sql.DataFrame): spark dataframe to be sampled
        sample_size (float): sampling fraction

    Returns:
        df(pyspark.sql.DataFrame): dataframe after sampled to targeted cell
    """
    initial_size = df.count()
    if initial_size == 0:
        return df
    elif 1 < initial_size <= sample_size:
        return df

    cols = df.columns
    if sample_size < 1:
        sample_group = min(math.ceil(sample_size * group_num), group_num)
    if sample_size >= 1:
        sample_group = min(
            math.ceil((float(sample_size) / initial_size) * group_num),
            group_num,
        )
    df = rdd_rank_by_col(df, "sampling_seg", "sampling_value", "sampling_rank")
    df = palindrome_rank_group(
        df, group_num, "sampling_rank", "sampling_group"
    )
    seed(hash(initial_size))
    print(group_num)
    print(sample_group)
    print(sample_size)
    print(initial_size)
    selected_groups = sample(list(range(1, group_num + 1)), int(sample_group))
    df = df.filter(df.sampling_group.isin(selected_groups))
    df = df.select(cols)
    return df


def deterministic_sample(df, sample_size, columns=None, seed="cat_in_the_hat"):
    """
    This function produces a deterministic random sample of a dataframe

    :param df: the df to sample
    :param sample_size: either a percentage or absolute number for the size of the sample
    :param columns: The columns to participate in the random ordering of the sample
    :param seed: An addition seed to be part of the sample
    :return:
    """
    initial_size = df.count()
    if initial_size == 0:
        return df
    elif 1 <= initial_size <= sample_size:
        return df

    columns = columns if columns else df.columns

    if sample_size < 1:
        sample_size = int(sample_size * initial_size)

    hash_columns = list([sqlf.col(x) for x in columns]) + [sqlf.lit(seed)]
    return df.orderBy(sqlf.hash(*hash_columns)).limit(sample_size)


def deterministic_df(
    df,
    order_by,
    seed,
    random_only=False,
    unique_column="unique_id",
    extra_hash_columns=None,
):
    """Add a deterministic unique random id to the dataframe in order to
    break ties in a deterministic way when ordering or to introduce
    deterministic variety when choosing coupons randomly.

    We chose SHA256 over hash, because hash supplies a seed internally which
    makes it non-deterministic.

    If the dataframe has a trips column, it will be used as the primary
    tie breaker. As opposed to agg by sum, agg by avg only uses the positive
    values.

    Parameters:
        df (pyspark.sql.DataFrame): a dataframe which needs a deterministic
            order
        order_by (list(pyspark.sql.Column)): list of criteria to order by
        seed (int): a deterministic unique value to start from when creating an
            order
        random_only (boolean): whether the deterministic will be solely based
            on hash or on other criteria
        unique_column (str): the name to use for the unique column
        extra_hash_columns (list(pyspark.sql.Column)): extra column to compute
            a unique hash. The columns cannot have NULL values and they have to
            be part of df.
    Returns:
        deterministic (pyspark.sql.DataFrame): a dataframe with a unique id for
            each row
        deterministic_order_by (list(pyspark.sql.Column)): a list of columns to
            order by in a deterministic way
        unique_column (str): the name of the unique column

    """
    deterministic_order_by = copy.copy(order_by)
    all_columns = [column.lower() for column in df.columns]

    deterministic = df
    if not random_only and "trips" in all_columns:
        deterministic = deterministic.fillna(0, ["trips"])

        w = Window.partitionBy("cpn_nbr")
        deterministic = deterministic.withColumn(
            "avg_cpn_trips",
            sqlf.avg(sqlf.when(sqlf.col("trips") > 0, sqlf.col("trips"))).over(
                w
            ),
        )
        deterministic_order_by.extend(
            [sqlf.col("trips").desc(), sqlf.col("avg_cpn_trips").desc()]
        )

    columns = [
        sqlf.lit(seed).cast("string"),
        sqlf.col("cpn_nbr").cast("string"),
    ]
    if "mbrshp_sid" in all_columns:
        columns.append(sqlf.col("mbrshp_sid").cast("string"))
    if extra_hash_columns:
        for column in extra_hash_columns:
            columns.append(column.cast("string"))

    deterministic = deterministic.withColumn(
        unique_column, sqlf.sha2(sqlf.concat(*columns), 256)
    )
    deterministic_order_by.append(sqlf.col(unique_column).desc())

    return deterministic, deterministic_order_by, unique_column


def load_past_longitudinal_mbrs(
    longitudinal_id, experiment_id, assignment_path, cells_path
):
    """
    Load longitudinal mbrs from past campaigns matching the longitudinal id.

    Parameters:
        longitudinal_id (str): id of current longitudinal group
        experiment_id (int): id of current campaign
        assignment_path (str): path to assignments by cell_id
        cells_path (str): path tp cell df with campaign cell level info

    Returns:
            df (pyspark.sql.DataFrame): unique mbrshp_sids
    """
    cells = read_s3_to_local(cells_path, ftype="csv")
    id_cols = ["experiment_id", "cell_id", "longitudinal_id"]
    cells = convert_id_cols_to_str(cells, id_cols)

    long_cells = (
        cells.query(
            f"experiment_id != '{experiment_id}' and longitudinal_id == '{longitudinal_id}'"
        )["cell_id"]
        .astype("float")
        .unique()
    )

    long_cells = list(long_cells)

    if len(long_cells) == 0:
        log.warn(f"longitudinal id {longitudinal_id} has no past cells")

        mbrs = spark.createDataFrame({}, "mbrshp_sid: string")

    else:
        assignment = read_subset_and_cast(assignment_path, "parquet")
        assignment = assignment.filter(sqlf.col("cell_id").isin(long_cells))

        mbrs = assignment.select("mbrshp_sid").distinct()

        if mbrs.count() == 0:
            log.warn(f"longitudinal id {longitudinal_id} has no past members")

        checks.check_loaded_long_cells(assignment, long_cells)

    return mbrs


def update_categories(transactions, item, ah4_or_ah5="both"):
    """
    Update categories (AH4, AH5, or both) in transactions, based on those in item master.

    Parameters:
        transactions (pyspark.sql.DataFrame): transactions data to use
        item (pyspark.sql.DataFrame): item master data to use
        ah4_or_ah5 (str): category to consider - AH4_CD, AH5_CD, or BOTH

    Returns:
        pyspark.sql.DataFrame: updated categories in transactions data

    """

    # Dedup item - it may have duplicates due to GTIN
    w = Window.partitionBy("ARTICLE_NBR", "EXP_DT").orderBy(
        sqlf.col("EXP_DT").desc_nulls_last()
    )
    item = item.withColumn("rank", sqlf.row_number().over(w))
    item = item.filter(item.rank == 1)

    category = ah4_or_ah5.upper()

    categories = ["AH4_CD", "AH5_CD"] if category == "BOTH" else [category]
    for cat in categories:
        item_cat = "ITEM_{}".format(cat)
        item_copy = item.select("ARTICLE_NBR", cat).withColumnRenamed(
            cat, item_cat
        )
        transactions = transactions.join(item_copy, "ARTICLE_NBR", "left")
        transactions = transactions.withColumn(
            cat,
            sqlf.coalesce(transactions[item_cat], transactions[cat]),
        ).drop(item_cat)

    return transactions
