"""Utilities and helper functions for models."""
import boto3
from datetime import datetime as dt
from dateutil import parser
import numpy as np
import re


def member_in_range(memberdata, start_date, end_date):
    """Filter member cube to contain only members within a date range.

    Subset member cube date (one item per member per week) to give us a list of desired
    members within a given date range. These will be all members
    limited to the 'real people' member types for paying memberships.

    Parameters:
        memberdata (pyspark.sql.DataFrame): Member Cube dataframe to filter. Requires:
            FISCAL_WEEK_END - Date column to filer on
            MEMBER_TYPE - filters to client provided subset of member types
            MEMBERSHIP_SID - Member ID column to select as output
        start_date (str): start date of retained range in 'YYYY-MM-dd' format (inclusive)
        end_date (str): end date of retained range in 'YYYY-MM-dd' format (inclusive)

    Returns:
        member_set (pyspark.sql.DataFrame): datafame with single column (MBRSHP_SID) of filtered members
    """
    members = memberdata[memberdata.FISCAL_WEEK_END >= start_date]
    members = members[members.FISCAL_WEEK_END <= end_date]
    if "MEMBER_TYPE" in members.columns:
        members = members[
            members.MEMBER_TYPE.isin([4, 5, 8])
        ]  # filter for 'real people'
    elif "LATEST_MBRSHP_TYPE_ID" in members.columns:
        members = members[
            members.LATEST_MBRSHP_TYPE_ID.isin([4, 5, 8])
        ]  # filter for 'real people'
    else:
        raise ValueError("missing member type column!")

    member_set = members.select("MBRSHP_SID").distinct()
    return member_set


def tenured_in_range(memberdata, start_date, end_date):
    """Filter member cube to contain only tenured members within a date range.

    Subset member cube date (one item per member per week) to give us a list of desired
    tenured members within a given date range. These will be tenured members
    only, limited of the 'real people' member types for paying memberships.

    Parameters:
        memberdata (pyspark.sql.DataFrame): Member Cube dataframe to filter. Requires:
            FISCAL_WEEK_END - Date column to filer on
            TENURE_GROUP - filters to tenured members
            MEMBER_TYPE - filters to client provided subset of member types
            MEMBERSHIP_SID - Member ID column to select as output
        start_date (str): start date of retained range in 'YYYY-MM-dd' format (inclusive)
        end_date (str): end date of retained range in 'YYYY-MM-dd' format (inclusive)

    Returns:
        member_set (pyspark.sql.DataFrame): datafame with single column (MBRSHP_SID) of filtered members
    """
    members = memberdata[
        memberdata.TENURE_GROUP == "tenured"
    ]  # tenured members only
    member_set = member_in_range(members, start_date, end_date)
    return member_set


def details_with_member(detaildata, headerdata):
    """Join the member number onto detail data where needed.

    The purchase detail data was recieved from the client with the member id missing. It is
    currently located on the header only. Given that we often need to work at the receipt-line
    level, joining meber ID onto the detail table has broad use for our analytics.

    Parameters:
        detaildata (pyspark.sql.DataFrame): transaction detail dataframe to join. Requires:
            PURCH_HDR_ID - Purchase header ID. Acts as Join Key
        headerdata (pyspark.sql.DataFrame): transaction header dataframe to join. Requires:
            PURCH_HDR_ID - Purchase header ID. Acts as Join Key
            MBERSHP_SID - Membership ID. Column to be added

    Returns:
        joined (pyspark.sql.DataFrame): transaction detail datagrame with membership ID column
    """
    detaildata = detaildata.repartition(2000, "PURCH_HDR_ID")
    headerdata = headerdata.select(["PURCH_HDR_ID", "MBRSHP_SID"])
    headerdata = headerdata.repartition(2000, "PURCH_HDR_ID")
    joined = headerdata.join(detaildata, "PURCH_HDR_ID", "inner")
    return joined


def join_category_details(itemdata, detaildata, category_column):
    """Join category column to transactions based on SKU.

    We often just want to limit ourselves to the category of a particular SKU,
    at a given item-heirarchy level of interest. This function will join item category
    onto transaction detail data. Will replace missing and blank categories with UNKNOWN.

     Parameters:
        itemdata (pyspark.sql.DataFrame): item data to pull category from. Requires:
            GTIN_CD - Item SKU code. Acts as join key 1
            ARTICLE_NBR - Item article number. Acts as join key 2
        detaildata (pyspark.sql.DataFrame): transaction detail data to join to. Requires:
            GTIN_CD - Item SKU code. Acts as join key 1
            ARTICLE_NBR - Item article number. Acts as join key 2
        category_column (str): name of heirarchy level to use as category. Must be present in itemdata.
            Must have both code and description. Input the naame of the level, not the column name.

    Returns:
        joined (pyspark.sql.DataFrame): transaction detail with added CATEGORY column
    """
    from pyspark.sql.functions import when, col

    category_id = category_column + "_CD"
    category_desc = category_column + "_DESC"
    item_cols = [
        col("GTIN_CD"),
        col("ARTICLE_NBR"),
        col(category_id).alias(category_id),
        col(category_desc).alias(category_desc),
    ]
    subset = itemdata.select(*item_cols)
    filled = subset.fillna({category_desc: "UNKNOWN"}).replace(" ", "UNKNOWN")
    unknown = col(category_desc) == "UNKNOWN"
    filled = filled.withColumn(
        "CATEGORY_ID", when(unknown, 0).otherwise(col(category_id))
    )
    renamed = filled.withColumnRenamed(category_desc, "CATEGORY_NAME").drop(
        category_desc
    )
    joined = detaildata.join(
        renamed,
        (
            (renamed.GTIN_CD == detaildata.GTIN_CD)
            & (renamed.ARTICLE_NBR == detaildata.ARTICLE_NBR)
        ),
        "inner",
    )
    return joined


def format_column_names(df):
    """Format column names to all caps with underscores convention.

    When saving to parquet format, we must limit our column names to not contain any
    special characters( &,[]|). This function will remove special characters from column
    names as well as format all names in All caps with underscores as is case convention.

     Parameters:
        df (pyspark.sql.DataFrame): dataframe to format columns of

    Returns:
        df (pyspark.sql.DataFrame): dataframe with renamed columns
    """
    df = df.toDF(*[c.replace(",", "") for c in df.columns])
    df = df.toDF(*[c.replace("&", "") for c in df.columns])
    df = df.toDF(*[c.replace(" ", "_") for c in df.columns])
    df = df.toDF(*[c.upper() for c in df.columns])
    return df


def filter_mincats(df, data_col, min_cats):
    """Filter member-category dataframe to those members with at least min_cats.

    When training for collaborative filtering, we have limitiations on which members
    we can understand, and need a minimum amount of examples to be able to fit. This
    function removes members for whom we don't have enough information.

    Parameters:
        df (pyspark.sql.DataFrame): Dataframe to filter. Requires:
            MBRSHP_SID - Member id for members to filter
            CATEGORY_ID - ID column of categories per member
            data_col - column of data to check against
        data_col (str): name of the column containing implicit feedback data
        min_cats (int): minmum number of categories to keep (keeps >=)

    Returns:
        filtered (pyspark.sql.DataFrame): Filtered dataframe
    """
    from pyspark.sql.functions import countDistinct

    grouped = df.groupBy("MBRSHP_SID").agg(
        countDistinct("CATEGORY_ID").alias("CAT_COUNT")
    )
    enough_cats = grouped[grouped.CAT_COUNT >= min_cats].select(["MBRSHP_SID"])
    filtered = df.join(enough_cats, "MBRSHP_SID", "inner")
    return filtered


def generate_hist(df, input_col, name, n_buckets):
    """Create a histogram from data for plotting or additional analysis.

    Reshape data into a histogram with buckets of `input col` as one column,
    and counts as the second. If multiple dataframes are specified (in a list),
    will generate a multi-histogram along `input col` from all inputs with identical
    bucketing.

    Parameters:
        df (pyspark.sql.dataframe or list): dataframe(s) containing column to create histogram from
        input_col (str): name of column to create histogram from
        name (str or list(str)): name(s) to use for count data cols per dataframe
        n_buckets (int): number of buckets to use in histogram

    Returns:
        bux (pandas.dataFrame): pandas dataframe containing histogram output Contains:
            input_col (float): contains the bucket centers for each histogram bucket
            [name] (int): number of items in each bucket for each name in names
    """
    # add some wiggle room  to min and max for rounding
    from pyspark.sql.functions import min as smin
    from pyspark.sql.functions import max as smax
    from pyspark.ml.feature import Bucketizer

    if not isinstance(df, list):
        df = [df]
    if not isinstance(name, list):
        name = [name]
    minvals = []
    maxvals = []
    for frame in df:
        minval = (
            float(
                frame.select(smin(input_col).alias("min")).toPandas()["min"][0]
            )
            - 0.001
        )
        minvals.append(minval)
        maxval = (
            float(
                frame.select(smax(input_col).alias("max")).toPandas()["max"][0]
            )
            + 0.001
        )
        maxvals.append(maxval)
    minval = min(minvals)
    maxval = max(maxvals)
    bucket_param = n_buckets + 2
    bucketizer = Bucketizer(
        splits=np.linspace(minval, maxval, bucket_param),
        inputCol=input_col,
        outputCol="p_bucket",
    )
    for i, frame in enumerate(df):
        buckets = bucketizer.transform(frame).groupby("p_bucket").count()
        buckets = buckets.toPandas()
        sort_buckets = buckets.sort_values("p_bucket", ascending=True)
        renamed = sort_buckets.rename(columns={"count": name[i]})
        if i == 0:
            bux = renamed
        else:
            bux = bux.merge(renamed, on="p_bucket", how="inner")
    bux = bux.sort_values("p_bucket", ascending=True)
    bux[input_col] = np.linspace(minval, maxval, len(bux))
    bux = bux[[input_col] + name]
    return bux


def plot_hist(
    df,
    input_col,
    n_buckets,
    height=None,
    width=None,
    title="",
    xlabel="",
    ylabel="",
):
    """Plot a histogram from a column of a dataframe.

    Generate histogram data from a dataframe and plot it using matplotlib.
    Duh. This does exactly what you think it does.

    Parameters:
        df (pyspark.sql.dataframe or list): dataframe containing data to plot
        input_col (str): name of column to create histogram from
        n_buckets (int): number of buckets to use in histogram
        height (int, optional): height of figure
        width (int, optional): width of figure
        title (str, optional): title for figure
        xlabel (str, optional): x-axis label for figure
        ylabel (str, optional): y-axis label for figure

    Returns:
        None!
    """
    import matplotlib

    matplotlib.use(
        "Agg"
    )  # need to specify THIS backend to get it to work on EMR clusters
    from matplotlib import pyplot as plt

    if height is None:
        height = 8
    if width is None:
        width = 8
    fig, axes = plt.subplots()
    fig.set_size_inches(width, height)
    plotdata = generate_hist(df, input_col, "num", n_buckets)
    plt.hist(plotdata[input_col], len(plotdata), weights=plotdata["num"])
    axes.set_title(title)
    axes.set_xlabel(xlabel)
    axes.set_ylabel(ylabel)
    plt.show()


def parse_data_range(data_filename):
    """Parse a filename for date range.

    Leverage our naming convention around date-based
    filenames to parse a filname for the date range
    that it contains.

    Parameters:
        data_filename (str): name of the file of interest

    Returns:
        range (dict): dictionary with the following keys:
            start -- start date
            end -- end date
    """
    start_date = data_filename[0:8]
    end_date = data_filename[9:17]
    start_dt = parser.parse(start_date)
    end_dt = parser.parse(end_date)
    obj = {"start": start_dt, "end": end_dt}
    return obj


def rescale_weight_factor(df, data_col):
    """Scale a column into a weight factor for business rules.

    When developing multiplication factors for scaling metrics into
    weight factors, we often desire to multiply by a number close to
    one, with a small devation from this value. This function will
    rescale input data to generate a weight factor from it.

    Parameters:
        dateofinterest (str or datetime): representation of date of interest

    Returns:
        fiscalend (str): string representation of end of fiscal week
            Returns string in YYYY-mm-dd format
    """
    from pyspark.sql.functions import when

    min_score = 0.05
    max_score = 1.75
    impact_factor = 1
    # force cast to float
    df = df.withColumn(data_col, df[data_col].cast("float"))
    median = df.approxQuantile(data_col, [0.5], 0.001)[0]
    df = df.withColumn(data_col, df[data_col] / median)
    # rescale for impact
    df = df.withColumn(data_col, (((df[data_col] - 1) * impact_factor) + 1))
    # limit scaling factors to min and max
    df = df.withColumn(
        data_col,
        when(df[data_col] > max_score, max_score).otherwise(df[data_col]),
    )
    df = df.withColumn(
        data_col,
        when(df[data_col] < min_score, min_score).otherwise(df[data_col]),
    )
    return df


def get_latest_prop_path(bucket, prefix, format="%Y_%m_%d", run_type="prod"):
    """
    Return the latest trip propensity prediction file path, found within 
    bucket/prefix. (based on the folder date)

    Assumes the folder contains run_type and only one prediction file inside
    every ..prod_<date> folder.

    Args:
        bucket (str): an s3 bucket name
        prefix (str): an s3 object prefix (e.g. path) of form pre1/pre2/etc/
    Returns:
        path (str): latest trip propensity prediction file path under the given
                    s3 bucket
    """

    client = boto3.client("s3")
    objs = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")

    max_dt = dt.strptime("2000_01_01", format)
    path = ""

    for i, obj in enumerate(objs["CommonPrefixes"]):
        if run_type + "_" in obj["Prefix"]:
            d = re.search(run_type + "_(.*)/", obj["Prefix"]).group(1)
            if len(d) == 10:
                d = dt.strptime(d, format)
                if d > max_dt:
                    max_dt = d
                    path = (
                        client.list_objects(
                            Bucket=bucket, Prefix=obj["Prefix"], Delimiter="/"
                        )
                    )["Contents"][0]["Key"]

    return path
