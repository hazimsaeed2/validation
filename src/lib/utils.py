"""Utility functions that span across pipelines."""

from datetime import datetime, timedelta
from dateutil import parser
from functools import reduce

import numpy as np


def next_fiscal_week_end(dateofinterest):
    """Find the end of the next fiscal week after a date.

    Allows matching of arbitrary dates to fiscal week
    partitioned data.

    Parameters:
        dateofinterest (str or datetime): representation of date of interest

    Returns:
        fiscalend (str): string representation of end of fiscal week
            Returns string in YYYY-mm-dd format
    """
    if isinstance(dateofinterest, str):
        date = parser.parse(dateofinterest)
    elif isinstance(dateofinterest, datetime):
        date = dateofinterest
    else:
        raise ValueError("Invalid date input datatype!")
    weekday = date.weekday()
    delta = timedelta((12 - weekday) % 7)
    week_end = date + delta
    fiscalend = datetime.strftime(week_end, "%Y-%m-%d")
    return fiscalend


def closest_fiscal_week_end(cube, dateofinterest):
    """Find the closest available fiscal week end to the date of interest.

    Provides the max available week end if no future dates available.

    Parameters:
        dateofinterest (str or datetime): representation of date of interest
        cube (psypark.sql.DataFrame): Member cube for application
            FISCAL_WEEK_END (date)

    Returns:
        fiscalend (str): string representation of end of fiscal week
            Returns string in YYYY-mm-dd format
    """
    from pyspark.sql.functions import max as fmax

    cube_wk = next_fiscal_week_end(dateofinterest)  # get relevant cube week
    max_cube_wk = cube.select(fmax("FISCAL_WEEK_END")).collect()[0][0]
    if str(cube_wk) > str(max_cube_wk):
        return max_cube_wk
    else:
        return cube_wk


def stack(df, by):
    """Unpivot a wide format dataframe to long format.

    Takes all df columns not included in 'by' and coverts
    them to a single column with an additional column of values
    Requires all values in the 'by' columns to be of the same
    datatype.

    Parameters:
        df (pyspark.sql.DataFrame): dataframe to convert to long format
        by (list[str]): list of columns to unpivot by, will be kept as-is.
    Returns
        df (pyspark.sql.DataFrame): long formatted dataframe
    """
    # Filter dtypes and split into column names and type description
    from pyspark.sql.functions import array, explode, struct, lit

    cols, dtypes = list(zip(*((c, t) for (c, t) in df.dtypes if c not in by)))
    # Spark SQL supports only homogeneous columns
    if len(set(dtypes)) != 1:
        raise ValueError("All columns have to be of the same type")
    # Create and explode an array of (column_name, column_value) structs
    arrays = array(
        [struct(lit(c).alias("key"), df[c].alias("val")) for c in cols]
    )
    kvs = explode(arrays).alias("kvs")

    return df.select(by + [kvs]).select(by + ["kvs.key", "kvs.val"])


def get_col_list(df, colname):
    """Convert pyspark df column to list of distinct values.

    Parameters:
        df (pyspark.sql.DataFrame): Dataframe containing col of interest
        colname (str): name of column to convert

    Returns:
        itemslist (list): list of distinct items in the column
    """
    distinct = df.select(colname).distinct().collect()
    itemslist = [item[colname] for item in distinct]
    return itemslist


def apply_unionall(*dfs):
    """Union a list of dataframes.

    Parameters:
        *dfs (list[pyspark.sql.DataFrame]): list of dataframes to union

    Returns:
        unioned dataframes
    """
    from pyspark.sql import DataFrame

    return reduce(DataFrame.unionAll, dfs)


def top_n(df, n, rank_col, group_col=None, keep=False):
    """Limit a dataframe to only the top `n` of something.

     This function limits a dataframe to the top `n` items in each group column,
     where items are ranked by a separate rank column

    Parameters:
        df (pyspark.sql.DataFrame): dataframe to filter. Must Contain:
            rank_col: column to rank dataframe by
            group_col: column to group data by
        n (int): number of 'top' items to limit to
        rank_col (str|list(pyspark.sql.Column)): name of column to rank by
        group_col (str|list(pyspark.sql.Column)): name of column to group by
        keep (bool): whether or not to keep the rank column in output

    Returns:
        top_per (pyspark.sql.DataFrame): filtered dataframe containing only top n items
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    from pyspark.sql.functions import desc, col

    if isinstance(rank_col, list):
        order_by = rank_col
    else:
        order_by = [col(rank_col).desc()]

    if group_col is None:
        df = df.orderBy(*order_by)
        count = df.count()
        if count > n:
            top_per = df.limit(int(n))
        else:
            top_per = df
    else:
        if isinstance(group_col, list):
            group_by = group_col
        else:
            group_by = [col(group_col)]

        window = Window.partitionBy(*group_by).orderBy(*order_by)
        top_per = df.withColumn("rank", row_number().over(window))
        top_per = top_per[top_per.rank <= n]
        if not keep:
            top_per = top_per.drop("rank")
    return top_per


def sample_by_count(df, sample_size):
    """Sample a dataframe by count instead of fraction.

    Parameters:
        df (pyspark.sql.DataFrame): dataframe to sample
        sample_size (int): integer count of items to sample

    Returns:
        sampled_df (pyspark.sql.DataFrame): sampled dataframe
    """
    itemcount = df.count()
    if itemcount > 0:
        sample_frac = sample_size / itemcount
    else:
        raise ValueError("Unable to sample! Would return Empty DataFrame")
    if sample_frac < 1:
        sampled_df = df.sample(False, sample_frac, 7)
    else:
        sampled_df = df
    return sampled_df


def trips_only(detaildata):
    """Filter detail data to only contain 'trip' related items.

    The defintion of 'trip' is was received by the client, and can be applied to transaction
    detail data using this convenience function. To count trips after applying this filter, one
    only needs to count unique header IDs.

    Parameters:
        detaildata (pyspark.sql.DataFrame): transaction detail dataframe to filter. Requires:
            SALES_CTGRY_CD - Sales category code, filter limits to merchandise sales
            MC_CD - Merch category code, filter removes gift cards and cafe
            SALES_QTY - quantity bought, must be above zero

    Returns:
        detail (pyspark.sql.DataFrame): filtered transaction detail dataframe
    """
    detail = detaildata[
        detaildata.SALES_CTGRY_CD == "03"
    ]  # only want in-club merchandise sales
    # gifts cards, music/apps, and cafe
    non_merch_codes = ["402030190", "402030191", "203010098"]
    detail = detail[~detail.MC_CD.isin(non_merch_codes)]
    # remove non purchase releated receipt-lines
    detail = detail[detail.SALES_QTY > 0]
    return detail


def capitalize_col_names(df):
    """convert column name to be capital letters

    Parameters:
        df (pyspark.sql.DataFrame): data frame to be converted
    Returns:
        df (pyspark.sql.DataFrame): output dataframe with capitalized column name
    """
    col_names = df.columns
    for name in col_names:
        df = df.withColumnRenamed(name, name.upper())
    return df


def lowercase_col_names(df):
    """convert column name to be capital letters

    Parameters:
        df (pyspark.sql.DataFrame): data frame to be converted
    Returns:
        df (pyspark.sql.DataFrame): output dataframe with capitalized column name
    """
    col_names = df.columns
    for name in col_names:
        df = df.withColumnRenamed(name, name.lower())
    return df


def convert_id_cols_to_str(df, cols):
    """
        Converts id columns to string w/o having the added decimal point due to
        nans being floats.

        Parameters:
            df (pd.DataFrame): df with id columns
            cols (list): list of columns to convert

        Return:
            df (pd.DataFrame): df with converted columns
        """
    for col in cols:
        if df[col].dtype not in [str, object]:
            df[col] = df[col].fillna(-1)
            df[col] = df[col].astype(int)
            df[col] = df[col].astype(str)
            df[col] = df[col].replace("-1", np.nan)

    return df
