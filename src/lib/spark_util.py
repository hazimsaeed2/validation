"""
Util functions that require a spark session
"""

# try:
#     pyspark
# except NameError:
#     import findspark

#     findspark.init()

import time
from datetime import datetime as dt

import pyspark.sql.functions as sqlf  # moving forward standard
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit  # legacy tech debt
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()


def get_logger(name="default"):
    log = spark._jvm.org.apache.log4j.LogManager.getLogger(name)
    log.setLevel(spark._jvm.org.apache.log4j.Level.INFO)
    return log


# log = get_logger("spark_util")

wait_time = 20
retry_times = 3


def safe_join(df1, df2, how, key):
    """
    Join two dataframes after they have been aggregated.
    If the key is equal to "_" it means that everything was aggregated using
    one value(the df should have only 1 record) and join them would lead to an
    implicit cross join, which leads to error.

    Parameters:
        df1 (spark dataframe): first aggregated dataframe
        df2 (spark dataframe): second aggregated dataframe
        how (str): how to join the two dataframes
        key (str): the key on which to join

    Returns:
        df1 (spark dataframe): the join result
    """
    is_aggregate_all = (key == "_")

    if is_aggregate_all:
        df2_collection = df2.collect()
        for column in df2.columns:
            df1 = df1.withColumn(
                column, sqlf.lit(df2_collection[0][column])
            )
    else:
        df1 = df1.join(df2, how=how, on=key)

    return df1


def join_all(dfs, columns):
    return _join_all(dfs, columns, 1)


def _join_all(dfs, columns, depth):
    print("list:{}\nColumns: {}".format(len(dfs), columns))
    if len(dfs) == 1:
        return dfs[0]
    elif len(dfs) == 2:
        return dfs[0].join(dfs[1], columns)
    else:
        split = len(dfs) // 2
        new_depth = depth + 1
        left = _join_all(dfs[:split], columns, new_depth)
        right = _join_all(dfs[split:], columns, new_depth)
        if depth % 3 == 0:
            print("Join all cache.")
            return truncate_history(left.join(right, columns), True)
        else:
            return left.join(right, columns)


def truncate_history(df, cache=False, storage=StorageLevel.MEMORY_ONLY):
    """Break Spark lineage to prevent overly complex query plans.
    
    Uses checkpoint() which truncates both Catalyst plan and RDD lineage,
    writing to reliable distributed storage (DBFS). Unlike localCheckpoint,
    this survives executor loss on shared/spot Databricks clusters.
    Requires spark.sparkContext.setCheckpointDir() to be called first.
    """
    if cache:
        truncated_df = df.checkpoint(eager=True)
    else:
        truncated_df = df.checkpoint(eager=False)
    return truncated_df


def checkpoint(
    df,
    base_dir="s3://memberanalytics-data-out/work/checkpoints",
    storage=StorageLevel.DISK_ONLY,
):
    start = time.time()
    dir = "{}/{}".format(base_dir, non_deterministic_hash(df))
    print("Checkpointing to: {}".format(dir))
    df.persist(storage)
    df.count()
    df.write.save(dir)
    df.unpersist()

    time.sleep(wait_time)

    ret = None
    for i in range(0, retry_times):
        if ret is None:
            try:
                ret = spark.read.load(dir)
            except:
                print(
                    "Reading from s3 failed.  Waiting {} seconds then retrying.  {} out of {} try.".format(
                        wait_time, i, retry_times
                    )
                )
                time.sleep(wait_time)

    runtime = time.time() - start
    print("Checkpoint overhead: {}s".format(runtime))
    return ret


def union_with_mismatched_columns(df1, df2):
    """
    Function that will correctly union two dataframes which have only a partial overlap in column names

    Parameters:
        df1: first df
        df2: second df
    Returns:
        unioned dataframe
    """
    df1_needs = [dtype for dtype in df2.dtypes if dtype not in df1.dtypes]
    df2_needs = [dtype for dtype in df1.dtypes if dtype not in df2.dtypes]

    full_cols = df1.columns + [
        col for col in df2.columns if col not in df1.columns
    ]

    df1_full = add_dtypes(df1, df1_needs).select(full_cols)
    df2_full = add_dtypes(df2, df2_needs).select(full_cols)
    return df1_full.union(df2_full)


def add_dtypes(df, df_needs):
    """
    This function takes a list of spark's dtypes, which is essentially a column name and type, and for each dtype
    creates a new constant null column of the correct type

    Parameters:
        df: a dataframe that will have columns added to it
        df_needs: a collection of dtypes that df needs
    """
    for dtype in df_needs:
        df = df.withColumn(dtype[0], lit(None).cast(dtype[1]))

    return df


def count_nulls(df, col, key=None):
    """
    Count the number of null values in <col>, optionally grouped by <key>.

    Parameters:
        df (spark dataframe)
        col: column to check for nulls
        key: column in df to group by

    Returns:
        int if key is None, else spark dataframe with columns key, nulls
    """
    df_notnull = df.filter(sqlf.col(col).isNotNull())

    if not key:
        return df.count() - df_notnull.count()

    tot = df.groupBy(key).agg(sqlf.count(key).alias("tot"))
    notnull = df_notnull.groupBy(key).agg(sqlf.count(key).alias("notnull"))

    stat = safe_join(tot, notnull, "left", key)

    stat = stat.fillna({"notnull": 0})
    stat = stat.withColumn("nulls", stat.tot - stat.notnull).select(
        key, "nulls"
    )
    return stat


def grouped_percentiles(df, key, col, pcts=[0.5]):
    """
    Calculate percentiles for <col>, optionally grouped by <key>.

    Parameters:
        df (spark dataframe)
        key: column in df to group by
        col: column to calculate percentiles for
        pcts: list of floats specifying the percentiles to calculate

    Return:
        spark dataframe with columns key, pct_<>, pct<> where pct_<> is a
        column per percentile requested, e.g. pcts=[0.5] has column pct_50
    """
    # handling columns that have hyphen in the name so that the whole
    # column name is evaluated instead of the subtraction of columns
    # with names being from the LHS and RHS of the hyphen
    if "-" in col:
        df = df.withColumn("__temp__", sqlf.col(col))
        col = "__temp__"
    grouped = df.groupBy(key)

    stats = None
    nms = []
    for p in pcts:
        nm = "pct_{}".format(str(int(p * 100)))
        nms.append(nm)
        expr = sqlf.expr("percentile({}, {})".format(col, str(p)))
        stat = grouped.agg(expr.alias(nm))
        if stats:
            stats = safe_join(stats, stat, "left", key)
        else:
            stats = stat
    return stats.select(key, *nms)


def pct_flagged(df, col, name, key=None):
    """
    Return the percent of flagged rows of col, by key, aliased as name. A flag
    is a value of 1.

    Parameters:
        df (pyspark.sql.DataFrame): df to check
        col (str): column to check (flagged rows have value 1)
        name (str): name of new pct column to output
        key (str:opt): optional key to group by
    Returns:
        (pyspark.sql.DataFrame): columns: (key), name
    """
    if key:
        dat = (
            df.groupBy(key)
            .agg((sqlf.sum(col) / sqlf.count(col)).alias(name))
            .select(key, name)
        )
    else:
        dat = df.agg((sqlf.sum(col) / sqlf.count(col)).alias(name)).select(
            name
        )
    return dat


def crosstab_pct(df, col, val, key=None):
    """
    Return the percent frequency cross tabulation of df against column, optionally grouped by key.

    Parameters:
        df (pyspark.sql.DataFrame): df containing col
        col (str): column to perform crosstabulation of
        val (str): value to feed into the counter
        key (str:opt): optional key to group by
    Returns:
        (pyspark.sql.DataFrame)

    Example:
      Input DF:
        key | col | val
        k1  | A   | 1
        k1  | B   | 1
        K1  | B   | 1
        K1  | B   | 1
        K2  | A   | 1
      Output DF:
        key | A    | B
        k1  | 0.25 | 0.75
        k2  | 1    | 0
    """
    if not key:
        tot = df.count()
        s = df.groupBy(col).agg(sqlf.count(val).alias("cnt"))
        s = s.withColumn("pct", sqlf.col("cnt") / tot)
        s = s.groupBy()  # required for pivot
    else:
        s = df.groupBy(key, col).agg(sqlf.count(val).alias("cnt"))
        s = s.withColumn(
            "tot", sqlf.sum("cnt").over(Window.partitionBy(key))
        ).withColumn("pct", sqlf.col("cnt") / sqlf.col("tot"))
        s = s.groupBy(key)

    s = s.pivot(col).sum("pct")
    s = s.fillna(0)
    return s


# TODO Move to not spark utils
def non_deterministic_hash(seed=""):
    return abs(hash(str(dt.now()) + str(seed)))
