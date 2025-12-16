from pyspark.sql.window import Window as W


def to_epoch(date_col):
    """
    Convert the date column to an epoch (cast as long)
    """
    return date_col.cast("timestamp").cast("long")


def weeks_to_seconds(weeks):
    """
    Produces a single number NOT a column  for use in window function
    """
    days_per_week = 7
    hours_per_day = 24
    minutes_per_hour = 60
    seconds_per_minute = 60

    final_scalar = (
        days_per_week * hours_per_day * minutes_per_hour * seconds_per_minute
    )

    return final_scalar * weeks


def create_window(win_partitions, ts_lb):
    lb_seconds = weeks_to_seconds(ts_lb)
    window = (
        W.partitionBy(win_partitions)
        .orderBy("EPOCH")
        .rangeBetween(-lb_seconds, 0)
    )
    return window
