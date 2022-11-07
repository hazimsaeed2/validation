import logging

import pe_memberdna.etl.lib.validations_ETL as validations
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


def getDf(
    spark,
    source_path,
    config_validation,
    table_name,
    input_format,
    sep,
    header,
    quote,
):
    # will be dict (multiple paths) or str (one path) depending on source
    if isinstance(source_path, dict):
        source_path = list(source_path.values())
    else:
        source_path = [source_path]

    if input_format == "csv":
        # csv accepts lists
        logging.info("Reading CSV input file(s) " + str(source_path))
        df = spark.read.csv(source_path, header=header, sep=sep, quote=quote)

        validations.validate_table(
            spark, "source", table_name, config_validation, df
        )

    elif input_format == "parquet":
        # can't accept lists (at all) or arguments (with partitions)
        logging.info("Reading parquet input file(s) " + str(source_path))
        df = spark.read.parquet(source_path[0])
        for s in source_path[1:]:
            df = df.union(spark.read.parquet(s))

    logging.info("Processing the file")

    return df


def createSchemaParquet(
    spark,
    source_path,
    config_validation,
    table_name,
    cast_sql,
    dest_path,
    repartition_val,
    filter_cond="True",
    input_format="csv",
    sep="|",
    header=True,
    quote='"',
    del_dup=True,
):

    df = getDf(
        spark,
        source_path,
        config_validation,
        table_name,
        input_format,
        sep,
        header,
        quote,
    )

    df.registerTempTable("df")

    schema_df = spark.sql(cast_sql)

    schema_df = schema_df.filter(filter_cond)
    if del_dup:
        schema_df = schema_df.dropDuplicates()

    validations.validate_table(
        spark, "intermediate", table_name, config_validation, schema_df
    )

    logging.info("Saving the intermediate file " + dest_path)

    if isinstance(repartition_val, int):
        schema_df.repartition(repartition_val).write.parquet(
            dest_path, mode="overwrite"
        )
    else:
        schema_df.repartition(repartition_val).write.parquet(
            dest_path, partitionBy=repartition_val, mode="overwrite"
        )
