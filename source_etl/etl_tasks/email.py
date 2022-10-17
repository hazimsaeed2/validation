# pyspark --executor-memory 7g --executor-cores 3 --num-executors 10000 --conf=spark.dynamicAllocation.enabled=false
import pyspark.sql.functions as F
from pyspark.sql.types import *
import logging

import memberdna.source_etl.utils.validations_ETL as validations

email_schema = StructType([
    StructField("MBRSHP_NBR", StringType(), True),
    StructField("MAIL_ID", StringType(), True),
    StructField("EMAIL_SUBJECT", StringType(), True),
    StructField("EMAIL_NAME", StringType(), True),
    StructField("FIRST_BOUNCE_DATE", DateType(), True),
    StructField("LAST_BOUNCE_DATE", DateType(), True),
    StructField("BOUNCE_TOTAL", IntegerType(), True),
    StructField("FIRST_SEND_DATE", DateType(), True),
    StructField("LAST_SEND_DATE", DateType(), True),
    StructField("SEND_TOTAL", IntegerType(), True),
    StructField("FIRST_OPEN_DATE", DateType(), True),
    StructField("LAST_OPEN_DATE", DateType(), True),
    StructField("OPEN_TOTAL", IntegerType(), True),
    StructField("FIRST_CLICK_DATE", DateType(), True),
    StructField("LAST_CLICK_DATE", DateType(), True),
    StructField("CLICK_TOTAL", IntegerType(), True),
    StructField("FIRST_UNSUB_DATE", DateType(), True),
    StructField("LAST_UNSUB_DATE", DateType(), True),
    StructField("UNSUB_TOTAL", IntegerType(), True),
])

select_cols_out = {'MBRSHP_NBR',
                  'MBRSHP_SID',
                  'MAIL_ID',
                  "EMAIL_SUBJECT",
                  "EMAIL_NAME",
                  'FIRST_BOUNCE_DATE',
                  'LAST_BOUNCE_DATE',
                  "BOUNCE_TOTAL",
                  "FIRST_SEND_DATE",
                  "LAST_SEND_DATE",
                  'SEND_TOTAL',
                  'FIRST_OPEN_DATE',
                  "LAST_OPEN_DATE",
                  "OPEN_TOTAL",
                  "FIRST_CLICK_DATE",
                  "LAST_CLICK_DATE",
                  "CLICK_TOTAL",
                  "FIRST_UNSUB_DATE",
                  "LAST_UNSUB_DATE",
                  "UNSUB_TOTAL",
                  "ID"
                  }

def load_email_data(spark, path, schema, config_validation):

    logging.info('Reading the input file ' + path)

    df = spark.read \
              .schema(schema) \
              .option("header", "false") \
              .option("sep", ",") \
              .option("quote", "\"") \
              .option("escape", '"') \
              .option("multiLine", "true") \
              .csv(path)

    validations.validate_table(
        spark,
        'source',
        'email',
        config_validation,
        df
        )

    # Filter out rows contains not valid number number
    df = df.where(F.length(F.col("MBRSHP_NBR")) == 11)

    return df

def main(spark, data_paths, config_validation):

    logging.info('Starting processing table email')

    source_path = data_paths['source']['email']
    email = load_email_data(spark, source_path, email_schema, config_validation)

    email = email.withColumn("ID", F.monotonically_increasing_id())

    member_extended = spark.read.parquet(data_paths['intermediate']['member_extended'])

    email = email.join(member_extended, "MBRSHP_NBR", "inner")\
                 .select(*select_cols_out)

    validations.validate_table(
        spark,
        'intermediate',
        'email',
        config_validation,
        email
        )

    logging.info('Saving the intermediate file ' + data_paths['intermediate']['email'])
    email.repartition("FIRST_SEND_DATE") \
         .write.parquet(\
         data_paths['intermediate']['email'],
         partitionBy="FIRST_SEND_DATE", mode='overwrite')
