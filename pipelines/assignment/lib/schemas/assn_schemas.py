"""Assignment Schemas."""

from pyspark.sql.types import (
    StringType,
    StructField,
    LongType,
    StructType,
    DateType,
    DoubleType,
    BooleanType,
)


# output schemas
ASSIGNMENTS_SCHEMA = StructType(
    [
        StructField("mbrship_sid", LongType(), False),
        StructField("experiment_id", LongType(), True),
        StructField("cell_id", LongType(), True),
        StructField("slot_nbr", LongType(), True),
        StructField("construct", StringType(), True),
        StructField("bf_construct", StringType(), True),
        StructField("cpn_nbr", StringType(), True),
        StructField("pool_type", StringType(), True),
    ]
)


MAILFILE_SCHEMA = StructType([StructField("mbrshp_sid", LongType(), False)])


QCFILE_SCHEMA = StructType(
    [
        StructField("MBRSHP_SID", LongType(), False),
        StructField("LAST_FIFTY-TWO_WEEK_TRIPS", LongType(), True),
        StructField("DECILE", LongType(), True),
        StructField("LFIFTY-TWOW_SPEND_IN_STORE", DoubleType(), True),
        StructField("CPN_NBR", StringType(), True),
        StructField("CELL_ID", LongType(), True),
        StructField("CONSTRUCT", StringType(), True),
        StructField("SLOT_NBR", LongType(), True),
        StructField("VERSION", StringType(), True),
    ]
)


ASSIGNMENTS = {
    "assignments": ASSIGNMENTS_SCHEMA,
    "mailfile": MAILFILE_SCHEMA,
    "qcfile": QCFILE_SCHEMA,
}
