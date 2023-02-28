from pyspark.sql.types import (
    StringType,
    StructField,
    LongType,
    StructType,
    DateType,
    DoubleType,
    BooleanType,
)


INPUT_MAIL_FILE_SCHEMA = StructType(
    [
        StructField("MBRSHP_NBR", LongType(), False),
        StructField("MBRSHP_SID", LongType(), False),
        StructField("CellName", StringType(), False),
        StructField("score", DoubleType(), False),
        StructField("decile", LongType(), False),
        StructField("FHH_IND", StringType(), False),
    ]
)
