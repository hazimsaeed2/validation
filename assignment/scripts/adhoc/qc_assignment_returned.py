from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


mail_file = spark.read.format("csv").option("header", "true").\
    load("s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/FY20/MMPC1/final_mailhouse")

file = spark.read.option("header", "true").\
    csv("s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/FY20/MMPC1/qc_file")


# Whitespace delimited
def extract_data(s):
    pattern = re.compile(r"(\d+)(\d{6})\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+")
    match=pattern.match(s)
    return match.groups()

array_schema = StructType([
    StructField('mbrshp_nbr', StringType(), nullable=False),
    StructField('cpn1', StringType(), nullable=False),
    StructField('cpn2', StringType(), nullable=False),
    StructField('cpn3', StringType(), nullable=False),
    StructField('cpn4', StringType(), nullable=False),
    StructField('cpn5', StringType(), nullable=False),
    StructField('cpn6', StringType(), nullable=False),
    StructField('cpn7', StringType(), nullable=False)
])

myUdf = udf(lambda y: extract_data(y), array_schema)

parsed = file.select(myUdf('_c0').alias("cols")).\
    select("cols.mbrshp_nbr",
           "cols.cpn1",
           "cols.cpn2",
           "cols.cpn3",
           "cols.cpn4",
           "cols.cpn5",
           "cols.cpn6",
           "cols.cpn7").cache()
parsed.count()

selected_mail_file = mail_file.select(parsed.columns).cache()
selected_mail_file.count()

joined = parsed.join(selected_mail_file, parsed.columns).cache()
joined.count()

# Column Delimited


qc_col = map(lambda x: "OFFER"+str(x), range(1, 12))
our_col = map(lambda x: "CPN"+str(x), range(1, 12))

for i in range(1, 12):
    file = file.withColumnRenamed(qc_col[i-1], our_col[i-1])

check = mail_file.join(file, file.columns)

check.count() == file.count()
file.distinct().count() == file.count()