try:
    pyspark
except NameError:
    import findspark
    findspark.init()


from pyspark.sql.functions import lit
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = (SparkConf().setAppName("combine_roanoke"))
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

core_dir = "s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/BBM17/"

roanoke_dir = core_dir + "BCG_BBM17_Roanoke.dat"
all_dir = core_dir + "BCG_All_Elig_Mail_BBM17_No_Roanoke.dat"
out_dir = core_dir + "BCG_BBM17_combined.dat"

roanoke = spark.read.option("header", "true").csv(roanoke_dir).cache()
all = spark.read.option("header", "true").csv(all_dir).cache()

roanoke.count()
all.count()

roanoke_schema_fixed = roanoke. \
    withColumn("score", lit("0.0")). \
    withColumn("decile", lit("0")). \
    select(all.columns)

combined = all.union(roanoke_schema_fixed)

if combined.count() == all.count() + roanoke_schema_fixed.count():
    combined.repartition(10).write.option("header", "true").csv(out_dir)
