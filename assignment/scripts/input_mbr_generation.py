import argparse
import findspark
findspark.init()
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = (SparkConf().setAppName("MMPC_mbr_input_file"))
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

parser = argparse.ArgumentParser(description="Update s3 paths.")
parser.add_argument("Rec_file_path", action="store", help="Latest file from scott")
parser.add_argument("DNA_path", action="store", help="Latest DNA dat")
parser.add_argument(
    "BBM11_purge_path", action="store", help="Default to be used"
)
parser.add_argument(
    "Output_path", action="store", help="output storage path"
)

args = parser.parse_args()

Rec_file_path = args.Rec_file_path
DNA_path = args.DNA_path
BBM11_purge_path = args.BBM11_purge_path
Output_path = args.Output_path


#read files in for process the input file for execution
mbr =spark.read.csv(Rec_file_path, header = True).withColumnRenamed('BBM Decile (Member DNA)', 'DECILE').withColumnRenamed('BBM Score (Member DNA)', 'score')
DNA = spark.read.parquet(DNA_path).select('mbrshp_sid', 'cpn_channel')

mbr.count()

mbr.dtypes

mbr.show(5)

mbr.groupBy('DECILE').count().show()

if 'EBT_FLAG' in mbr.columns :
	mbr.groupBy('EBT_FLAG').count().show()
else:
	pass


mbr.createOrReplaceTempView('mbr')

mbr = mbr.withColumn("FHH_IND", lit('N')) #Engine doesn't use this column but still call this column

mbr.select('MBRSHP_NBR').dropDuplicates().count() #check if duplicate from Scott is correct or not

mbr.select('MBRSHP_NBR').dropna().dropDuplicates().count()

mbr = mbr.dropna(subset='MBRSHP_NBR')

mbr.count()

#purge(LONGINITIDUNAL TEST)
BBM11_purge= spark.read.csv(BBM11_purge_path, header = True )#.select('MBRSHP_SID', 'MBRSHP_NBR')

mbr.join(BBM11_purge, 'MBRSHP_NBR').count() #CHECK IF 0, LONGITIDINAL COUNT SHOULD BE == 0

BBM11_purge.count()

mbr.join(BBM11_purge, 'MBRSHP_NBR', 'LEFT_ANTI').count() #left_anti ==> CHECK IF  count == mbr input

mbr.count()

mbr.dropDuplicates()

mbr.count()

mbr.groupBy('DECILE').count().show()

mbr_filtered = mbr.join(DNA, "MBRSHP_SID").filter(col("cpn_channel").isin("paper","dual","digital")).filter(col("DECILE").isin(1,2,3,4))

mbr_filtered.count()

mbr_filtered.groupBy("cpn_channel").count().show()

mbr_filtered.groupBy("DECILE").count().show()

mbr.count()

mbr.show()

mbr.dropDuplicates().repartition(10).write.option("header", "true").mode("overwrite").csv(Output_path)

mbr.show(10)
