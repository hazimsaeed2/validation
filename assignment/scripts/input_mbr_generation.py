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
mbr = (
        spark.read.csv(Rec_file_path, header=True)
        .withColumnRenamed("BBM Decile (Member DNA)", "DECILE")
        .withColumnRenamed("BBM Score (Member DNA)", "score")
    )
DNA = spark.read.parquet(DNA_path).select('mbrshp_sid', 'cpn_channel')

print(f"Count of the Eligible REC file : {mbr.count()}")

print(f"Checking dtypes of Eligible REC file : {mbr.dtypes}")

print("Showing top 5 records of REC file")
mbr.show(5)

print("mbr.groupBy('DECILE').count().show()")
mbr.groupBy('DECILE').count().show()

if 'EBT_FLAG' in mbr.columns :
	print("mbr.groupBy('EBT_FLAG').count().show()")
	mbr.groupBy('EBT_FLAG').count().show()
else:
	print("No EBT_FLAG for this campaign")

mbr.createOrReplaceTempView('mbr')

mbr = mbr.withColumn("FHH_IND", lit('N')) #Engine doesn't use this column but still call this column

print(f"check if duplicate from Scott is correct or not :{mbr.select('MBRSHP_NBR').dropDuplicates().count() }")

print(f"check after dropping na and duplicates : {mbr.select('MBRSHP_NBR').dropna().dropDuplicates().count()}")

mbr = mbr.dropna(subset='MBRSHP_NBR')

print(f"check count after dropping na and duplicates in mbr : {mbr.count()}")

BBM11_purge= spark.read.csv(BBM11_purge_path, header = True )

mbr.join(BBM11_purge, 'MBRSHP_NBR').count()

print(f"BBM11_purge.count() : {BBM11_purge.count()}")

print("left_anti ==> CHECK IF  count == mbr input")
print(mbr.join(BBM11_purge, 'MBRSHP_NBR', 'LEFT_ANTI').count())

print(f"mbr count after joining left_anti with BBM11_purge : {mbr.count()}")

mbr.dropDuplicates()

print(f"mbr count after dropping duplicates : {mbr.count()}")

print("mbr.groupBy('DECILE').count().show()")
mbr.groupBy('DECILE').count().show()

mbr_filtered = mbr.join(DNA, "MBRSHP_SID").filter(col("cpn_channel").isin("paper","dual","digital")).filter(col("DECILE").isin(1,2,3,4))

print(f"mbr filtered for P&D cpn_channel : {mbr_filtered.count()}")

print("mbr by cpn_channel")
mbr_filtered.groupBy("cpn_channel").count().show()

print("mbr by decile")
mbr_filtered.groupBy("DECILE").count().show()

print(f"mbr count : {mbr.count()}")

print(f"writing to output path : {Output_path}")
mbr.dropDuplicates().repartition(10).write.option("header", "true").mode("overwrite").csv(Output_path)

print("checking output data top 10 rows")
mbr.show(10)
