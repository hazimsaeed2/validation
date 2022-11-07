from pyspark.sql.functions import lit, col, when, row_number
from pyspark.sql.window import Window

messages = spark.read.option("header","true").csv("s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/MMPC1FY20/input_message_mail/").cache()

mail_subset = spark.read.option("header", "true").csv("s3://memberanalytics-data-out/ASSIGNMENTS/cdsa/assn_output/PROD/FY20MMPC1/run1_2018-12-19/mail_subset").cache()

mail_template = mail_subset.select(['MBRSHP_SID', 'EXPERIMENT_ID', 'CELL_ID', 'mail_flag']).\
    distinct().\
    withColumn("CONSTRUCT",lit("content")).\
    withColumn("slot_nbr",lit("11"))

message_template = messages.select(['MBRSHP_SID', 'message']).withColumnRenamed("message", "cpn_nbr").distinct()

assert message_template.select(['MBRSHP_SID', 'cpn_nbr']).count() == message_template.select(['MBRSHP_SID']).distinct().count()

content_assignments = mail_template.join(message_template,"mbrshp_sid").select(mail_subset.columns).cache()

content_assignments.filter("slot_nbr == 11").groupBy("cpn_nbr").count().show()
content_assignments.groupBy("cpn_nbr").count().show()

assert content_assignments.count()*10 == mail_subset.count()

all_mail_subset = mail_subset.union(content_assignments).cache()

all_mail_subset.filter("mbrshp_sid == 1128930").show()
all_mail_subset.filter("slot_nbr == 11").groupBy("cpn_nbr").count().show()


all_mail_subset.groupBy("mbrshp_sid").count().groupby("count").count().show()

all_mail_subset.orderBy(col("mbrshp_sid"), col("slot_nbr").cast("long")).coalesce(1).write.mode("overwrite").\
    option("header", "true").csv("s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/FY20/MMPC1/mail_subset_content")
