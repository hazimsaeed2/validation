from pyspark.sql.functions import lit, col, when, row_number
from pyspark.sql.window import Window

messages = spark.read.option("header","true").csv("s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/MMPC20/input_message_mail_1/message.csv").cache()

mail_subset = spark.read.option("header", "true").csv("s3://memberanalytics-data-out/ASSIGNMENTS/cdsa/assn_output/PROD/mail_subset-MMPC20-20181217_lambda30-category-estimate").cache()

mail_template = mail_subset.select(['MBRSHP_SID', 'EXPERIMENT_ID', 'CELL_ID', 'mail_flag']).\
    distinct().\
    withColumn("CONSTRUCT",lit("content")).\
    withColumn("slot_nbr",lit("11"))

message_template = messages.select(['MBRSHP_SID', 'message']).withColumnRenamed("message", "cpn_nbr").distinct()

assert message_template.select(['MBRSHP_SID', 'cpn_nbr']).count() == message_template.select(['MBRSHP_SID']).distinct().count()

hba_cell = ((col("cell_id") == lit(217)) & (col("slot_nbr") == lit(11)))
content_assignments = mail_template.join(message_template,"mbrshp_sid").\
    withColumn("cpn_nbr",when(hba_cell, lit("F")).otherwise(col("cpn_nbr"))).select(mail_subset.columns).cache()

content_assignments.filter("slot_nbr == 11").groupBy("cpn_nbr").count().show()
content_assignments.groupBy("cpn_nbr").count().show()

assert content_assignments.count()*10 == mail_subset.count()

all_mail_subset = mail_subset.union(content_assignments).cache()
all_mail_subset.filter("mbrshp_sid == 1128930").show()
all_mail_subset.filter("slot_nbr == 11").groupBy("cpn_nbr").count().show()

# cells with 10 hooks up to 4 category
cells_10_4 = [216, 220]


window = Window.partitionBy('MBRSHP_SID').orderBy(col("cpn_type").desc(), col("prev_slot_nbr").cast("long"))

reordered_cells = []

for cell_id in cells_10_4:
    cell = all_mail_subset.filter("cell_id == {}".format(cell_id)).\
        join(spark.read.option("header", "true").csv("s3://memberanalytics-data-out/ASSIGNMENTS/cdsa/coupons/PROD/coupon_bank"), 'cpn_nbr', 'left')
    cell.groupBy("slot_nbr", "cpn_type").count().orderBy(col("slot_nbr").cast("long"), col("cpn_type")).show()
    reordered_cell = cell.withColumnRenamed("slot_nbr", "prev_slot_nbr").withColumn('slot_nbr', row_number().over(window))
    reordered_cell.groupBy("slot_nbr", "cpn_type").count().orderBy(col("slot_nbr").cast("long"), col("cpn_type")).show()
    reordered_cells.append(reordered_cell.select(mail_subset.columns))

no_reorder_cells = all_mail_subset.filter(~col("cell_id").isin(cells_10_4)).select(mail_subset.columns)

final_mail_subset = reduce(lambda x,y: x.union(y), [no_reorder_cells] + reordered_cells)

final_mail_subset.filter("mbrshp_sid == 1128930").show()

final_mail_subset.groupBy("mbrshp_sid").count().groupby("count").count().show()

final_mail_subset.orderBy(col("mbrshp_sid"), col("slot_nbr").cast("long")).coalesce(1).write.mode("overwrite").\
    option("header", "true").csv("s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/MMPC20/mail_subset_content")
