"""
Output subset of columns for final mail file

This script is not being maintained as digital is on hold and provider is
changing.

To run, you must include the following the in the config (sample paths shown):
paths:
      DIGITAL_GLOBAL_CONTROL: 's3://memberanalytics-data-out/ASSIGNMENTS/campaigns/EMAIL/Email6/Email_global_control.csv'
      RAW_MAIL_FILE: 's3://memberanalytics-data-out/ASSIGNMENTS/campaigns/EMAIL/Email6/Emailable_010419_Mbr_SIDs.dat'
"""
import pyspark.sql.functions as sqlf

from assn_io import JobManager

job = JobManager("EmailPostProcess", "Email Assignment Post Process")

job.data.read(
    "assgn",
    "INPUT_ASSIGNMENTS",
    filetype="csv",
    cols=["mbrshp_sid", "cpn_nbr"],
)
job.data.read(
    "coupons",
    "CATEGORY_COUPON_PATH",
    filetype="csv",
    cols=["cpn_nbr", "cpn_desc"],
)
job.data.read("mbr", "RAW_MEMBER", cols=["MBRSHP_SID", "MBRSHP_NBR"])

tbls = job.data.tables
dat = tbls["assgn"].withColumnRenamed("mbrshp_sid", "MBRSHP_SID")
print(dat.count())
cpn = tbls["coupons"].dropDuplicates()
cpn = cpn.withColumn(
    "cpn_nbr",
    sqlf.regexp_replace(sqlf.col("cpn_nbr"), r"(?=\d)0(?=\d{5})", ""),
)
dat = dat.join(cpn, on="cpn_nbr")
print(dat.count())
dat = dat.join(tbls["mbr"], on="MBRSHP_SID")
dat = dat.drop("MBRSHP_SID")
print(dat.count())
dat = dat.withColumnRenamed("cpn_nbr", "coupon_code").withColumnRenamed(
    "cpn_desc", "coupon_description"
)

# dat = dat.head(10000)

dat.select(["MBRSHP_NBR", "coupon_code", "coupon_description"]).repartition(
    1
).write.csv(job.config.paths["EMAIL_MAIL_FILE"], mode="overwrite", header=True)

print("Done")
