"""
Subset input mail list to exclude the global control

This script is not being maintained as digital is on hold and provider is
changing.

To run, you must include the following the in the config (sample paths shown):
paths:
      DIGITAL_GLOBAL_CONTROL: 's3://memberanalytics-data-out/ASSIGNMENTS/campaigns/EMAIL/Email6/Email_global_control.csv'
      RAW_MAIL_FILE: 's3://memberanalytics-data-out/ASSIGNMENTS/campaigns/EMAIL/Email6/Emailable_010419_Mbr_SIDs.dat'
"""
import pyspark.sql.functions as sqlf

from assn_io import JobManager

job = JobManager("EmailPreProcess", "Email Assignment Pre Process")

job.data.read(
    "global_control",
    "DIGITAL_GLOBAL_CONTROL",
    filetype="csv",
    cols=["MBRSHP_SID"],
)
job.data.read("mail_file", "RAW_MAIL_FILE", filetype="csv")

tbls = job.data.tables
ctrl = tbls["global_control"].withColumn("control", sqlf.lit(1))
dat = tbls["mail_file"].join(ctrl, on="MBRSHP_SID", how="left_outer")
dat = dat.filter(dat.control.isNull()).drop("control")

dat.select(["MBRSHP_SID", "MBRSHP_NBR"]).coalesce(1).write.csv(
    job.config.paths["MAIL_LIST"], mode="overwrite", header=True
)

print("Done")
