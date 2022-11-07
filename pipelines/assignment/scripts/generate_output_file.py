"""Spark job to generate the output file

Hardware Requirements:
- Recommend at least 6 r4 worker nodes to run
- Runtime: Approx.

TODO:
[] Convert trial calculation to member DNA
"""


# ---- Initiate Spark Context --- #
import pyspark.sql.functions as sqlf

from pe_member_dna.pipelines.assignment.lib.assn_io import JobManager
from pe_member_dna.pipelines.assignment.lib.assn_utils import (
    check_cpn_nbr_or_version,
    read_subset_and_cast,
    subset_by_time,
)
from pe_member_dna.pipelines.assignment.lib.checks import (
    check_execution_overwrite,
)


def main(conf_path_in=None):

    name = "MailFile"
    job = JobManager(name, name, conf_path_in)

    if job.config.params["run_type"].lower() == "prod":
        check_execution_overwrite(
            paths_to_check=[job.config.paths["MAILFILE"]]
        )

    job.log.info("1. Reading input data...")
    data = read_subset_and_cast(
        job.config.paths["MAIL_POPULATION_ASSIGNMENT"], "csv"
    )
    renames = data.columns
    for name in renames:
        data = data.withColumnRenamed(name, name.upper())
    cols = ["MBRSHP_NBR", "MBRSHP_SID"]
    mail_list = read_subset_and_cast(job.config.paths["MAIL_LIST"], "csv")
    if "MBRSHP_SID" in mail_list.columns:
        mbr_lkup = mail_list.select(*cols)
    else:
        mbr_lkup = read_subset_and_cast(
            job.config.paths["RAW_MEMBER"], "parquet", cols
        )

    if "MBRSHP_NBR" not in [x.upper() for x in data.columns]:
        data = data.join(mbr_lkup, "MBRSHP_SID", "left")

    cols = [
        "MBRSHP_SID",
        "LAST_FIFTY-TWO_WEEK_TRIPS",
        "LFIFTY-TWOW_SPEND_IN_STORE",
        "FISCAL_WEEK_END",
    ]
    cube = read_subset_and_cast(job.config.paths["CUBE"], "parquet", cols)
    cube = subset_by_time(
        cube, job.config.params["assignment_date"], "fiscal_week"
    )
    cube = cube.withColumn(
        "LAST_FIFTY-TWO_WEEK_TRIPS",
        sqlf.when(sqlf.col("LAST_FIFTY-TWO_WEEK_TRIPS").isNull(), 0).otherwise(
            sqlf.col("LAST_FIFTY-TWO_WEEK_TRIPS")
        ),
    )
    cube = cube.withColumn(
        "LFIFTY-TWOW_SPEND_IN_STORE",
        sqlf.when(
            sqlf.col("LFIFTY-TWOW_SPEND_IN_STORE").isNull(), 0
        ).otherwise(sqlf.col("LFIFTY-TWOW_SPEND_IN_STORE")),
    )

    if "bbm" in job.config.params["campaign"].lower():
        if job.config.paths.get("VERSION_MAP") is not None:
            job.data.read("version_map", "VERSION_MAP", filetype="csv")
            version_map = job.data.tables["version_map"]
            # if the cpn_nbr assigned is a letter - it is actually the version
            version_map = version_map.withColumnRenamed("CPN1", "CPN_NBR")
            data = data.join(version_map, "CPN_NBR", "left")
            data = data.withColumn(
                "VERSION",
                sqlf.when(
                    sqlf.col("VERSION").isNull(), sqlf.col("CPN_NBR")
                ).otherwise(sqlf.col("VERSION")),
            )
        else:
            data = data.withColumn("VERSION", sqlf.col("CPN_NBR"))
        data = check_cpn_nbr_or_version(data, "CPN_NBR")
    else:
        data = data.withColumn("VERSION", sqlf.lit(""))

    if "DECILE" not in data.columns:
        cols = ["MBRSHP_NBR", "decile"]
        job.data.read("decile", "MAIL_LIST", filetype="csv", cols=cols)
        dec = job.data.tables["decile"]
        data = data.join(dec, "MBRSHP_NBR", "left")
        data = data.withColumnRenamed("decile", "DECILE")

    job.log.info("2. Generate mailfile dataset...")
    mail = data.withColumn("slot", sqlf.concat(sqlf.lit("CPN"), data.SLOT_NBR))

    if "mail_flag" not in [x.lower() for x in mail.columns]:
        mail = mail.withColumn("mail_flag", sqlf.lit("1"))

    mail = mail.withColumn(
        "mail_flag",
        sqlf.when(sqlf.col("mail_flag") != 0, "CIRC").otherwise("NO MAIL"),
    )

    job.log.info(
        "Not Mailed members: {}".format(
            mail.filter("mail_flag == 'NO MAIL'").count()
        )
    )
    job.log.info(
        "Mailed members: {}".format(mail.filter("mail_flag == 'CIRC'").count())
    )

    mail = (
        mail.groupBy("MBRSHP_NBR", "MBRSHP_SID", "CELL_ID", "mail_flag")
        .pivot("slot")
        .agg(sqlf.first("CPN_NBR"))
    )
    mail = mail.join(cube, "MBRSHP_SID", "left")
    mail = mail.join(
        data.select("MBRSHP_SID", "VERSION", "DECILE").distinct(),
        "MBRSHP_SID",
        "left",
    )
    mail = mail.repartition(1)
    data = data.withColumn("SLOT_NBR", data.SLOT_NBR.cast("integer"))
    slot_list = sorted(
        data.select("SLOT_NBR").distinct().toPandas()["SLOT_NBR"]
    )
    slot_name = ["CPN" + str(s) for s in slot_list]

    col_list = (
        ["MBRSHP_NBR", "CELL_ID", "mail_flag", "VERSION"]
        + slot_name
        + [
            "MBRSHP_SID",
            "DECILE",
            "LAST_FIFTY-TWO_WEEK_TRIPS",
            "LFIFTY-TWOW_SPEND_IN_STORE",
        ]
    )
    mail = mail.select(col_list)

    # ---- Write Output ---- #
    job.log.info("3. Writing output...")

    job.log.info("writing mailfile...")
    job.data.add("final_mailhouse", mail)
    job.data.write(
        "final_mailhouse",
        "MAILFILE",
        mode="overwrite",
        singlefile=True,
        ftype="csv",
    )

    job.log.info("done")


if __name__ == "__main__":
    main()
