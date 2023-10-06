import pyspark.sql.functions as sqlf

import pe_memberdna.assignment.lib.assn_io as assn_io
import pe_memberdna.assignment.lib.checks as checks


def main(conf_path_in=None):
    """
    Read two input files and combine those to one file.

    Parameters:
        conf_path_in: the path of the configuration file.

    Returns:
         None
    """
    name = "Generate Input Mail"
    job = assn_io.JobManager(name, name, conf_path_in)

    if job.config.params["run_type"].lower() == "prod":
        paths_to_check = [
            job.config.paths["MAIL_LIST"],
        ]
        checks.check_execution_overwrite(paths_to_check)

    job.log.info("reading input files...")

    job.data.read("bbm_scored", "BBM_SCORED", filetype="csv")
    job.data.read("muhh", "MUHH", filetype="csv")

    bbm_scored = job.data.tables["bbm_scored"]
    muhh = job.data.tables["muhh"]

    job.log.info("joining two tables...")
    muhh = muhh.select("MEMBERSHIP_ID").withColumnRenamed(
        "MEMBERSHIP_ID", "mbrshp_nbr"
    )

    input_sid = (
        muhh.join(
            bbm_scored.select(["mbrshp_sid", "mbrshp_nbr", "decile", "score"]),
            ["mbrshp_nbr"],
            "inner",
        )
        .withColumnRenamed("mbrshp_nbr", "MBRSHP_NBR")
        .withColumn("FHH_IND", sqlf.lit("N"))
    )

    job.log.info("writing the combined file...")

    job.data.add("input_sid", input_sid)

    job.data.write(
        "input_sid",
        "MAIL_LIST",
        mode="overwrite",
        singlefile=True,
        ftype="csv",
    )

    job.log.info("done")


if __name__ == "__main__":
    main()
