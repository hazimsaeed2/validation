import warnings

from lib_assignment.assn_io import JobManager
from lib_assignment.assn_utils import env_path
from lib_assignment.input_checks import checker
from lib_assignment.validators import check_column_duplicates
# from pe_memberdna.lib.iotools import read_s3_to_local

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def check_dups_membership(job):
    """
    Checks MAIL_LIST file for duplicates

    Parameters:
        job (JobManager):
    Returns:
        [Check(), Check()]: list of checks
    """

    member_checks = []

    member_data = spark.read.csv(env_path(job.config.paths["MAIL_LIST"], job.vol_base, job.env), header=True).toPandas()
    member_data = member_data.astype("str")

    validators = [
        (
            "duplicate",
            check_column_duplicates(member_data, ["MBRSHP_NBR"]),
        ),
    ]

    name = validators[0][0]
    status, details = validators[0][1]
    obj = checker.Check(name, "input_mail_list", status, details)
    member_checks.append(obj)

    print("member_check: Completed")
    return member_checks


if __name__ == "__main__":
    job = JobManager("member_check", "member_check")
    checks = check_dups_membership(job)
    checker.print_summary(checks)
