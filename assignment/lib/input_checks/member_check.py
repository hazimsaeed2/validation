import warnings

from pemember_dna.pipelines.assignment.lib.assn_io import JobManager
from pemember_dna.pipelines.assignment.lib.input_checks import checker
from pemember_dna.pipelines.assignment.lib.validators import (
    check_column_duplicates,
)
from pemember_dna.pipelines.lib.iotools import read_s3_to_local


def check_dups_membership(job):
    """
    Checks MAIL_LIST file for duplicates

    Parameters:
        job (JobManager):
    Returns:
        [Check(), Check()]: list of checks
    """

    member_checks = []

    member_data = read_s3_to_local(job.config.paths["MAIL_LIST"])
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

    job.log.info("member_check: Completed")
    return member_checks


if __name__ == "__main__":
    job = JobManager("member_check", "member_check")
    checks = check_dups_membership(job)
    checker.print_summary(checks)
