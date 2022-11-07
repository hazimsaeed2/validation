import warnings

from pe_member_dna.pipelines.assignment.lib.assn_io import JobManager
from pe_member_dna.pipelines.assignment.lib.input_checks.checker import (
    Check,
    print_summary,
)
from pe_member_dna.pipelines.assignment.lib.input_checks.exceptions import (
    AssignmentInputError,
    AssignmentInputWarning,
)
from pe_member_dna.pipelines.lib.iotools import (
    is_s3_file,
    is_s3_path,
    split_path_bucket_key,
)


def check_paths(job):
    """
    Checks the existence of the paths from .yml config file.
    Warnings are printed if the optional pre-calculated paths are different
    from the ones in the config.

    Parameters:
        job (JobManager):
    Returns:
        [Check(), Check()]: list of checks
    """

    checks = list()

    invalid_message = "Parameter {param} has invalid path assigned: {path}"

    for param, value in job.config.original_paths.items():
        if value:
            bucket, key = split_path_bucket_key(value)
            is_invalid_path = not (
                is_s3_path(bucket, key) or is_s3_file(bucket, key)
            )
        else:
            is_invalid_path = True
        if is_invalid_path and param not in [
            "INPUT_ASSIGNMENTS",
            "INPUT_CONSTRUCTS",
            "INPUT_MAILHOUSE",
            "MAIL_POPULATION_ASSIGNMENT",
        ]:
            raise AssignmentInputError(
                invalid_message.format(param=param, path=value)
            )

    checks.append(Check("paths_exist", job.config.cfg_path, status=True))

    not_assign_message = (
        "Pre-calculated path {precalculated} does not match"
        " optional input path from config {original}"
    )

    optional_precalculated = [
        ("INPUT_ASSIGNMENTS", "ASSIGNMENT_PATH"),
        ("INPUT_CONSTRUCTS", "CONSTRUCTS_PATH"),
        ("INPUT_MAILHOUSE", "MAILFILE"),
    ]

    if "BBM" in job.config.params["campaign"].upper():
        optional_precalculated.append(
            ("MAIL_POPULATION_ASSIGNMENT", "ASSIGNMENT_PATH")
        )
    elif "MMPC" in job.config.params["campaign"].upper():
        optional_precalculated.append(("MAIL_POPULATION_ASSIGNMENT", "SUBSET"))

    for original, precalculated in optional_precalculated:
        if job.config.paths[original] != job.config.paths[precalculated]:
            optional_matches_precalculated_status = False
            details = not_assign_message.format(
                original=job.config.paths[original],
                precalculated=job.config.paths[precalculated],
            )
            warnings.warn(details, AssignmentInputWarning)
        else:
            details = None
            optional_matches_precalculated_status = True

        checks.append(
            Check(
                "optional_matches_precalculated",
                original,
                optional_matches_precalculated_status,
                details,
            )
        )

    job.log.info("check_paths: Completed")

    return checks


if __name__ == "__main__":
    job = JobManager("check_paths", "check_paths")
    path_checks = check_paths(job)
    print_summary(path_checks)
