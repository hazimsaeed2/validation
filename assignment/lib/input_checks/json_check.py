"""
Spark job to perform json input checks.
"""
import json
import warnings

from pemember_dna.pipelines.assignment.lib.assn_io import JobManager
from pemember_dna.pipelines.assignment.lib.assn_utils import (
    decompose_construct,
    decompose_segment,
)
from pemember_dna.pipelines.assignment.lib.input_checks import (
    checker,
    exceptions,
)
from pemember_dna.pipelines.lib.iotools import read_s3_to_local


def converts_to_json(to_check, is_file=False):
    """
    Determine whether the given contents may be represented as a valid json
    object.

    Parameters:
        to_check (str): input string to check
        is_file (Bool:opt): indicator of whether to_check is a file containing
                         json to check or to_check is already the string to
                         be checked
    Returns:
        (boolean, str): Indicator of whether valid and error details if not
    """

    try:
        if is_file:
            _ = read_s3_to_local(to_check, ftype="json")
        else:
            _ = json.loads(to_check)
    except ValueError as e:
        return (False, e)
    return (True, None)


def name_matches_id(json_file):
    """
    Determine whether the json file name matches the id inside.

    Every construct and segment json is identified by both its file name and
    <>_id property. For example, construct 75 is named 75.json with
    construct_id:75.

    Parameters:
        json_file (str): path to json file to check
    Returns:
        (boolean, str): Indicator of whether id/name match and error details
        if not
    """
    nm = json_file.split(".json")[0].split("/")[-1]
    content = read_s3_to_local(json_file, ftype="json")
    if "construct_id" in content:
        key = "construct_id"
    elif "segment_id" in content:
        key = "segment_id"
    else:
        return (False, "File contains neither construct_id nor segment_id")
    identifier = str(content[key])
    if nm != identifier:
        return (
            False,
            "File name {} does not match content id {}".format(nm, identifier),
        )
    return (True, None)


def has_correct_static_data(to_check, is_file=False):
    """
    If the given contents use a static slot, does it use correct data?

    For use with construct json. The static slot function should only be used
    with 'dummy' data. Using a different type causes unexpected behavior not
    yet fully troubleshooted.

    Parameters:
        to_check (str): input string to check
        is_file (Bool:opt): indicator of whether to_check is a file containing
                         json to check or to_check is already the string to
                         be checked
    Returns:
        (boolean, str): Indicator of whether correct data is used and
        associated details. If the json does not call static slot, returns None
        as the first item.
    """
    if is_file:
        s = read_s3_to_local(to_check, ftype="json")
    else:
        s = json.loads(to_check)

    static_slot_present = False

    for slot in s["slots"]:
        if slot["slot_type"] == "static":
            data_type = slot["offer_data"]
            if data_type != "dummy":
                return (
                    False,
                    "{} data being used with static slot".format(data_type),
                )
            else:
                static_slot_present = True

    if not static_slot_present:
        return (None, "no static slot")
    else:
        return (True, "Static slot(s) is using dummy data")


def get_jsons(job):
    """
    Return json ids used for the given campaign, as specified in cells.csv.

    Parameters:
        job (JobManager):
    Returns:
        ((), ()): construct and segment json ids, respectively
        e.g. ((1,2,3), (5,6,7))
    """
    cells = read_s3_to_local(job.config.paths["CELL"])
    cells = cells.astype("str")
    cells = cells[
        cells["experiment_id"] == str(job.config.params["experiment"])
    ]

    constructs = []
    construct_pairs = cells[
        ["construct_id", "bf_construct_id"]
    ].values.tolist()
    for construct_pair in construct_pairs:
        for construct_id in construct_pair:
            construct_ids = decompose_construct(construct_id)
            construct_ids = [x for x in construct_ids if x is not None]
            constructs.extend(construct_ids)

    constructs = set(constructs)

    segments = []
    for segment_id in cells["segment_id"].drop_duplicates():
        segment_ids = decompose_segment(segment_id)
        segment_ids = [x for x in segment_ids if x is not None]
        segments.extend(segment_ids)

    segments = set(segments)

    return constructs, segments


def check_jsons(job):
    """
    Checks JSON files for structural and functional validity

    Parameters:
        job (JobManager):
    Returns:
        [Check(), Check()]: list of checks
    """

    constructs, segments = get_jsons(job)

    json_stack = {
        "constructs": {
            "desc_nm": "CONSTRUCTS/{fname}",
            "BANK": "CONSTRUCT_BANK",
            "ids": constructs,
            "checks": [
                (
                    "converts_to_json",
                    converts_to_json,
                    {"is_file": True},
                    exceptions.AssignmentInputError,
                ),
                (
                    "name_matches_id",
                    name_matches_id,
                    {},
                    exceptions.AssignmentInputError,
                ),
                (
                    "has_correct_static_data",
                    has_correct_static_data,
                    {"is_file": True},
                    exceptions.AssignmentInputWarning,
                ),
            ],
        },
        "segments": {
            "desc_nm": "SEGMENTS/{fname}",
            "BANK": "SEGMENT_BANK",
            "ids": segments,
            "checks": [
                (
                    "converts_to_json",
                    converts_to_json,
                    {"is_file": True},
                    exceptions.AssignmentInputError,
                ),
                (
                    "name_matches_id",
                    name_matches_id,
                    {},
                    exceptions.AssignmentInputError,
                ),
            ],
        },
    }

    checks = []

    for json_type, stack in json_stack.items():
        for identifier in stack["ids"]:
            fname = identifier + ".json"
            desc_nm = stack["desc_nm"].format(fname=fname)
            path = job.config.paths[stack["BANK"]] + fname
            for check_name, check_call, kwargs, exception in stack["checks"]:
                is_valid, details = check_call(path, **kwargs)
                checks.append(
                    checker.Check(check_name, desc_nm, is_valid, details)
                )
                if is_valid is False:
                    if issubclass(exception, Warning):
                        warnings.warn(
                            "{}: {}".format(desc_nm, details), exception
                        )
                    else:
                        raise exception("{}: {}".format(desc_nm, details))

    job.log.info("json_checks: Completed")
    return checks


if __name__ == "__main__":
    job = JobManager("json_check", "json_check")
    checks = check_jsons(job)
    checker.print_summary(checks)
