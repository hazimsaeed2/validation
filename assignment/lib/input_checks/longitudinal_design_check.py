import os
import warnings

import pandas as pd

import pe_memberdna.assignment.lib.assn_io as assn_io
import pe_memberdna.assignment.lib.assn_utils as assn_utils
import pe_memberdna.assignment.lib.formatters as formatters
import pe_memberdna.assignment.lib.input_checks.checker as checker
import pe_memberdna.assignment.lib.input_checks.exceptions as excp
import pe_memberdna.assignment.lib.validators as validators
import pe_memberdna.lib.iotools as iotools


def check_longitudinal_design(job):
    """
    Checks cdsa files for mistakes in the longitudinal design.

    Parameters:
        job (JobManager):

    Returns:
        checks (list): list of completed checks
    """

    checks = []
    experiment_id = int(job.config.params["experiment"])
    job.data.read("cell", "CELL", readtype="local", filetype="csv")
    cells = job.data.tables["cell"]
    cells = formatters.format_cells(cells)

    current_cells = cells.query(f"experiment_id == '{experiment_id}'")

    status, details = validators.check_column_duplicates(
        current_cells[pd.notna(current_cells["longitudinal_id"])],
        ["longitudinal_id"],
    )
    checks.append(
        checker.Check(
            "check_longitudinal_id_duplicates", "cells.csv", status, details
        )
    )

    segments = set()
    for index, row in current_cells.dropna(subset=["segment_id"]).iterrows():
        segment_ids = assn_utils.decompose_segment(row.segment_id)
        segment_ids = [x for x in segment_ids if x is not None]
        if len(segment_ids) > 1 and not pd.isna(row.longitudinal_id):
            raise excp.AssignmentInputError(
                "Longitudinal cell {} cannot have composed segments"
                " assigned".format(row.cell_id)
            )

        segments.update(segment_ids)

    current_segments = {}
    for segment_id in sorted(segments):  # required for deterministic unit test
        seg_path = os.path.join(
            job.config.paths["SEGMENT_BANK"], f"{segment_id}.json"
        )
        segment = iotools.read_s3_to_local(seg_path, ftype="json")
        current_segments[segment_id] = segment

    longitudinal_cells = (
        current_cells[pd.notna(current_cells["longitudinal_id"])][
            ["cell_id", "segment_id"]
        ]
        .astype(str)
        .values.tolist()
    )

    for cell_id, segment_id in longitudinal_cells:
        segment = current_segments.get(segment_id)
        if (
            not segment
            or segment["filters"][0]["filter_type"] != "longitudinal"
        ):
            raise excp.AssignmentInputError(
                "Longitudinal cell {} does not have a longitudinal segment"
                " assigned".format(cell_id)
            )

    checks.append(
        checker.Check(
            "check_longitudinal_cells_segment_type",
            "cells.csv",
            status=True,
            details="All cell have valid longitudinal segments assigned",
        )
    )

    longitudinal_segments = [
        segment_id
        for segment_id, segment in current_segments.items()
        if segment["filters"][0]["filter_type"] == "longitudinal"
    ]
    incorrect_long_cells = current_cells[
        (current_cells["segment_id"].isin(longitudinal_segments))
        & (pd.isna(current_cells["longitudinal_id"]))
    ]

    if len(incorrect_long_cells) > 0:
        raise excp.AssignmentInputError(
            "Longitudinal cell {} has a longitudinal segment set, but no"
            " longitudinal id".format(incorrect_long_cells.cell_id.tolist())
        )
    else:
        checks.append(
            checker.Check(
                "check_longitudinal_segment_longitudinal_id",
                "cells.csv",
                status=True,
                details="All longitudinal segments have a longitudinal id",
            )
        )

    longitudinal_segments = current_cells[
        pd.notna(current_cells["longitudinal_id"])
    ].segment_id.tolist()

    for segment_id in longitudinal_segments:
        status, details = validators.check_segment_configuration(
            experiment_id, current_segments[segment_id], cells
        )
        checks.append(
            checker.Check(
                "check_segment_configuration",
                "{}.json".format(segment_id),
                status,
                details,
            )
        )

    job.data.read("handshakes", "HANDSHAKES", readtype="local", filetype="csv")
    handshakes = job.data.tables["handshakes"]
    handshakes = handshakes.astype("str")
    handshakes["cell_id"] = handshakes["cell_id"].astype(int)
    handshakes["comparison_id"] = handshakes["comparison_id"].astype(int)
    handshakes["comparison_base_flag"] = handshakes[
        "comparison_base_flag"
    ].astype(int)

    status, details = validators.check_longitudinal_handshakes(
        cells, handshakes, experiment_id
    )
    checks.append(
        checker.Check(
            "check_longitudinal_handshakes_consistency",
            "handshakes.csv and cells.csv",
            status,
            details,
        )
    )

    max_experiment_id = cells.experiment_id.max()
    current_long_ids = set(
        cells[cells.experiment_id == experiment_id].longitudinal_id.tolist()
    )
    bigger_exp_long_ids = set(
        cells[cells.experiment_id > experiment_id].longitudinal_id.tolist()
    )

    experiment_is_in_order = max_experiment_id == experiment_id or (
        current_long_ids.intersection(bigger_exp_long_ids) == set()
    )
    checks.append(
        checker.Check(
            "check_longitudinal_experiment_order",
            "cells.csv",
            experiment_is_in_order,
            "Experiments have been executed in the order they were created",
        )
    )

    if not experiment_is_in_order:
        warnings.warn(
            "There is at least one campaign with a greater experiment id which"
            " uses the current longitudinal ids",
            excp.AssignmentInputWarning,
        )

    return checks


if __name__ == "__main__":
    job = assn_io.JobManager(
        "check_longitudinal_design", "check_longitudinal_design"
    )
    longitudinal_checks = check_longitudinal_design(job)
    checker.print_summary(longitudinal_checks)
