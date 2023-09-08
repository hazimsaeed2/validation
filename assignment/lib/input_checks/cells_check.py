from pe_memberdna.assignment.lib.assn_io import JobManager
from pe_memberdna.assignment.lib.input_checks.checker import (
    Check,
    print_summary,
)
from pe_memberdna.assignment.lib.input_checks.exceptions import (
    AssignmentInputError,
)
from pe_memberdna.assignment.lib.schemas.cdsa_schemas import CELLS_SCHEMA
from pe_memberdna.assignment.lib.validators import (
    check_date_format,
    check_date_range,
    check_header,
)
from pe_memberdna.lib.iotools import read_s3_to_local


def check_cells_csv(job):
    """
    Checks cells.csv file for structural and functional validity

    Parameters:
        job (JobManager):
    Returns:
        [Check(), Check()]: list of checks
    """

    checks = []
    cells = read_s3_to_local(job.config.paths["CELL"])

    cells = cells.astype("str")

    current_experiment_cells = cells[
        cells["experiment_id"] == str(job.config.params["experiment"])
    ]
    if len(current_experiment_cells) == 0:
        raise AssignmentInputError(
            "The current experiment_id is not present" "in the cells.csv"
        )
    else:
        checks.append(Check("check_experiment_id", "cells.csv", True))

    cell_experiment_pairs = current_experiment_cells.groupby(
        ["cell_id", "experiment_id"]
    )

    is_entry_unique = len(current_experiment_cells) == len(
        cell_experiment_pairs.groups
    )
    if not is_entry_unique:
        raise AssignmentInputError(
            "Cell/Experiment pair duplicates"
            " in the cells.csv((cell_id, exp_id): times): {pairs}".format(
                pairs=",".join(
                    [
                        "%s: %sx" % (pair, len(row))
                        for pair, row in cell_experiment_pairs.groups.items()
                        if len(row) > 1
                    ]
                )
            )
        )
    else:
        checks.append(Check("check_duplicate_entries", "cells.csv", True))

    status, details = check_date_format(
        current_experiment_cells, ["cell_start", "cell_end", "inhome_date"]
    )
    checks.append(Check("check_date_format", "cells.csv", status, details))

    status, details = check_date_range(
        current_experiment_cells, "cell_start", "cell_end"
    )

    checks.append(
        Check("check_cell_start_before_cell_end", "cells.csv", status, details)
    )

    status, details = check_date_range(
        current_experiment_cells, "inhome_date", "cell_end"
    )

    checks.append(
        Check("check_inhome_before_cell_end", "cells.csv", status, details)
    )

    status, details = check_header(cells, CELLS_SCHEMA)
    checks.append(
        Check("check_header_cells.csv", "cells.csv", status, details)
    )
    job.log.info("check_cells_csv: Completed")

    return checks


if __name__ == "__main__":
    job = JobManager("check_cells_csv", "check_cells_csv")
    cells_checks = check_cells_csv(job)
    print_summary(cells_checks)
