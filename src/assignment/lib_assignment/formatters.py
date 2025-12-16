import numpy as np


def format_cells(cells):
    """
    Convert cells columns to types that can be easily used in queries
    or filters.
    For example segment_id has to be a string with the ids as integers and not
    floats.

    Parameters:
        cells (pandas.DataFrame): input cells dataframe

    Returns:
        (pandas.DataFrame): with all column with a correct type
    """
    cells = cells.astype("str")

    cells["experiment_id"] = cells["experiment_id"].astype(int)
    cells["cell_id"] = cells["cell_id"].astype(int)
    cells["longitudinal_id"] = cells["longitudinal_id"].astype(float)

    cells["construct_id"] = cells["construct_id"].apply(
        lambda x: str(int(float(x))) if "." in x else x
    ).apply(
        lambda x: np.nan if x == 'nan' else x
    )
    cells["bf_construct_id"] = cells["bf_construct_id"].apply(
        lambda x: str(int(float(x))) if "." in x else x
    ).apply(
        lambda x: np.nan if x == 'nan' else x
    )
    cells["segment_id"] = cells["segment_id"].apply(
        lambda x: str(int(float(x))) if "." in x else x
    ).apply(
        lambda x: np.nan if x == 'nan' else x
    )

    return cells
