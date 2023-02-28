import functools

import pandas
import pyspark.sql.functions as sqlf

import memberdna.pipelines.assignment.lib.input_checks.exceptions as excp

DATE_PATTERN = "^([1-9]|1[0-2])\/([1-9]|[12]\d|3[01])\/([12]\d{3})$"


def check_header(df, schema):
    """
    Takes a dataframe (pyspark/pandas) and a schema, check if the dataframe contains all the columns specified in the schema.
    If yes, proceed with following function calls, if not exit the program

    Parameters:

        df: the dataframe
        schema: the schema
    """
    if isinstance(df.columns, list):
        columns = df.columns
    else:
        columns = list(df.columns.values)
    if columns != schema.names:
        msg = "The headers of this file do not match the specified schema\n"
        if len(columns) != len(set(columns)):
            duplicated_headers = set(
                [x for x in columns if columns.count(x) > 1]
            )
            msg += "There are duplicated column header: {}".format(
                duplicated_headers
            )
            raise excp.AssignmentInputError(msg)

        sym_dif = set(columns).symmetric_difference(set(schema.names))
        if sym_dif:
            msg += "Symmetric difference between df and schema: \n {}".format(
                sym_dif
            )
        else:
            different_headers = [
                (i, j) for i, j in zip(columns, schema.names) if i != j
            ]
            msg += "The order of df header is different from the schema: {}".format(
                different_headers
            )
        raise excp.AssignmentInputError(msg)
    else:
        return (True, "The headers of this file match the specified schema")


def check_data_type_convertable(df, schema):
    """
    Takes a dataframe, subsets to the columns in the schema, casts each column to the specified schema.
    Pyspark cast will not raise error but will change the column to empty if it is unsure how to perform the cast
    If before the cast, the column is not completely empty but after the cast it is, then we know that the data type is not
    convertable using the specified schema

    Parameters:
        df: the dataframe that one wants to enforce the schema
        schema: the schema

    Output:
        the dataframe with the schema enforced
    """

    df = df.select(schema.names)
    total_record = df.count()
    for field in schema.fields:
        if (
            df.where(sqlf.col(field.name).isNull()).count() != total_record
        ):  # before cast, the column is not completely empty
            df = df.withColumn(
                field.name,
                sqlf.col(field.name).cast(field.dataType.simpleString()),
            )
            if (
                df.where(sqlf.col(field.name).isNull()).count() == total_record
            ):  # after cast, the column is completely empty
                msg = "{} column can not be converted into its specified schema, potential error in input data".format(
                    field.name
                )
                raise excp.AssignmentInputError(msg)
    return (True, "All columns can be converted to the specified schema")


def check_column_empty(df, column_names=None, ignore_empty_rows=False):
    """
    Check if any specified columns have empty values
    If check passed, proceed with following function calls, if not exit the program

    Parameters:

        df (pySpark DataFrame): a dataframe
        column_names (list[str]): a list of column names that one wants to check, if nothing passed into column_name, check every column
        ignore_empty_rows (boolean): whether to ignore or consider the empty rows

    """
    column_names = column_names or df.columns

    if ignore_empty_rows:
        df = df.dropna(how="all")

    if set([df[df[i].isNull()].count() for i in column_names]) != set([0]):
        msg = "there are missing values in this file\n"
        msg += "See above sample missing: "
        df.where(
            functools.reduce(
                lambda x, y: x | y,
                (sqlf.col(x).isNull() for x in column_names),
            )
        ).limit(10).show()
        raise excp.AssignmentInputError(msg)
    else:
        return (
            True,
            "There is no missing value in this file for the following columns: {}".format(
                column_names
            ),
        )


def check_date_format(df, column_names):
    """
    Check the date format of the the specified columns for the dataframe
    This function checks if the date format is like 1/3/2019 (aka. no leading
    zeros) for both pandas and pySpark Dataframe

    Parameters:

        df: a dataframe (pandas or pySpark)
        column_names: a list of column names that one wants to check

    """

    error_message = (
        "{} column does not have the correct dateformat,"
        " correct format should be like 1/3/2019"
    )

    df = df.dropna(how="all")

    if isinstance(df, pandas.DataFrame):
        total_dates = len(df)
        for column in column_names:
            valid_dates = len(df[df[column].str.match(DATE_PATTERN)])
            if valid_dates != total_dates:
                msg = error_message.format(column)
                raise excp.AssignmentInputError(msg)
    else:
        total_row = df.count()
        df = df.select(*column_names)
        for col_name in column_names:
            if (
                total_row
                != df.where(sqlf.col(col_name).rlike(DATE_PATTERN)).count()
            ):
                msg = error_message.format(col_name)
                raise excp.AssignmentInputError(msg)
    return (
        True,
        "All below columns have the correct date format"
        " (e.g. 1/3/2019): {}".format(column_names),
    )


def check_date_range(df, earlier_date_column, later_date_column):
    """
    Checks the date range for a pandas or pySpark dataframe, make sure the
     earlier_date column has date before or equals to later_date_column

    Parameters:

        df: a dataframe (pandas or pySpark)
        earlier_date_column: a column name
        later_date_column: a column name

    """
    df = df.dropna(how="all")

    if isinstance(df, pandas.DataFrame):
        ranges = df[[earlier_date_column, later_date_column]].apply(
            pandas.to_datetime, format="%m/%d/%Y"
        )
        num_wrong_date_range = len(
            ranges[ranges[earlier_date_column] > ranges[later_date_column]]
        )
    else:
        df = df.withColumn(
            earlier_date_column,
            sqlf.to_timestamp(sqlf.col(earlier_date_column), "MM/dd/yyyy"),
        ).withColumn(
            later_date_column,
            sqlf.to_timestamp(sqlf.col(later_date_column), "MM/dd/yyyy"),
        )

        num_wrong_date_range = (
            df.withColumn(
                "date_range_correct",
                sqlf.when(
                    sqlf.col(earlier_date_column)
                    <= sqlf.col(later_date_column),
                    sqlf.lit(0),
                ).otherwise(sqlf.lit(1)),
            )
            .agg(sqlf.sum("date_range_correct"))
            .collect()[0][0]
        )

    if num_wrong_date_range != 0:
        msg = "{} has date after {}".format(
            earlier_date_column, later_date_column
        )
        raise excp.AssignmentInputError(msg)
    else:
        return (
            True,
            "All dates in {} precedes {}".format(
                earlier_date_column, later_date_column
            ),
        )


def _compare_keys_between(df1, df2, key, compare="equal"):
    """
    Compare keys between 2 df's with a toggle of (1) them being the same or
    (2) df1 being a subset of df2 or (3) if a float is provided then at least
    that % of keys in df2 are in df1.

    Parameters:
        df (PySpark DataFrame): Dataframe containing id's
        df2 (PySpark DataFrame): Other dataframe containing id's
        key (str): name of the key in the first dataframe
        compare (str or int): "equal", "subset", integer ...
    Returns:
        True / False if the toggled definition is satisfied.
    """
    keys1 = df1.select(key).distinct()
    keys2 = df2.select(key).distinct()
    same = df1.join(df2, on=key, how="inner")

    key1_count = keys1.count()
    key2_count = keys2.count()
    same_count = same.count()

    if isinstance(compare, float):
        pct_shared = same_count / key2_count  # Yes, we're using Python3 !
        if pct_shared >= compare:
            return True
        return False

    if same_count != key1_count:
        return False
    if compare == "subset":
        return True
    elif key1_count == key2_count:
        return True
    return False


def check_price_in_article(price, article):
    """
    Check that all PMR Offer IDs in price input are in article coupons.

    Parameters:
        price(PySpark DataFrame):
        article(PySpark DataFrame):

    """
    # Discount data is only good to us if we have it for "enough" coupons
    MIN_PCT_MATCHED_CPNS = 0.75
    price = price.withColumnRenamed("PMR_Offer_ID", "PMR Offer ID")
    if _compare_keys_between(
        price, article, "PMR Offer ID", MIN_PCT_MATCHED_CPNS
    ):
        return (
            True,
            f"At least {MIN_PCT_MATCHED_CPNS} of cpns present in COUPON_PRICE_PATH",
        )

    raise excp.AssignmentInputError(
        "Not enough article coupons present in COUPON_PRICE_PATH file"
    )


def check_cap_in_lkup(cap, lookup):
    """
    Check that cap closure input and lookup files have the same and only the
    same PMR Offer IDs.

    Parameters:
        cap(PySpark DataFrame):
        lookup(PySpark DataFrame):
    """
    if _compare_keys_between(cap, lookup, "PMR Offer ID", "equal"):
        return (True, "All PMR_ID's in EXTRA_INFO_CLOSURE_COUPON_LOOKUP_PATH")

    raise excp.AssignmentInputError(
        "Not all PMR_ID's in EXTRA_INFO_CLOSURE_COUPON_LOOKUP_PATH"
    )


def check_column_name_value(df, col_name, col_value):
    """
    Check that there is a column of a specified named and the value equals
    a specified value for each element

    Parameters:
        df (PySpark DataFrame): Dataframe containing id's
        col_name (str): name of the column to check in df
        col_value (object): desired value that each element in df should equal
    """
    if col_name not in df.columns:
        raise excp.AssignmentInputError("{} is not in file".format(col_name))

    grouped = df.groupBy(col_name).count()
    has_val = grouped.filter(sqlf.col(col_name) == col_value).count() > 0

    if grouped.count() != 1:
        raise excp.AssignmentInputError(
            "There are {} distinct values for column {}".format(
                count, col_name
            )
        )

    if not has_val:
        raise excp.AssignmentInputError(
            "The value equals {} instead of {}".format(value, col_value)
        )

    return (True, "All values in {} equal {}".format(col_name, col_value))


def check_column_duplicates(df, column_names=None):
    """
    Check if duplicates are present in specified column(s)
    If check passed, proceed with following function calls, if not exit the program

    Parameters:

        df: a dataframe (pandas or pySpark)
        column_names (list[str]): a list of column names that one wants to check
            for duplicates, if nothing passed into column_name, check every column

    """
    column_names = column_names or df.columns

    df = df.dropna(how="all")

    if isinstance(df, pandas.DataFrame):
        has_duplicates = df.duplicated(subset=column_names).any()
    else:
        has_duplicates = df.count() > df.dropDuplicates(column_names).count()

    if has_duplicates:
        raise excp.AssignmentInputError(
            "Duplicate values are present in the following column(s): {}".format(
                column_names
            )
        )
    else:
        return (True, "No duplicates found")


def check_segment_configuration(experiment_id, segment, cells):
    """
    Checks that the segment has been configured correctly to be part of a
    longitudinal test.

    Parameters:
        experiment_id (int): the experiment for which the segments are checked
        segment (dict): the segment to be checked
        cells (pandas.DataFrame): the cells input

    Returns:
        (status, details) (tuple(bool, str)): the status of the check as
            passed of failed(True/False) and a details message.
    """
    longitudinal_filter = segment["filters"][0]
    filter_past_mbrs = longitudinal_filter["filter_parameters"].get(
        "filter_past_mbrs"
    )
    mbr_type = longitudinal_filter["filter_parameters"].get("mbr_type")

    try:
        segment_id = int(segment["segment_id"])
    except Exception:
        raise excp.AssignmentInputError(
            "Longitudinal multi segment is not support"
        )

    if mbr_type is None:
        raise excp.AssignmentInputError(
            f"mbr_type is not set for segment {segment_id}"
        )
    else:
        use_new_mbrs = mbr_type in ("new", "both")
        use_past_mbrs = mbr_type in ("past", "both")

    if not (use_past_mbrs or use_new_mbrs):
        raise excp.AssignmentInputError(
            "Please set mbr type to 'new', 'past', or 'both' in filter "
            "parameters"
        )

    if filter_past_mbrs is None and use_past_mbrs:
        raise excp.AssignmentInputError(
            "If using past mbrs, please set filter past mbrs to True or False"
        )

    if filter_past_mbrs and not use_past_mbrs:
        raise excp.AssignmentInputError(
            "If you want to filter past mbrs, please set mbr type to 'both' or"
            " 'past'"
        )

    current_cells = cells.query(f"experiment_id=={experiment_id}")
    past_cells = cells.query(f"experiment_id!={experiment_id}")

    longitudinal_ids = current_cells.query(f"segment_id=='{segment_id}'")[
        "longitudinal_id"
    ].unique()

    past_long_ids = past_cells[
        past_cells["longitudinal_id"].isin(longitudinal_ids)
    ]["longitudinal_id"].unique()

    if use_past_mbrs and len(past_long_ids) != len(longitudinal_ids):
        missing_long_ids = set(longitudinal_ids).difference(past_long_ids)
        raise excp.AssignmentInputError(
            f"The following longitudinal ids have not been used before, "
            f"but are set to load past mbrs: {missing_long_ids}"
        )

    elif not use_past_mbrs and len(past_long_ids) > 0:
        raise excp.AssignmentInputError(
            f"The following longitudinal ids have been used before, but "
            f"are not set to load past mbrs: {past_long_ids}"
        )

    status = True
    details = "Longitudinal settings are not missing or inconsistent"

    return status, details


def find_longitudinal_comparisons(longitudinal_cells, handshakes):
    """
    Checks if the handshakes for longitudinal cells are consistent with
    historical handshakes for those longitudinal ids.

    Parameters:
        longitudinal_cells (pandas.DataFrame): cell level campaign design
                                           information for longitudinal cells
        handshakes (pandas.DataFrame): comparison information for cell_ids

    Returns:
        longitudinal_comparison (pandas.DataFrame): lists longitudinal tests for
                                                each longitudinal control
    """
    longitudinal_handshakes = pandas.merge(
        longitudinal_cells, handshakes, on="cell_id", how="inner"
    )

    base_handshakes = longitudinal_handshakes.query("comparison_base_flag==1")
    test_handshakes = longitudinal_handshakes.query("comparison_base_flag==0")

    base_handshakes = base_handshakes.rename(
        columns={"longitudinal_id": "base_longitudinal_id"}
    )
    test_handshakes = test_handshakes.rename(
        columns={"longitudinal_id": "test_longitudinal_id"}
    )

    longitudinal_pairs = pandas.merge(
        base_handshakes[["comparison_id", "base_longitudinal_id"]],
        test_handshakes[["comparison_id", "test_longitudinal_id"]],
        on="comparison_id",
        how="outer",
    )

    longitudinal_comparisons = (
        longitudinal_pairs.groupby("base_longitudinal_id")[
            "test_longitudinal_id"
        ]
        .apply(set)
        .reset_index()
    )

    longitudinal_comparisons["test_longitudinal_id"] = (
        longitudinal_comparisons["test_longitudinal_id"]
        .apply(list)
        .apply(lambda x: str(sorted(x)))
    )
    longitudinal_comparisons = longitudinal_comparisons.reset_index()

    return longitudinal_comparisons


def check_longitudinal_handshakes(cells, handshakes, experiment_id):
    """
    Checks if the handshakes for longitudinal cells are consistent with
    historical handshakes for those longitudinal ids.

    Parameters:
        cells (pandas.DataFrame): cell level campaign design information
        handshakes (pandas.DataFrame): comparison information for cell_ids
        experiment_id (str): id for the current assignment campaign

    Returns:
        status (boolean): True if checks passed
        details (str): summary of checks passed
    """
    current_cells = cells.query(f"experiment_id=={experiment_id}")
    past_cells = cells.query(f"experiment_id!={experiment_id}")

    current_long_ids = current_cells[
        pandas.notna(current_cells["longitudinal_id"])
    ]["longitudinal_id"].tolist()

    relevant_long_ids = past_cells.query(
        f"longitudinal_id in {current_long_ids}"
    )["longitudinal_id"].tolist()

    current_long_cells = current_cells.query(
        f"longitudinal_id in {relevant_long_ids}"
    )
    current_long_comps = find_longitudinal_comparisons(
        current_long_cells, handshakes
    )

    past_long_cells = past_cells.query(
        f"longitudinal_id in {relevant_long_ids}"
    )
    past_long_comps = find_longitudinal_comparisons(
        past_long_cells, handshakes
    )

    past_long_comps = past_long_comps.rename(
        columns={"test_longitudinal_id": "past_test_long_ids"}
    )
    current_long_comps = current_long_comps.rename(
        columns={"test_longitudinal_id": "current_test_long_ids"}
    )
    longitudinal_comparisons = pandas.merge(
        past_long_comps,
        current_long_comps,
        on="base_longitudinal_id",
        how="outer",
    )

    inconsistent_longitudinal_handshakes = longitudinal_comparisons.query(
        "past_test_long_ids != current_test_long_ids"
    )
    if len(inconsistent_longitudinal_handshakes) > 0:
        long_ids_to_fix = (
            inconsistent_longitudinal_handshakes["base_longitudinal_id"]
            .astype(int)
            .unique()
            .tolist()
        )
        raise excp.AssignmentInputError(
            f"The following longitudinal_ids have different historical "
            f"handshakes than in the current campaign: {long_ids_to_fix}"
        )

    status = True
    details = "Longitudinal handshakes consistent with historical campaigns"

    return status, details
