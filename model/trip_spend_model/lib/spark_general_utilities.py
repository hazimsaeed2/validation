# -*- coding: utf-8 -*-
"""
created on thu apr 12 11:23:20 2018

@author: skatteboe karoline
"""
import csv
import datetime

from pandas.tseries.holiday import USFederalHolidayCalendar
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
import pyspark.sql.functions as sqlf
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window as W

from pe_memberdna.lib.misc import get_max_fiscal_week


def column_to_array(data, column_name):
    """
    Converts DataFrame column to flat array

    Converts spefified column to flat array by iterating through the values and
    storing them in an array
    Args:
        data (DataFrame): customer cube
        column (string): path to input file.
    Returns:
        array (<array>)
    """
    return [i.column_name for i in data.collect()]


def create_independent_variables(
    data,
    low_frequency_visits_last_26_weeks_cutoff,
    high_frequency_visits_last_12_weeks_cutoff,
):
    """
    create independent variables from the data

    Creates the following variables:
        MEMBER_FREQUENCY_GROUP => if the person have visited less than "low_frequency_visits_last_26_weeks_cutoff"
        in the last 26 weeks it is classified as LOW, if more than "high_frequency_visits_last_12_weeks_cutoff"
        classified as HIGH, otherwise MEDIUM
        SPEND_IN_STORE_BY_TRIPS_LAST_TWENTY-SIX_WEEKS => spend in the last 26 weeks / number of trips in the last 26 weeks

    Args:
        data (DataFrame): customer cube
        low_frequency_visits_last_26_weeks_cutoff (int): numeric cutoff for visits
        high_frequency_visits_last_12_weeks_cutoff (int): numeric cutoff for visits
    Returns:
        data (DataFrame)
    """
    data = data.withColumn(
        "MEMBER_FREQUENCY_GROUP",
        sqlf.when(
            data["LAST_TWENTY-SIX_WEEK_TRIPS"]
            <= low_frequency_visits_last_26_weeks_cutoff,
            "LOW",
        )
        .when(
            data["LAST_TWELVE_WEEK_TRIPS"]
            >= high_frequency_visits_last_12_weeks_cutoff,
            "HIGH",
        )
        .otherwise("MEDIUM"),
    )
    data = data.withColumn(
        "SPEND_IN_STORE_BY_TRIPS_LAST_TWENTY-SIX_WEEKS",
        sqlf.when(data["LAST_TWENTY-SIX_WEEK_TRIPS"] == 0, 0).otherwise(
            (data["LAST_TWENTY-SIX_WEEK_SPEND"] * 1.1)
            / data["LAST_TWENTY-SIX_WEEK_TRIPS"]
        ),
    )
    return data


def create_dependent_variables(data, start_windows, increment, model_type):
    """
    Create a set of dependent variables

    Loops through the window of potental start window's, calls the
    function to create the dependent variable based on the model type
    Args:
        data (DataFrame): customer cube
        start_windows (array<Int>): array with start week window
        increment (Int): days of weeks in BBM window
        model_type (String): column header dependent variable
    Returns:
        data (DataFrame)
    """
    for start in start_windows:
        start = start - 1
        end = start + increment - 1
        print(start)
        print(end)
        if model_type == "bin":
            header = "will_visit_from_%s_%s" % (start, end)
            data = create_dependent_binary_variables(data, start, end, header)
        if model_type == "cont":
            header = "spend_from_%s_%s" % (start, end)
            data = create_dependent_continious_variables(
                data, start, end, header
            )
    return data


def create_dependent_binary_variables(
    data, window_start, window_end, variable_name
):
    """
    Create binary dependent variables from the data

    Creates dependent variable by summing over the given window for each member and
    counting the number of trips made in the time window. The binary dependent variable is
    1 if the member has made at least trip and 0 otherwise
    Args:
        data (DataFrame): customer cube
        window_start (Int): start of week window
        window_end (Int): end of week window
        variable_name (String): column header dependent variable
    Returns:
        data (DataFrame)
    """
    w = (
        W.partitionBy("MBRSHP_SID")
        .orderBy(sqlf.col("FISCAL_WEEK_END"))
        .rowsBetween(window_start, window_end)
    )
    data = data.na.fill(0)
    data = data.withColumn("temp", sqlf.sum("WEEK_TRIPS").over(w))
    data = data.withColumn(
        variable_name, sqlf.when(sqlf.col("temp") >= 1, 1).otherwise(0)
    )
    data = data.drop("temp")
    return data


def create_dependent_continious_variables(
    data, window_start, window_end, variable_name
):
    """
    Create continious dependent variables from the data

    Creates dependent variable by the spend over the given window for each member.
    Args:
        data (DataFrame): customer cube
        window_start (Int): start of week window
        window_end (Int): end of week window
        variable_name (String): column header dependent variable
    Returns:
        data (DataFrame)
    """
    w = (
        W.partitionBy("MBRSHP_SID")
        .orderBy("FISCAL_WEEK_END")
        .rowsBetween(window_start, window_end)
    )
    data = data.withColumn(
        variable_name, sqlf.sum("FW_SPEND_IN_STORE").over(w)
    )
    data = data.na.fill(0)
    return data


def get_features_list(features):
    """
    Transform dataframe into two arrays of categorical and continious features

    Loops through each column and collects the values in an array.
    Filter the array for missing values
    Args:
        features (DataFrame): customer cube
    Returns:
        continious_features (array<String>)
        categorical_features (array<String>)
    """
    continious_features = [i.continuous_features for i in features.collect()]
    categorical_features = [i.categorical_features for i in features.collect()]
    categorical_features = [j for j in categorical_features if j]
    continious_features = [j for j in continious_features if j]
    return categorical_features, continious_features


def filter_customer_cube(
    data,
    last_fiscal_week_for_training,
    first_fiscal_week_for_training,
    bbm_creation_dates,
    start_windows,
    bbm_week_window,
    cube_path,
):
    """
    Transform dataframe into two arrays of categorical and continious features

    Loops through each column and collects the values in an array.
    Filter the array for missing values
    Args:
        data (DataFrame): customer cube
        last_fiscal_week_for_training (String): string representing the upper date limit
        for the data
        first_fiscal_week_for_training (String): string representing the lower date limit
        for the data
        bbm_dates (array<String>): List of dates where BBM was sent out
        bbm_creation_dates (array<String>): List of dates where BBM was created
        start_windows (array<Int>): possible weeks from assignment to start of campaign
        bbm_week_window (Int): length of campaign in weeks
        cube_path (String): path to customer cube data
    Returns:
        data (DataFrame)
    """
    last_training_date = datetime.datetime.strptime(
        last_fiscal_week_for_training, "%Y-%m-%d"
    )
    last_cube_date_str = get_max_fiscal_week(cube_path)
    last_cube_date = datetime.datetime.strptime(last_cube_date_str, "%Y-%m-%d")
    last_fiscal_week_date = min(last_training_date, last_cube_date)
    max_weeks_till_bbm_end = max(start_windows) + bbm_week_window - 2
    assignment_bbm_time_diff = datetime.timedelta(weeks=max_weeks_till_bbm_end)
    last_fiscal_week_for_assignment = (
        last_fiscal_week_date - assignment_bbm_time_diff
    )
    last_fiscal_week_for_assignment = datetime.datetime.strftime(
        last_fiscal_week_for_assignment, "%Y-%m-%d"
    )

    data = data.filter(data.FISCAL_WEEK_END <= last_fiscal_week_for_assignment)
    data = data.filter(data.FISCAL_WEEK_END >= first_fiscal_week_for_training)
    data = data.filter(data.FISCAL_WEEK_END.isin(bbm_creation_dates))
    data = data.filter(data.TENURE >= 0)
    data = data.filter(data.TENURE_GROUP != "expired")
    data = data.filter(sqlf.col("LAST_FIFTY-TWO_WEEK_TRIPS") != 0)
    # data = data.filter((data.MEMBER_TYPE != 5) & (data.MEMBER_TYPE != 8))
    return data


def filter_for_prediction(data, predictions):
    """
    Filter cube based on predicted value of making a trip

    filter predictions for less than 0.05 and more than 0.95. Then inner join with
    customer data to filter out customers that are outside the range.
    Args:
        data (DataFrame): customer cube
        predictions (DataFrame): dataframe with member ids and prediction proabilities
    Returns:
        data (DataFrame)
    """
    predictions = predictions.filter(
        (sqlf.col("PROBABILITY_OF_TRIP") > 0.10)
        & (sqlf.col("PROBABILITY_OF_TRIP") < 0.95)
    )
    data = predictions.join(data, ["MBRSHP_SID"], "inner")
    return data


def remove_outliers(data, column):
    """
    Remove outliers above 95th percentile

    Calculate percentiles of given column. Remove rows that are ourside the 95th percentile
    Args:
        data (DataFrame): customer cube
        column (string): column header of column to filter for.
    Returns:
        data (DataFrame)
    """
    quantile = data.approxQuantile(column, [0.05, 0.95], 0.01)
    data = data.filter(
        (sqlf.col(column) >= quantile[0]) & (sqlf.col(column) <= quantile[1])
    )
    return data


def prepare_data(data, categorical_variables, continuous_variables):
    """
    Transforms input data to to correct format for Spark ML using Pipelines

    Transforms data, by one-hot encoding categorical variables, then transforming using
    Spark ML pipelines and VectorIndexer.
    Args:
        data (DataFrame): customer cube
        categorical_variables (array<String>): column headers of categorical variables
        continious_variables (array<String>): column headers of continous variables
    Returns:
        data (DataFrame)
    """
    categorical_variables_out = [
        string + "_tmp" for string in categorical_variables
    ]

    indexers = [
        StringIndexer(
            inputCol=column, outputCol=column + "_tmp", handleInvalid="keep"
        )
        for column in categorical_variables
    ]
    cols_now = continuous_variables + categorical_variables_out
    assembler_features = VectorAssembler(
        inputCols=cols_now, outputCol="features"
    )

    transformaton_stages = indexers + [assembler_features]
    pipeline = Pipeline(stages=transformaton_stages)
    return pipeline.fit(data).transform(data)


def prepare_data_for_regression(data, features):
    data = data.drop("features")
    assembler_features = VectorAssembler(
        inputCols=features, outputCol="features"
    )
    pipeline = Pipeline(stages=[assembler_features])
    data = pipeline.fit(data).transform(data)
    return data


def prepare_data_string_indexer(
    data, categorical_variables, continuous_variables
):
    """
    Transforms input data to to correct format for Spark ML using Pipelines

    Transforms data, by one-hot encoding categorical variables, then transforming using
    Spark ML pipelines and VectorIndexer.
    Args:
        data (DataFrame): customer cube
        categorical_variables (array<String>): column headers of categorical variables
        continious_variables (array<String>): column headers of continous variables
    Returns:
        data (DataFrame)
    """
    categorical_variables_out = [
        string + "_tmp" for string in categorical_variables
    ]

    indexers = [
        StringIndexer(
            inputCol=column, outputCol=column + "_tmp", handleInvalid="keep"
        )
        for column in categorical_variables
    ]
    cols_now = continuous_variables + categorical_variables_out
    assembler_features = VectorAssembler(
        inputCols=cols_now, outputCol="features"
    )
    transformaton_stages = indexers + [assembler_features]
    pipeline = Pipeline(stages=transformaton_stages)
    return pipeline.fit(data).transform(data)


def write_list_to_file(path, input_list):
    """
    Write python list to .txt

    Write python list to .txt by iterating through elements and appending them
    to output file. Overwrites if the file already exists.
    Args:
        path (String): path of output file
        list (arrray): array of elements to write to file
    Returns:
    """
    with open(path, "w") as output:
        output.write(str(input_list))
    return


def string_index_column(data, column):
    indexer = StringIndexer(
        inputCol=column, outputCol=column + "_tmp", handleInvalid="keep"
    ).fit(data)
    return indexer.transform(data)


def test_and_train_to_pandas(
    data, features, dependent_variable_column, split_rate, sample_rate
):
    data = data.sample(False, sample_rate)

    training_data, testing_data = data.randomSplit(
        [split_rate, 1 - split_rate], seed=0
    )

    categorical_features, continious_features = get_features_list(features)
    categorical_features = [string + "_tmp" for string in categorical_features]
    columns = (
        categorical_features
        + continious_features
        + [dependent_variable_column]
    )
    training = training_data.select(*columns).toPandas()
    testing = testing_data.select(*columns).toPandas()
    return training, testing, columns


def write_list_to_csv(path, input_list, headers):
    """
    Write python list to .csv

    Write python list to .csv by iterating through elements and appending them
    to output file. Overwrites if the file already exists.
    Args:
        path (String): path of output file
        list (arrray): array of elements to write to file
        headers (array<string>): array of header strings
    Returns:
    """

    with open(path, "w") as output:
        file_writer = csv.writer(
            output, delimiter=",", quoting=csv.QUOTE_MINIMAL
        )
        file_writer.writerow(headers)
        # for row in zip(*input_list):
        # file_writer.writerow(row)
        file_writer.writerows(input_list)
    return


def transform_segments(segments):
    """
    Clean up segments input file

    Select relevant columns.
    Removes leading ' from date column. Casting column to datetype.
    Args:
        segments (DataFrame): path of output file
    Returns:
        segmenst (DataFrame)
    """
    segments = segments.select(
        "MBRSHP_SID",
        "TNRD_SGMNT",
        "FRST_YR_SGMNT",
        "TNRD_DCLNR_SGMNT",
        "FRST_YR_DCLNR_SGMNT",
        "EFF_DT",
    )
    segments = segments.withColumn("EFF_DT", sqlf.col("EFF_DT")[2:10])
    segments = segments.withColumn(
        "EFF_DT", sqlf.to_date(segments.EFF_DT, "yyyy-MM-dd")
    )
    segments = segments.na.fill(0)
    return segments


def find_customer_segments(data, segments):
    """
    Add field for customer segment

    Subset customer cube for membership ID and week start. Join with segments.
    filter entries where the segments have expired. Join back with cube based on
    id and date to get the segment the customer was in, in the iven week

    Args:
        data (DataFrame): customer cube
        segments_filter (arrray<int>): array of segments to filter for
    Returns:
        data (DataFrame)
    """
    weeks_in_data = data.select("MBRSHP_SID", "FISCAL_WEEK_START")
    segments = segments.join(weeks_in_data, ["MBRSHP_SID"])
    segment_groups = ["TNRD_SGMNT", "FRST_YR_SGMNT"]
    segments = segments.filter(
        sqlf.col("FISCAL_WEEK_START") >= sqlf.col("EFF_DT")
    )
    w = W.partitionBy("MBRSHP_SID").orderBy(sqlf.col("EFF_DT").desc())
    segments = (
        segments.withColumn("rn", sqlf.row_number().over(w))
        .where(sqlf.col("rn") == 1)
        .drop(sqlf.col("rn"))
    )
    segments = segments.withColumn("SEGMENT", greatest(*segment_groups))
    segments = segments.select("SEGMENT", "MBRSHP_SID")
    data = data.join(segments, ["MBRSHP_SID"], "left_outer")
    return data


def filter_customers_in_segment(data, segments, segments_filter):
    """
    Filter customer cube based on member segment

    Args:
        data (DataFrame): customer cube
        segments_filter (arrray<int>): array of segments to filter for
    Returns:
        data (DataFrame)
    """
    data = data.where(data.SEGMENT.isin(segments_filter))
    return data


def subset_oberservations(data, number_of_observations):
    """
    Select n rows from each member in cube

    Add column for row number, order by random number. Group by each member and select
    one row.
    Args:
        data (DataFrame): customer cube
        number_of_observations (int): number of obeservations to select from each member
    Returns:
        data (DataFrame)
    """
    subset_data = (
        data.withColumn("rnd_", sqlf.rand())  # Add random numbers column
        .withColumn(
            "rn_",
            sqlf.row_number().over(
                W.partitionBy("MBRSHP_SID").orderBy(sqlf.rand())
            ),
        )
        .where(
            sqlf.col("rn_") <= number_of_observations
        )  # Take n observations
        .drop("rn_")  # drop helper columns
        .drop("rnd_")
    )
    return subset_data


def transform_membership_price_column(data):
    """
    Cast price column from string to double

    Args:
        data (DataFrame): customer cube
    Returns:
        data (DataFrame)
    """
    data = data.withColumn(
        "MEMBERSHIP_PRICE", sqlf.col("MEMBERSHIP_PRICE").cast("double")
    )
    return data


def join_cubes(cube, additional_variable_cube):
    """
    Join most recent cube to legacy cube for additional features
    (should be removed when final stable cube has all features)

    Filter additional cube for needed fields. Join master cube to
    additional cube on fiscal week and member id.
    Args:
        data (DataFrame): customer cube
        cube (DataFrame): number of obeservations to select from each member
    Returns:
        data (DataFrame)
    """
    additional_variable_cube = additional_variable_cube.select(
        "MBRSHP_SID", "L52W_STDEV", "FISCAL_WEEK_END"
    )
    cube = cube.join(
        additional_variable_cube,
        ["FISCAL_WEEK_END", "MBRSHP_SID"],
        "left_outer",
    )
    return cube


def join_preferred_cube(cube, additional_variable_cube):
    """
    Join most recent cube to legacy cube for additional features
    (should be removed when final stable cube has all features)

    Filter additional cube for needed fields. Join master cube to
    additional cube on fiscal week and member id.
    Args:
        data (DataFrame): customer cube
        cube (DataFrame): number of obeservations to select from each member
    Returns:
        data (DataFrame)
    """
    additional_variable_cube = additional_variable_cube.select(
        "MBRSHP_SID",
        "L52W_PERCENT_TRIPS_PREFERRED_CLUB",
        "FISCAL_WEEK_END",
        "L52W_PREFERRED_CLUB_TRIPS",
        "PREFERRED_CLUB_HAS_GAS",
    )
    cube = cube.join(
        additional_variable_cube,
        ["FISCAL_WEEK_END", "MBRSHP_SID"],
        "left_outer",
    )
    return cube


def join_tenure_cube(cube, additional_variable_cube):
    """
    Join most recent cube to legacy cube for additional features
    (should be removed when final stable cube has all features)

    Filter additional cube for needed fields. Join master cube to
    additional cube on fiscal week and member id.
    Args:
        data (DataFrame): customer cube
        cube (DataFrame): number of obeservations to select from each member
    Returns:
        data (DataFrame)
    """
    additional_variable_cube = additional_variable_cube.select(
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        "L52W_PERCENT_TRIPS_PREFERRED_CLUB",
        "PREFERRED_CLUB_HAS_GAS",
    )
    cube = cube.join(
        additional_variable_cube,
        ["FISCAL_WEEK_END", "MBRSHP_SID"],
        "left_outer",
    )
    return cube


def join_category_cubes(cube, category_cube):
    """
    Join most recent cube to legacy cube for additional features
    (should be removed when final stable cube has all features)

    Filter additional cube for needed fields. Join master cube to
    additional cube on fiscal week and member id.
    Args:
        data (DataFrame): customer cube
        cube (DataFrame): number of obeservations to select from each member
    Returns:
        data (DataFrame)
    """
    category_cube = category_cube.drop(
        "FISCAL_WEEK_START",
        "FISCAL_L4W_END",
        "FISCAL_L8W_END",
        "FISCAL_L12W_END",
        "FISCAL_L26W_END",
        "FISCAL_L52W_END",
    )
    cube = cube.join(
        category_cube, ["FISCAL_WEEK_END", "MBRSHP_SID"], "left_outer"
    )
    return cube


def create_seasonality_fields(data):
    """
    Add seasonality field

    Add field for month and week of year based on fscal week start
    Args:
        data (DataFrame): customer cube
    Returns:
        data (DataFrame)
    """
    data = data.withColumn(
        "WEEK_OF_YEAR", sqlf.weekofyear(data.FISCAL_WEEK_END)
    )
    data = data.withColumn("MONTH", sqlf.month(data.FISCAL_WEEK_END))
    return data


def get_features(data):
    """
    Get a list of features from each file

    Loop through feature types and get the feature name wthn each category
    Args:
        data (DataFrame): transformed customer cube
    Returns:
        features (Array<string>)
    """
    feature_types = data.schema["features"].metadata["ml_attr"]["attrs"]
    features = []
    for feature_type in feature_types:
        features += data.schema["features"].metadata["ml_attr"]["attrs"][
            feature_type
        ]
    return features


def range_in_holiday(start_date, holidays):
    """
    Check if range is US holiday

    Add the 4 week inclusive BBM window to the end of the current week, 6 weeks out of the current timeframe
    Loop through each day and check if the date is withn period

    Args:
        start_date (DateTime): fiscal week end, start date to test for
        holidays (array<DateTime>): list of US holidays for time period
    Returns:
        is_holday (boolean)
    """
    days_to_add = list(range(35, 63))
    for day in days_to_add:
        if sqlf.date_add(start_date, day) in holidays:
            return True
    return False


def is_holiday(data, first_fiscal_week, last_fiscal_week):
    """
    Check if date is US holiday

    Find US holidays in the training period. Create variable checking if each fiscal week's BBM period
    has a holiday in the window.

    Args:
        first_fiscal_week (DateTime): lower bound on training data
        last_fiscal_week (DateTime): upper bound on training data
        data (DataFrame): customer cube
    Returns:
        data (DataFrame): updated customer cube
    """
    range_in_holiday_udf = sqlf.udf(range_in_holiday, BooleanType())
    cal = USFederalHolidayCalendar()
    holidays = cal.holidays(
        start=first_fiscal_week, end=last_fiscal_week
    ).to_pydatetime()
    data = data.withColumn(
        "HOLIDAY", range_in_holiday_udf(sqlf.col("FISCAL_WEEK_END"), holidays)
    )
    return data
