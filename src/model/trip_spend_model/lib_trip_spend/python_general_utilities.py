# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 16:27:00 2018

@author: Skatteboe Karoline
"""
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# UNCOMMENT THIS IF YOU NEED TO USE THE PLOTTING
# METHODS
# this is commented because matplotlib is not installed on EC2
# import matplotlib
# ###########################
# matplotlib.use('Agg')
# import matplotlib.pyplot as plt
# ###########################
import numpy as np
import sklearn.metrics as metrics


def get_df_from_list(table, headings):
    """
    Transforms a list into a Pandas DataFrame

    Args:
        table (Array): multi dimmentional array of input data
        headings (Array<string>): column headers
    Returns:
        df (Pandas DataFrame): input data converted to pandas dataframe
    """
    df = pd.DataFrame(table)
    df.columns = headings
    return df


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
    continious_features = features["continuous_features"].tolist()
    categorical_features = features["categorical_features"].tolist()
    categorical_features = [
        i for i in categorical_features if not i in ["nan", np.nan]
    ]
    continious_features = [
        i for i in continious_features if not i in ["nan", np.nan]
    ]
    return categorical_features, continious_features


def string_index_column(data, column):
    """
    Transform a string column to integer index mapping
    Args:
        data (DataFrame): customer cube
    Returns:
        data (DataFrame)
    """
    data[column] = data[column].astype("category")
    data[column] = data[column].cat.codes
    return data


def transform_categorical_features(data, categorical_features):
    """
    Transform a list of columns to string

    Iterates through a list of column names and transform the column
    to integer index matching
    Args:
        data (DataFrame): customer cube
        categorical_features: array<string>
    Returns:
        data (DataFrame)
    """
    for feature in categorical_features:
        data[feature + "_string"] = data[feature]
        data["temp"] = data[feature].astype("category")
        data[feature + "_tmp"] = data["temp"].cat.codes
        data[feature] = data[feature].astype("category")
        data[feature] = data[feature].cat.codes
        data = data.drop(columns=["temp"])
    return data


def get_column_headers(features, dependent_variable_column=[]):
    """
    Renames column headers for reusability and returns different column types
    Args:
        data (DataFrame): customer cube
    Returns:
        columns: array<string>
        continious_features: array<string>
        categorical_features: array<string>
    """
    categorical_features, continious_features = get_features_list(features)
    categorical_feature_transformed = [
        string + "_tmp" for string in categorical_features
    ]
    columns = (
        categorical_feature_transformed
        + continious_features
        + dependent_variable_column
    )
    return columns, categorical_features, continious_features


def test_and_train_to_pandas(data, split_rate, sample_rate):
    """
    Splits data into test and train datasets

    Splits a data into testing and training sets based on the
    sample value and split value
    Args:
        data (DataFrame): customer cube
        split rate: integer
        sample rate: integer
    Returns:
        training (DataFrame)
        testing (DataFrame)
    """
    data = data.sample(frac=sample_rate, replace=False)
    training, testing = train_test_split(data, test_size=split_rate)
    return training, testing


# UNCOMMENT THIS IF YOU NEED TO USE THE PLOTTING
# METHODS
# this is commented because matplotlib is not installed on EC2
# import matplotlib
# ###########################
# def save_roc_curve(predictions, actuals, name, save, iteration = ''):
#     """
#     Save ROC plot
#
#     Args:
#         predictions: (array<float>) predicted values
#         actuals: (array<float>) actual values
#         iteration: (string) count for iteration
#         name: (string) path for curve
#     Returns:
#     """
#     false_positive_rate, true_positive_rate, thresholds = roc_curve(actuals, predictions)
#     roc_auc = auc(false_positive_rate, true_positive_rate)
#     plt.title('Receiver Operating Characteristic')
#     plt.plot(false_positive_rate, true_positive_rate, 'b',
#     label='AUC %s iteration = %0.2f'% (iteration,roc_auc))
#     plt.legend(loc='lower right')
#     plt.plot([0,1],[0,1],'r--')
#     plt.xlim([-0.1,1.2])
#     plt.ylim([-0.1,1.2])
#     plt.ylabel('True Positive Rate')
#     plt.xlabel('False Positive Rate')
#     if save:
#         plt.savefig(name+'.png')
#     plt.gcf().clear()
#
#
# def save_histogram(column, path, save):
#     """
#     Save histogram based on given column using matplotlib
#
#     Args:
#         column: (array<float>) column to bin
#         path: (string) path for histogram
#     Returns:
#     """
#     hist, bin_edges = np.histogram(column, bins='auto')
#     plt.title('Histogram')
#     plt.bar(bin_edges[:-1], hist, width = 1)
#     plt.xlim(min(bin_edges), max(bin_edges))
#     if save:
#         plt.savefig(path+'.png')
#     plt.gcf().clear()
# ###########################


def create_trip_propensity_metric(predictions, testing_y):
    """
    Return classifications metrics

    Combine metrics for classifications type models
    Args:
        predictions: (array<float>)
        testing_y: (array<float>)
    Returns:
        metrics (array<float>)
    """
    return [
        metrics.roc_auc_score(testing_y, predictions),
        metrics.accuracy_score(testing_y, predictions),
        metrics.precision_score(testing_y, predictions),
        metrics.recall_score(testing_y, predictions),
    ]


def create_spend_propensity_metric(predictions, testing_y):
    """
    Return regression metrics

    Combine metrics for regression type models
    Args:
        predictions: (array<float>)
        testing_y: (array<float>)
    Returns:
        metrics (array<float>)
    """
    return [
        metrics.median_absolute_error(testing_y, predictions),
        metrics.mean_squared_error(testing_y, predictions),
        metrics.r2_score(testing_y, predictions),
    ]


def create_seasonality_fields(data):
    """
    Add seasonality field

    Add field for month and week of year based on fscal week start
    Args:
        data (DataFrame): customer cube
    Returns:
        data (DataFrame)
    """

    data["MONTH"] = (
        pd.to_datetime(data["FISCAL_WEEK_START"], format="%Y-%m-%d")
    ).dt.month
    data["WEEK_OF_YEAR"] = (
        pd.to_datetime(data["FISCAL_WEEK_START"], format="%Y-%m-%d")
    ).dt.week

    return data


def transform_segments(segments, week):
    """
    Clean up segments input file

    Select relevant columns.
    Removes leading ' from date column. Casting column to datetype.
    Args:
        segments (DataFrame): path of output file
    Returns:
        segmenst (DataFrame)
    """
    segments = segments[
        [
            "MBRSHP_SID",
            "TNRD_SGMNT",
            "FRST_YR_SGMNT",
            "TNRD_DCLNR_SGMNT",
            "FRST_YR_DCLNR_SGMNT",
            "EFF_DT",
        ]
    ]
    segments["EFF_DT"] = segments["EFF_DT"].str[1:]
    segments["EFF_DT"] = pd.to_datetime(segments["EFF_DT"], format="%Y-%m-%d")
    segments = segments.fillna(0)
    segments = segments.loc[segments["EFF_DT"] <= week]
    return segments


def find_segment_with_date(data, segments):
    """
    Merges segments and the main customer cube to map segments and members

    Pick the most recent entry from the segments file from each member.
    Join with the main member cube. Pick the max of the segments column into
    a new column names SEGMENT so that new and tenure segments have the same column
    header.

    Args:
        data (DataFrame): customer cube
        segment (DataFrame):
    Returns:
        data (DataFrame)
    """
    segments = (
        segments.sort_values(["EFF_DT"], ascending=False)
        .groupby("MBRSHP_SID")
        .first()
    )
    data = data.join(segments, how="left", lsuffix="_left", rsuffix="_right")
    segment_groups = ["TNRD_SGMNT", "FRST_YR_SGMNT"]
    data[segment_groups] = data[segment_groups].fillna(value=0)
    data["SEGMENT"] = data[segment_groups].max(axis=1)
    data[["SEGMENT"]] = data[["SEGMENT"]].fillna(value=0)
    return data


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
    data["MEMBER_FREQUENCY_GROUP"] = np.where(
        data["LAST_TWENTY-SIX_WEEK_TRIPS"]
        <= low_frequency_visits_last_26_weeks_cutoff,
        "LOW",
        np.where(
            data["LAST_TWELVE_WEEK_TRIPS"]
            >= high_frequency_visits_last_12_weeks_cutoff,
            "HIGH",
            "MEDIUM",
        ),
    )
    data["SPEND_IN_STORE_BY_TRIPS_LAST_TWENTY-SIX_WEEKS"] = np.where(
        data["LAST_TWENTY-SIX_WEEK_TRIPS"] == 0,
        0,
        data["LAST_TWENTY-SIX_WEEK_SPEND"]
        * 1.1
        / data["LAST_TWENTY-SIX_WEEK_TRIPS"],
    )

    return data


def join_cube(cube, additional_variable_cube, columns, drop_select_indicator):

    additional_variable_cube = (
        additional_variable_cube[columns]
        if drop_select_indicator == "select"
        else additional_variable_cube.drop(columns, axis=1)
    )
    cube = cube.merge(
        additional_variable_cube,
        on=["MBRSHP_SID", "FISCAL_WEEK_START"],
        how="inner",
    )
    return cube


def join_bbm_decile(data, decile_path):
    deciles = read_csv_pandas_s3("memberanalytics-data-in", decile_path)
    deciles = deciles[["decile", "MBRSHP_NBR"]]
    deciles.set_index("MBRSHP_NBR", inplace=True)
    conversion = read_csv_pandas_s3(
        "memberanalytics-data-in", "extended_mbr_table_032318.csv"
    )
    conversion = conversion[["MBRSHP_SID", "MBRSHP_NBR"]]
    deciles = deciles.join(conversion.set_index("MBRSHP_NBR"), how="left")
    data = data.merge(
        deciles, left_index=True, right_on=["MBRSHP_SID"], how="left"
    )
    return data


def filter_decile(data, deciles_to_filter_for):
    """
    Filter the dataframe based on decile values

    Args:
        data (DataFrame): customer cube
        deciles_to_filter_for (array<int>): list of deciles
    Returns:
        data (DataFrame)
    """
    return data[data.decile.isin(deciles_to_filter_for)]


def join_sid_nbr(data, conversion):
    """
    Merges the main cube dataframe with conversion file

    Args:
        data (DataFrame): customer cube
        conversion (DataFrame): conversion file dataframe
    Returns:
        data (DataFrame)
    """
    return data.merge(
        conversion, left_on="MBRSHP_SID", right_on="MBRSHP_SID", how="left"
    )


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
        # subtract 1 here to increase understandability in the script
        increment = increment - 1
        start = start - 1
        end = start + increment
        if model_type == "bin":
            header = "will_visit_from_%s_%s" % (start, end)
            data = create_dependent_binary_variables(
                data, start, increment, header
            )
        if model_type == "cont":
            header = "spend_from_%s_%s" % (start, end)
            data = create_dependent_continious_variables(
                data, start, increment, header
            )
    return data


def create_dependent_binary_variables(
    data, window_start, length, variable_name
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
    data["temp"] = (
        data.sort(["MBRSHP_SID", "FISCAL_WEEK_START"], ascending=False)
        .groupby("MBRSHP_SID")
        .apply(
            pd.rolling_sum(
                data["WEEK_TRIPS"].shift(window_start), window=length
            )
        )
    )
    data[variable_name] = np.when(data["temp"] >= 1, 1, 0)
    data = data.fillna(0)
    return data


def create_dependent_continious_variables(
    data, window_start, length, variable_name
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
    data[variable_name] = (
        data.sort(["MBRSHP_SID", "FISCAL_WEEK_START"], ascending=False)
        .groupby("MBRSHP_SID")
        .apply(
            pd.rolling_sum(
                data["FW_SPEND_IN_STORE"].shift(window_start), window=length
            )
        )
    )
    data = data.fillna(0)
    return data


def create_sklearn_features(features, column_headers):
    """
    Zips feature names and feature importance

    Args:
        features (array<float>): array of numbers to bucket on
        features (array<float>): array of numbers to bucket on
    Returns:
        importance_and_names (array<string,float>)
    """
    importance_and_names = list(zip(features, column_headers))
    importance_and_names = pd.DataFrame(
        importance_and_names, columns=["Importance", "Feature_Name"]
    )
    return importance_and_names


def train_initial_model(
    training_X,
    training_y,
    testing_X,
    split_rate,
    testing_y,
    label_column_header,
):
    """
    Trains first pass of trip propensty model

    Trains first pass at trip propensity model, predict on all members,
    filter for top and bottom predictions. Combine dataframes back together and
    add lable column

    Args:
        features (array<float>): array of numbers to bucket on
        features (array<float>): array of numbers to bucket on
    Returns:
        importance_and_names (array<string,float>)
    """
    model = RandomForestClassifier(
        n_jobs=-1,
        max_depth=20,
        max_features=0.5,
        warm_start=True,
        n_estimators=1000,
    )

    model.fit(training_X, training_y.values.ravel())

    combined = training_X.append(testing_X)
    combined = combined[~combined.index.duplicated()]
    combined = combined.fillna(0)
    combined["prediction"] = model.predict_proba(combined)[:, 1]
    combined = combined.loc[
        (combined["prediction"] >= 0.05) & (combined["prediction"] <= 0.95)
    ]
    combined.reset_index(inplace=True)
    combined = combined[~combined.index.duplicated()]
    column = training_y.append(testing_y, ignore_index=True)
    combined[label_column_header] = column
    combined = combined.drop(columns=["prediction"])
    combined = combined.fillna(0)
    return train_test_split(combined, test_size=split_rate)


def create_buckets(data, column, buckets):
    """
    Buckets dataframe based on column vales

    Add new column to the dataframe with bucket id from bucketing given column into
    the passed in indexes
    Args:
        data (DataFrame): customer cube
        column (string): column header of column to index
        buckets (array<float>): array of numbers to bucket on
    Returns:
        data (DataFrame)
    """
    data["bucket"] = pd.cut(data[column], bins=buckets)
    data["bucket_idx"] = pd.cut(data[column], bins=buckets, labels=False)
    return data


def backtest_percentile(predictions, path):
    """
    Aggregated predictions in buckets and calculate bucketwise accuracy

    Round the predicted values to nearest 0.1, aggregate predictions by
    predicted bucket. Calculate bucketwise accuracy. Write to specfied filepath.

    Args:
        predictions (pandas.Dataframe):
        path (string): desired output path
    Returns:
        data (DataFrame)
    """
    predictions.prediction_prob = predictions["prediction_prob"].round(1)
    grouped = predictions.groupby("prediction_prob").agg(
        {"will_visit_from_5_7": ["sum", "count"]}
    )
    grouped.columns = grouped.columns.droplevel()
    grouped.reset_index(inplace=True)
    grouped["percentage"] = grouped["sum"] / grouped["count"]
    grouped.to_csv(path)
