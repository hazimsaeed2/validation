from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sklearn.utils import shuffle

import pe_memberdna.lib.iotools as general_iotools
import pe_memberdna.model.trip_spend_model.lib.python_general_utilities as util_func
import pe_memberdna.model.trip_spend_model.lib.trip_spend_iotools as iotools


def delta_spend(probability, spend, incremental_trips):
    return incremental_trips * (probability) * spend


def delta_spend_scaled(spend, shop_rate_control, shop_rate_treated):
    return spend * ((shop_rate_treated / shop_rate_control))


def delta_trip_increase(data):
    data["delta"] = 1 - (3 * data["LAST_FIFTY-TWO_WEEK_TRIPS"]) / 52
    data["zero"] = 0
    data["delta_trips"] = data[["delta", "zero"]].max(axis=1)
    data = data.drop(columns=["delta", "zero"])
    return data


def join_shop_rate(
    data, path, default_treated_shop_rate, default_control_shop_rate, bucket
):
    """
    Join with shop rate for treated and control by segment

    Join the two datasets, if there is no mapping
    the default values are used.

    Args:
        path (string): path to conversion file
        bucket (string): s3 bucket
        default_treated_shop_rate (int): Default shop rate value from past BBMs for treated
        default_control_shop_rate (int): Default shop rate value from past BBMs for control
        data: member data (DataFrame)
    Returns:
        data (DataFrame)
    """
    data[["SEGMENT"]] = data[["SEGMENT"]].apply(pd.to_numeric)
    shop_rates = iotools.read_csv_pandas_s3(bucket, path)
    shop_rates = shop_rates[
        [
            "SEGMENT",
            "shop_rate_treated",
            "shop_rate_control",
            "redemption_rate_basket_offer",
            "redemption_rate_among_shoppers_basket_offer",
            "TENURE_GROUP",
        ]
    ]
    shop_rates[["SEGMENT"]] = shop_rates[["SEGMENT"]].apply(pd.to_numeric)
    data = data.merge(
        shop_rates,
        left_on=["SEGMENT", "TENURE_GROUP"],
        right_on=["SEGMENT", "TENURE_GROUP"],
        how="left",
    )
    data["shop_rate_treated"] = np.where(
        data["shop_rate_treated"].isnull(),
        default_treated_shop_rate,
        data["shop_rate_treated"],
    )
    data["shop_rate_control"] = np.where(
        data["shop_rate_control"].isnull(),
        default_control_shop_rate,
        data["shop_rate_control"],
    )
    return data


def join_bbm_decile(data, path, bucket, lookup_path):
    """
    Join with decile assignments

    Args:
        variables (array<string>): column headers of offers to propagate
        data: member data (DataFrame)
    Returns:
        data (DataFrame)
    """
    deciles = iotools.read_csv_pandas_s3(bucket, path)
    deciles = deciles[["decile", "MBRSHP_NBR"]]
    data = util_func.join_sid_nbr(
        data, iotools.read_csv_pandas_s3(bucket, lookup_path)
    )
    data = data.merge(deciles, on="MBRSHP_NBR", how="left")
    return data


def propagate_variables_for_new_members(data, variables):
    """
    Iterate through variables in the input list and propagate up for members with tenure < 365

    Args:
        variables (array<string>): column headers of offers to propagate
        data: member data (DataFrame)
    Returns:
        data (DataFrame)
    """
    data["TENURE"] = np.where(data["TENURE"] == 0, 1, data["TENURE"])
    new = data.loc[data["TENURE"] <= 365]
    tenured = data.loc[data["TENURE"] > 365]
    for variable in variables:
        new[variable] = np.ceil(new[variable] / (new["TENURE"] / 365))
    pd.concat([tenured, new], ignore_index=True)
    return pd.concat([tenured, new], ignore_index=True)


def get_tenure(membership_start_date, current_fiscal_week):
    """
    Get tenure for a given member in a given fiscal week in days

    Args:
        membershp_start_date (datetime): start date for membership
        current_fiscal_week (datetime): current fiscal week
    Returns:
        tenure (days)
    """
    return (current_fiscal_week - membership_start_date).dt.days


def fill_na(columns, data):
    """
    Replace NaNs with 0 for the input columns

    Args:
        coumns (array<string>): columns to replace missing values in
        data: member data (DataFrame)
    Returns:
        data (DataFrame)
    """
    data[columns] = data[columns].fillna(value=0)
    return data


def create_BBM_window(week):
    """
    Return a 3 week window 6 weeks after the input date

    Args:
        week (string)
    Returns:
        BBM_weeks (array<string>)
    """
    BBM_weeks = []
    week_object = datetime.strptime(week, "%Y-%m-%d")
    for i in range(3):
        BBM_weeks += [
            (week_object + timedelta(weeks=(6 + i))).strftime("%Y-%m-%d")
        ]
    return BBM_weeks


def find_coupons_in_category(bucket, read_only_bucket):
    """
    find the corresponding coupon for each AH4 category

    Reads a list of all the eligible coupons. Filter for coupons in BBM10/PC9.
    Match with item list to get category for each coupon. Join with item level sales.
    Aggregate first to coupon and then to category picking the row with the higest sales/
    return a dateframe with one coupon for each category.

    Args:
        bucket (string): s3 bucket
        read_only_bucket (string): s3 bcuket
    Returns:
        groupes (Pandas.DataFrame)
    """
    coupons = iotools.read_csv_pandas_s3(
        read_only_bucket, "coupons/coupons_master_v3.csv"
    )
    coupons[["styles_elgible"]] = coupons[["styles_elgible"]].apply(
        pd.to_numeric, errors="coerce"
    )
    coupons.dropna(subset=["mailer_name", "styles_elgible"], inplace=True)
    coupons = coupons[coupons["mailer_name"].str.contains("BBM#10")]

    article_maps = iotools.read_parquet_pandas_s3(
        bucket, "pipelined_intermediates/master/item"
    )
    article_maps[["ARTICLE_NBR"]] = article_maps[["ARTICLE_NBR"]].apply(
        pd.to_numeric, errors="coerce"
    )
    article_maps.dropna(subset=["ARTICLE_NBR"], inplace=True)

    article_detail = iotools.read_csv_pandas_s3(
        bucket, "propensity_model_for_trips/AH4_category_stats.csv"
    )

    article_detail[["Article \nNbr"]] = article_detail[
        ["Article \nNbr"]
    ].apply(pd.to_numeric, errors="coerce")
    article_detail.dropna(subset=["Article \nNbr"], inplace=True)

    combined = coupons.merge(
        article_maps, left_on="styles_elgible", right_on="ARTICLE_NBR"
    )
    combined = combined.merge(
        article_detail, left_on="ARTICLE_NBR", right_on="Article \nNbr"
    )

    combined["Total Sales In Cat"] = combined["Total Sales In Cat"].map(
        lambda x: x.lstrip("$")
    )
    combined["Total Sales In Cat"] = combined["Total Sales In Cat"].apply(
        pd.to_numeric, errors="coerce"
    )

    combined = combined.loc[
        combined.reset_index()
        .groupby(["coupon_number"], as_index=False)["Total Sales In Cat"]
        .idxmax()
    ][
        [
            "coupon_number",
            "Total Sales In Cat",
            "AH4_DESC",
            "ARTICLE_NBR",
            "ARTICLE_DESC",
        ]
    ]
    combined = combined.loc[
        combined.reset_index()
        .groupby(["AH4_DESC"], as_index=False)["Total Sales In Cat"]
        .idxmax()
    ][
        [
            "coupon_number",
            "Total Sales In Cat",
            "AH4_DESC",
            "ARTICLE_NBR",
            "ARTICLE_DESC",
        ]
    ]
    return combined


def assign_quantiles(data, column_header):
    """
    Return quantiles based on column

    Args:
        column_header: (string): column to bucket on
        data (DataFrame): member data
    Returns:
        data (Pandas.DataFrame)
    """
    data["quantile"] = pd.qcut(data[column_header], 10, labels=False)
    return data


def assign_to_test_cell(data, number_of_cells):
    """
    Randomly assigns data to test cells

    Args:
        number_of_cells:  (int): number of test cells
        data (DataFrame): member data
    Returns:
        data (Pandas.DataFrame)
    """
    data["cell_assignment"] = np.random.randint(
        0, number_of_cells, size=len(data)
    )
    return data


def split_into_test_and_control(data, group):
    """
    Randomly split data into test and control cells

    Create an index column. Shuffle order of dataframe.
    Assign half to test.
    Return dataframe

    Args:
        group (string): name for cell group
        data (DataFrame): member data
    Returns:
        data (Pandas.DataFrame)
    """
    data = data.iloc[np.random.permutation(len(data))]
    data["index_column"] = list(range(1, len(data) + 1))
    data.set_index(["index_column"], inplace=True)
    data.loc[0 : (len(data) / 2), "coupon"] = "{}_control".format(group)
    return data


def find_most_popular_categories(categories, number_of_categories_to_pick):
    """
    Find the n most popular categories in a set of recommendations

    Group by category name, sum up and sort by count. Pick the top n rows.
    Return category names as a list.

    Args:
        group (string): name for cell group
        data (DataFrame): member data
    Returns:
        column_headers (string<array>)
    """
    aggregated_categories = categories.groupby("CATEGORY_NAME").agg(
        {"MBRSHP_SID": ["count"]}
    )
    aggregated_categories.columns = aggregated_categories.columns.droplevel()
    aggregated_categories = aggregated_categories.sort_values(
        "count", ascending=False
    )
    aggregated_categories.reset_index(inplace=True)
    return (
        aggregated_categories.head(number_of_categories_to_pick)
        .iloc[:, 0]
        .tolist()
    )


def assign_category_offer(category_offer, categories, category_names):
    """
    Assign category offers based on a list of categories

    filter category recommendations to those in the list.
    Select the max of each of the recommendations for each memebrs, remove recommendations that
    are lower than 5% of the max. Randomly select a recommendation per member.

    Args:
        category_offer (pandas.Dataframe): members to recieve category offer
        categories (pandas.Dataframe): category recommendations
        category_names (array<string>): list of category names
    Returns:
        category_offer (pandas.DataFrame)
    """
    # merge with members
    categories = categories.loc[categories.CATEGORY_NAME.isin(category_names)]
    category_offer = category_offer.merge(
        categories, left_on="MBRSHP_SID", right_on="MBRSHP_SID", how="left"
    )
    category_offer = fill_na("prediction", category_offer)
    category_offer["max_prediction"] = category_offer.groupby("MBRSHP_SID")[
        "prediction"
    ].transform("max")
    # select all recs within 5% of the max
    category_offer = category_offer[
        category_offer.prediction >= category_offer.max_prediction - 0.05
    ]
    category_offer = shuffle(category_offer)
    # randomly select a recommendation
    category_offer.drop_duplicates(subset="MBRSHP_SID", inplace=True)
    category_offer["cell_assignment"] = category_offer["CATEGORY_NAME"]
    category_offer = category_offer.drop(
        columns=["CATEGORY_NAME", "prediction", "max_prediction"]
    )
    # category_offer = split_into_test_and_control(category_offer, 'basket')

    return category_offer
