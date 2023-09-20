"""Ingestion object for handling dynamic data ingestion for experiment.

TODO:
    Test generate_article_offer function with real data
    Rich method for backfill ingestion
"""
import warnings

# Do NOT import the max function as it will collide with a use later
# Do NOT import the max function as it will collide with a use later
from pyspark.sql.functions import (
    array,
    broadcast,
    coalesce,
    col,
    concat,
    count,
    desc,
    explode,
    hash,
    lit,
    monotonically_increasing_id,
    rand,
    row_number,
)
from pyspark.sql.functions import sum as fsum
from pyspark.sql.functions import when
from pyspark.sql.window import Window as W

from pe_member_dna.pipelines.assignment.lib.assn_utils import deterministic_df
from pe_member_dna.pipelines.lib.spark_util import get_logger
from pe_member_dna.pipelines.lib.utils import top_n

# ---- HELPERS ---- #
log = get_logger("ingest")


def _broadcast_cross_join(df_large, df_small):
    """broadcast small data frame before cross join

    Parameters:
        df_large (pyspark.sql.DataFrame): large data frame
        df_small (pyspark.sql.DataFrame): small data frame to be broadcast

    Returns:
        df (pyspark.sql.DataFrame): data frame after cross join
    """
    df_small = df_small.repartition(1)
    df = df_large.crossJoin(broadcast(df_small))
    return df


def broadcast_filter(df_large, df_small, columns):
    """
    use a broadcast to filter out the larger dataframe
    :param df_large: the larger df
    :param df_small: the smaller df
    :param columns: columns to join on
    :return:
    """
    df_small = df_small.select(columns)
    return df_large.join(broadcast(df_small), columns, "inner")


def join_category_agnostic(coupons):
    """fill the coupon category with ah5 first, if ah5 is null, fill with ah4

    Parameters:
        coupons (pyspark.sql.DataFrame): relevant coupon list

    Returns:
        coupons (pyspark.sql.DataFrame): coupon list convert to ah level agnostic
    """
    cat_cols = ["ah4_cd", "ah5_cd"]
    # use ah5 as prioritized category given it is more granular level
    coupons = coupons.fillna(0)
    coupons = coupons.withColumn(
        "CATEGORY_ID",
        when(col("ah5_cd") > 0, col("ah5_cd")).otherwise(col("ah4_cd")),
    )
    coupons = coupons.filter(col("CATEGORY_ID") > 0)
    coupons = coupons.filter(col("cpn_nbr").isNotNull())
    coupons = coupons.drop(*cat_cols)
    coupons = coupons.dropDuplicates()
    return coupons


def _generate_category(members, coupons, cf_pred, trips, seed):
    """Generate the base form of category offer data.

    Designed to take category coupon list.
    Important feature of cat data is that it is tied to cat
    level (AH4/AH5) CF predictions.

    Parameters:
        members (pyspark.sql.DataFrame): relevant member mail list
        coupons (pyspark.sql.DataFrame): relevant coupon list
        cf_pred (pyspark.sql.DataFrame): complete set of CF preds fof ALL cats considered
        trips (pyspark.sql.DataFrame): complete set of aggregate of categories matched
            trips per member per coupon
        seed (int): a deterministic unique value to start from when creating an
            order
    Returns:
        df_and_coups: member/coupon list with predicted scores for BEST cat mapping of coupon
    """
    coupons = join_category_agnostic(coupons)
    df_and_coups = _broadcast_cross_join(members, coupons)
    withpreds = df_and_coups.join(
        cf_pred, ["MBRSHP_SID", "CATEGORY_ID"], "left"
    )
    # Subset to best member/coupon/cat/prediction by best prediction
    withpreds = withpreds.withColumn(
        "groupcol", concat(withpreds.MBRSHP_SID, withpreds.cpn_nbr)
    )

    deterministic, order_by, _ = deterministic_df(
        withpreds,
        [col("prediction").desc()],
        seed,
        random_only=True,
        extra_hash_columns=[col("CATEGORY_ID")],
    )

    withpreds = top_n(deterministic, 1, order_by, "groupcol", keep=False)

    # Join on deduplicated trips
    trips = top_n(
        trips,
        1,
        [
            col("days_since_last").asc(),
            col("adjusted_trips").desc(),
            col("trips").desc(),
        ],
        [col("MBRSHP_SID"), col("cpn_nbr")],
        keep=False,
    )
    withpreds = withpreds.join(trips, ["MBRSHP_SID", "cpn_nbr"], "left")
    withpreds = withpreds.fillna(0, subset=["trips", "adjusted_trips"])

    withpreds = withpreds.drop("groupcol")
    return withpreds


def _generate_cross_join(members, coupons):
    """Generate the base form of basket offer data.

    Designed to take basket coupon list
    Important feature of bas data is that it is tied to
    average basket size.

    Parameters:
        members (pyspark.sql.DataFrame): relevant member mail list
        coupons (pyspark.sql.DataFrame): relevant basket coupon list

    Returns:
        df_and_coups: member/coupon list
    """
    df_and_coups = _broadcast_cross_join(members, coupons)
    return df_and_coups


def _generate_static(members):
    """Generate the base form of dummy offer data.

    Designed to generate dummy coupon for static offer
    E.g. gas, trial, etc

    Parameters:
        members (pyspark.sql.DataFrame): relevant member mail list

    Returns:
        df_and_coups: member/coupon list
    """
    df_and_coups = members.withColumn("cpn_nbr", lit("0"))
    df_and_coups = df_and_coups.withColumn(
        "offer_id", lit(None).cast("string")
    )
    df_and_coups = df_and_coups.withColumn(
        "cpn_class_id", lit(None).cast("string")
    )
    df_and_coups = df_and_coups.withColumn("cpn_type", lit("dummy"))
    return df_and_coups


def _generate_article(
    members, coupons, cf_pred, trips, usage, discounts, seed
):
    """Generate the base form of article offer data.

    Designed to take article coupons that have been mapped with
    CF prediction and # of trips (BAU logic)

    Parameters:
        members (pyspark.sql.DataFrame): relevant member mail list
        coupons (pyspark.sql.DataFrame): relevant coupon list
        cf_pred (pyspark.sql.DataFrame): complete set of CF preds for ALL cats considered
        trips (pyspark.sql.DataFrame): complete set of aggregate of article matched trips per member
            per coupon
        usage (pyspark.sql.DataFrame): complete set of aggregate article
            matched past cpn usage per member per coupon
        discounts (pyspark.sql.DataFrame): fraction discount per coupon
        seed (int): a deterministic unique value to start from when creating an
            order
    Returns:
        article_data: member/coupon list with predicted scores for BEST cat mapping of coupon
    """
    # 1. make member/coupon dataset
    coupons = join_category_agnostic(coupons)
    coupons.cache()
    coupons = coupons.dropDuplicates()
    df_and_coups = _broadcast_cross_join(members, coupons)
    df_and_coups = df_and_coups.repartition(1000)
    coupons.unpersist()
    # 2. Join on CF predictions
    withpreds = df_and_coups.join(
        cf_pred, ["MBRSHP_SID", "CATEGORY_ID"], "left"
    )
    withpreds = withpreds.withColumn(
        "groupcol", concat(withpreds.MBRSHP_SID, withpreds.cpn_nbr)
    )

    deterministic, order_by, _ = deterministic_df(
        withpreds,
        [col("prediction").desc()],
        seed,
        random_only=True,
        extra_hash_columns=[col("CATEGORY_ID")],
    )

    withpreds = top_n(deterministic, 1, order_by, "groupcol", keep=False)
    withpreds = withpreds.fillna(0, subset=["prediction"])
    # 3. Join on Trips
    # Join on deduplicated trips
    trips = top_n(
        trips,
        1,
        [
            col("days_since_last").asc(),
            col("adjusted_trips").desc(),
            col("trips").desc(),
        ],
        [col("MBRSHP_SID"), col("cpn_nbr")],
        keep=False,
    )
    withpreds = withpreds.join(trips, ["MBRSHP_SID", "cpn_nbr"], "left")
    withpreds = withpreds.join(usage, ["MBRSHP_SID", "cpn_nbr"], "left")
    if discounts and discounts.count() > 0:
        withpreds = withpreds.join(discounts, ["cpn_nbr"], "left")
    withpreds = withpreds.fillna(
        0,
        subset=[
            "trips",
            "adjusted_trips",
            "purchases",
            "redemptions",
            "fraction_purchases_with_coupon",
        ],
    )
    # 4. drop extra cols and invalid offers
    withpreds = withpreds[(withpreds.trips > 0) | (withpreds.prediction > 0)]
    withpreds = withpreds.drop("groupcol")
    return withpreds


# ---- Ingestion ---- #
def ingest(
    offer_data, mbr, coups, cf_pred, trips, usage, discounts, coupon_type, seed
):
    """ingest offer data

    Designed to call ingestion function based upon offer data

    Parameters:
        offer_data (string): offer data name
        mbr (pyspark.sql.DataFrame): relevant member mail list
        coups (pyspark.sql.DataFrame): relevant coupon list
        cf_pred (pyspark.sql.DataFrame): cf prediction score by category
        trips (pyspark.sql.DataFrame): trips on member level by coupon
        usage (pyspark.sql.DataFrame): fraction purchases w/ coupon in past
            per mbr per cpn
        discounts (pyspark.sql.DataFrame): fraction discount per cpn
        seed (int): a deterministic unique value to start from when creating an
            order
    Returns:
        df: member/coupon list
    """

    log.info(
        "Ingest {} coupons by {} Method.  Coupon Count {}".format(
            coupon_type, offer_data, coups.count()
        )
    )
    if offer_data == "category":
        data = _generate_category(mbr, coups, cf_pred, trips, seed)
        data = data.dropDuplicates(subset=["MBRSHP_SID", "cpn_nbr"])
    elif offer_data == "basket":
        data = _generate_cross_join(mbr, coups)
        data = data.dropDuplicates(subset=["MBRSHP_SID", "cpn_nbr"])
    elif offer_data == "dummy":
        data = _generate_static(mbr)
        data = data.dropDuplicates(subset=["MBRSHP_SID", "cpn_nbr"])
    elif offer_data == "article":
        data = _generate_article(
            mbr, coups, cf_pred, trips, usage, discounts, seed
        )
        data = data.dropDuplicates(subset=["MBRSHP_SID", "cpn_nbr"])
    elif offer_data == "special":
        coups = join_category_agnostic(coups)
        data = _generate_cross_join(mbr, coups)
        data = data.dropDuplicates(
            subset=["MBRSHP_SID", "cpn_nbr", "CATEGORY_ID"]
        )
    else:
        raise Exception(
            "{offer_data}: Not valid offer data".format(offer_data=offer_data)
        )
    return data
