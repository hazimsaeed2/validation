"""Schemata for coupon ETL."""

from pyspark.sql.types import (
    StringType,
    StructField,
    LongType,
    StructType,
    DateType,
    DoubleType,
    IntegerType,
)


BASKET_COUPON_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("offer_id", LongType(), True),
        StructField("cpn_class_id", LongType(), True),
        StructField("cpn_dollar_off", DoubleType(), True),
        StructField("cpn_dollar_threshold", DoubleType(), True),
        StructField("cpn_desc", StringType(), True),
        StructField("cpn_start", StringType(), True),
        StructField("cpn_end", StringType(), True),
        StructField("self_funded_flag", LongType(), True),
    ]
)

ARTICLE_COUPON_SCHEMA = StructType(
    [
        StructField("Promo #", LongType(), True),
        StructField("Promo Description", StringType(), True),
        StructField("Promo Type", StringType(), True),
        StructField("Valid From", StringType(), True),
        StructField("Valid To", StringType(), True),
        StructField("Article Number", StringType(), True),
        StructField("Quantity Threshold", LongType(), True),
        StructField("Discount Value", DoubleType(), True),
        StructField("PMR Offer ID", StringType(), True),
        StructField("Eligible for MMPC", LongType(), True),
        StructField("self_funded_flag", LongType(), True),
    ]
)

COUPON_REDEMPTION_SCHEMA = StructType(
    [
        StructField("CPN_NBR", StringType(), True),
        StructField("OFFER_ID", StringType(), True),
        StructField("CPN_CLASS_ID", StringType(), True),
        StructField("CPN_TYPE", StringType(), True),
        StructField("CPN_DESC", StringType(), True),
        StructField("CPN_IMP", StringType(), True),
        StructField("CPN_RED", StringType(), True),
        StructField("CPN_COST", DoubleType(), True),
        StructField("SALES_LIFT", StringType(), True),
    ]
)


# input is one row per coupon per CATEGORY
# only AH4 OR AH5 code field can be filled per coupon. Other column must be null
CAT_COUPON_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("offer_id", LongType(), True),
        StructField("cpn_class_id", LongType(), True),
        StructField("cpn_ah4_cd", StringType(), True),
        StructField("cpn_ah5_cd", StringType(), True),
        StructField("cpn_desc", StringType(), True),
        StructField("cpn_dollar_off", DoubleType(), True),
        StructField("cpn_dollar_threshold", DoubleType(), True),
        StructField("cpn_start", StringType(), True),
        StructField("cpn_end", StringType(), True),
        StructField("self_funded_flag", LongType(), True),
    ]
)

CAP_CLOSURE_COUPON_SCHEMA = StructType(
    [
        StructField("PMR Offer ID", StringType(), False),
        StructField("closure_ah5_cd", StringType(), False),
        StructField("closure_window", LongType(), False),
        StructField("min_distribution", LongType(), False),
        StructField("max_distribution", LongType(), False),
        StructField("closure_priority", LongType(), False),
        StructField("equivalent_offer_id", StringType(), True),
    ]
)

VERSION_MAP_SCHEMA = StructType(
    [
        StructField("CPN1", StringType(), False),
        StructField("VERSION", StringType(), True),
    ]
)

SPECIAL_COUPON_SCHEMA = StructType(
    [
        StructField("cpn_nbr", LongType(), False),
        StructField("offer_id", LongType(), True),
        StructField("cpn_class_id", LongType(), True),
        StructField("cpn_ah4_cd", LongType(), True),
        StructField("cpn_ah5_cd", LongType(), True),
        StructField("cpn_article_nbr", LongType(), True),
        StructField("cpn_desc", StringType(), True),
        StructField("cpn_dollar_off", DoubleType(), True),
        StructField("cpn_dollar_threshold", DoubleType(), True),
        StructField("cpn_start", StringType(), True),
        StructField("cpn_end", StringType(), True),
        StructField("self_funded_flag", LongType(), True),
    ]
)

COUPON_PRICE_SCHEMA = StructType(
    [
        StructField("Promo_Description", StringType(), True),
        StructField("PMR_Offer_ID", StringType(), True),
        StructField("Offer Type", StringType(), True),
        StructField("Article_Number", StringType(), True),
        StructField("Discount_Value", DoubleType(), True),
        StructField("Chain_Level_Retail_P", DoubleType(), True),
    ]
)

# output schemas

CPN_QUALS_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("experiment_id", LongType(), False),
        StructField("hero_eligible", LongType(), False),
        StructField("backfill_eligible", LongType(), False),
    ]
)

MEM_TRIP_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("mbrshp_sid", LongType(), False),
        StructField("trips", DoubleType(), True),
        StructField("adjusted_trips", DoubleType(), True),
        StructField("days_since_last", DoubleType(), True),
    ]
)

MEM_USAGE_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("mbrshp_sid", LongType(), False),
        StructField("purchases", DoubleType(), True),
        StructField("redemptions", DoubleType(), True),
        StructField("fraction_purchases_with_coupon", DoubleType(), True),
    ]
)

CPN_MAP_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("ah4_cd", LongType(), True),
        StructField("ah5_cd", LongType(), True),
        StructField("article_nbr", LongType(), True),
    ]
)

CPN_BNK_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("cpn_type", StringType(), True),
        StructField("offer_id", LongType(), True),
        StructField("cpn_class_id", LongType(), True),
        StructField("self_funded_flag", LongType(), True),
        StructField("cpn_desc", StringType(), True),
        StructField("cpn_start", StringType(), True),
        StructField("cpn_end", StringType(), True),
        StructField("cpn_dollar_off", DoubleType(), True),
        StructField("cpn_qty_threshold", DoubleType(), True),
        StructField("cpn_dollar_threshold", DoubleType(), True),
    ]
)

CPN_DISCOUNT_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("fraction_discount", DoubleType(), True),
    ]
)


EXTRA_INFO_CLOSURE_COUPON_LOOKUP_SCHEMA = StructType(
    [
        StructField("cpn_nbr", StringType(), False),
        StructField("PMR Offer ID", StringType(), False),
        StructField("is_closure", IntegerType(), False),
    ]
)

COUPONS = {
    "article_coupon": ARTICLE_COUPON_SCHEMA,
    "basket_coupon": BASKET_COUPON_SCHEMA,
    "category_coupon": CAT_COUPON_SCHEMA,
    "cap_closure_coupon": CAP_CLOSURE_COUPON_SCHEMA,
    "prices": COUPON_PRICE_SCHEMA,
    "coupon_quals": CPN_QUALS_SCHEMA,
    "member_trips": MEM_TRIP_SCHEMA,
    "member_usage": MEM_USAGE_SCHEMA,
    "coupon_map": CPN_MAP_SCHEMA,
    "coupon_bank": CPN_BNK_SCHEMA,
    "coupon_redemption": COUPON_REDEMPTION_SCHEMA,
    "coupon_discount": CPN_DISCOUNT_SCHEMA,
    "extra_info_closure_coupon_lookup": EXTRA_INFO_CLOSURE_COUPON_LOOKUP_SCHEMA,
}
