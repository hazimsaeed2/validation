# exclusions
# from pyspark.storagelevel import StorageLevel

from lib_dna_category.exclusion_inheritance import *
from lib_dna_category.square import Square
from lib_dna_category.features import *
from lib_dna_category.seasonality_features import *
from lib_dna_category.post_processors import *


class Build:
    @staticmethod
    def execute(spark, reqs, config, tables_dict):
        s = Square(spark, reqs, config, tables_dict)
        # compute features
        base(s)
        unit_retail_price(s)
        per(s)
        monthly_mbrs(s)
        compute_seasonality(s)
        compute_purchase_cycle(s)

        if s.category == "ARTICLE_NBR":
            add_ah_to_article(s)

        # s.square.persist(storageLevel=StorageLevel.DISK_ONLY)

        # apply post processors
        Build.post_process(s)
        # s.square.persist(storageLevel=StorageLevel.DISK_ONLY)

        # apply exclusions
        exclusions(s)

        # temporarily add blank margin columns to avoid breaking DS pipelines
        s.square = s.square.withColumn(
            "PCT_GROSS_MARGIN", F.lit(0)
        ).withColumn("GROSS_MARGIN", F.lit(0))

        # s.square.persist(storageLevel=StorageLevel.DISK_ONLY)
        return s.square, s.config

    @staticmethod
    def post_process(s):
        # apply lower and upper bounds to unitless features
        standard = {"lower": 0.1, "upper": 1.5}

        bounds = {
            "MONTH_1_SEASONALITY": standard,
            "MONTH_2_SEASONALITY": standard,
            "MONTH_3_SEASONALITY": standard,
            "MONTH_4_SEASONALITY": standard,
            "MONTH_5_SEASONALITY": standard,
            "MONTH_6_SEASONALITY": standard,
            "MONTH_7_SEASONALITY": standard,
            "MONTH_8_SEASONALITY": standard,
            "MONTH_9_SEASONALITY": standard,
            "MONTH_10_SEASONALITY": standard,
            "MONTH_11_SEASONALITY": standard,
            "MONTH_12_SEASONALITY": standard,
            "PURCHASE_CYCLE": standard,
        }
        # center unitless features at 1
        standard_mean = 1

        means = {
            "MONTH_1_SEASONALITY": standard_mean,
            "MONTH_2_SEASONALITY": standard_mean,
            "MONTH_3_SEASONALITY": standard_mean,
            "MONTH_4_SEASONALITY": standard_mean,
            "MONTH_5_SEASONALITY": standard_mean,
            "MONTH_6_SEASONALITY": standard_mean,
            "MONTH_7_SEASONALITY": standard_mean,
            "MONTH_8_SEASONALITY": standard_mean,
            "MONTH_9_SEASONALITY": standard_mean,
            "MONTH_10_SEASONALITY": standard_mean,
            "MONTH_11_SEASONALITY": standard_mean,
            "MONTH_12_SEASONALITY": standard_mean,
            "PURCHASE_CYCLE": standard_mean,
        }

        s.square = df_apply_bounds(bounds, s.square)
        s.square = df_change_averages(means, s.square)
