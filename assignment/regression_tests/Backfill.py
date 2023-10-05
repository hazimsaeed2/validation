from pyspark.sql import Window
from pyspark.sql.functions import col, lit, rank, when

import pe_memberdna.testing_support.regression_framework.regression as regression
from pe_memberdna.assignment.lib.assn_utils import read_subset_and_cast
from pe_memberdna.assignment.lib.ingest import _generate_cross_join
from pe_memberdna.assignment.lib.slots import hardest, rank_by_agg_col


class Backfill(regression.RegressionTest):
    """
    The purpose of the test is to verify basic backfill functionality.

    1. Dataset

    The CDSA is structured as follows:
        1. campaign.csv - 1 campaign
        2. cells.csv - has 1 cell
        3. constructs.csv -  1 construct with size 7 (3 article slots,
        3 category slots, 1 basket)

    The input coupons are structured as follow:
        1. category.csv - 10 category coupons
        2. article.csv - 10 article coupons
        3. basket.csv - 5 basket coupons

    2. Members

    The test case uses 5 members.

    3. Special configuration

    Each member has cf scores for 2 categories which are assigned to
    the coupons from category.csv . This should lead to 1 backfill per member
    for category coupons(and 2 frontfills).

    Each member has transactions for 2 article numbers which are assigned to
    the coupons from article.csv . This should lead to 1 backfill per member
    for article coupons(and 2 frontfills).

    There will be no basket coupon that will fit the members.

    Test Output:

    1. The engine should assign 2 category coupons per member using
    cf_combined and backfill 1 category coupons per member using
    rank_by_agg_cf.

    2. The engine should assign 2 article coupons per member using rank_by_col
    ADJUSTED_TRIPS and backfill 1 article coupon per member using
    rank_by_agg_trips.

    3. The engine should not frontfill any basket, it should only backfill
    with the hardest basket.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def assert_category_frontfill(self, current_assignment):
        category_frontfill_slots = [4, 5]
        normalization_unit = 3  # how much to subtract to mimic range(1, x)

        member_rank = Window.partitionBy("MBRSHP_SID").orderBy(
            col("prediction").desc()
        )
        cf_pred = read_subset_and_cast(
            self.config.paths["PRED_LIST"], "parquet"
        )
        expected_category_assignment = cf_pred.withColumn(
            "rank", rank().over(member_rank)
        ).select(
            col("MBRSHP_SID").alias("mbrshp_sid"),
            col("CATEGORY_ID").alias("category"),
            col("rank"),
        )

        coupon_category = (
            read_subset_and_cast(self.config.paths["COUPON_MAP"], "csv")
            .withColumn(
                "category",
                when(col("ah4_cd").isNotNull(), col("ah4_cd"))
                .when(col("ah5_cd").isNotNull(), col("ah5_cd"))
                .otherwise(col("article_nbr")),
            )
            .select("cpn_nbr", "category")
        )

        category_assignment = current_assignment.where(
            col("slot_nbr").isin(category_frontfill_slots)
        )

        actual_category_assignment = category_assignment.join(
            coupon_category, "cpn_nbr", "inner"
        )

        compare_assignment = actual_category_assignment.join(
            expected_category_assignment, ["mbrshp_sid", "category"], "inner"
        ).withColumn(
            "incorrect_assignment",
            when(
                col("slot_nbr") - normalization_unit != col("rank"), lit(1)
            ).otherwise(lit(0)),
        )

        total_incorrect_assignments = compare_assignment.where(
            col("incorrect_assignment") == 1
        ).count()

        self.assertEqual(total_incorrect_assignments, 0)
        self.assertEqual(compare_assignment.count(), 10)

    def assert_category_backfill(self, current_assignment):
        category_backfill_slots = [6]
        normalization_unit = 5  # how much to subtract to mimic range(1, x)

        expected_backfilled_coupons = 5  # 1 per member
        backfill_number = 2 * 7  # 2 x number max slots
        backfill_rank = 3

        category_assignment = current_assignment.where(
            col("slot_nbr").isin(category_backfill_slots)
        )

        members = category_assignment.select(col("mbrshp_sid")).distinct()

        coupon_bank = read_subset_and_cast(
            self.config.paths["COUPON_BANK"], "csv"
        )
        coupon_category = (
            read_subset_and_cast(self.config.paths["COUPON_MAP"], "csv")
            .withColumn(
                "category",
                when(col("ah4_cd").isNotNull(), col("ah4_cd"))
                .when(col("ah5_cd").isNotNull(), col("ah5_cd"))
                .otherwise(col("article_nbr")),
            )
            .select("cpn_nbr", "category")
        )
        cf_pred = read_subset_and_cast(
            self.config.paths["PRED_LIST"], "parquet"
        ).select(
            col("MBRSHP_SID").alias("mbrshp_sid"),
            col("CATEGORY_ID").alias("category"),
            col("prediction"),
        )
        coupons = coupon_bank.join(coupon_category, "cpn_nbr", "inner")
        coupons = coupons.join(cf_pred, "category", "inner")

        normalized_rank = Window.partitionBy("mbrshp_sid").orderBy("rank")
        expected_backfill = rank_by_agg_col(
            members,
            coupons,
            None,
            backfill_number,
            ["cpn_nbr", "offer_id", "cpn_class_id"],
            "prediction",
            seed=1,
        )
        expected_backfill = expected_backfill.withColumn(
            "rank", rank().over(normalized_rank)
        )

        compare_assignment = category_assignment.join(
            expected_backfill, ["mbrshp_sid", "cpn_nbr"], "inner"
        ).withColumn(
            "incorrect_assignment",
            when(
                col("slot_nbr") - normalization_unit != col("rank"), lit(1)
            ).otherwise(lit(0)),
        )

        total_correct_assignments = compare_assignment.where(
            col("incorrect_assignment") == 0
        ).count()

        # one member already has the global backfilling coupon, as determined
        # by _bf_rank_by_agg_col, as part of his frontfill
        self.assertEqual(
            total_correct_assignments, expected_backfilled_coupons - 1
        )

        # for this member check to see if the backfill is still correct
        # the coupon for this member should at the position "backfill_rank"
        is_actual_correct_assignment = compare_assignment.where(
            (col("incorrect_assignment") == 1) & (col("rank") == backfill_rank)
        ).count()

        self.assertEqual(is_actual_correct_assignment, 1)

        self.assertEqual(
            total_correct_assignments + is_actual_correct_assignment,
            expected_backfilled_coupons,
        )

    def assert_article_frontfill(self, current_assignment):
        article_frontfill_slots = [1, 2]
        expected_assigned_coupons = 2 * 5  # 2 coupons per member

        trips = read_subset_and_cast(
            self.config.paths["COUPON_MEMTRIP"], "parquet"
        )

        adjusted_trips_rank = Window.partitionBy("mbrshp_sid").orderBy(
            col("adjusted_trips").desc()
        )
        expected_assignment = trips.withColumn(
            "rank", rank().over(adjusted_trips_rank)
        )

        actual_assignment = current_assignment.where(
            col("slot_nbr").isin(article_frontfill_slots)
        )

        compare_assignment = actual_assignment.join(
            expected_assignment, ["mbrshp_sid", "cpn_nbr"], "inner"
        ).withColumn(
            "incorrect_assignment",
            when(col("slot_nbr") != col("rank"), lit(1)).otherwise(lit(0)),
        )

        total_incorrect_assignments = compare_assignment.where(
            col("incorrect_assignment") == 1
        ).count()

        self.assertEqual(total_incorrect_assignments, 0)
        self.assertEqual(compare_assignment.count(), expected_assigned_coupons)

    def assert_article_backfill(self, current_assignment):
        article_backfill_slots = [3]
        normalization_unit = 2  # how much to subtract to mimic range(1, x)

        expected_backfilled_coupons = 5  # 1 per member
        backfill_number = 2 * 7  # 2 x number max slots
        backfill_rank = 2

        members = current_assignment.select(col("mbrshp_sid")).distinct()

        trips = read_subset_and_cast(
            self.config.paths["COUPON_MEMTRIP"], "parquet"
        )
        coupon_bank = read_subset_and_cast(
            self.config.paths["COUPON_BANK"], "csv"
        )
        coupons_with_trips = coupon_bank.join(trips, ["cpn_nbr"], "inner")

        expected_assignment = rank_by_agg_col(
            members,
            coupons_with_trips,
            None,
            backfill_number,
            ["cpn_nbr", "offer_id", "cpn_class_id"],
            "trips",
            experiment_id=1,
        )

        normalized_rank = Window.partitionBy("mbrshp_sid").orderBy("rank")
        expected_assignment = expected_assignment.withColumn(
            "rank", rank().over(normalized_rank)
        )

        actual_assignment = current_assignment.where(
            col("slot_nbr").isin(article_backfill_slots)
        )

        compare_assignment = actual_assignment.join(
            expected_assignment, ["mbrshp_sid", "cpn_nbr"], "inner"
        ).withColumn(
            "incorrect_assignment",
            when(
                col("slot_nbr") - normalization_unit != col("rank"), lit(1)
            ).otherwise(lit(0)),
        )

        # one member already has the global backfilling coupon, as determined
        # by rank_by_agg_col, as part of his frontfill
        total_correct_assignments = compare_assignment.where(
            col("incorrect_assignment") == 0
        ).count()

        # for this member check to see if the backfill is still correct
        # the coupon for this member should at the position "backfill_rank"
        self.assertEqual(
            total_correct_assignments, expected_backfilled_coupons - 1
        )

        is_actual_correct_assignment = compare_assignment.where(
            (col("incorrect_assignment") == 1) & (col("rank") == backfill_rank)
        ).count()

        self.assertEqual(is_actual_correct_assignment, 1)

        self.assertEqual(
            total_correct_assignments + is_actual_correct_assignment,
            expected_backfilled_coupons,
        )

    def assert_basket_backfill(self, current_assignment):
        basket_backfill_slots = [7]
        normalization_unit = 6  # how much to subtract to mimic range(1, x)

        expected_backfilled_coupons = 5  # 1 per member
        backfill_number = 2 * 7  # 2 x number max slots

        members = current_assignment.select(col("mbrshp_sid")).distinct()
        coupon_bank = read_subset_and_cast(
            self.config.paths["COUPON_BANK"], "csv"
        )

        assignment_pool = _generate_cross_join(members, coupon_bank)
        assignment_pool = assignment_pool.dropDuplicates(
            subset=["MBRSHP_SID", "cpn_nbr"]
        )

        expected_assignment = hardest(
            members, assignment_pool, coupon_bank, backfill_number, seed=1
        )

        normalized_rank = Window.partitionBy("mbrshp_sid").orderBy("rank")
        expected_assignment = expected_assignment.withColumn(
            "rank", rank().over(normalized_rank)
        )

        actual_assignment = current_assignment.where(
            col("slot_nbr").isin(basket_backfill_slots)
        )

        compare_assignment = actual_assignment.join(
            expected_assignment, ["mbrshp_sid", "cpn_nbr"], "inner"
        ).withColumn(
            "incorrect_assignment",
            when(
                col("slot_nbr") - normalization_unit != col("rank"), lit(1)
            ).otherwise(lit(0)),
        )

        total_correct_assignments = compare_assignment.where(
            col("incorrect_assignment") == 0
        ).count()

        self.assertEqual(
            total_correct_assignments, expected_backfilled_coupons
        )

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        current_assignment = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )

        self.assert_category_frontfill(current_assignment)
        self.assert_category_backfill(current_assignment)

        self.assert_article_frontfill(current_assignment)
        self.assert_article_backfill(current_assignment)

        self.assert_basket_backfill(current_assignment)

    def execute_assignment_scripts(self):
        super(Backfill, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    Backfill.execute_test()
