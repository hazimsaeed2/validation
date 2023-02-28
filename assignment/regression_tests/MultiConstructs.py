from pyspark.sql.functions import col, countDistinct, lit, when

import memberdna.testing_support.regression_framework.regression as regression


class MultiConstructs(regression.RegressionTest):

    """
    The purpose of the test is to check the basic use cases for
    multi constructs.

    The test data is split into 3 cells:
        1. The first cell has multi construct(0.json and 1.json) which
        has a total of 12 coupons(1 category and 11 articles).
        2. The second cell has a simple construct (2.json) which has 12
        coupons (4 slots with static coupons and 8 with articles)
        3. The third cell has a multi construct (3.json, 4.json, 5.json and
        and 6.json) mimicking the second cell's layout. Construct 3 has 1 slot,
        construct 4 has 2 slots, construct 5 has 1 slot and construct 6 has 8
        slots.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)
        expected_coupons_per_member = 12
        total_matching_slots = 4

        input_assignments = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )
        coupon_file = self.spark.read.option("header", "true").csv(
            self.config.paths["CDSA_CPN_BANK"]
        )
        coupon_file = coupon_file.select(["cpn_nbr", "cpn_type"]).distinct()

        input_assignments = input_assignments.join(
            coupon_file, "cpn_nbr", "left"
        )

        cell_0 = input_assignments.filter(col("cell_id") == 0)
        cell_0 = cell_0.withColumn(
            "incorrect_assignment",
            when(
                (col("slot_nbr") == 1) & (col("cpn_type") == "category"),
                lit(0),
            )
            .when(
                (col("slot_nbr") != 1) & (col("cpn_type") == "article"), lit(0)
            )
            .otherwise(lit(1)),
        )
        self.assertEqual(
            cell_0.select(["incorrect_assignment"])
            .groupBy()
            .sum()
            .collect()[0][0],
            0,
        )

        are_top4_slots_identical_for_cell_1_and_2 = (
            input_assignments.filter(
                col("cell_id").isin([1, 2])
                & col("slot_nbr").isin([1, 2, 3, 4])
            )
            .groupBy("slot_nbr")
            .agg(countDistinct("cpn_nbr").alias("number_of_distinct_coupons"))
            .where(col("number_of_distinct_coupons") == 1)
            .count()
            == total_matching_slots
        )
        self.assertTrue(are_top4_slots_identical_for_cell_1_and_2)

        cell_1_and_2_articles = input_assignments.filter(
            col("cell_id").isin([1, 2])
        ).filter(col("slot_nbr") > total_matching_slots)
        cell_1_and_2_articles = cell_1_and_2_articles.withColumn(
            "incorrect_assignment",
            when((col("cpn_type") == "article"), lit(0)).otherwise(lit(1)),
        )
        self.assertEqual(
            cell_1_and_2_articles.select("incorrect_assignment")
            .groupBy()
            .sum()
            .collect()[0][0],
            0,
        )

        coupons_per_member = input_assignments.groupBy("mbrshp_sid").agg(
            countDistinct("cpn_nbr").alias("number_of_distinct_coupons")
        )
        correct_assigned_coupons = coupons_per_member.withColumn(
            "incorrect_assignment",
            when(
                (
                    col("number_of_distinct_coupons")
                    == expected_coupons_per_member
                ),
                lit(0),
            ).otherwise(lit(1)),
        )
        self.assertEqual(
            correct_assigned_coupons.select("incorrect_assignment")
            .groupBy()
            .sum()
            .collect()[0][0],
            0,
        )

    def execute_assignment_scripts(self):
        super(MultiConstructs, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    MultiConstructs.execute_test()
