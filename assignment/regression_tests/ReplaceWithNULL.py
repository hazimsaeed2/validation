import pe_memberdna.testing_support.regression_framework.regression as regression


class ReplaceWithNULL(regression.RegressionTest):
    """
    Test that TO_REPLACE value set in construct .json as a static slot on the
    3rd position is replaced with NULL.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)
        slot_number_under_test = 3

        input_assignments = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )

        slots_under_test = input_assignments.filter(
            input_assignments["slot_nbr"] == slot_number_under_test
        )

        null_coupon_numbers = input_assignments.filter(
            input_assignments["cpn_nbr"].isNull()
        )

        slots_under_test_count = slots_under_test.count()
        null_coupon_numbers_count = null_coupon_numbers.count()

        self.assertEqual(slots_under_test_count, null_coupon_numbers_count)

        slots_under_test = slots_under_test.withColumnRenamed(
            "cpn_nbr", "cpn_nbr_1"
        )
        null_coupon_numbers = null_coupon_numbers.withColumnRenamed(
            "cpn_nbr", "cpn_nbr_2"
        )

        slots_under_test_are_null = slots_under_test.join(
            null_coupon_numbers,
            [
                "mbrshp_sid",
                "slot_nbr",
                "experiment_id",
                "cell_id",
                "construct",
            ],
            "inner",
        )
        slots_under_test_are_null = slots_under_test_are_null.filter(
            slots_under_test_are_null["cpn_nbr_1"].isNull()
            & slots_under_test_are_null["cpn_nbr_2"].isNull()
        )

        slots_under_test_are_null_count = slots_under_test_are_null.count()

        self.assertEqual(slots_under_test_count, null_coupon_numbers_count)

        self.assertEqual(
            slots_under_test_are_null_count, slots_under_test_count
        )

        self.assertEqual(
            slots_under_test_are_null_count, null_coupon_numbers_count
        )

    def execute_assignment_scripts(self):
        super(ReplaceWithNULL, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
                "backtest_sizing.py",
                "generate_output_file.py",
                "qc_assignments.py",
            ]
        )


if __name__ == "__main__":
    ReplaceWithNULL.execute_test()
