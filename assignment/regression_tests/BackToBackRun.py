import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt

import pe_memberdna.assignment.lib.assn_utils as assn_utils
import pe_memberdna.lib.iotools as iotools
import pe_memberdna.testing_support.regression_framework.regression as regression


class BackToBackFirstRun(regression.RegressionTest):
    """
    This regression test is the first test from the suite.
    It does not test anything by itself, the rest of the tests use Avoid B2B
    feature in order to not assign the same coupons assign by this run.

    For more details please check and the CDSA.

    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def setUp(self):
        msmt_cells = (
            self.spark.read.option("header", "True")
            .csv(self.config.paths["MSMT_CELL"])
            .limit(0)
            .toPandas()
        )
        iotools.write_local_to_s3(msmt_cells, self.config.paths["MSMT_CELL"])

        assignments = self.spark.createDataFrame(
            [[-1, -1, "0", "123456", 99.0]],
            sqlt.StructType(
                [
                    sqlt.StructField("mbrshp_sid", sqlt.IntegerType()),
                    sqlt.StructField("experiment_id", sqlt.IntegerType()),
                    sqlt.StructField("slot_nbr", sqlt.StringType()),
                    sqlt.StructField("cpn_nbr", sqlt.StringType()),
                    sqlt.StructField("cell_id", sqlt.DoubleType()),
                ]
            ),
        )
        assignments.write.mode("overwrite").partitionBy("cell_id").parquet(
            self.config.paths["MSMT_ASSGN"]
        )

        afeynmants = self.spark.createDataFrame(
            [[-1, -1, "0", "123456", 99.0, 99.0]],
            sqlt.StructType(
                [
                    sqlt.StructField("mbrshp_sid", sqlt.IntegerType()),
                    sqlt.StructField("experiment_id", sqlt.IntegerType()),
                    sqlt.StructField("slot_nbr", sqlt.StringType()),
                    sqlt.StructField("feynman_cpn_nbr", sqlt.StringType()),
                    sqlt.StructField("cell_id", sqlt.DoubleType()),
                    sqlt.StructField("feynman_cell_id", sqlt.DoubleType()),
                ]
            ),
        )
        afeynmants.write.mode("overwrite").partitionBy(
            "feynman_cell_id"
        ).parquet(self.config.paths["MSMT_AFEYN"])

    def tearDown(self):
        self.print_results()

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

    def execute_assignment_scripts(self):
        super(BackToBackFirstRun, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
                "backtest_sizing.py",
                "generate_output_file.py",
                "generate_msmt_input.py",
            ]
        )


class BackToBackSecondRun(regression.RegressionTest):
    """
    This regression test is the third test to run, but the second to get in
    home.
    It uses both Avoid B2B(against the first run) and Avoid B2B Future(against
    the third run) to assign coupons which were not assigned in the other 2
    runs. It uses a 35 day exclusion around its inhome date.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)
        past_assignments = assn_utils.read_subset_and_cast(
            self.config.paths["CDSA_ASSGN"], "parquet"
        )

        current_assignment = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["INPUT_ASSIGNMENTS"])
            .filter(sqlf.col("bf_construct") == "-")
        )

        common_coupons = (
            past_assignments.select("mbrshp_sid", "cpn_nbr", "experiment_id")
            .withColumnRenamed("experiment_id", "prev_exp_id")
            .join(current_assignment, ["mbrshp_sid", "cpn_nbr"], "inner")
        )

        self.assertTrue(past_assignments.count() > 0)
        self.assertTrue(current_assignment.count() > 0)
        self.assertEqual(common_coupons.count(), 0)

    def execute_assignment_scripts(self):
        super(BackToBackSecondRun, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
            ]
        )


class BackToBackThirdRun(regression.RegressionTest):
    """
    This regression test is the second test to run, but the third to get in
    home.
    This test uses Avoid B2B in order to not assign the same coupons as the
    first run did(uses a 60 day exclusion).
    Also it is used by the second run in order to test Avoid B2B Future(avoid
    assigning the same coupons with a another run which has an inhome date in
    the future)
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def tearDown(self):
        self.print_results()

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        past_assignments = assn_utils.read_subset_and_cast(
            self.config.paths["CDSA_ASSGN"], "parquet"
        )
        first_assignment = past_assignments.filter(
            sqlf.col("experiment_id") == 1
        )
        current_assignment = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["INPUT_ASSIGNMENTS"])
            .filter(sqlf.col("bf_construct") == "-")
        )

        common_coupons = first_assignment.select("mbrshp_sid", "cpn_nbr").join(
            current_assignment, ["mbrshp_sid", "cpn_nbr"], "inner"
        )

        self.assertTrue(first_assignment.count() > 0)
        self.assertTrue(current_assignment.count() > 0)
        self.assertEqual(common_coupons.count(), 0)

    def execute_assignment_scripts(self):
        super(BackToBackThirdRun, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
                "backtest_sizing.py",
                "generate_output_file.py",
                "generate_msmt_input.py",
            ]
        )


if __name__ == "__main__":
    BackToBackFirstRun.execute_test()
    BackToBackThirdRun.execute_test()
    BackToBackSecondRun.execute_test()
