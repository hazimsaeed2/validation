import pandas as pd
import pyspark.sql.functions as sqlf

import pe_memberdna.assignment.lib.assn_utils as assn_utils
import pe_memberdna.lib.iotools as iotools
import pe_memberdna.testing_support.regression_framework.regression as regression


class CFBAUDynamicCategoriesMultiCell(regression.RegressionTest):
    """
    Execute the multi cell design.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def tearDown(self):
        # In case of success the cleanup will be done by CFBAUDynamicCategoriesSingleCell
        self.print_results()

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

    def execute_assignment_scripts(self):
        super(
            CFBAUDynamicCategoriesMultiCell, self
        ).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


class CFBAUDynamicCategoriesSingleCell(regression.RegressionTest):
    """
    The regression test checks the CF BAU slot function using multiple
    category offers.

    The QC is against a multi cell design:
    1. 12 articles with no backfill + dummy
    2. 1 category with no backfill + 11 articles without backfill + dummy
    3. 1 category with backfill + 11 articles without backfill + dummy
    2. 2 category with no backfill + 10 articles + dummy
    3. 2 category with backfill + 10 articles + dummy
    """

    CONSTRUCT_SIZE = 13

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def tearDown(self):
        """
        Clean the output directories for assignment for both runs.

        Parameters:
            None
        Returns:
             None
        """
        self.print_results()

        if not self.wasSuccessful():
            return

        allowed_to_delete = [
            "REGRESSION_TESTS/assignment/{}".format(self.test_name)
        ]

        paths = [
            "OUTPUT_DIR",
            "COUPON_BANK",
            "COUPON_QUALS",
            "COUPON_MAP",
            "COUPON_MEMTRIP",
        ]
        for path_name in paths:
            path = self.config.paths[path_name]
            self.delete_path(path, allowed_to_delete)
            path = path.replace("single_cell", "multi_cell")
            self.delete_path(path, allowed_to_delete)

    def delete_path(self, path, allowed_to_delete):
        """
        Deletes and logs the files in path location.

        Parameters:
            path (str): location of files to delete
            allowed_to_delete (str): location that deleted files must be within

        Returns:
            None
        """
        self.log.info("Deleting {}".format(path))
        bucket, dest_key = iotools.split_path_bucket_key(path)
        iotools.s3_delete(bucket, dest_key, allowed_paths=allowed_to_delete)

    def get_multi_cell(self):
        path = self.config.paths["INPUT_ASSIGNMENTS"].replace(
            "single_cell", "multi_cell"
        )
        path = self.config.paths["INPUT_ASSIGNMENTS"].replace(
            "single_cell", "multi_cell"
        )
        return self.spark.read.option("header", "true").csv(path)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        coupon_bank = (
            assn_utils.read_subset_and_cast(
                self.config.paths["COUPON_BANK"], "csv"
            )
            .select("cpn_nbr", "cpn_type")
            .distinct()
        )

        assignment_with_multi_cells = self.get_multi_cell()
        assignment_with_multi_cells = assignment_with_multi_cells.withColumn(
            "is_backfill",
            sqlf.when(sqlf.col("bf_construct") == "-", 0).otherwise(1),
        ).join(coupon_bank, "cpn_nbr", "left")
        assignment_with_multi_cells = assignment_with_multi_cells.fillna(
            {"cpn_type": "None"}
        )

        assignment = (
            self.spark.read.csv(
                self.config.paths["INPUT_ASSIGNMENTS"],
                header=True,
                inferSchema=True,
            )
            .withColumn(
                "is_backfill",
                sqlf.when(sqlf.col("bf_construct") == "-", 0).otherwise(1),
            )
            .join(coupon_bank, "cpn_nbr", "left")
        )
        assignment = assignment.fillna({"cpn_type": "None"})

        qc = assignment_with_multi_cells.join(
            assignment,
            ["mbrshp_sid", "slot_nbr", "is_backfill", "cpn_type"],
            "inner",
        )

        self.assertEqual(
            assignment.groupBy("mbrshp_sid")
            .agg(sqlf.count("mbrshp_sid").alias("number"))
            .filter(sqlf.col("number") != self.CONSTRUCT_SIZE)
            .count(),
            0,
        )

        self.assertEqual(
            assignment_with_multi_cells.groupBy("mbrshp_sid")
            .agg(sqlf.count("mbrshp_sid").alias("number"))
            .filter(sqlf.col("number") != self.CONSTRUCT_SIZE)
            .count(),
            0,
        )

        self.assertEqual(
            assignment.count(), assignment_with_multi_cells.count()
        )
        self.assertEqual(qc.count(), assignment.count())

    def execute_assignment_scripts(self):
        super(
            CFBAUDynamicCategoriesSingleCell, self
        ).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    CFBAUDynamicCategoriesMultiCell.execute_test()
    CFBAUDynamicCategoriesSingleCell.execute_test()
