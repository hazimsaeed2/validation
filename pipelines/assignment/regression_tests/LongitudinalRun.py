import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt

import memberdna.pipelines.assignment.lib.campaign as campaign
from pe_member_dna.pipelines.assignment.lib.assn_utils import (
    load_past_longitudinal_mbrs,
)
import memberdna.pipelines.assignment.lib.schemas.cdsa_schemas as schemas
import memberdna.pipelines.lib.iotools as iotools
import memberdna.testing_support.regression_framework.regression as regression


class LongitudinalFirstRun(regression.RegressionTest):
    """
    Test the first run for logitudinal.

    The test uses 7 logitudinal cells (3 control, 4 tests) + 1 normal cell and
    verifies that the member pool for each longitudinal cell stays the same
    between 2 test runs.

    It also tests that longitudinal segments catch appropriately filter mbrs.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def setUp(self):
        cells = (
            self.spark.read.csv(
                self.config.paths["CDSA_LOC"] + "cells_template.csv",
                header=True,
                schema=schemas.CELLS_SCHEMA,
            )
            .filter(
                sqlf.col("experiment_id") == self.config.params["experiment"]
            )
            .toPandas()
        )
        iotools.write_local_to_s3(cells, self.config.paths["CELL"])

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
        # In case of success the cleanup will be done by LongitudinalSecondRun
        self.print_results()

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        cells = campaign._read_and_subset_exp_csv(
            self.config.params["experiment"], self.config.paths["CELL"]
        )
        cells = cells.astype("str")
        cell_ids = cells[cells.longitudinal_id.notnull()].cell_id.tolist()

        previous_assignment = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["ASSN_LOC"] + "assignments")
            .filter(sqlf.col("cell_id").isin(cell_ids))
        )
        current_assignment = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["INPUT_ASSIGNMENTS"])
            .filter(sqlf.col("cell_id").isin(cell_ids))
        )

        overlap_assignment = previous_assignment.join(
            current_assignment, ["mbrshp_sid", "cell_id", "cpn_nbr"], "inner"
        )
        overlap_count = overlap_assignment.count()

        self.assertEqual(
            previous_assignment.count(),
            overlap_count,
            "The input run has assignments that are different from the 1st regression run",
        )
        self.assertEqual(
            current_assignment.count(),
            overlap_count,
            "The 1st regression run has all of the assignments from the input run and more",
        )

        ltv = self.spark.read.option("header", "true").csv(
            self.config.paths["LTV_PATH"]
        )

        cell_counts = (
            current_assignment.dropDuplicates(["mbrshp_sid"])
            .groupby("cell_id")
            .count()
            .sort("cell_id")
        ).toPandas()["count"]
        cell_counts.index = cell_counts.index.astype("str")

        ctrl_tests_map = {"0": ["1"], "2": ["3"], "4": ["5", "6"]}

        ltv_counts = (
            (
                ltv.dropDuplicates(["mbrshp_sid"])
                .groupby("LTV_CLASS")
                .count()
                .sort("LTV_CLASS")
            )
            .toPandas()["count"]
            .tolist()
        )

        for ltv_count, (ctrl_cell, test_cells) in zip(
            ltv_counts, ctrl_tests_map.items()
        ):
            cells = test_cells + [ctrl_cell]
            total_cell_count = cell_counts[cells].sum()
            self.assertEqual(
                ltv_count,
                total_cell_count,
                f"The assingment counts from the 1st regression run for cells {cells} do not match the counts from the LTV file",
            )

    def execute_assignment_scripts(self):
        super(LongitudinalFirstRun, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
                "backtest_sizing.py",
                "generate_output_file.py",
                "qc_assignments.py",
                "generate_msmt_input.py",
            ]
        )


class LongitudinalSecondRun(regression.RegressionTest):
    """
    Test the second run for logitudinal.

    The test uses 7 logitudinal cells (3 control, 4 test) + 1 normal cell and
    verifies that the member pool:
        1. increases for cells in the first logitudinal cell handshake group
        2. decreases for cells in the second longitudinal cell handshake group
        3. stays the same for cells in the third longitudinal cell handshake group
        4. has no member longitudinal crossover within a handshake group
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def setUp(self):
        cells = self.spark.read.csv(
            self.config.paths["CDSA_LOC"] + "cells_template.csv",
            header=True,
            schema=schemas.CELLS_SCHEMA,
        ).toPandas()
        iotools.write_local_to_s3(cells, self.config.paths["CELL"])

    def tearDown(self):
        """Clean the output directories for assignment for both runs.

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
            "MSMT_ASSGN",
            "MSMT_AFEYN",
        ]
        for path_name in paths:
            path = self.config.paths[path_name]
            self.delete_path(path, allowed_to_delete)
            path = path.replace("second_run", "first_run")
            self.delete_path(path, allowed_to_delete)

        path = self.config.paths["MSMT_CELL_ARCHIVE"]
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

    def get_first_run(self, cells):
        """
        Gets the first longitudinal regression test's assignments.

        Parameters:
            cells (pyspark.sql.DataFrame): cell level campaign information

        Returns:
            long_mbrs (pyspark.sql.DataFrame): past assigned mbrs with cell_id
        """
        cell_rows = cells.filter("longitudinal_id is not NULL").collect()
        first_run = None

        for cell in cell_rows:
            longitudinal_id = int(float(cell["longitudinal_id"]))

            long_mbrs = load_past_longitudinal_mbrs(
                longitudinal_id,
                self.config.params["experiment"],
                self.config.paths["CDSA_ASSGN"],
                self.config.paths["CELL"],
            )
            long_mbrs = long_mbrs.withColumn(
                "cell_id", sqlf.lit(cell["cell_id"])
            )

            if first_run is None:
                first_run = long_mbrs
            else:
                first_run = first_run.union(long_mbrs)

        return first_run

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        cells = self.spark.read.option("header", "true").csv(
            self.config.paths["CELL"]
        )

        previous_assignment = self.get_first_run(cells)

        current_assignment = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )
        current_assignment = current_assignment.select(
            "mbrshp_sid", "cell_id"
        ).dropDuplicates()

        previous_assignment = previous_assignment.join(
            cells.select("cell_id", "longitudinal_id"),
            how="left",
            on="cell_id",
        )
        previous_assignment = previous_assignment.withColumnRenamed(
            "longitudinal_id", "prev_longitudinal_id"
        )

        current_assignment = current_assignment.join(
            cells.select("cell_id", "longitudinal_id"),
            how="left",
            on="cell_id",
        )

        checks = [self.assertGreater, self.assertLess, self.assertEqual]

        ctrl_tests_map = {"8": ["9"], "10": ["11"], "12": ["13", "14"]}
        n_cells = len(ctrl_tests_map)
        for test_cells in ctrl_tests_map.values():
            n_cells += len(test_cells)

        for (ctrl_cell, test_cells), check in zip(
            ctrl_tests_map.items(), checks
        ):
            cell_group = test_cells + [ctrl_cell]

            for cell_id in cell_group:
                prev_cell_id = str(int(cell_id) - n_cells - 1)
                current_count = current_assignment.filter(
                    sqlf.col("cell_id") == cell_id
                ).count()
                previous_count = previous_assignment.filter(
                    sqlf.col("cell_id") == prev_cell_id
                ).count()
                current_msg = f"current cell {cell_id} {current_count} mbrs"
                previous_msg = (
                    f"prev cell {prev_cell_id} {previous_count} mbrs"
                )
                msg = f"{current_msg}; {previous_msg}"
                check(current_count, previous_count, msg)

            prev_cell_group = [
                str(int(cell_id) - n_cells - 1) for cell_id in cell_group
            ]

            current_group_assignment = current_assignment.filter(
                sqlf.col("cell_id").isin(cell_group)
            )
            prev_group_assignment = previous_assignment.filter(
                sqlf.col("cell_id").isin(prev_cell_group)
            )

            assignment_comparison = current_group_assignment.join(
                prev_group_assignment, on="mbrshp_sid", how="inner"
            )

            overlap = assignment_comparison.filter(
                "longitudinal_id != prev_longitudinal_id"
            )

            self.assertEqual(
                overlap.count(), 0, "There are mbrs who switched longitudinal"
            )

    def execute_assignment_scripts(self):
        super(LongitudinalSecondRun, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    LongitudinalFirstRun.execute_test()
    LongitudinalSecondRun.execute_test()
