import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt

from pe_member_dna.pipelines.lib.iotools import (
    s3_copy,
    split_path_bucket_key,
    s3_delete,
)
from pe_member_dna.pipelines.assignment.scripts.assign_closure import (
    ClosureMethod,
    CAT_HRRCHY,
    CAT_LIST,
)

import memberdna.testing_support.regression_framework.regression as regression


class AssignClosuresWithDedup(regression.RegressionTest):
    """
    The regression test checks the assign closure a5h dedup functionality.

    It has 5 basic asserts:
    1. it verifies that the output data has the same size after assignment
    2. check that the number of expected closures matches
    3. check that all 3 outputs have the same closures
    4. check that the assignment has unique ah5 per member
    5. check that the assignment has unique coupons per member

    The CDSA is based on a campaign execution having the data reduced to
    40 members.

    The test is using 4 cells, each construct has 12 slots.
    for more details please check the CDSA.

    """

    TOTAL_MEMBERS = 40
    TOTAL_SLOTS = 12
    TOTAL_EXPECTED_CLOSURES = 33

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

        bucket, src = split_path_bucket_key(
            self.config.paths["ASSN_LOC"] + "coupon_closure_test3.parquet"
        )
        bucket, dest = split_path_bucket_key(
            self.config.paths["COUPON_CLOSURE"]
        )
        s3_delete(
            bucket,
            dest,
            allowed_paths="/REGRESSION_TESTS/assignment/"
            "AssignClosuresWithDedup/ASSIGNMENTS/cdsa/",
        )

        s3_copy(bucket, src, dest)

        bucket, key = split_path_bucket_key(
            self.config.paths["CAP_ORIGINAL_INPUT_ASSIGNMENTS"]
        )
        s3_delete(
            bucket,
            key,
            allowed_paths="/REGRESSION_TESTS/assignment/"
            "AssignClosuresWithDedup/ASSIGNMENTS/cdsa/assn_output/",
        )

        bucket, key = split_path_bucket_key(
            self.config.paths["CAP_ORIGINAL_INPUT_CONSTRUCTS"]
        )
        s3_delete(
            bucket,
            key,
            allowed_paths="/REGRESSION_TESTS/assignment/"
            "AssignClosuresWithDedup/ASSIGNMENTS/cdsa/assn_output/",
        )

        bucket, key = split_path_bucket_key(
            self.config.paths["CAP_ORIGINAL_MAIL_POPULATION_ASSIGNMENT"]
        )
        s3_delete(
            bucket,
            key,
            allowed_paths="/REGRESSION_TESTS/assignment/"
            "AssignClosuresWithDedup/ASSIGNMENTS/cdsa/assn_output/",
        )

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        cap_mail = self.spark.read.option("header", "true").csv(
            self.config.paths["MAIL_POPULATION_ASSIGNMENT"]
        )
        cap_assignments = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )
        original_constructs = self.spark.read.parquet(
            self.config.paths["CAP_ORIGINAL_INPUT_CONSTRUCTS"]
        )
        cap_constructs = self.spark.read.parquet(
            self.config.paths["INPUT_CONSTRUCTS"]
        )

        cap_closure_coupons = self.spark.read.csv(
            self.config.paths["CLOSURE_COUPON_PATH"],
            header=True,
            inferSchema=True,
        )

        # check the same size
        self.assertEqual(cap_constructs.count(), original_constructs.count())

        # check expected number of closure
        self.assertEqual(
            cap_assignments.filter(
                cap_assignments[ClosureMethod.COLUMN_NAME]
                != ClosureMethod.NONE
            ).count(),
            self.TOTAL_EXPECTED_CLOSURES,
        )

        # check that the same cap closures are assigned to all 3 outputs
        result = (
            cap_mail.filter(
                sqlf.col(ClosureMethod.COLUMN_NAME) != ClosureMethod.NONE
            )
            .join(
                cap_assignments.filter(
                    sqlf.col(ClosureMethod.COLUMN_NAME) != ClosureMethod.NONE
                ),
                ["MBRSHP_SID", "construct", "cpn_nbr"],
                "inner",
            )
            .join(
                cap_constructs.filter(
                    sqlf.col(ClosureMethod.COLUMN_NAME) != ClosureMethod.NONE
                ),
                ["MBRSHP_SID", "construct", "cpn_nbr"],
                "inner",
            )
        )

        self.assertEqual(
            result.count(),
            cap_mail.filter(
                sqlf.col(ClosureMethod.COLUMN_NAME) != ClosureMethod.NONE
            ).count(),
        )
        self.assertEqual(
            result.count(),
            cap_assignments.filter(
                sqlf.col(ClosureMethod.COLUMN_NAME) != ClosureMethod.NONE
            ).count(),
        )
        self.assertEqual(
            result.count(),
            cap_constructs.filter(
                sqlf.col(ClosureMethod.COLUMN_NAME) != ClosureMethod.NONE
            ).count(),
        )

        cap_assignments_with_ah5 = (
            cap_assignments.filter(
                cap_assignments[ClosureMethod.COLUMN_NAME]
                != ClosureMethod.HIT_MAX
            )
            .join(
                cap_closure_coupons.select("PMR Offer ID", CAT_LIST),
                cap_assignments["cpn_nbr"]
                == cap_closure_coupons["PMR Offer ID"],
                "inner",
            )
            .withColumn(CAT_HRRCHY, sqlf.explode(sqlf.split(CAT_LIST, ",")))
            .withColumn(CAT_HRRCHY, sqlf.trim(sqlf.col(CAT_HRRCHY)))
            .withColumn(CAT_HRRCHY, sqlf.col(CAT_HRRCHY).cast(sqlt.LongType()))
        )

        # check unique AH5
        cap_assignments_with_ah5 = cap_assignments_with_ah5.groupBy(
            "MBRSHP_SID"
        ).agg(
            sqlf.size(sqlf.collect_set(CAT_HRRCHY)).alias("total_unique"),
            sqlf.size(sqlf.collect_list(CAT_HRRCHY)).alias("total"),
        )
        self.assertEqual(
            cap_assignments_with_ah5.filter(
                sqlf.col("total_unique") != sqlf.col("total")
            ).count(),
            0,
        )
        # check unique coupon
        unique_cpn_assignments = cap_assignments.groupBy("MBRSHP_SID").agg(
            sqlf.size(sqlf.collect_set("cpn_nbr")).alias("total_unique"),
            sqlf.size(sqlf.collect_list("cpn_nbr")).alias("total"),
        )
        self.assertEqual(
            unique_cpn_assignments.filter(
                sqlf.col("total_unique") != sqlf.col("total")
            ).count(),
            0,
        )

    def execute_assignment_scripts(self):
        super(AssignClosuresWithDedup, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
                "assign_closure.py 1 False None True",
            ]
        )


if __name__ == "__main__":
    AssignClosuresWithDedup.execute_test()
