import pyspark.sql.functions as sqlf
from pemember_dna.pipelines.assignment.scripts.assign_closure import (
    ClosureMethod,
)
from pemember_dna.pipelines.lib.iotools import (
    s3_copy,
    s3_delete,
    split_path_bucket_key,
)

import pe_memberdna.testing_support.regression_framework.regression as regression


class AssignClosures(regression.RegressionTest):
    """
    The regression test checks the assign closure functionality.

    It has 5 basic asserts:
    1. it verifies that the output data has the same size after assignment
    2. it checks that all members have their 12 slots populated
    3. it checks that all members got unique coupons
    4. it checks that the swap was made taking the eligibility into account
    5. it checks that the distribution for hitting min/max is done in
     proper order and was using the eligibility.
    6. it checks that all 3 outputs have the same closures
    7. it checks that the number of expected closures

    The CDSA is based on a campaign execution having the data reduced to
    40 members.

    The test is using 4 cells, each construct has 12 slots.
    for more details please check the CDSA.

    """

    TOTAL_MEMBERS = 40
    TOTAL_SLOTS = 12
    TOTAL_EXPECTED_CLOSURES = 25

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

        bucket, src = split_path_bucket_key(
            self.config.paths["ASSN_LOC"] + "coupon_closure_test.parquet"
        )
        bucket, dest = split_path_bucket_key(
            self.config.paths["COUPON_CLOSURE"]
        )

        s3_delete(
            bucket,
            dest,
            allowed_paths="/REGRESSION_TESTS/assignment/AssignClosures/"
            "ASSIGNMENTS/cdsa/",
        )

        s3_copy(bucket, src, dest)

        bucket, key = split_path_bucket_key(
            self.config.paths["CAP_ORIGINAL_INPUT_ASSIGNMENTS"]
        )
        s3_delete(
            bucket,
            key,
            allowed_paths="/REGRESSION_TESTS/assignment/AssignClosures/"
            "ASSIGNMENTS/cdsa/assn_output/",
        )

        bucket, key = split_path_bucket_key(
            self.config.paths["CAP_ORIGINAL_INPUT_CONSTRUCTS"]
        )
        s3_delete(
            bucket,
            key,
            allowed_paths="/REGRESSION_TESTS/assignment/AssignClosures/"
            "ASSIGNMENTS/cdsa/assn_output/",
        )

        bucket, key = split_path_bucket_key(
            self.config.paths["CAP_ORIGINAL_MAIL_POPULATION_ASSIGNMENT"]
        )
        s3_delete(
            bucket,
            key,
            allowed_paths="/REGRESSION_TESTS/assignment/AssignClosures/"
            "ASSIGNMENTS/cdsa/assn_output/",
        )

    def assert_correct_distribution(
        self,
        member,
        coupons,
        closure_bank,
        cap_closure_coupons,
        current_method,
    ):
        under_test_coupons = coupons.filter(
            coupons[ClosureMethod.COLUMN_NAME] == current_method
        ).orderBy(sqlf.col("slot_nbr").desc())

        possible_slots = [
            entry["slot_nbr"] for entry in under_test_coupons.collect()
        ]

        eligible_coupons = (
            closure_bank.filter(
                (closure_bank["MBRSHP_SID"] == member)
                & (closure_bank["closure_flag"] == 1)
            )
            .join(
                cap_closure_coupons.select("PMR Offer ID", "closure_priority"),
                closure_bank["cpn_nbr"] == cap_closure_coupons["PMR Offer ID"],
                "inner",
            )
            .join(under_test_coupons, ["MBRSHP_SID", "cpn_nbr"])
            .orderBy("closure_priority")
        )

        for idx, coupon in enumerate(eligible_coupons.collect()):
            self.assertEquals(coupon["slot_nbr"], possible_slots[idx])

            if coupon["slot_nbr"] <= 10:
                self.assertFalse(coupon["bf_construct"] == "-")

    def check_closure(self, closure_path, original_path):
        cap_closure_coupons = self.spark.read.option("header", "true").csv(
            self.config.paths["CLOSURE_COUPON_PATH"]
        )

        closure_bank = self.spark.read.parquet(
            self.config.paths["COUPON_CLOSURE"]
        )

        current_assignment = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths[original_path])
            .withColumn("slot_nbr", sqlf.col("slot_nbr").cast("integer"))
        )
        current_closures = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths[closure_path])
            .withColumn("slot_nbr", sqlf.col("slot_nbr").cast("integer"))
        )

        ###################################
        # CHECK QUANTITY
        ###################################

        # test - same number of assignments
        self.assertEquals(current_assignment.count(), current_closures.count())

        # test - each member has the exact slots
        slots_per_member = current_closures.groupBy("MBRSHP_SID").agg(
            sqlf.countDistinct("slot_nbr").alias("unique_slots"),
            sqlf.count("slot_nbr").alias("total_slots"),
        )
        self.assertEquals(
            slots_per_member.filter(
                (slots_per_member["unique_slots"] == self.TOTAL_SLOTS)
                & (slots_per_member["total_slots"] == self.TOTAL_SLOTS)
            ).count(),
            self.TOTAL_MEMBERS,
        )

        ###################################
        # CHECK QUALITY
        ###################################

        # test unique coupons

        unique_coupons = current_closures.groupBy("MBRSHP_SID").agg(
            sqlf.countDistinct("cpn_nbr").alias("unique_cpns"),
        )
        self.assertEquals(
            unique_coupons.filter(
                unique_coupons["unique_cpns"] == self.TOTAL_SLOTS
            ).count(),
            self.TOTAL_MEMBERS,
        )

        # test - swap count and swap correctness, but not order

        # swaps
        swaps = current_closures.join(
            cap_closure_coupons,
            (
                current_closures["cpn_nbr"]
                == cap_closure_coupons["PMR Offer ID"]
            )
            & (
                current_closures["original"]
                == cap_closure_coupons["equivalent_offer_id"]
            ),
            "inner",
        )
        swap_count = swaps.count()

        # eligible swap
        valid_swaps = swaps.join(
            closure_bank, ["MBRSHP_SID", "cpn_nbr"], "inner"
        ).count()

        # test count
        self.assertEquals(valid_swaps, swap_count)

        # test distribution order (closure_priority)
        # at least per method
        # test that the closures that were assgined were at least eligibly
        # assigned and in the correct order

        members = [
            row["MBRSHP_SID"]
            for row in current_assignment.select("MBRSHP_SID")
            .distinct()
            .collect()
        ]

        for member in members:
            coupons = current_closures.filter(
                current_closures["MBRSHP_SID"] == member
            )

            has_no_closure = (
                coupons.filter(
                    coupons[ClosureMethod.COLUMN_NAME].isin(
                        sqlf.lit(ClosureMethod.HIT_MAX),
                        sqlf.lit(ClosureMethod.HIT_MIN),
                    )
                ).count()
                == 0
            )
            if has_no_closure:
                continue

            # min distribution could be in slot marked with NONE, MIN or MAX
            # but not swap or natural
            self.assert_correct_distribution(
                member,
                coupons,
                closure_bank,
                cap_closure_coupons,
                ClosureMethod.HIT_MIN,
            )
            # max distribution could be in slot marked with NONEor MAX
            # but not swap, natural or min
            self.assert_correct_distribution(
                member,
                coupons,
                closure_bank,
                cap_closure_coupons,
                ClosureMethod.HIT_MAX,
            )

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)
        self.check_closure(
            "MAIL_POPULATION_ASSIGNMENT",
            "CAP_ORIGINAL_MAIL_POPULATION_ASSIGNMENT",
        )
        self.check_closure(
            "INPUT_ASSIGNMENTS",
            "CAP_ORIGINAL_INPUT_ASSIGNMENTS",
        )

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

        # check the same size
        self.assertEqual(cap_constructs.count(), original_constructs.count())

        # check expected number of closure
        self.assertEqual(
            cap_assignments.filter(
                cap_assignments["closure_method"] != ClosureMethod.NONE
            ).count(),
            self.TOTAL_EXPECTED_CLOSURES,
        )

        # check that the same cap closures are assigned to all 3 outputs
        result = (
            cap_mail.filter(sqlf.col("closure_method") != ClosureMethod.NONE)
            .join(
                cap_assignments.filter(
                    sqlf.col("closure_method") != ClosureMethod.NONE
                ),
                ["MBRSHP_SID", "construct", "cpn_nbr"],
                "inner",
            )
            .join(
                cap_constructs.filter(
                    sqlf.col("closure_method") != ClosureMethod.NONE
                ),
                ["MBRSHP_SID", "construct", "cpn_nbr"],
                "inner",
            )
        )

        self.assertEqual(
            result.count(),
            cap_mail.filter(
                sqlf.col("closure_method") != ClosureMethod.NONE
            ).count(),
        )
        self.assertEqual(
            result.count(),
            cap_assignments.filter(
                sqlf.col("closure_method") != ClosureMethod.NONE
            ).count(),
        )
        self.assertEqual(
            result.count(),
            cap_constructs.filter(
                sqlf.col("closure_method") != ClosureMethod.NONE
            ).count(),
        )

    def execute_assignment_scripts(self):
        super(AssignClosures, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
                "assign_closure.py 1 False None False",
            ]
        )


if __name__ == "__main__":
    AssignClosures.execute_test()
