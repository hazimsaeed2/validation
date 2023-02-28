import pyspark.sql.functions as sqlf

import memberdna.pipelines.assignment.lib.assn_utils as assn_utils
import memberdna.testing_support.regression_framework.regression as regression



class StaticBauTargeting(regression.RegressionTest):
    """
    The regression test checks static bau slot function: members are able
    to receive different offers based on their eligibility.

    In this particular test case, members are receiving different static
    offers: 1 static basket cpn or 1 static article coupon or no static.
    The rest of the slots up to number 12 are filled with articles using
    standard hook logic and standard backfill logic.

    The tests checks:
    1. different static coupons are assigned in the same cell
    2. there are no duplicates
    3. every members gets at most one static offer
    4. static coupons have higher priority than hooks
    5. compare with reference
    """

    TOTAL_SLOTS = 12
    TOTAL_MEMBERS = 40

    BASKET_CPN = 1000083
    ARTICLE_CPN = 2041124

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        coupon_bank = assn_utils.read_subset_and_cast(
            self.config.paths["COUPON_BANK"], "csv"
        ).select("cpn_nbr", "cpn_type").distinct()

        input_assignments = self.spark.read.csv(
            self.config.paths["INPUT_ASSIGNMENTS"],
            header=True,
            inferSchema=True
        )

        assignment = input_assignments.withColumn(
            "is_backfill",
            sqlf.when(sqlf.col("bf_construct") == "-", 0).otherwise(1)
        ).join(
            coupon_bank,
            "cpn_nbr",
            "left"
        )

        assignment = assignment.fillna(
            {"cpn_type": "None"}
        )

        self.assertEquals(
            assignment.filter(sqlf.col("cpn_nbr") == self.BASKET_CPN).count(),
            4
        )
        self.assertEquals(
            assignment.filter(sqlf.col("cpn_nbr") == self.ARTICLE_CPN).count(),
            13
        )

        slots_per_member = assignment.groupBy("MBRSHP_SID").agg(
            sqlf.countDistinct("slot_nbr").alias("unique_slots"),
            sqlf.count("slot_nbr").alias("total_slots"),
        )
        self.assertEquals(
            slots_per_member.filter(
                (slots_per_member["unique_slots"] == self.TOTAL_SLOTS)
                & (slots_per_member["total_slots"] == self.TOTAL_SLOTS)
            ).count(),
            self.TOTAL_MEMBERS
        )

        unique_coupons = assignment.groupBy("MBRSHP_SID").agg(
            sqlf.countDistinct("cpn_nbr").alias("unique_cpns"),
        )
        self.assertEquals(
            unique_coupons.filter(
                unique_coupons["unique_cpns"] == self.TOTAL_SLOTS
            ).count(),
            self.TOTAL_MEMBERS
        )

        coupons = assignment.groupBy("MBRSHP_SID").agg(
            sqlf.collect_list("cpn_nbr").alias("cpn_list")
        )

        disjunct_static_offer = coupons.filter(
            sqlf.array_contains(sqlf.col("cpn_list"), self.BASKET_CPN) == True
        )
        self.assertEquals(
            disjunct_static_offer.filter(
                sqlf.array_contains(
                    sqlf.col("cpn_list"), self.ARTICLE_CPN
                ) == True
            ).count(),
            0
        )

        priority = assignment.filter(
            sqlf.col("cpn_nbr").isin(
                [sqlf.lit(self.BASKET_CPN), sqlf.lit(self.ARTICLE_CPN)]
            )
        ).select("slot_nbr").distinct().toPandas().slot_nbr.tolist()

        self.assertEquals([1], priority)

        prev = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["ASSN_LOC"] + "reference_assignment/")
            .fillna("NA", subset=["cpn_nbr"])
        )
        same = input_assignments.join(prev, input_assignments.columns, "inner")
        self.assertEqual(prev.count(), same.count())

    def execute_assignment_scripts(self):
        super(StaticBauTargeting, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py"
            ]
        )

if __name__ == "__main__":
    StaticBauTargeting.execute_test()
