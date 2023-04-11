import unittest
import xmlrunner

import pyspark.sql.functions as sqlf

import pe_memberdna.testing_support.regression_framework.regression as regression


class AnniversaryDynamicContent(regression.RegressionTest):
    """
    Anniversary dynamic content tends to be unique in that it allows backfill.
    Using waterfall slots here is also unique because the slot function was
    originally intended to allow multiple sequential backfill logic to be
    applied. However, here we know the exact value for every slot per the JSON,
    no backfills.

    This test ensures that the use of waterfall slots does not shift, i.e.
    lose, any of our anniversary slots.

    It should assign 7 dummy slots, 1 category and 11 articles. Dummy slots
    should allow duplicates while the others should not.
    """

    TOTAL_MEMBERS = 40
    TOTAL_SLOTS = 19

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        input_assignments = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["INPUT_ASSIGNMENTS"])
            .fillna("NA", subset=["cpn_nbr"])
        )

        prev = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["ASSN_LOC"] + "reference_assignment/")
            .fillna("NA", subset=["cpn_nbr"])
        )

        # test - each member has the exact slots
        slots_per_member = input_assignments.groupBy("MBRSHP_SID").agg(
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
        same = input_assignments.join(prev, input_assignments.columns, "inner")

        self.assertEqual(prev.count(), same.count())

    def execute_assignment_scripts(self):
        super(AnniversaryDynamicContent, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    AnniversaryDynamicContent.execute_test()
