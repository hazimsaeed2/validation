import pyspark.sql.functions as sqlf
import pe_memberdna.testing_support.regression_framework.regression as regression


class RewardsContent(regression.RegressionTest):
    """
    The regression test checks reward content slot function:
        - it checks that 2 members get label A
        - it checks that 2 members get label B(third member eligible for this
         has the trial flag set)
        - it checks that 2 members get label C
        - it checks that 1 members get label D
        - rest get lable E

    It uses a composed construct, one of which uses the reward_content().
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        input_assignments = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["INPUT_ASSIGNMENTS"])
            .filter(sqlf.col("slot_nbr") == 13)
        )

        self.assertEqual(
            input_assignments.filter(
                (sqlf.col("MBRSHP_SID") == 110) & (sqlf.col("cpn_nbr") == "A")
            ).count(),
            1,
        )

        self.assertEqual(
            input_assignments.filter(
                (sqlf.col("MBRSHP_SID") == 601) & (sqlf.col("cpn_nbr") == "A")
            ).count(),
            1,
        )

        self.assertEqual(
            input_assignments.filter(
                (sqlf.col("MBRSHP_SID") == 1978) & (sqlf.col("cpn_nbr") == "B")
            ).count(),
            1,
        )

        self.assertEqual(
            input_assignments.filter(
                (sqlf.col("MBRSHP_SID") == 2110) & (sqlf.col("cpn_nbr") == "B")
            ).count(),
            1,
        )

        self.assertEqual(
            input_assignments.filter(
                (sqlf.col("MBRSHP_SID") == 69) & (sqlf.col("cpn_nbr") == "C")
            ).count(),
            1,
        )

        self.assertEqual(
            input_assignments.filter(
                (sqlf.col("MBRSHP_SID") == 1840) & (sqlf.col("cpn_nbr") == "C")
            ).count(),
            1,
        )

        self.assertEqual(
            input_assignments.filter(
                (sqlf.col("MBRSHP_SID") == 3620) & (sqlf.col("cpn_nbr") == "D")
            ).count(),
            1,
        )

        self.assertEqual(
            input_assignments.filter(
                (sqlf.col("MBRSHP_SID") == 183) & (sqlf.col("cpn_nbr") == "E")
            ).count(),
            1,
        )

        self.assertEqual(
            input_assignments.filter(sqlf.col("cpn_nbr") == "E").count(), 33
        )

    def execute_assignment_scripts(self):
        super(RewardsContent, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    RewardsContent.execute_test()
