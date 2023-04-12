from pyspark.sql.functions import col

import pe_memberdna.testing_support.regression_framework.regression as regression


class NoCoupon(regression.RegressionTest):
    """
    NoCoupon regression test tests BBM campaign that are assigning no
    coupons. The test verifies that the number of people in input mail list,
    output mail list and the assignment file is identical. It also verifies
    that the correct cpn_nbr is assigned to each cell.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        self.log.info("Testing campaign with no coupons...")

        MAIL_LIST = self.spark.read.option("header", "true").csv(
            self.config.paths["MAIL_LIST"]
        )

        MAIL_POPULATION_ASSIGNMENT = self.spark.read.option(
            "header", "true"
        ).csv(self.config.paths["MAIL_POPULATION_ASSIGNMENT"])

        num_member_in_MAIL_POPULATION_ASSIGNMENT = (
            MAIL_POPULATION_ASSIGNMENT.select(["mbrshp_sid"])
            .distinct()
            .count()
        )

        num_member_in_MAIL_LIST = (
            MAIL_LIST.select(["mbrshp_sid"]).distinct().count()
        )

        self.assertEqual(
            num_member_in_MAIL_LIST,
            num_member_in_MAIL_POPULATION_ASSIGNMENT,
            msg=(
                "The count of rows in the MAIL_POPULATION_ASSIGNMENT file "
                + "and MAIL_LIST file does not match. "
            ),
        )

        bad_cpn_nbr_cnt = MAIL_POPULATION_ASSIGNMENT.where(
            ~(
                ((col("cell_id") == "0") & (col("cpn_nbr") == "A"))
                | ((col("cell_id") == "1") & (col("cpn_nbr") == "B"))
            )
        ).count()

        self.assertEqual(
            bad_cpn_nbr_cnt,
            0,
            msg=(
                "There are {} rows in the output assingment file "
                + "where members with cell_id=0 did not receive cpn_nbr=A "
                + "or where members with cell_id=1 did not receive cpn_nbr=B "
            ).format(str(bad_cpn_nbr_cnt)),
        )

    def execute_assignment_scripts(self):
        super(NoCoupon, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "backtest_sizing.py",
                "generate_output_file.py",
                "qc_assignments.py",
            ]
        )


if __name__ == "__main__":
    NoCoupon.execute_test()
