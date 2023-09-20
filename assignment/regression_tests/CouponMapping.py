import re

from pe_member_dna.pipelines.assignment.lib.assn_utils import CONSTRUCT_COLUMN
import pe_memberdna.testing_support.regression_framework.regression as regression


class CouponMapping(regression.RegressionTest):
    """
    The regression test checks that the assignment output is mapped correctly
    based on the slot_map parameter defined in the construct.

    Following are the two cases that this regression test covers:
    1. Cell 1 (construct 10001) assigns static cpn_nbr A,B and C to 3 slots and
        defines a slot map = {1:3, 2:1, 3:2}
    2. Cell 2 (Multi-construct 10002 and 10003) constains the following:
        Construct 10002 assigns static cpn_nbr A and B to 2 slots and defines a
        slot_map = {1:2, 2:1}
        Construct 10003 assigns static cpn_nbr C and D to 2 slots and defines a
        slot_map = {1:3, 2:4, 3:1, 4:2}

    This regression_tests test verifies the following:
    1. Cell mapping is applied to both the cells correctly.
    2. In case of multi-construct (cell 2), only the mapping of the second
        constuct is applied
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def validate_coupon_mapping(self):
        """
        This function reads the assignment_allconstructs output and checks if
        the slot mapping is correctly applied to the 2 cells described above
        """
        assignment = self.spark.read.parquet(
            self.config.paths["CONSTRUCTS_PATH"]
        )
        for row in assignment.collect():
            construct, slot = re.match(
                CONSTRUCT_COLUMN, row.construct
            ).groups()
            if construct == "10001":
                if slot == "1":
                    self.assertEquals(row.cpn_nbr, "B")
                elif slot == "2":
                    self.assertEquals(row.cpn_nbr, "C")
                elif slot == "3":
                    self.assertEquals(row.cpn_nbr, "A")
                else:
                    raise Exception(
                        "Slot number {} does not exist in Construct 10001".format(
                            slot
                        )
                    )
            elif construct == "10002_10003":
                if slot == "1":
                    self.assertEquals(row.cpn_nbr, "C")
                elif slot == "2":
                    self.assertEquals(row.cpn_nbr, "D")
                elif slot == "3":
                    self.assertEquals(row.cpn_nbr, "A")
                elif slot == "4":
                    self.assertEquals(row.cpn_nbr, "B")
                else:
                    raise Exception(
                        "Slot number {} does not exist in Construct 10002_10003".format(
                            slot
                        )
                    )
            else:
                raise Exception(
                    "Construct {} does not exist".format(construct)
                )

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)
        self.validate_coupon_mapping()

    def execute_assignment_scripts(self):
        super(CouponMapping, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    CouponMapping.execute_test()
