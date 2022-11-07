import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt

from pe_member_dna.pipelines.assignment.lib.assn_utils import CONSTRUCT_COLUMN
import memberdna.testing_support.regression_framework.regression as regression


class CouponSorting(regression.RegressionTest):
    """
    The regression test checks that the assignment output is sorted correctly
    based on the sort parameters defined in the construct.

    Following are the two cases that this regression test covers:
    1. Cell 1 (construct 10001) sorted based on 1 dummy (dummy_1) parameter
        mapped to the cpn_nbr
    2. Cell 2 (Multi-construct 10002 and 10003) sorted sequentially on 2 dummy
        parameters (dummy_2 then dummy_3) both mapped to cpn_nbr

    dummy variables are stored in CDSA/sort_coupon_info*.csv
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def compare_sorted_cpns(self, assignment, mbr, construct, sorted_df):
        """
        Checks if the sequence of cpn_nbr in assignment for the given construct
        and the given member (mbr) matches that in the sorted_df
        Parameters:
            assignment (pyspark.sql.DataFrame): assignment_allconstructs
                dataframe from the output of assign_offers
            mbr (string): membership id
            construct (string): construct id
            sorted_df (pyspark.sql.DataFrame): dataframe with cpn_nbrs sorted
                based on dummy variable
        """
        cpns_per_mbr = [
            int(cpn.cpn_nbr)
            for cpn in assignment.filter(
                (assignment["mbrshp_sid"] == mbr)
                & (assignment["construct"].startswith(construct))
            ).collect()
        ]

        sorted_cpns = [
            int(cpn.cpn_nbr)
            for cpn in sorted_df.filter(
                sqlf.col("cpn_nbr").isin(cpns_per_mbr)
            ).collect()
        ]

        self.assertEquals(cpns_per_mbr, sorted_cpns)

    def validate_sorted_output(self):
        """
        Read the extra "dummy" data mapped to the cpn_nbrs and then check if
        the assigned cpn numbers are sorted correctly.
        """
        data_1 = self.spark.read.csv(
            self.config.paths["EXTRA_INFO_FOR_SORT"],
            header=True,
            inferSchema=True,
        )
        data_2 = self.spark.read.csv(
            self.config.paths["EXTRA_INFO_FOR_SORT_2"],
            header=True,
            inferSchema=True,
        )
        data_3 = self.spark.read.csv(
            self.config.paths["EXTRA_INFO_FOR_SORT_3"],
            header=True,
            inferSchema=True,
        )

        sort_1 = data_1.sort(data_1.dummy.desc())
        sort_2 = data_2.join(data_3, "cpn_nbr", "inner").sort(
            data_2.dummy_2.desc(), data_3.dummy_3.desc()
        )

        assignment = self.spark.read.parquet(
            self.config.paths["CONSTRUCTS_PATH"]
        )
        assignment = assignment.withColumn(
            "construct_name",
            sqlf.regexp_extract(assignment["construct"], CONSTRUCT_COLUMN, 1),
        )
        assignment = assignment.withColumn(
            "slot_nbr",
            sqlf.regexp_extract(
                assignment["construct"], CONSTRUCT_COLUMN, 2
            ).cast(sqlt.IntegerType()),
        )
        assignment = assignment.orderBy(
            "MBRSHP_SID", "construct_name", "slot_nbr"
        )
        mbrs = [
            int(mbr.mbrshp_sid)
            for mbr in assignment.select("mbrshp_sid").distinct().collect()
        ]

        for mbr in mbrs:
            self.compare_sorted_cpns(assignment, mbr, "c10001", sort_1)
            self.compare_sorted_cpns(assignment, mbr, "c10002", sort_2)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)
        self.validate_sorted_output()

    def execute_assignment_scripts(self):
        super(CouponSorting, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    CouponSorting.execute_test()
