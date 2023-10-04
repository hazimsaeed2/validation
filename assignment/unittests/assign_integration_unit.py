"""Integration Tests for offer assignment."""
import unittest

import pandas as pd
import xmlrunner
from pemember_dna.pipelines.lib.iotools import read_s3_to_local


class TestCoupon(unittest.TestCase):
    """Integration Tests for coupon etl."""

    def test_coupon(self):
        """Initial ETL test."""
        print("test coupon")
        base_path = "s3://memberanalytics-data-out-prod/ASSIGNMENTS/tests/cdsa_test/coupons/TEST/mmpc_test"
        bank = base_path + "/coupon_bank"
        coupmap = base_path + "/coupon_map"
        quals = base_path + "/coupon_quals"
        memtrips = base_path + "/member_trips_coupon"

        bank = read_s3_to_local(bank)
        quals = read_s3_to_local(quals)
        coupmap = read_s3_to_local(coupmap)
        # memtrips = read_s3_to_local(memtrips)

        self.assertEqual(len(bank), 26)
        self.assertEqual(len(bank[bank.cpn_type == "basket"]), 4)
        self.assertEqual(len(bank[bank.cpn_type == "category"]), 12)
        self.assertEqual(len(bank[bank.cpn_type == "article"]), 10)
        self.assertEqual(len(quals), 26)
        self.assertEqual(len(quals[quals.hero_eligible == 1]), 6)
        self.assertEqual(len(quals[quals.backfill_eligible == 1]), 23)
        self.assertEqual(len(coupmap), 28)
        # self.assertEqual(len(memtrips), 44)
        # self.assertEqual(memtrips.trips.max(), 5)


class TestAssign(unittest.TestCase):
    """Integration Tests for offer assignment."""

    def test_assign(self):
        """Initial preds test."""
        print("test assign 1...")
        base_path = "s3://memberanalytics-data-out-prod/ASSIGNMENTS/tests/"
        exp_path = base_path + "campaigns_test/test1/expected_assignment"
        assign_path = (
            base_path + "cdsa_test/assn_output/TEST/mmpc_test/assignments/"
        )
        # construct_path = base_path + "test_constructs"
        exp_assigns = read_s3_to_local(exp_path)
        assigns = read_s3_to_local(assign_path)
        # constructs = read_s3_to_local(construct_path)
        cellone = assigns[assigns.cell_id == 15]
        celltwo = assigns[assigns.cell_id == 16]
        cellthree = assigns[assigns.cell_id == 17]
        cellfour = assigns[assigns.cell_id == 18]
        overlap = pd.merge(
            assigns,
            exp_assigns,
            how="inner",
            on=[
                "mbrshp_sid",
                "experiment_id",
                "cell_id",
                "slot_nbr",
                "construct",
                "cpn_nbr",
            ],
        )
        mbrgrouped = assigns.groupby("mbrshp_sid").count()
        unique = assigns.groupby("mbrshp_sid").agg({"cpn_nbr": "nunique"})
        unique = unique["cpn_nbr"].sum()
        self.assertEqual(len(assigns), 16)
        # self.assertEqual(len(constructs), 60)
        self.assertTrue(len(cellone) == 2)
        self.assertTrue(len(celltwo) == 2)
        self.assertTrue(len(cellthree) == 3)
        self.assertTrue(len(cellfour) == 9)
        self.assertEqual(len(mbrgrouped), 10)
        self.assertEqual(unique, len(assigns))
        self.assertEqual(len(overlap), len(assigns))

        print("test assign 2...")
        base_path = "s3://memberanalytics-data-out-prod/ASSIGNMENTS/tests/"
        exp_path = base_path + "campaigns_test/test2/expected_assignment"
        assign_path = (
            base_path + "cdsa_test/assn_output/TEST/mmpc_test_2/assignments/"
        )
        exp_assigns = read_s3_to_local(exp_path)
        assigns = read_s3_to_local(assign_path)
        overlap = pd.merge(
            assigns,
            exp_assigns,
            how="inner",
            on=[
                "mbrshp_sid",
                "experiment_id",
                "cell_id",
                "slot_nbr",
                "construct",
                "cpn_nbr",
            ],
        )
        self.assertEqual(len(overlap), len(assigns))

    def test_subsample(self):
        """Initial subsample test."""
        print("test subsample")
        base_path = "s3://memberanalytics-data-out-prod/ASSIGNMENTS/tests/"
        circ_path = (
            base_path + "cdsa_test/assn_output/TEST/mmpc_test/mail_subset/"
        )
        circ = read_s3_to_local(circ_path)
        nomail = circ[(circ.mail_flag == 0) & (circ.slot_nbr == 1)]
        mail = circ[(circ.mail_flag == 1) & (circ.slot_nbr == 1)]
        self.assertTrue(len(nomail) <= 5)
        self.assertTrue(len(mail) >= 5)
        self.assertEqual(len(circ), 16)

    def test_mailhouse_mmpc(self):
        """Initial mailhouse test."""
        print("test mailhouse")
        base_path = "s3://memberanalytics-data-out-prod/ASSIGNMENTS/tests/"
        mail_path = (
            base_path + "cdsa_test/assn_output/TEST/mmpc_test/final_mailhouse"
        )
        exp_path = base_path + "campaigns_test/test1/expected_mailfile_mmpc"
        mailfile = read_s3_to_local(mail_path)
        exp_mail = read_s3_to_local(exp_path)
        nomail = mailfile[mailfile.mail_flag == "NO MAIL"]
        mail = mailfile[mailfile.mail_flag == "CIRC"]
        overlap = pd.merge(
            mailfile,
            exp_mail,
            how="inner",
            on=[
                "MBRSHP_SID",
                "MBRSHP_NBR",
                "CPN1",
                "CPN2",
                "CPN3",
                "LAST_FIFTY-TWO_WEEK_TRIPS",
                "LFIFTY-TWOW_SPEND_IN_STORE",
            ],
        )
        self.assertTrue(len(nomail) <= 5)
        self.assertTrue(len(mail) >= 5)
        self.assertEqual(len(overlap), len(exp_mail))

    def test_mailhouse_bbm(self):
        """Initial mailhouse test."""
        print("test mailhouse")
        base_path = "s3://memberanalytics-data-out-prod/ASSIGNMENTS/tests/"
        mail_path = (
            base_path + "cdsa_test/assn_output/TEST/bbm_test/final_mailhouse"
        )
        exp_path = base_path + "campaigns_test/test1/expected_mailfile_bbm"
        mailfile = read_s3_to_local(mail_path)
        exp_mail = read_s3_to_local(exp_path)
        nomail = mailfile[mailfile.mail_flag == "NO MAIL"]
        mail = mailfile[mailfile.mail_flag == "CIRC"]
        overlap = pd.merge(
            mailfile,
            exp_mail,
            how="inner",
            on=[
                "MBRSHP_SID",
                "MBRSHP_NBR",
                "VERSION",
                "CPN1",
                "CPN2",
                "CPN3",
                "LAST_FIFTY-TWO_WEEK_TRIPS",
                "LFIFTY-TWOW_SPEND_IN_STORE",
            ],
        )
        self.assertTrue(len(nomail) <= 5)
        self.assertTrue(len(mail) >= 5)
        self.assertEqual(len(overlap), len(exp_mail))

    # TODO:  add other test here, we can add things like:
    #  whether outputs match expected outputs
    #  if outputs are within viable ranges
    #  if outputs follow business rules correctly:
    #  no sensitive categories
    #  gendered are gendered etc.
    #  mostly we will add test cases from the data end
    #  and then check their validity with assert statements


if __name__ == "__main__":
    import findspark

    findspark.init()
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import Row, SparkSession

    name = "assign_integration_test"

    conf = SparkConf().setAppName(name)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
