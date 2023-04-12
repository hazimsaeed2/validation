from pyspark.sql.functions import col, lit, when

import pe_memberdna.testing_support.regression_framework.regression as regression


class AdjustedTrips(regression.RegressionTest):
    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        INPUT_CONSTRUCTS = self.spark.read.option("header", "true").parquet(
            self.config.paths["INPUT_CONSTRUCTS"]
        )
        COUPON_MEMTRIP = self.spark.read.option("header", "true").parquet(
            self.config.paths["COUPON_MEMTRIP"]
        )
        TRANSACTIONS = self.spark.read.option("header", "true").parquet(
            self.config.paths["TRANSACTIONS_PATH"]
        )
        CPG = self.spark.read.option("header", "true").csv(
            self.config.paths["CPG_COUPON_LIST_PATH"]
        )
        CPG = CPG.withColumn("cpn_nbr", col("PMR Offer ID"))

        CAT = self.spark.read.option("header", "true").csv(
            self.config.paths["CATEGORY_COUPON_PATH"]
        )

        # obtain a test_mbrshp_sid
        test_sid = (
            INPUT_CONSTRUCTS.sample(False, 0.1)
            .limit(1)
            .select(["mbrshp_sid"])
            .collect()[0][0]
        )

        # subset of TRANSACTIONS with only relevant columns for the test_sid
        test_transactions = TRANSACTIONS.where(col("MBRSHP_SID") == test_sid)
        test_transactions_article = test_transactions.select(
            ["ARTICLE_NBR", "PURCH_HDR_ID"]
        )
        test_transactions_ah4 = test_transactions.select(
            ["AH4_CD", "PURCH_HDR_ID"]
        ).withColumnRenamed("AH4_CD", "category")

        test_transactions_ah5 = test_transactions.select(
            ["AH5_CD", "PURCH_HDR_ID"]
        ).withColumnRenamed("AH5_CD", "category")

        test_transactions_category = test_transactions_ah4.union(
            test_transactions_ah5
        ).dropna(subset=["category"])

        # subset the INPUT_CONSTRUCTS for that test_mbrshp_sid
        test_construct = INPUT_CONSTRUCTS.where(
            col("mbrshp_sid") == test_sid
        ).select(["mbrshp_sid", "cpn_nbr", "is_backfill"])

        # join construct with CPG to match 'Article Number'
        test_construct_article = test_construct.join(
            CPG.select(["cpn_nbr", "Article Number"]), "cpn_nbr", "inner"
        ).withColumnRenamed("Article Number", "ARTICLE_NBR")

        # join construct with CAT to match both "cpn_ah4_cd" and "cpn_ah5_cd"
        test_construct_category_ah4 = test_construct.join(
            CAT.select(["cpn_nbr", "cpn_ah4_cd"]), "cpn_nbr", "inner"
        ).withColumnRenamed("cpn_ah4_cd", "category")

        test_construct_category_ah5 = test_construct.join(
            CAT.select(["cpn_nbr", "cpn_ah5_cd"]), "cpn_nbr", "inner"
        ).withColumnRenamed("cpn_ah5_cd", "category")

        test_construct_category = test_construct_category_ah4.union(
            test_construct_category_ah5
        ).dropna(subset=["category"])

        coupons = COUPON_MEMTRIP.where(col("MBRSHP_SID") == test_sid).select(
            ["cpn_nbr", "adjusted_trips"]
        )

        # Run the adjusted trip tests for each of Article and Categories
        self.run_adjusted_trips(
            test_construct_article,
            test_transactions_article,
            coupons,
            "ARTICLE_NBR",
        )

        self.run_adjusted_trips(
            test_construct_category,
            test_transactions_category,
            coupons,
            "category",
        )

    def run_adjusted_trips(
        self, test_construct, test_transactions, coupons, column
    ):
        """
        This will execute the actual Adjusted trip test on the test_construct
        produced which now can be either based on Article or Category data.

        Parameters:
            test_construct (pyspark.sql.DataFrame): The construct filtered with
                trips for either article or category.
            test_transactions (pyspark.sql.DataFrame): The filtered transactions
                for selected member.
            coupons (pyspark.sql.DataFrame): The coupon numbers "cpn_nbr" with
                "adjusted_trips" information.
            column (str): Column name to join test_transactions and test_construct
                which can be "ARTICLE_NBR", "AH4_CD" or "AH5_CD"
        Returns:
            None
        """

        # join construct with TRANSACTIONS to verify purchase based on column name
        # which depends on being an article or category dataframe
        test_construct = test_construct.join(
            test_transactions,
            column,
            "left",
        )

        # join with COUPON_MEMTRIP to verify order
        test_construct = test_construct.join(
            coupons,
            "cpn_nbr",
            "left",
        )

        # verify that 1. backfills are not purchased and do not have adjusted_trips
        #             2. non-backfills are purchased and do have adjusted_trips
        #             3. the backfills uses the highest adjusted_trips (results of the left join with COUPON_MEMTRIP)
        test_construct = test_construct.withColumn(
            "correct",
            when(
                (col("is_backfill") == 0)
                & (col("adjusted_trips") > 0)
                & (col("PURCH_HDR_ID") > 0),
                lit(0),
            )
            .when(
                (col("is_backfill") == 1)
                & (col("adjusted_trips").isNull())
                & (col("PURCH_HDR_ID").isNull()),
                lit(0),
            )
            .otherwise(lit(1)),
        )

        self.assertEqual(
            test_construct.select(["correct"]).groupBy().sum().collect()[0][0],
            0,
        )

    def execute_assignment_scripts(self):
        super(AdjustedTrips, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    AdjustedTrips.execute_test()
