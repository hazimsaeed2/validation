import pyspark.sql.functions as sqlf

from pe_member_dna.pipelines.assignment.lib.assn_utils import CONSTRUCT_COLUMN
import pe_memberdna.testing_support.regression_framework.regression as regression


class CustomizableBackfill(regression.RegressionTest):
    """
    The regression test checks that the assignment output stays the same
    from one run to another in the specified ERROR margin when using
    custom backfill.

    The test focuses on the backfill functionality, therefor all constructs
    use a backfill strategy:

    1. cell 1(cosntruct 100001) - test backwards compatibility
              - (1 category + 11 articles, default backfill)
              - the cell uses the default backfill from the config
    2. cell 2(construct 10004) - tests construct satisfied filter(filters)
                                 and custom backfill
              - (12 category, 12 articles), construct satisfied filter
              - the cell has a filter construct satisfied using the 1st
                cell
              - backfill replaces category with articles
    3. cell 3(construct 10002) - test the use of the same frontfill with
                                 different backfills
              - (1 category + 11 articles, 1 + 11 articles)
              - has the same ff construct as cell 4
              - the backfill replaces category with article
    4. cell 4(construct 10003) - test the use of the same frontfill with
                                 different backfills
              - (1 category + 11 articles, 1 + 11 categories)
              - has the same ff construct as cell 3
              - the backfill replaces articles with categories

    For more details please check the CDSA/CONSTRUCTS and CDSA/cells.csv

    TODO: Make test use the same construct id for 3 and 4
    Currently, only the same json content is used.
    In the future, we have to use the same id to test that the backfill is
    used correctly for each frontfill and that one backfill is not applied
    to a wrong frontfill.

    The allconstructs currently does not have the layout id, only the
    construct and bf construct. In order to use the same id we have to
    introduce the layout id in allconstructs.

    For now, because we use different ids we can safely group by
    construct id when we test, otherwise we would end up with duplicates
    and the join would be faulty.

    """

    ERROR = 0  # ERROR margin
    TOTAL_MEMBERS = 40
    TOTAL_CONSTRUCTS = 4
    TOTAL_SLOTS = 12

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def validate_input_assignment(self):
        input_assignments = self.spark.read.csv(
            self.config.paths["INPUT_ASSIGNMENTS"],
            header=True,
            inferSchema=True,
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

        # test unique coupons
        unique_coupons = input_assignments.groupBy("MBRSHP_SID").agg(
            sqlf.countDistinct("cpn_nbr").alias("unique_cpns"),
        )
        self.assertEquals(
            unique_coupons.filter(
                unique_coupons["unique_cpns"] == self.TOTAL_SLOTS
            ).count(),
            self.TOTAL_MEMBERS,
        )

    def validate_input_constructs(self):
        prev = self.spark.read.parquet(
            self.config.paths["ASSN_LOC"] + "allconstructs_reference/"
        )
        prev = prev.withColumn(
            "construct",
            sqlf.regexp_extract(sqlf.col("construct"), CONSTRUCT_COLUMN, 1),
        )

        assignment = (
            self.spark.read.parquet(self.config.paths["INPUT_CONSTRUCTS"])
            .withColumn(
                "slot_nbr",
                sqlf.regexp_extract(
                    sqlf.col("construct"), CONSTRUCT_COLUMN, 2
                ),
            )
            .withColumn(
                "construct",
                sqlf.regexp_extract(
                    sqlf.col("construct"), CONSTRUCT_COLUMN, 1
                ),
            )
        )

        # test - each member has the exact slots
        slots_per_member = assignment.groupBy("MBRSHP_SID", "construct").agg(
            sqlf.countDistinct("slot_nbr").alias("unique_slots"),
            sqlf.count("slot_nbr").alias("total_slots"),
        )
        self.assertEquals(
            slots_per_member.filter(
                (slots_per_member["unique_slots"] == self.TOTAL_SLOTS)
                & (slots_per_member["total_slots"] == self.TOTAL_SLOTS)
            ).count(),
            self.TOTAL_CONSTRUCTS * self.TOTAL_MEMBERS,
        )

        # test unique coupons
        unique_coupons = assignment.groupBy("MBRSHP_SID", "construct").agg(
            sqlf.countDistinct("cpn_nbr").alias("unique_cpns"),
        )
        self.assertEquals(
            unique_coupons.filter(
                unique_coupons["unique_cpns"] == self.TOTAL_SLOTS
            ).count(),
            self.TOTAL_CONSTRUCTS * self.TOTAL_MEMBERS,
        )

        # test that each run always assigns the same number of coupons per
        # construct to each member
        prev_count = (
            prev.groupBy("mbrshp_sid", "construct")
            .count()
            .withColumnRenamed("count", "prev_count")
        )
        curr_count = (
            assignment.groupBy("mbrshp_sid", "construct")
            .count()
            .withColumnRenamed("count", "curr_count")
        )
        test_count = prev_count.join(
            curr_count, ["mbrshp_sid", "construct"], "left"
        )
        test_count = test_count.fillna({"curr_count": 0})
        self.assertEquals(
            test_count.filter(
                test_count["prev_count"] - test_count["curr_count"] != 0
            ).count(),
            0,
        )

        # test that each run always assigns the roughly the same of coupons per
        # construct to each member
        test_same = assignment.join(
            prev,
            ["mbrshp_sid", "construct", "cpn_nbr", "is_backfill", "pool_type"],
            "inner",
        )

        prev = prev.groupBy("mbrshp_sid", "construct").count()
        test_same = test_same.groupBy("mbrshp_sid", "construct").count()

        test_diff = test_same.join(
            prev, ["mbrshp_sid", "construct"], "inner"
        ).withColumn("error", sqlf.abs(prev["count"] - test_same["count"]))

        self.assertEquals(
            test_diff.filter(test_diff["error"] > self.ERROR).count(), 0
        )

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        self.validate_input_assignment()
        self.validate_input_constructs()

    def execute_assignment_scripts(self):
        super(CustomizableBackfill, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
            ]
        )


if __name__ == "__main__":
    CustomizableBackfill.execute_test()
