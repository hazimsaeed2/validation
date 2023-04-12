import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt

import pe_memberdna.testing_support.regression_framework.regression as regression

from pe_member_dna.pipelines.assignment.lib.assn_io import ConfigManager
from pe_member_dna.pipelines.assignment.lib.campaign import Campaign
from pe_member_dna.pipelines.assignment.lib.slots import rank_by_agg_trips
from pe_member_dna.pipelines.lib.spark_util import truncate_history


@sqlf.udf(returnType=sqlt.ArrayType(sqlt.IntegerType()))
def missing_ranks(column):
    """
    Checks the ranks to see if they have gaps.

    Parameters:
        column (pyspark.sql.Column): array of ranks
    Returns:
        missing (list): the missing ranks
    """
    values = sorted(column)
    previous = values[0]
    missing = []
    for value in values[1:]:
        if value - previous != 1:
            missing.extend([rank for rank in range(previous + 1, value)])
        previous = value

    return missing


class BackfillCouponReservation(regression.RegressionTest):
    """
    The test checks that backfill does not reserve coupons per member while
    assigning offers.

    In order to check this aspect the test verifies that the ranks for all
    assigned backfill coupons are consecutive. If there is a missing rank
    between two backfill coupons, the test looks to see if that coupon has
    been assigned as part of the frontfill.

    The test also takes into consideration randomness in ranking and
    reports success when the error pct. is less that ERROR_PCT.

    In order to check the ranking easily the CDSA was constructed
    accordingly:
        1 cell
            1 simple construct
                i. frontfill
                    a. slot group: 12 articles by top adjusted trips
                    b. slot group: 4 articles by top adjusted trips
                    c. slot group: 4 articles by top adjusted trips
                ii. backfill
                    default rank_by_agg_trips

    Using this CDSA we expect to see front fill articles until a certain
    slot number and after that all the backfills should follow having
    consecutive ranks, because all slot groups use the same ranking
    function (rank_by_agg_trips).

    If we see a gap in backfill ranks, it means that a slot group reserved
    coupons making the next slot group assign coupons with ranks
    starting from the last reerved coupon rank.
    """

    TOTAL_COUPONS = 40

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        config = ConfigManager("assignment", self.config_file_path)
        campaign = Campaign(config.params, config.paths)

        assignment_pool, coupon_pool = campaign.ingest_offer_data()

        assignments = self.spark.read.csv(
            self.config.paths["INPUT_ASSIGNMENTS"],
            header=True,
            inferSchema=True,
        )
        all_constructs = self.spark.read.parquet(
            self.config.paths["INPUT_CONSTRUCTS"],
        )

        join_keys = [
            "mbrshp_sid",
            "experiment_id",
            "cell_id",
            "construct",
            "cpn_nbr",
        ]
        assignments = assignments.join(
            all_constructs, join_keys, "left"
        ).select("MBRSHP_SID", "cpn_nbr", "is_backfill")

        members = self.spark.read.csv(
            self.config.paths["MAIL_LIST"], header=True
        )

        expected_backfill = rank_by_agg_trips(
            members,
            assignment_pool["article"].filter(
                sqlf.col("backfill_eligible") == 1
            ),
            coupon_pool["article"].filter(sqlf.col("backfill_eligible") == 1),
            self.TOTAL_COUPONS,
            seed=campaign.experiment,
        )

        expected_backfill = truncate_history(expected_backfill, True)

        coupon_ranks = assignments.join(
            expected_backfill, ["MBRSHP_SID", "cpn_nbr"], "left"
        )

        backfill_coupons = coupon_ranks.filter(sqlf.col("is_backfill") == 1)

        backfill_coupons = backfill_coupons.groupBy("MBRSHP_SID").agg(
            sqlf.collect_list("rank").alias("backfill_ranks")
        )

        backfill_reserved_coupons = backfill_coupons.withColumn(
            "reserved_coupons", missing_ranks(sqlf.col("backfill_ranks"))
        )

        frontfill_coupons = (
            coupon_ranks.filter(
                (sqlf.col("is_backfill") == 0) & (sqlf.col("rank").isNotNull())
            )
            .groupBy("MBRSHP_SID")
            .agg(sqlf.collect_list("rank").alias("frontfill_ranks"))
        )

        backfill_reservation_check = backfill_reserved_coupons.join(
            frontfill_coupons, "MBRSHP_SID", "left"
        )

        backfill_reservation_check = backfill_reservation_check.withColumn(
            "frontfill_ranks",
            sqlf.coalesce(
                sqlf.col("frontfill_ranks"),
                sqlf.array().cast(sqlt.ArrayType(sqlt.IntegerType())),
            ),
        )

        backfill_reservation_check = backfill_reservation_check.withColumn(
            "reserved",
            sqlf.size(sqlf.col("reserved_coupons"))
            - sqlf.size(
                sqlf.array_intersect(
                    sqlf.col("reserved_coupons"), sqlf.col("frontfill_ranks")
                )
            ),
        )

        reserved = backfill_reservation_check.filter(
            sqlf.col("reserved") > 0
        ).count()

        total = backfill_reservation_check.count()

        print("RESERVED BACKFILL")
        backfill_reservation_check.show(total, truncate=False)

        self.assertEquals(reserved, 0)

    def execute_assignment_scripts(self):
        super(BackfillCouponReservation, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
            ]
        )


if __name__ == "__main__":
    BackfillCouponReservation.execute_test()
