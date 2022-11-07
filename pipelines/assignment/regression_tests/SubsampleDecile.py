import memberdna.testing_support.regression_framework.regression as regression


class SubsampleDecile(regression.RegressionTest):
    """
    This class tests the statistical correctness of the
    subsample_population.py script.

    The test specifically focuses on the potencial error
    where the population with mail_flag=1 has different
    mean bbm score than mail_flag=0. Since some memebr DNA
    features and bbm score are correlated this would result
    in mail_flag=1 and mail_flag=0 groups within single
    decile having different mean member DNA feature values.

    The test first creates two distinct histogram peaks within
    each decile - one at high end and the other at low end.

    The population of the bbm propensity table will look
    as follows

    decile  mean(score) count(*)
    ----------------------------
    0       0.0001      500
    0       0.0999      500
    1       0.1001      500
    1       0.1999      500
    2       0.2001      500
    2       0.2999      500
    3       0.3001      500
    3       0.3999      500
    4       0.4001      500
    4       0.4999      500
    5       0.5001      500
    5       0.5999      500
    6       0.6001      500
    6       0.6999      500
    7       0.7001      500
    7       0.7999      500
    8       0.8001      500
    8       0.8999      500
    9       0.9001      500
    9       0.9999      500

    If the subsample algorithm is biased towards lower scores
    the averages within mail and nomail population will be
    significantly different for the highest subsampled decile.

    Ideally the mean score within each decile should be the same.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def tearDown(self):
        regression.RegressionTest.print_results(self)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        df_subset = self.spark.read.csv(
            self.config.paths["SUBSET"], header=True
        )

        df_subset.registerTempTable("df_subset")

        df_bbm_prop = self.spark.read.csv(
            self.config.paths["MAIL_LIST"], header=True
        )

        df_bbm_prop.registerTempTable("df_bbm_prop")

        input_mail_list = self.spark.read.option("header", "true").csv(
            self.config.paths["FHH"]
        )

        input_mail_list.registerTempTable("input_mail_list")

        decile_mean_difference = self.spark.sql(
            """
        SELECT
            (
            abs(SUM(CASE WHEN mail_flag=1 THEN mean_score ELSE 0 END)
            - SUM(CASE WHEN mail_flag=0 THEN mean_score ELSE 0 END))
            ) as decile_mean_difference,
            decile
        FROM
            (
            SELECT
                mean(b.score) as mean_score,
                COUNT(*) as cnt,
                b.decile as decile,
                s.mail_flag as mail_flag
            FROM
                df_subset as s
            LEFT JOIN
                input_mail_list as m
            ON
                s.MBRSHP_SID = m.MBRSHP_SID
            LEFT JOIN
                df_bbm_prop as b
            ON
                b.mbrshp_nbr = m.mbrshp_nbr
            WHERE
                s.slot_nbr = 1
            GROUP BY
                b.decile,
                s.mail_flag
            ORDER BY
                b.decile,
                s.mail_flag
            ) AS tab1
        GROUP BY
            decile
        HAVING
            sum(cnt) > 250
            /*
            Since we are comparing two means we want to make sure
            that the counts in every decile are high enough
            I picked 250 as it is 1/4 of the decile count.
            */
            AND COUNT(*) = 2
        ORDER BY
            decile
        """
        ).collect()

        col_decile_mean_difference = 1
        col_decile_id = 0

        for row in decile_mean_difference:
            # As for the meaning of the number 0.02:
            # If subsample_population script is broken the difference between
            # deciles should be (0.9001+0.9999)/2-0.9001 ~ 0.025
            # If the script works correctly the difference between
            # deciles should be (0.9001+0.9999)/2-(0.9001+0.9999)/2 ~ 0
            # I have picket number 0.02 as it is far enough from 0 for
            # script to be considered broken when this number is
            # exceeded
            self.assertLess(
                row[col_decile_id],
                0.02,
                msg=(
                    "The mean score for mail_flag=0 and mail_flag=1 "
                    + "for decile {} differs by {}"
                ).format(row[col_decile_mean_difference], row[col_decile_id]),
            )

        row_desired_count = 0
        col_desired_count = 5

        intended_counts = int(
            self.spark.read.csv(
                self.config.paths["CDSA_LOC"] + "campaign.csv", header=True
            ).collect()[row_desired_count][col_desired_count]
        )

        real_subsampled_counts = df_subset.where(
            (df_subset.mail_flag == 1) & (df_subset.slot_nbr == 1)
        ).count()

        # Since we are filling 5 deciles and maximum allowed count error when
        # filling deciles is 2 (see the config file from this test) the
        # overall count error should be smaller than 10
        self.assertLess(
            abs(intended_counts - real_subsampled_counts),
            10,
            msg=(
                "Subsample should produce a file with {} rows per slot_nbr"
                + " but instead produces file with {} rows per slot_nbr"
            ).format(intended_counts, real_subsampled_counts),
        )

    def execute_assignment_scripts(self):
        """
        Fills the BBM propensity table with artificial data.
        """

        input_mail_list = self.spark.read.option("header", "true").csv(
            self.config.paths["FHH"]
        )

        input_mail_list.registerTempTable("input_mail_list")

        self.spark.sql(
            """
        SELECT
            decile,
            mbrshp_nbr,
            '1/12/2019' AS end_date,
            CASE WHEN
                MBRSHP_NBR % 2 = 0
            THEN
                1 - CAST(decile AS FLOAT)/10 - 0.0001
            ELSE
                1 - CAST(decile AS FLOAT)/10 - 0.0999
            END as score
        FROM
            input_mail_list
        """
        ).write.option("header", "true").csv(
            self.config.paths["MAIL_LIST"], mode="overwrite"
        )

        super(SubsampleDecile, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
            ]
        )


if __name__ == "__main__":
    SubsampleDecile.execute_test()
