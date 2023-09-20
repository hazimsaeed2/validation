import pyspark.sql.functions as sqlf

import pe_memberdna.pipelines.assignment.lib.assn_utils as assn_utils
import pe_memberdna.testing_support.regression_framework.regression as regression


class GenerateInputMail(regression.RegressionTest):
    """
    The regression test checks that 2 aspects:
    1. decile 10 has been added to the input mail file.
    2. the generated input mail file is used by all steps of the pipeline
    having a correct assignment output.
    """

    TOTAL_MEMBERS = 40
    TOTAL_CONSTRUCTS = 4
    TOTAL_SLOTS = 12

    def __init__(self, method_name):
        regression.RegressionTest.__init__(self, method_name)
        self.additional_paths_to_clean = ["MAIL_LIST"]

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        bbm_scored = self.spark.read.csv(
            self.config.paths["BBM_SCORED"], header=True, inferSchema=True
        )

        muhh = (
            self.spark.read.csv(
                self.config.paths["MUHH"], header=True, inferSchema=True
            )
            .select("MEMBERSHIP_ID")
            .withColumnRenamed("MEMBERSHIP_ID", "mbrshp_nbr")
        )

        input_sid = (
            muhh.join(
                bbm_scored.select(
                    ["mbrshp_nbr", "mbrshp_sid", "decile", "score"]
                ),
                ["mbrshp_nbr"],
                "left",
            )
            .withColumnRenamed("mbrshp_nbr", "MBRSHP_NBR")
            .withColumn("FHH_IND", sqlf.lit("N"))
        )

        input_mail = self.spark.read.csv(
            self.config.paths["MAIL_LIST"],
            header=True,
            inferSchema=True,
        )

        expected_columns = {
            "MBRSHP_NBR",
            "decile",
            "score",
            "FHH_IND",
            "mbrshp_sid",
        }

        self.assertTrue(
            {column.lower() for column in expected_columns}
            == {column.lower() for column in input_mail.columns}
        )

        self.assertEqual(
            input_mail.filter(input_mail["decile"] != "10")
            .join(
                input_sid.filter(input_sid["decile"] != "10"),
                "mbrshp_nbr",
                "left",
            )
            .count(),
            input_sid.filter(input_sid["decile"] != "10").count(),
        )
        self.assertEqual(
            input_mail.filter(input_mail["decile"] != "10").count(), 37
        )

        self.assertEqual(
            input_mail.filter(input_mail["decile"] == "10")
            .join(
                input_sid.filter(input_sid["decile"] == "10"),
                "mbrshp_nbr",
                "left",
            )
            .count(),
            input_sid.filter(input_sid["decile"] == "10").count(),
        )
        self.assertEqual(
            input_mail.filter(input_mail["decile"] == "10").count(), 3
        )

        assignment = (
            self.spark.read.parquet(self.config.paths["INPUT_CONSTRUCTS"])
            .withColumn(
                "slot_nbr",
                sqlf.regexp_extract(
                    sqlf.col("construct"), assn_utils.CONSTRUCT_COLUMN, 2
                ),
            )
            .withColumn(
                "construct",
                sqlf.regexp_extract(
                    sqlf.col("construct"), assn_utils.CONSTRUCT_COLUMN, 1
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

    def execute_assignment_scripts(self):
        super(GenerateInputMail, self).execute_assignment_scripts(
            _scripts=[
                "generate_input_mail.py",
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
                "backtest_sizing.py",
                "generate_output_file.py",
                "qc_assignments.py",
            ]
        )


if __name__ == "__main__":
    GenerateInputMail.execute_test()
