import unittest

import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
import xmlrunner

import pe_memberdna.testing_support.regression_framework.regression as regression


class LayoutVsUniversal(regression.RegressionTest):
    """
        The regression test checks that layout and universal slot group
        function properly. Overall the regression test checks that universal
        slot groups allow inner slot group duplicates and cross slot group
        duplicates, while layout slot groups do not.

        In order to test the functionality the following construct is used:
            1. universal slot group with static offer
            2. universal slot group with static offer
            3. universal slot group with static offer
            4. universal slot group with placeholder
            5. universal slot group with placeholder
            6. universal slot group with 1+2+3+4+5
            7. layout slot group with 1+2+3+4
            8. layout slot group with article rank_by_col

        The test checks that:
            1. the first 5 slot groups are identical to slot group 6 and
               7(without duplicates for 7)
            2. the 7th and 8th slot group do not share any coupons

        A sample layout of the output:
    +----------+-------------+-------+--------+---------+------------+-----------+---------+
    |mbrshp_sid|experiment_id|cell_id|slot_nbr|construct|bf_construct|    cpn_nbr|pool_type|
    +----------+-------------+-------+--------+---------+------------+-----------+---------+
    |  51228720|            1|      0|       1|   c129s1|           -|    1000041|universal|
    |  51228720|            1|      0|       2|   c129s2|           -|    1000081|universal|
    |  51228720|            1|      0|       3|   c129s3|           -|    1000561|universal|
    |  51228720|            1|      0|       4|   c129s4|           -|PLACEHOLDER|universal|
    |  51228720|            1|      0|       5|   c129s5|           -|PLACEHOLDER|universal|
    |  51228720|            1|      0|       6|   c129s6|           -|    1000041|universal|
    |  51228720|            1|      0|       7|   c129s7|           -|    1000081|universal|
    |  51228720|            1|      0|       8|   c129s8|           -|    1000561|universal|
    |  51228720|            1|      0|       9|   c129s9|           -|PLACEHOLDER|universal|
    |  51228720|            1|      0|      10|  c129s10|           -|PLACEHOLDER|universal|
    |  51228720|            1|      0|      11|  c129s11|           -|    1000041|   layout|
    |  51228720|            1|      0|      12|  c129s12|           -|    1000081|   layout|
    |  51228720|            1|      0|      13|  c129s13|           -|    1000561|   layout|
    |  51228720|            1|      0|      14|  c129s14|           -|PLACEHOLDER|   layout|
    |  51228720|            1|      0|      15|  c129s15|     c129s15|    1001011|   layout|
    |  51228720|            1|      0|      16|  c129s16|     c129s16|    1000021|   layout|
    |  51228720|            1|      0|      17|  c129s17|     c129s17|    1000071|   layout|
    +----------+-------------+-------+--------+---------+------------+-----------+---------+
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        def _is_distinct(x, y):
            if set(x).intersection(set(y)):
                return False
            else:
                return True

        is_distinct = sqlf.udf(lambda x, y: _is_distinct(x, y))

        input_assignments = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )

        # test first five and sixth slot group are equal
        first_5_slots = (
            input_assignments.filter(input_assignments["SLOT_NBR"] <= 5)
            .groupBy("MBRSHP_SID")
            .agg(
                sqlf.sort_array(sqlf.collect_list("CPN_NBR")).alias(
                    "coupon_list_first_five"
                )
            )
        )

        self.assertEqual(
            first_5_slots.count(),
            first_5_slots.filter(
                sqlf.size(
                    sqlf.udf(
                        lambda col: list(set(col)),
                        sqlt.ArrayType(sqlt.StringType()),
                    )(first_5_slots["coupon_list_first_five"])
                )
                == 4
            ).count(),
        )

        sixth_slot_group = (
            input_assignments.filter(
                (input_assignments["SLOT_NBR"] > 5)
                & (input_assignments["SLOT_NBR"] <= 10)
            )
            .groupBy("MBRSHP_SID")
            .agg(
                sqlf.sort_array(sqlf.collect_list("CPN_NBR")).alias(
                    "coupon_list_sixth"
                )
            )
        )

        equals = first_5_slots.join(sixth_slot_group, ["MBRSHP_SID"], "inner")
        equals = equals.withColumn(
            "equals",
            sqlf.when(
                (
                    equals["coupon_list_first_five"]
                    == equals["coupon_list_sixth"]
                ),
                True,
            ).otherwise(False),
        )
        self.assertEqual(
            equals.count(), equals.filter(equals["equals"] == True).count()
        )

        # test that sixth slot group is equal to seventh slot group
        # ignoring duplicates
        seventh_slot_group = (
            input_assignments.filter(
                (input_assignments["SLOT_NBR"] > 10)
                & (input_assignments["SLOT_NBR"] <= 14)
            )
            .groupBy("MBRSHP_SID")
            .agg(
                sqlf.sort_array(sqlf.collect_list("CPN_NBR")).alias(
                    "coupon_list_seventh"
                )
            )
        )

        equals = seventh_slot_group.join(
            sixth_slot_group, ["MBRSHP_SID"], "inner"
        )
        equals = equals.withColumn(
            "equals",
            sqlf.when(
                (
                    sqlf.sort_array(
                        sqlf.udf(
                            lambda col: list(set(col)),
                            sqlt.ArrayType(sqlt.StringType()),
                        )(equals["coupon_list_sixth"])
                    )
                    == equals["coupon_list_seventh"]
                ),
                True,
            ).otherwise(False),
        )
        self.assertEqual(
            equals.count(), equals.filter(equals["equals"] == True).count()
        )

        # test that eighth slot group have no shared coupons with seventh slot
        # group
        eighth_slot_group = (
            input_assignments.filter((input_assignments["SLOT_NBR"] > 14))
            .groupBy("MBRSHP_SID")
            .agg(
                sqlf.sort_array(sqlf.collect_list("CPN_NBR")).alias(
                    "coupon_list_eighth"
                )
            )
        )

        distinct = eighth_slot_group.join(
            seventh_slot_group, ["MBRSHP_SID"], "inner"
        )

        distinct = distinct.withColumn(
            "distinct",
            is_distinct(
                distinct["coupon_list_seventh"], distinct["coupon_list_eighth"]
            ),
        )

        self.assertEqual(
            distinct.count(),
            distinct.filter(distinct["distinct"] == True).count(),
        )

    def execute_assignment_scripts(self):
        super(LayoutVsUniversal, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "subsample_population.py",
                "backtest_sizing.py",
                "generate_output_file.py",
                "qc_assignments.py",
            ]
        )


if __name__ == "__main__":
    LayoutVsUniversal.execute_test()
