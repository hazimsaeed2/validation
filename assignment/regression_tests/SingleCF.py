from pemember_dna.pipelines.assignment.lib.ingest import join_category_agnostic
from pyspark.sql.functions import desc

import pe_memberdna.testing_support.regression_framework.regression as regression


class SingleCF(regression.RegressionTest):
    """
    A regression test showing interaction between coupon etl and cf assignment function
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        assignment = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["INPUT_ASSIGNMENTS"])
            .filter("slot_nbr == 1")
        )

        cf = self.spark.read.load(self.config.paths["PRED_LIST"]).select(
            ["prediction_v2", "category_id", "hs_ind_lambdav2_30"]
        )

        cpn_bank = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["COUPON_BANK"])
            .filter('cpn_type == "category"')
        )

        cpn_map = self.spark.read.option("header", "true").csv(
            self.config.paths["COUPON_MAP"]
        )

        category_cpn_categories = join_category_agnostic(
            cpn_map.join(cpn_bank, "cpn_nbr")
        )

        top_category = (
            cf.filter('hs_ind_lambdav2_30 == "stretch"')
            .join(category_cpn_categories, "category_id")
            .orderBy(desc("prediction_v2"))
            .limit(1)
            .collect()[0]["category_id"]
        )
        coupon_categories = list(
            map(
                lambda x: int(x["category_id"]),
                join_category_agnostic(assignment.join(cpn_map, "cpn_nbr"))
                .select("category_id")
                .collect(),
            )
        )

        assert top_category in coupon_categories

        # this is to make sure you can do comma delimited category coupons
        self.assertEqual(
            category_cpn_categories.where(cpn_bank.cpn_nbr == 100032).count(),
            2,
        )
        self.assertEqual(
            category_cpn_categories.where(cpn_bank.cpn_nbr == 100033).count(),
            2,
        )

    def execute_assignment_scripts(self):
        super(SingleCF, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    SingleCF.execute_test()
