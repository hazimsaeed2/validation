"""Integration Tests for Models."""
import unittest

import xmlrunner

from pe_member_dna.pipelines.cf_model.lib.cf_io import (
    make_predictions_filename,
)
from pe_member_dna.pipelines.cf_model.lib.models import add_cf_reg
from pe_member_dna.pipelines.lib.iotools import read_s3_to_local


class TestCF(unittest.TestCase):
    """Integration Tests for collaborative filtering."""

    @unittest.expectedFailure
    def test_preds(self):
        """Initial preds test."""
        pred_bucket = "s3://memberanalytics-data-out-prod/"
        fname = make_predictions_filename("cf", "vtest", "test")
        pred_path = (
            pred_bucket + "MODELDATA/TESTS/PREDICTIONS/" + fname + ".csv"
        )

        preds = read_s3_to_local(pred_path)
        max_score = preds.prediction.max()
        max_score_cat = preds[
            preds.prediction == max_score
        ].CATEGORY_NAME.iloc[0]
        indicators = set(preds.hs_ind_lambda10.tolist())
        v2_not_null = set(preds.prediction_v2.isNotNull())
        self.assertEqual(len(preds), 25)
        self.assertIn("hs_ind_lambda10", preds.columns)
        self.assertIn("hs_ind_lambda20", preds.columns)
        self.assertNotIn("hs_ind_lambda5", preds.columns)
        self.assertEqual(max_score_cat, "snacks")
        self.assertEqual(indicators, {"hook", "stretch"})
        self.assertIn("predcition_v2", preds.column)

    # TODO:  add other test here, we can add things like:
    #  whether outputs match expected outputs
    #  if outputs are within viable ranges
    #  if outputs follow business rules correctly:
    #  no sensitive categories
    #  gendered are gendered etc.
    #  mostly we will add test cases from the data end
    #  and then check their validity with assert statements


if __name__ == "__main__":
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
