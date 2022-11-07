import json
import os
import unittest
from unittest import mock

import pandas as pd
import xmlrunner
import pyspark.sql.functions as sqlf


from pe_member_dna.pipelines.assignment.lib.campaign import (
    Campaign,
    global_min,
    _clean_constructs,
    _parse_constructs,
    _parse_segments,
    decompose_construct,
)


FF = Campaign.FillType.FF
BF = Campaign.FillType.BF


class TestCampaign(unittest.TestCase):
    def setUp(self):
        """prepare test data"""
        rdd = sc.parallelize(
            [(0, 5, 2), (10, 18, np.nan), (13, 6, np.nan), (9, 8, 5)]
        )
        self.df = rdd.toDF(["mmpc_days", "bbm_days", "col_with_nan"])

        self.constructs = pd.DataFrame(
            columns=[
                "construct_id",
                "construct_desc",
                "num_offers",
                "construct_json",
            ],
            data=[
                ["10001", "article_and_category", "12", "10001"],
                ["10002", "article_and_category", "12", "10002"],
                ["10022", "article_and_article", "12", "10022"],
                ["10003", "article_and_category", "12", "10003"],
                ["10033", "article_and_category", "12", "10033"],
            ],
        )

        self.segments = pd.DataFrame(
            columns=["segment_id", "segment_desc", "segment_json"],
            data=[
                ["10001", "construct satisfied", "10001"],
                ["10002", "construct satisfied", "10002"],
                ["10003", "construct satisfied", "10003"],
            ],
        )

        segment_bank = self._load_segments()
        self.segments_side_effect = [self.segments] + segment_bank

        self.paths = {
            "CONSTRUCT": "construct_dummy_path",
            "CONSTRUCT_BANK": "construct_bank_dummy_path",
            "SEGMENT": "segment_dummy_path",
            "SEGMENT_BANK": "segment_bank_dummy_path",
            "MAIL_LIST": "mail_list_dummy_path",
            "RAW_MEMBER": "raw_member_dummy_path",
            "COUPON_BANK": "coupon_bank_dummy_path",
            "COUPON_QUALS": "coupon_quals_dummy_path",
            "COUPON_MAP": "coupon_map_dummy_path",
            "ARTICLE_AH4_AH5_MAP": "article_ah4_ah5_map_dummy_path",
            "COUPON_MEMTRIP": "coupon_memtrip_dummy_path",
            "COUPON_MEMUSAGE": "coupon_memusage_dummy_path",
            "PRED_LIST": "pred_list_dummy_path",
            "CAMPAIGN": "campaign_dummy_path",
            "CUBE": "cube_dummy_path",
            "CELL": "cells_dummy_path",
            "CDSA_ASSGN": "assign_dummy_path",
        }

        self.backfill_default_constructs = {
            "category": "rank_by_agg_cf",
            "basket": "hardest",
            "dummy": "random",
            "article": "rank_by_cf",
        }

        self.downsample_coupons = [{"cpn_nbr": 607, "ratio": 0.2}]

        self.parameters = {
            "experiment": 1,
            "replace_with_null": [],
            "backfill_method": self.backfill_default_constructs,
            "backfill_priority": "low",
            "run_size": 100,
            "downsample_coupons": self.downsample_coupons,
            "assignment_date": "2019-01-01",
        }

    def tearDown(self):
        pass

    @staticmethod
    def _load_constructs(construct_ids=("10001", "10002")):
        constructs = list()
        construct_bank_dir = os.path.join(
            os.path.dirname(__file__),
            "test_data",
            "campaign",
            "construct_bank",
        )
        for construct_id in construct_ids:
            with open(
                os.path.join(
                    construct_bank_dir, "{}.json".format(construct_id)
                ),
                "r",
            ) as f:
                constructs.append(json.load(f))
        return constructs

    @staticmethod
    def _load_segments(segment_ids=("10001", "10002")):
        segments = list()
        segment_bank_dir = os.path.join(
            os.path.dirname(__file__), "test_data", "campaign", "segment_bank"
        )
        for segment_id in segment_ids:
            with open(
                os.path.join(segment_bank_dir, "{}.json".format(segment_id)),
                "r",
            ) as f:
                segments.append(json.load(f))
        return segments

    def _assert_sources(self, source_list, expected_source_list):
        self.assertEquals(len(source_list), len(expected_source_list))
        for source in source_list:
            # match source with source from expected_source_list
            for expected_source in expected_source_list:
                if (
                    source["name"] == expected_source["name"]
                    and source["ftype"] == expected_source["ftype"]
                ):
                    cols = source["cols"]
                    cols.sort()
                    expected_cols = expected_source["cols"]
                    expected_cols.sort()
                    self.assertEqual(cols, expected_cols)
                    break

    def test_global_min(self):

        self.df = self.df.withColumn(
            "last_exp_days_no_null",
            global_min(
                self.df["mmpc_days", "bbm_days", "col_with_nan"],
                missing_value_exclude=True,
            ),
        ).withColumn(
            "last_exp_days_with_null",
            global_min(
                self.df["mmpc_days", "bbm_days", "col_with_nan"],
                missing_value_exclude=False,
            ),
        )

        self.assertEqual(
            [
                row.last_exp_days_no_null
                for row in self.df.select("last_exp_days_no_null").collect()
            ],
            [0, 10, 6, 5],
        )
        self.assertEqual(
            [
                row.last_exp_days_with_null
                for row in self.df.select("last_exp_days_with_null").collect()
            ],
            [0, None, None, 5],
        )

    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    def test_parse_constructs_construct_not_in_list(self, read_s3_to_local):
        """
        Construct_id 10006 does not exist in construct list.
        """
        construct_bank = self._load_constructs(
            construct_ids=("10001", "10002", "10002")
        )
        constructs_side_effect = [self.constructs] + construct_bank

        read_s3_to_local.side_effect = constructs_side_effect

        cells = pd.DataFrame(
            columns=["cell_id", "construct_id", "bf_construct_id"],
            data=[
                ["1", "10001"],
                ["2", "10002", "10002"],
                ["3", "10006", "10006"],
            ],
        )

        with self.assertRaisesRegex(
            ValueError, "10006 is not an existing construct_id!"
        ):
            _parse_constructs(
                cells, self.paths, self.backfill_default_constructs
            )

    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    @mock.patch("memberdna.pipelines.assignment.lib.campaign.write_json_to_s3")
    def test_parse_constructs_constructs_do_not_match(
        self, write_json_to_s3, read_s3_to_local
    ):
        """
        Construct 10001_10002 slot groups do not align with those of construct
        10001.

        """
        write_json_to_s3.return_value = None

        construct_bank = self._load_constructs(
            construct_ids=("10001", "10001", "10002")
        )
        constructs_side_effect = [self.constructs] + construct_bank

        read_s3_to_local.side_effect = constructs_side_effect
        cells = pd.DataFrame(
            columns=["cell_id", "construct_id", "bf_construct_id"],
            data=[["1", "10001"], ["2", "10001_10002", "10001"]],
        )

        with self.assertRaisesRegex(Exception, "Constructs do not match"):
            _parse_constructs(
                cells, self.paths, self.backfill_default_constructs
            )

    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    @mock.patch("memberdna.pipelines.assignment.lib.campaign.write_json_to_s3")
    def test_parse_constructs(self, write_json_to_s3, read_s3_to_local):
        """
        Test format of parse_constructs
        """
        write_json_to_s3.return_value = None
        side_effect = [self.constructs] + self._load_constructs(
            construct_ids=("10002", "10022", "10001", "10003")
        )
        read_s3_to_local.side_effect = side_effect
        cells = pd.DataFrame(
            columns=["cell_id", "construct_id", "bf_construct_id"],
            data=[
                ["1", "10002", "10022"],
                ["2", "10002"],
                ["3", "10001_10003"],
            ],
        )

        expected_source_list = [
            {
                "name": "category_tenure",
                "ftype": "parquet",
                "path": "CUBE",
                "offer_data": "category",
                "time_partition": "fiscal_week",
                "cols": ["TENURE", "MBRSHP_SID", "DUMMY_COL"],
            },
            {
                "name": "basket_dna",
                "ftype": "parquet",
                "path": "CUBE",
                "offer_data": "basket",
                "time_partition": "fiscal_week",
                "cols": [
                    "LTWENTY-SIXW_SPEND_IN_STORE",
                    "MBRSHP_SID",
                    "LAST_TWENTY-SIX_WEEK_TRIPS",
                ],
            },
        ]

        expected_slot_set_list = json.load(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "test_data",
                    "campaign",
                    "results",
                    "parsed_constructs.json",
                ),
                "r",
            )
        )

        source_list, slot_set_list, lambda_list = _parse_constructs(
            cells, self.paths, self.backfill_default_constructs
        )

        self._assert_sources(source_list, expected_source_list)
        self.assertEqual(
            json.loads(json.dumps(slot_set_list)), expected_slot_set_list
        )

        self.assertEqual(lambda_list, [10, 30])
        write_json_to_s3.assert_called()

    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    def test_parse_constructs_dedupe_sources(self, read_s3_to_local):
        """
        # note: Adding construct 10003 which extends columns for
        category tenure with dummy col in order to show how
        expected source list is generated
        """
        construct_bank = self._load_constructs(
            construct_ids=("10001", "10002", "10003")
        )
        read_s3_to_local.side_effect = [self.constructs] + construct_bank
        cells = pd.DataFrame(
            columns=["cell_id", "construct_id", "bf_construct_id"],
            data=[
                ["1", "10001"],
                ["2", "10002", "10002"],
                ["3", "10003", "10003"],
            ],
        )

        expected_source_list = [
            {
                "name": "category_tenure",
                "ftype": "parquet",
                "path": "CUBE",
                "offer_data": "category",
                "time_partition": "fiscal_week",
                "cols": ["TENURE", "MBRSHP_SID", "DUMMY_COL"],
            },
            {
                "name": "basket_dna",
                "ftype": "parquet",
                "path": "CUBE",
                "offer_data": "basket",
                "time_partition": "fiscal_week",
                "cols": [
                    "LTWENTY-SIXW_SPEND_IN_STORE",
                    "MBRSHP_SID",
                    "LAST_TWENTY-SIX_WEEK_TRIPS",
                ],
            },
        ]

        source_list, slot_set_list, lambda_list = _parse_constructs(
            cells, self.paths, self.backfill_default_constructs
        )

        self._assert_sources(source_list, expected_source_list)

    def test_clean_constructs(self):
        """
        Test return values of clean_constructs
        """
        cells = pd.DataFrame(
            columns=["cell_id", "construct_id", "bf_construct_id"],
            data=[
                ["1", "10001_10002"],
                ["2", "10002", "10022"],
                ["3", "1000u", "10033"],
                ["4", "10004", "10044_10055"],
            ],
        )
        cell_constructs = cells[
            ["cell_id", "construct_id", "bf_construct_id"]
        ].values.tolist()

        expected = [
            ("1", ("10001_10002", None)),
            ("2", ("10002", "10022")),
            ("3", (None, "10033")),
            ("4", ("10004", "10044_10055")),
        ]

        clean_constr_list = _clean_constructs(cell_constructs)
        self.assertEqual(clean_constr_list, expected)

    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    def test_parse_segments_segment_not_in_list(self, read_s3_to_local):
        """
        Segment_id 10005 not in segments list.
        """
        read_s3_to_local.side_effect = self.segments_side_effect
        cells = pd.DataFrame(
            columns=["cell_id", "segment_id"],
            data=[["1", "10001"], ["2", "10005"]],
        )
        with self.assertRaisesRegex(
            ValueError, "10005 is not an existing segment_id!"
        ):
            _parse_segments(cells=cells, paths=self.paths)

    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    @mock.patch("memberdna.pipelines.assignment.lib.campaign.write_json_to_s3")
    def test_parse_segments(self, write_json_to_s3, read_s3_to_local):
        """
        Test format of parse_segments
        """
        write_json_to_s3.return_value = None

        segments_side_effect = [self.segments] + self._load_segments(
            segment_ids=("10002", "10003", "10002", "10003", "10002", "10001")
        )
        read_s3_to_local.side_effect = segments_side_effect
        cells = pd.DataFrame(
            columns=["cell_id", "segment_id"],
            data=[
                ["1", "10001"],
                ["2", "10002"],
                ["3", "10002_10003"],
                ["4", "10002|10003"],
            ],
        )

        expected_source_list = [
            {
                "cols": ["MBRSHP_SID", "TENURE", "DUMMY_COL"],
                "ftype": "parquet",
                "name": "category_tenure",
                "offer_data": "category",
                "path": "CUBE",
                "time_partition": "fiscal_week",
            },
            {
                "cols": ["cpn_nbr", "enriched"],
                "ftype": "csv",
                "name": "coupon_info_category",
                "offer_data": "category",
                "path": "EXTRA_INFO_COUPON_PATH",
            },
        ]

        expected_segment_list = json.load(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "test_data",
                    "campaign",
                    "results",
                    "parsed_segments.json",
                ),
                "r",
            )
        )

        expected_filter_list = json.load(
            open(
                os.path.join(
                    os.path.dirname(__file__),
                    "test_data",
                    "campaign",
                    "results",
                    "parsed_filters.json",
                ),
                "r",
            )
        )

        segment_list, source_list, filter_list = _parse_segments(
            cells=cells, paths=self.paths
        )

        self.assertEqual(
            json.loads(json.dumps(filter_list)), expected_filter_list
        )
        self.assertEqual(
            json.loads(json.dumps(segment_list)), expected_segment_list
        )

        self._assert_sources(source_list, expected_source_list)

    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    def test_parse_segments_dedupe_sources(self, read_s3_to_local):
        """
        Test segments deduplication by adding segment 10004 with additional
        DUMMY_COL in category_tenure data source
        """
        segment_bank = self._load_segments(
            segment_ids=("10001", "10002", "10003")
        )
        read_s3_to_local.side_effect = [self.segments] + segment_bank
        cells = pd.DataFrame(
            columns=["cell_id", "segment_id"],
            data=[["1", "10001"], ["2", "10002"], ["3", "10003"]],
        )

        expected_source_list = [
            {
                "cols": ["TENURE", "MBRSHP_SID", "DUMMY_COL"],
                "ftype": "parquet",
                "name": "category_tenure",
                "offer_data": "category",
                "path": "CUBE",
                "time_partition": "fiscal_week",
            },
            {
                "cols": ["cpn_nbr", "enriched"],
                "ftype": "csv",
                "name": "coupon_info_category",
                "offer_data": "category",
                "path": "EXTRA_INFO_COUPON_PATH",
            },
        ]

        segment_list, source_list, filter_list = _parse_segments(
            cells=cells, paths=self.paths
        )

        self._assert_sources(source_list, expected_source_list)

    def test_decompose_construct(self):
        """
        Test return values of decompose_construct
        """

        constructs = decompose_construct(construct="10001")
        self.assertEqual(constructs, ["10001"])

        constructs = decompose_construct(construct="10001_10002")
        self.assertEqual(constructs, ["10001", "10002"])

        constructs = decompose_construct(construct="1011u")
        self.assertEqual(constructs, [None])

        constructs = decompose_construct(construct="10001_1011u")
        self.assertEqual(constructs, [None])

    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    @mock.patch(
        "memberdna.pipelines.assignment.lib.campaign._read_and_subset_exp_csv"
    )
    @mock.patch(
        "memberdna.pipelines.assignment.lib.campaign._parse_campaign_details"
    )
    @mock.patch(
        "memberdna.pipelines.assignment.lib.campaign.read_subset_and_cast"
    )
    def test_ingest_offer_data(
        self,
        read_subset_and_cast,
        parse_campaign_details,
        read_and_subset_exp_csv,
        read_s3_to_local,
    ):
        """
        Test ingest offer data:
            1. no duplicates
            2. coupons are ingested according to their offer type
            3. coupons are ingested by experiment_id
            4. source data is appended to the correct offer_type
        """
        side_effect = (
            [self.constructs]
            + self._load_constructs(construct_ids=("10002", "10022"))
            + [self.segments]
        )

        read_s3_to_local.side_effect = side_effect

        cells = pd.DataFrame(
            columns=[
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[["1", "10002", "10022", None, None]],
        )
        read_and_subset_exp_csv.return_value = cells

        parse_campaign_details.return_value = (
            "description",
            "2019",
            "FY19",
            "MMPC",
            "40",
        )

        mail_list = spark.createDataFrame(
            [
                ["001", 1, "Non Trial No FHH", 0.1271, 5, "N", "NONE"],
                ["002", 2, "Non Trial No FHH", 0.0638, 7, "N", "NONE"],
                ["003", 3, "Non Trial No FHH", 0.2531, 3, "N", "NONE"],
                [
                    "004",
                    4,
                    "Non Trial No FHH",
                    0.0199,
                    9,
                    "N",
                    "AUGUST TENURED",
                ],
                ["005", 5, "Non Trial FHH", 0.1124, 6, "Y", "NONE"],
                ["006", 6, "Non Trial No FHH", 0.7087, 1, "N", "NONE"],
                ["007", 7, "Non Trial No FHH", 9.43, 2, "N", "NONE"],
                ["008", 8, "Non Trial FHH", 0.1407, 5, "Y", "NONE"],
                ["009", 9, "Non Trial No FHH", 0.0306, 8, "N", "NONE"],
                [
                    "010",
                    10,
                    "Non Trial No FHH",
                    0.017,
                    10,
                    "N",
                    "AUGUST TENURED",
                ],
            ],
            [
                "MBRSHP_NBR",
                "MBRSHP_SID",
                "CellName",
                "score",
                "decile",
                "FHH_IND",
                "DTR_FLAG",
            ],
        )

        coupon_bank = spark.createDataFrame(
            [
                [
                    133,
                    "basket",
                    1,
                    4,
                    1,
                    "Basket Coupon 133",
                    "3/14/2010",
                    "3/14/2028",
                    10.0,
                    None,
                    150.0,
                ],
                [
                    123,
                    "basket",
                    1,
                    3,
                    1,
                    "Basket Coupon 123",
                    "3/14/2010",
                    "3/14/2028",
                    20.0,
                    None,
                    100.0,
                ],
                [
                    143,
                    "basket",
                    1,
                    5,
                    1,
                    "Basket Coupon 143",
                    "3/14/2010",
                    "3/14/2028",
                    15.0,
                    None,
                    100.0,
                ],
                [
                    278,
                    "category",
                    71,
                    18,
                    1,
                    "$2 off Any Bath Tissue",
                    "12/26/2019",
                    "1/12/2020",
                    3.0,
                    1.0,
                    0.0,
                ],
                [
                    278,
                    "category",
                    71,
                    19,
                    1,
                    "$3 off Any Bath Tissue",
                    "12/26/2019",
                    "1/12/2020",
                    3.0,
                    1.0,
                    0.0,
                ],
                [
                    242,
                    "category",
                    26,
                    28,
                    1,
                    "$2 off Any Fresh Vegetables Purchase",
                    "12/26/2019",
                    "1/12/2020",
                    2.0,
                    1.0,
                    0.0,
                ],
                [
                    252,
                    "category",
                    36,
                    32,
                    1,
                    "$2 off Any Purchase of $10 or More",
                    "12/26/2019",
                    "1/12/2020",
                    2.0,
                    1.0,
                    10.0,
                ],
                [
                    423,
                    "article",
                    None,
                    None,
                    0,
                    "$2",
                    "12/26/2019",
                    "1/15/2020",
                    2.0,
                    1.0,
                    None,
                ],
                [
                    739,
                    "article",
                    None,
                    None,
                    0,
                    "SAVE$2.00",
                    "12/26/2019",
                    "1/22/2020",
                    2.0,
                    1.0,
                    None,
                ],
                [
                    968,
                    "article",
                    None,
                    None,
                    0,
                    "+$2",
                    "12/26/2019",
                    "1/15/2020",
                    2.0,
                    1.0,
                    None,
                ],
                [
                    542,
                    "article",
                    None,
                    None,
                    0,
                    "$1bonus",
                    "12/26/2019",
                    "1/15/2020",
                    1.0,
                    1.0,
                    None,
                ],
                [
                    597,
                    "article",
                    None,
                    None,
                    0,
                    "$10off",
                    "12/26/2019",
                    "1/15/2020",
                    6.0,
                    2.0,
                    None,
                ],
                [
                    538,
                    "article",
                    None,
                    None,
                    0,
                    "JACKET_$5",
                    "12/26/2019",
                    "1/22/2020",
                    5.0,
                    1.0,
                    None,
                ],
                [
                    925,
                    "article",
                    None,
                    None,
                    0,
                    "FY20_BBM19_WFDRIEDFRUIT_SAVE$1.00",
                    "12/26/2019",
                    "1/22/2020",
                    1.0,
                    1.0,
                    None,
                ],
                [
                    105,
                    "article",
                    None,
                    None,
                    0,
                    "LUNCHMEAT_$2",
                    "12/26/2019",
                    "1/22/2020",
                    2.0,
                    1.0,
                    None,
                ],
                [
                    569,
                    "article",
                    None,
                    None,
                    0,
                    "$1.50 OFF",
                    "12/26/2019",
                    "1/15/2020",
                    1.5,
                    1.0,
                    None,
                ],
                [
                    918,
                    "article",
                    None,
                    None,
                    0,
                    "SAVE$1.00",
                    "12/26/2019",
                    "1/15/2020",
                    1.0,
                    1.0,
                    None,
                ],
                [
                    927,
                    "article",
                    None,
                    None,
                    0,
                    "SAVE$2.00",
                    "12/26/2019",
                    "1/15/2020",
                    2.0,
                    1.0,
                    None,
                ],
                [
                    286,
                    "article",
                    None,
                    None,
                    0,
                    "Save$4",
                    "12/26/2019",
                    "1/22/2020",
                    4.0,
                    1.0,
                    None,
                ],
                [
                    607,
                    "article",
                    None,
                    None,
                    0,
                    "20%  OFF",
                    "12/26/2019",
                    "1/15/2020",
                    20.0,
                    1.0,
                    None,
                ],
            ],
            [
                "cpn_nbr",
                "cpn_type",
                "offer_id",
                "cpn_class_id",
                "self_funded_flag",
                "cpn_desc",
                "cpn_start",
                "cpn_end",
                "cpn_dollar_off",
                "cpn_qty_threshold",
                "cpn_dollar_threshold",
            ],
        )

        coupon_quals = spark.createDataFrame(
            [
                [133, 1, 0, 1],
                [123, 2, 0, 1],
                [143, 1, 0, 0],
                [278, 1, 0, 0],
                [242, 2, 0, 1],
                [252, 1, 0, 0],
                [423, 1, 0, 1],
                [739, 1, 0, 1],
                [968, 2, 0, 1],
                [542, 1, 0, 1],
                [597, 1, 0, 1],
                [538, 1, 0, 1],
                [925, 1, 0, 1],
                [105, 1, 0, 0],
                [569, 1, 0, 1],
                [918, 1, 0, 1],
                [927, 1, 0, 1],
                [286, 1, 0, 1],
                [607, 1, 0, 1],
            ],
            ["cpn_nbr", "experiment_id", "hero_eligible", "backfill_eligible"],
        )

        coupon_map = spark.createDataFrame(
            [
                [133, "", "", ""],
                [123, "", "", ""],
                [143, "", "", ""],
                [278, "111", "", ""],
                [278, "222", "", ""],
                [242, "111", "", ""],
                [242, "333", "", ""],
                [252, "222", "", ""],
                [423, "", "", "1111"],
                [423, "", "", "2222"],
                [739, "", "", "3333"],
                [968, "", "", "1111"],
                [542, "", "", "4444"],
                [597, "", "", "5555"],
                [538, "", "", "6666"],
                [925, "", "", "7777"],
                [105, "", "", "8888"],
                [105, "", "", "1111"],
                [569, "", "", "2222"],
                [918, "", "", "9999"],
                [927, "", "", "3333"],
                [286, "", "", "6666"],
                [607, "", "", "7777"],
            ],
            ["cpn_nbr", "ah4_cd", "ah5_cd", "article_nbr"],
        )

        article_map = spark.createDataFrame(
            [
                [1111, None, None],
                [2222, 1, 10],
                [3333, 1, 10],
                [4444, 2, 20],
                [5555, 3, 30],
                [6666, 4, 40],
                [7777, 5, 50],
                [8888, 6, 60],
                [9999, 6, 60],
            ],
            ["ARTICLE_NBR", "AH4_CD", "AH5_CD"],
        )

        trips = spark.createDataFrame(
            [
                [278, 1, 2.0, 1.1739185177974645, 10.0],
                [242, 1, 1.0, 1.3422578130929423, 427.0],
                [278, 2, 1.0, 1.365338432805376, 321.0],
                [242, 3, 0.5, 0.40888037189545356, 546.0],
                [423, 3, 1.0, 0.7293200868779882, 426.0],
                [423, 4, 1.0, 0.7293200868779882, 426.0],
                [739, 5, 1.0, 1.1543482807969239, 537.0],
                [538, 5, 14.0, 13.936556670196175, 348.0],
                [286, 1, 2.0, 1.542773087101173, 326.0],
                [925, 2, 6.0, 3.529058785358741, 321.0],
                [927, 6, 1.0, 0.7796623673990232, 513.0],
                [607, 7, 0.5, 0.40888037189545356, 546.0],
                [607, 8, 2.0, 1.0882550626756518, 441.0],
                [607, 9, 2.0, 1.0882550626756518, 441.0],
                [607, 1, 2.0, 1.0882550626756518, 441.0],
                [607, 3, 2.0, 1.0882550626756518, 441.0],
                [597, 10, 1.0, 0.6982624442121753, 664.0],
            ],
            [
                "cpn_nbr",
                "mbrshp_sid",
                "trips",
                "adjusted_trips",
                "days_since_last",
            ],
        )

        usage = spark.createDataFrame(
            [
                [278, 1, 2, 0, 0.0],
                [242, 1, 1, 1, 1.0],
                [278, 2, 1, 1, 1.0],
                [242, 3, 0, 0, 0.0],
                [423, 3, 1, 0, 0.0],
                [423, 4, 1, 0, 0.0],
                [739, 5, 1, 1, 1.0],
                [538, 5, 12, 3, 0.25],
                [286, 1, 2, 1, 0.5],
                [925, 2, 6, 3, 0.5],
                [927, 6, 1, 0, 0.0],
                [607, 7, 0, 0, 0.0],
                [607, 8, 2, 1, 0.5],
                [607, 9, 2, 1, 0.5],
                [607, 1, 2, 1, 0.5],
                [607, 3, 2, 2, 1.0],
                [597, 10, 1, 0, 0.0],
            ],
            [
                "cpn_nbr",
                "mbrshp_sid",
                "purchases",
                "redemptions",
                "fraction_purchases_with_coupon",
            ],
        )

        cf_pred = spark.createDataFrame(
            [
                [
                    0.05477039888501167,
                    0.0547704,
                    "stretch",
                    "stretch",
                    "SPECIALTY FRESH MEAT",
                    "stretch",
                    "stretch",
                    "stretch",
                    1,
                    "AH4_CD",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    111,
                ],
                [
                    0.40684735774993896,
                    0.40684736,
                    "stretch",
                    "stretch",
                    "SPECIALTY FRESH MEAT",
                    "stretch",
                    "stretch",
                    "stretch",
                    2,
                    "AH4_CD",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    222,
                ],
                [
                    0.2996640205383301,
                    0.29966402,
                    "stretch",
                    "stretch",
                    "SPECIALTY FRESH MEAT",
                    "stretch",
                    "stretch",
                    "stretch",
                    3,
                    "AH4_CD",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    333,
                ],
                [
                    0.4584089517593384,
                    0.45840895,
                    "stretch",
                    "stretch",
                    "FRESH FRUIT",
                    "stretch",
                    "stretch",
                    "stretch",
                    5,
                    "AH5_CD",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    40,
                ],
                [
                    0.4584089517593384,
                    0.45840895,
                    "stretch",
                    "stretch",
                    "FRUIT",
                    "stretch",
                    "stretch",
                    "stretch",
                    5,
                    "AH4_CD",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    "stretch",
                    4,
                ],
            ],
            [
                "prediction_v2",
                "prediction",
                "hs_ind_lambda15",
                "hs_ind_lambda10",
                "CATEGORY_NAME",
                "hs_ind_lambda30",
                "hs_ind_lambdav2_15",
                "hs_ind_lambdav2_10",
                "MBRSHP_SID",
                "CATEGORY_LVL",
                "hs_ind_lambda1",
                "hs_ind_lambda3",
                "hs_ind_lambda5",
                "hs_ind_lambdav2_30",
                "hs_ind_lambdav2_5",
                "hs_ind_lambdav2_1",
                "hs_ind_lambdav2_3",
                "hs_ind_lambda20",
                "hs_ind_lambdav2_40",
                "hs_ind_lambda40",
                "hs_ind_lambdav2_20",
                "CATEGORY_ID",
            ],
        )

        read_subset_and_cast.side_effect = [
            mail_list,
            mail_list,
            coupon_bank,
            coupon_quals,
            coupon_map,
            article_map,
            trips,
            usage,
            cf_pred,
            spark.createDataFrame(
                pd.read_csv(
                    os.path.join(
                        os.path.dirname(__file__),
                        "test_data",
                        "campaign",
                        "cube.csv",
                    )
                )
            ),
        ]

        campaign = Campaign(self.parameters, self.paths)
        assignment_pools, coupon_pools = campaign.ingest_offer_data()

        self.assertEqual(assignment_pools.keys(), {"article", "category"})
        self.assertEqual(coupon_pools.keys(), {"article", "category"})

        # check for duplicates
        for offer_type in ("category", "article"):
            assignment = (
                assignment_pools[offer_type]
                .groupBy("MBRSHP_SID")
                .agg(
                    sqlf.collect_set("cpn_nbr").alias("distinct_cpns"),
                    sqlf.collect_list("cpn_nbr").alias("cpns"),
                )
            )
            assignment = assignment.withColumn(
                "duplicates",
                sqlf.when(
                    sqlf.size(sqlf.col("distinct_cpns"))
                    == sqlf.size(sqlf.col("cpns")),
                    0,
                ).otherwise(1),
            )
            self.assertEqual(
                assignment.filter(sqlf.col("duplicates") == 1).count(), 0
            )

        articles = coupon_pools["article"]
        self.assertEqual(
            articles.count(),
            articles.select("cpn_nbr", "ah4_cd", "ah5_cd").distinct().count(),
        )
        categories = coupon_pools["category"]
        self.assertEqual(
            categories.count(),
            categories.select(
                "cpn_nbr", "ah4_cd", "ah5_cd", "offer_id", "cpn_class_id"
            )
            .distinct()
            .count(),
        )

        # check that coupons are assigned to the correct pool
        # and that only coupons marked for the current experiment are used
        all_coupons = coupon_bank.join(coupon_quals, "cpn_nbr", "inner")
        for offer_type in ("category", "article"):
            self.assertEqual(
                assignment_pools[offer_type]
                .join(
                    all_coupons.filter(sqlf.col("cpn_type") == offer_type)
                    .filter(
                        sqlf.col("experiment_id")
                        == self.parameters["experiment"]
                    )
                    .select("cpn_nbr")
                    .distinct(),
                    "cpn_nbr",
                    "inner",
                )
                .count(),
                assignment_pools[offer_type].count(),
            )

        # check that only category has extra data
        self.assertTrue("TENURE" in assignment_pools["category"].columns)
        self.assertTrue("DUMMY_COL" in assignment_pools["category"].columns)
        self.assertTrue("TENURE" not in assignment_pools["article"].columns)
        self.assertTrue("DUMMY_COL" not in assignment_pools["article"].columns)

        # check downsample for coupon
        self.assertTrue(
            assignment_pools["article"]
            .filter(
                sqlf.col("cpn_nbr") == self.downsample_coupons[0]["cpn_nbr"]
            )
            .count()
            < 5
        )

    @mock.patch(
        "memberdna.pipelines.assignment.lib.campaign."
        "load_past_longitudinal_mbrs"
    )
    @mock.patch("memberdna.pipelines.assignment.lib.campaign.read_s3_to_local")
    @mock.patch(
        "memberdna.pipelines.assignment.lib.campaign._read_and_subset_exp_csv"
    )
    @mock.patch(
        "memberdna.pipelines.assignment.lib.campaign._parse_campaign_details"
    )
    def test_find_past_longitudinal_mbrs(
        self,
        _parse_campaign_details,
        _read_and_subset_exp_csv,
        read_s3_to_local,
        load_past_longitudinal_mbrs,
    ):
        _parse_campaign_details.return_value = (
            "desc",
            "2019",
            "FY19",
            "mmpc",
            "40",
        )
        current_campaign_cells = pd.DataFrame(
            [
                ["1", "1", 10002, "", 10001, "1"],
                ["1", "2", 10002, "", 10001, "2"],
                ["1", "3", 10002, "", None, None],
            ],
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
        )
        _read_and_subset_exp_csv.return_value = current_campaign_cells
        read_s3_to_local.side_effect = (
            [self.constructs]
            + self._load_constructs(construct_ids=("10002",))
            + [self.segments]
            + self._load_segments(segment_ids=("10001",))
        )

        longitudinal_mbrs = [
            spark.createDataFrame(
                [["001"], ["002"], ["003"], ["004"], ["005"], ["006"]],
                ["mbrshp_sid"],
            ),
            spark.createDataFrame(
                [["003"], ["004"], ["005"], ["007"]], ["mbrshp_sid"]
            ),
        ]
        load_past_longitudinal_mbrs.side_effect = longitudinal_mbrs

        memberdata = spark.createDataFrame(
            [
                ["001"],
                ["002"],
                ["003"],
                ["004"],
                ["005"],
                ["006"],
                ["007"],
                ["008"],
                ["009"],
            ],
            ["mbrshp_sid"],
        )

        campaign = Campaign(self.parameters, self.paths)
        campaign.experiment = 2
        member_data = campaign.find_past_longitudinal_mbrs(memberdata)

        expected = spark.createDataFrame(
            [
                ["001", 1, 0],
                ["002", 1, 0],
                ["003", 1, 1],
                ["004", 1, 1],
                ["005", 1, 1],
                ["006", 1, 0],
                ["007", 0, 1],
                ["008", 0, 0],
                ["009", 0, 0],
            ],
            ["mbrshp_sid", "l1_past_mbr", "l2_past_mbr"],
        )

        self.assertEqual(
            expected.join(member_data, member_data.columns, "inner").count(),
            expected.count(),
        )


class TestCalculateOffers(unittest.TestCase):
    """
    This will test if the parameters sent to calculate_offers are
    correctly pre-processed by the time they reach _run_slot.
    Behavior of _run_slot is mocked here so we don't get the final
    output of the function.
    """

    coupon_header = "cpn_nbr STRING, cpn_type STRING"
    assign_basket_header = (
        "MBRSHP_SID INT, MBRSHP_NBR STRING, cpn_nbr STRING, cpn_type STRING"
    )
    assign_header = (
        "MBRSHP_SID INT, CATEGORY_ID STRING, MBRSHP_NBR STRING, cpn_nbr STRING, "
        "cpn_type STRING, cpn_class_id INT"
    )

    def setUp(self):
        """
        Setup list of members and empty exposure dataframes
        For coupons and assignment pools, setting up one of each element.
        """
        self.members = spark.createDataFrame([[1], [2], [3]], "MBRSHP_SID INT")
        self.offer_exposure = spark.createDataFrame(
            [], "MBRSHP_SID INT, cpn_class_id INT"
        )
        self.coupon_exposure = spark.createDataFrame(
            [], "MBRSHP_SID INT, cpn_type STRING"
        )

        coupon_a = spark.createDataFrame(
            [["10", "article"]], self.coupon_header
        )
        coupon_b = spark.createDataFrame(
            [["20", "basket"]], self.coupon_header
        )
        coupon_c = spark.createDataFrame(
            [["30", "category"]], self.coupon_header
        )
        assign_a = spark.createDataFrame(
            [[1, "123456", "0", "10", "article", 1000]], self.assign_header
        )
        assign_b = spark.createDataFrame(
            [[1, "0", "20", "basket"]], self.assign_basket_header
        )
        assign_c = spark.createDataFrame(
            [[1, "123456", "0", "30", "category", 1000]], self.assign_header
        )
        self.assignment_pools = {
            "article": assign_a,
            "basket": assign_b,
            "category": assign_c,
        }
        self.coupon_pools = {
            "article": coupon_a,
            "basket": coupon_b,
            "category": coupon_c,
        }

        with mock.patch.object(Campaign, "__init__", lambda self, a, b: None):
            self.campaign = Campaign({}, {})
            self.campaign.experiment = 0

    @staticmethod
    def run_slots_get_args(mock_obj):
        """
        This function checks if _run_slots was called and returns the list of
        all 7 arguments used, including default values if any for the last 2.
        """
        mock_obj.assert_called()
        args, kwargs = mock_obj.call_args
        mock_obj.reset_mock()  # make sure next run is clean
        backfill = False
        seed = 0
        if len(args) > 5:
            backfill = args[5]
        if len(args) > 6:
            seed = args[6]
        if "backfill" in kwargs:
            backfill = kwargs["backfill"]
        if "seed" in kwargs:
            seed = kwargs["seed"]

        members, a_pools, c_pools, group, total_coupons = args[:5]
        return [
            members,
            a_pools,
            c_pools,
            group,
            total_coupons,
            backfill,
            seed,
        ]

    @mock.patch("memberdna.pipelines.assignment.lib.campaign._run_slot")
    def call_calculate_offers(
        self, fill_type, run_slot, *, check_run_slots=True
    ):
        """
        This function will call the calculate_offers with data from
        unittest setup except for Fill type which is sent here.
        If requested, it will return the arguments used to call
        _run_slots
        """
        out = self.campaign.calculate_offers(
            self.members,
            self.assignment_pools,
            self.coupon_pools,
            fill_type,
            self.offer_exposure,
            self.coupon_exposure,
        )
        if check_run_slots:
            return self.run_slots_get_args(run_slot)
        return out

    def test_empty_slots(self):
        """
        Empty slots for FF and BF
        This is the only time _run_slots is not called.
        """
        self.campaign.slots = []
        self.assertIsNone(
            self.call_calculate_offers(FF, check_run_slots=False),
            "Front fill offers should be empty without slots",
        )

        self.assertIsNone(
            self.call_calculate_offers(BF, check_run_slots=False),
            "Back fill offers should be empty without slots",
        )

    def test_invalid_slots(self):
        """
        This test verifies if calculate_offers has all the needed slot
        information to proceed. Tests are: without 'frontfill', without
        'backfill', without 'slot_size' and with empty/filled backfill slots.
        """
        self.campaign.slots = [{"groups": [{"backfill": {}}]}]
        with self.assertRaises(
            KeyError, msg="Cannot proceed without 'frontfill' element"
        ):
            self.call_calculate_offers(FF)

        self.campaign.slots = [{"groups": [{"frontfill": {}}]}]
        with self.assertRaises(
            KeyError, msg="Cannot proceed without 'backfill' element"
        ):
            self.call_calculate_offers(BF)

        self.campaign.slots = [{"groups": [{"frontfill": {}, "backfill": {}}]}]
        with self.assertRaises(
            KeyError, msg="Should not proceed without 'slot_size' element"
        ):
            self.call_calculate_offers(FF)

        with self.assertRaises(
            KeyError, msg="Should not proceed without 'slot_size' element"
        ):
            self.call_calculate_offers(BF)

        self.campaign.slots = [
            {
                "groups": [
                    {"frontfill": {"slot_size": 3}, "backfill": {}},
                    {
                        "frontfill": {"slot_size": 2},
                        "backfill": {"slot_size": 2},
                    },
                ]
            }
        ]

        self.call_calculate_offers(BF)

    def test_ff_no_exposure(self):
        """
        Testing FF without exposure data. This test is separated from BF
        because there are a few differences in output, specially on total
        of coupons and checking information about eligibility

        Setting up slots for front fill and back fill
        On the real application, slot size will be the same, but for
        making sure of the results, here they are set to different values
        """
        FF_SLOT = 3
        BF_SLOT = 4
        self.campaign.slots = [
            {
                "groups": [
                    {
                        "frontfill": {"slot_size": FF_SLOT},
                        "backfill": {"slot_size": BF_SLOT},
                    }
                ]
            }
        ]

        (
            members,
            a_pools,
            c_pools,
            group,
            total_coupons,
            bf,
            seed,
        ) = self.call_calculate_offers(FF)

        self.assertEqual(members, self.members, "Members should be unchanged")
        self.assertEqual(bf, False, f"Should be front fill")
        self.assertEqual(
            seed, self.campaign.experiment, "Seed should be fixed"
        )
        self.assertEqual(
            c_pools,
            self.coupon_pools,
            "Coupons should be unchanged on front fill",
        )
        self.assertEqual(
            group["slot_size"], FF_SLOT, "Slot size should be fixed"
        )
        self.assertEqual(
            total_coupons,
            FF_SLOT,
            "Total coupons should always match FF slot or twice that for BF",
        )
        for k, v in a_pools.items():
            self.assertTrue(
                "MMPC" not in v.toPandas().columns,
                f"Assignment {k} should not have MMPC exposure",
            )
            self.assertTrue(
                "BBM" not in v.toPandas().columns,
                f"Assignment {k} should not have BBM exposure",
            )

    def test_mmpc_exposure(self):
        """
        Front fill and back fill with coupon exposure for MMPC
        Set offer/coupon exposure for one member
        """
        self.offer_exposure = spark.createDataFrame(
            [[1, 1000, 0]], "MBRSHP_SID INT, cpn_class_id INT, MMPC INT"
        )
        self.coupon_exposure = spark.createDataFrame(
            [[1, "basket", 0]], "MBRSHP_SID INT, cpn_type STRING, MMPC INT"
        )

        self.campaign.slots = [
            {
                "groups": [
                    {
                        "frontfill": {
                            "slot_size": 3,
                            "exclude_exp_channel": ["MMPC"],
                        },
                        "backfill": {
                            "slot_size": 3,
                            "exclude_exp_channel": ["MMPC"],
                        },
                    }
                ]
            }
        ]

        for fill_type in (FF, BF):
            (
                members,
                a_pools,
                c_pools,
                group,
                total_coupons,
                bf,
                seed,
            ) = self.call_calculate_offers(fill_type)

            self.assertEqual(
                group["exclude_exp_channel"],
                ["MMPC"],
                "Excluding based on MMPC exposure",
            )
            for k, v in a_pools.items():
                self.assertTrue(
                    (~v.toPandas()["MMPC"].isna()).any(),
                    f"Assignment {k} should have MMPC exposure",
                )
                self.assertTrue(
                    "BBM" not in v.toPandas().columns,
                    f"Assignment {k} should not have BBM exposure",
                )

    def test_mmpc_bbm_exposure(self):
        """
        Front fill and back fill with coupon exposure for MMPC and BBM
        On slots we are still only excluding MMPC channel and not BBM
        """
        self.offer_exposure = spark.createDataFrame(
            [[1, 1000, 0, 2]],
            "MBRSHP_SID INT, cpn_class_id INT, MMPC INT, BBM INT",
        )
        self.coupon_exposure = spark.createDataFrame(
            [[1, "basket", 0, 2]],
            "MBRSHP_SID INT, cpn_type STRING, MMPC INT, BBM INT",
        )

        self.campaign.slots = [
            {
                "groups": [
                    {
                        "frontfill": {
                            "slot_size": 3,
                            "exclude_exp_channel": ["MMPC"],
                        },
                        "backfill": {
                            "slot_size": 3,
                            "exclude_exp_channel": ["MMPC"],
                        },
                    }
                ]
            }
        ]

        for fill_type in (FF, BF):
            (
                members,
                a_pools,
                c_pools,
                group,
                total_coupons,
                bf,
                seed,
            ) = self.call_calculate_offers(fill_type)

            self.assertEqual(
                group["exclude_exp_channel"],
                ["MMPC"],
                "Excluding based on MMPC exposure",
            )
            for k, v in a_pools.items():
                self.assertTrue(
                    (~v.toPandas()["MMPC"].isna()).any(),
                    f"Assignment {k} should have MMPC exposure",
                )
                self.assertTrue(
                    (~v.toPandas()["BBM"].isna()).any(),
                    f"Assignment {k} should have BBM exposure",
                )

    def test_bf_no_exposure(self):
        """
        Testing BF without exposure data. This test is similar to test #3
        except some values should differ and we also verify the assignments
        are there, since the backfill_eligible column is not set yet.
        """
        FF_SLOT = 3
        BF_SLOT = 4
        self.campaign.slots = [
            {
                "groups": [
                    {
                        "frontfill": {"slot_size": FF_SLOT},
                        "backfill": {"slot_size": BF_SLOT},
                    }
                ]
            }
        ]

        (
            members,
            a_pools,
            c_pools,
            group,
            total_coupons,
            bf,
            seed,
        ) = self.call_calculate_offers(BF)

        self.assertEqual(members, self.members, "Members should be unchanged")
        self.assertEqual(bf, True, f"Should be back fill")
        self.assertEqual(
            seed, self.campaign.experiment, "Seed should be fixed"
        )
        self.assertEqual(
            c_pools,
            self.coupon_pools,
            "Coupons should be unchanged on front fill",
        )
        self.assertEqual(
            group["slot_size"], BF_SLOT, "Slot size should be fixed"
        )
        self.assertEqual(
            total_coupons,
            2 * FF_SLOT,
            "Total coupons should match twice of FF slot",
        )
        for k, v in a_pools.items():
            self.assertTrue(
                "MMPC" not in v.toPandas().columns,
                f"Assignment {k} should not have MMPC exposure",
            )
            self.assertTrue(
                "BBM" not in v.toPandas().columns,
                f"Assignment {k} should not have BBM exposure",
            )

        for k, val in a_pools.items():
            self.assertFalse(
                val.toPandas().empty, "Assignment {k} should not be empty"
            )

        for k, val in c_pools.items():
            self.assertFalse(
                val.toPandas().empty, "Coupon {k} should not be empty"
            )

    def test_bf_backfill_eligible(self):
        """
        Test backfill calculation with added backfill_eligible column which will
        remove some elements and keep others.
        """
        self.campaign.slots = [
            {
                "groups": [
                    {
                        "frontfill": {"slot_size": 3},
                        "backfill": {"slot_size": 3},
                    }
                ]
            }
        ]

        coupon_header = self.coupon_header + ", backfill_eligible INT"
        assign_basket_header = (
            self.assign_basket_header + ", backfill_eligible INT"
        )
        assign_header = self.assign_header + ", backfill_eligible INT"

        coupon_a = spark.createDataFrame([["10", "article", 0]], coupon_header)
        coupon_b = spark.createDataFrame([["20", "basket", 0]], coupon_header)
        coupon_c = spark.createDataFrame(
            [["30", "category", 1]], coupon_header
        )
        assign_a = spark.createDataFrame(
            [[1, "123456", "0", "10", "article", 1000, 1]], assign_header
        )
        assign_b = spark.createDataFrame(
            [[1, "0", "20", "basket", 1]], assign_basket_header
        )
        assign_c = spark.createDataFrame(
            [[1, "123456", "0", "30", "category", 1000, 0]], assign_header
        )
        self.assignment_pools = {
            "article": assign_a,
            "basket": assign_b,
            "category": assign_c,
        }
        self.coupon_pools = {
            "article": coupon_a,
            "basket": coupon_b,
            "category": coupon_c,
        }

        (
            members,
            a_pools,
            c_pools,
            group,
            total_coupons,
            bf,
            seed,
        ) = self.call_calculate_offers(BF)

        self.assertFalse(a_pools["article"].toPandas().empty)
        self.assertFalse(a_pools["basket"].toPandas().empty)
        self.assertTrue(a_pools["category"].toPandas().empty)

        self.assertTrue(c_pools["article"].toPandas().empty)
        self.assertTrue(c_pools["basket"].toPandas().empty)
        self.assertFalse(c_pools["category"].toPandas().empty)


class TestFitCoupons(unittest.TestCase):
    """
    This will test if the member data sent to fit_coupons gets
    split correctly into multiple
    """

    header = "MBRSHP_SID INT, cpn_nbr STRING, slot_structure STRING"

    def setUp(self):
        """
        Setup list of members and empty exposure dataframes
        For coupons and assignment pools, setting up one of each element.
        """
        with mock.patch.object(Campaign, "__init__", lambda self, a, b: None):
            self.campaign = Campaign({}, {})
            self.campaign.experiment = 0
            self.campaign.backfill_priority = Campaign.BackfillPriority.LOW

    @mock.patch.object(Campaign, "_frontfill_backfill_slotgroup")
    @mock.patch.object(Campaign, "_frontfill_backfill_slotset")
    def call_fit_coupons(self, member_data, slotset, slotgroup):
        """
        This function calls fit_coupons and mocks inner functions that
        are called depending on backfill_priority being low or high.
        It asserts if either was called and get the only argument
        passed to the function.

        Parameters:
            member_data (pyspark.sql.DataFrame): represents coupons assign to each
            slot group for each member.
            Other parameters are filled by the Mock object decorator.

        Returns:
            memberdata (pyspark.sql.DataFrame): a complete assignment without
                                                segmentation
        """

        def add_rank(df):
            """
            Those mocked functions add a column "rank" to the dataset
            which is also mocked here when it doesn't exist
            Parameters:
                df (pyspark.sql.DataFrame): A data frame to be edited

            Returns:
                df (pyspark.sql.DataFrame): Same data frame with "rank"
            """
            if "rank" in df.columns:
                return df
            return df.withColumn("rank", sqlf.lit(1))

        slotgroup.side_effect = add_rank
        slotset.side_effect = add_rank
        out = self.campaign.fit_coupons(member_data)

        if self.campaign.backfill_priority == Campaign.BackfillPriority.HIGH:
            slotgroup.assert_called()
            slotset.assert_not_called()
        else:
            slotgroup.assert_not_called()
            slotset.assert_called()

        preprocess_data = slotgroup.call_args or slotset.call_args
        return out, preprocess_data[0][0]

    def test_split_valid_slot(self):
        """
        Test the slot structure if it is split correctly without
        backfill data
        """
        slot = "c0__0s1g2p3"
        data = spark.createDataFrame([[1, "2", slot]], self.header)
        out, data = self.call_fit_coupons(data)

        data = data.toPandas()
        self.assertEqual(data.at[0, "EXPERIMENT_ID"], 0)
        self.assertEqual(data.at[0, "CONSTRUCT_NAME"], "0__0")
        self.assertEqual(data.at[0, "SLOT_NBR"], 1)
        self.assertEqual(data.at[0, "SLOT_GRP"], 2)
        self.assertEqual(data.at[0, "PARENT_CONSTRUCT"], 3)
        self.assertEqual(data.at[0, "IS_BACKFILL"], 0)

    def test_split_valid_slot_backfill(self):
        """
        Test the slot structure if it is split correctly with
        backfill data
        """
        slot = "c5__6s7g8p9b"
        data = spark.createDataFrame([[1, "2", slot]], self.header)
        out, data = self.call_fit_coupons(data)

        data = data.toPandas()
        self.assertEqual(data.at[0, "EXPERIMENT_ID"], 0)
        self.assertEqual(data.at[0, "CONSTRUCT_NAME"], "5__6")
        self.assertEqual(data.at[0, "SLOT_NBR"], 7)
        self.assertEqual(data.at[0, "SLOT_GRP"], 8)
        self.assertEqual(data.at[0, "PARENT_CONSTRUCT"], 9)
        self.assertEqual(data.at[0, "IS_BACKFILL"], 1)

    def test_split_invalid_slot(self):
        """
        If slot format is not perfectly valid, all fields will
        become null after preprocessing
        """
        slot = "c5__6s7g8x9b"
        data = spark.createDataFrame([[1, "2", slot]], self.header)
        out, data = self.call_fit_coupons(data)

        data = data.toPandas()
        self.assertFalse(data.at[0, "CONSTRUCT_NAME"])
        self.assertFalse(data.at[0, "IS_BACKFILL"])
        self.assertTrue(data.isna().at[0, "SLOT_NBR"])
        self.assertTrue(data.isna().at[0, "SLOT_GRP"])
        self.assertTrue(data.isna().at[0, "PARENT_CONSTRUCT"])

    def test_ranking_coupons_backfill_low(self):
        """
        This will test if coupons belonging to same member is ordered based
        on group value and ranking which is mocked in here.
        Uses backfill_priority "low"
        """
        data = spark.createDataFrame(
            [
                [1, "10", "c0__0s1g9p3", 3],
                [1, "20", "c0__0s1g9p3", 2],
                [1, "30", "c0__0s1g8p3", 1],
                [1, "40", "c0__0s1g7p3", 3],
            ],
            self.header + ", rank INT",
        )
        out, data = self.call_fit_coupons(data)

        out = out.toPandas()
        self.assertEqual(out.at[0, "cpn_nbr"], "40")
        self.assertEqual(out.at[1, "cpn_nbr"], "30")
        self.assertEqual(out.at[2, "cpn_nbr"], "20")
        self.assertEqual(out.at[3, "cpn_nbr"], "10")

    def test_ranking_coupons_backfill_high(self):
        """
        This will test if coupons belonging to same member is ordered based
        on group value and ranking which is mocked in here.
        Uses backfill_priority "high"
        """
        self.campaign.backfill_priority = Campaign.BackfillPriority.HIGH
        data = spark.createDataFrame(
            [
                [1, "80", "c0__0s1g9p3", 3],
                [1, "70", "c0__0s1g9p3", 2],
                [1, "60", "c0__0s1g8p3", 1],
                [1, "50", "c0__0s1g7p3", 3],
            ],
            self.header + ", rank INT",
        )
        out, data = self.call_fit_coupons(data)

        out = out.toPandas()
        self.assertEqual(out.at[0, "cpn_nbr"], "50")
        self.assertEqual(out.at[1, "cpn_nbr"], "60")
        self.assertEqual(out.at[2, "cpn_nbr"], "70")
        self.assertEqual(out.at[3, "cpn_nbr"], "80")


if __name__ == "__main__":

    try:
        pyspark
    except NameError:
        import findspark

        findspark.init()
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession, Row
    from pyspark.sql.functions import when, col
    import numpy as np

    name = "Campaign.py - unittests"
    spark = SparkSession.builder.appName(name).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
