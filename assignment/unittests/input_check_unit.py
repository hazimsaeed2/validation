import io
import os
import re
import sys
import unittest
import warnings

import pandas
import pe_memberdna.pipelines.assignment.lib.input_checks.exceptions as excp
import pe_memberdna.pipelines.assignment.lib.input_checks.longitudinal_design_check as ldc
import pe_memberdna.pipelines.assignment.lib.validators as validators
import xmlrunner
from mock import Mock, patch
from pemember_dna.pipelines.assignment.lib.assn_io import JobManager
from pemember_dna.pipelines.assignment.lib.input_checks.campaign_check import (
    check_campaign,
)
from pemember_dna.pipelines.assignment.lib.input_checks.cells_check import (
    check_cells_csv,
)
from pemember_dna.pipelines.assignment.lib.input_checks.checker import Check
from pemember_dna.pipelines.assignment.lib.input_checks.json_check import (
    check_jsons,
    converts_to_json,
    get_jsons,
    has_correct_static_data,
    name_matches_id,
)
from pemember_dna.pipelines.assignment.lib.input_checks.path_check import (
    check_paths,
)


class CampaignCheckTestCase(unittest.TestCase):
    """Unit tests for check_campaign"""

    def setUp(self):
        self.columns = [
            "experiment_id",
            "experiment_desc",
            "fiscal_year",
            "fiscal_num",
            "channel",
            "expected_distribution",
        ]
        self.valid_campaigns = pandas.DataFrame(
            columns=self.columns,
            data=[
                ["2", "raw_cf_2slot", "2019", "7", "MMPC", "2500000.00001"],
                [
                    "3",
                    "personalized_BBM_cover",
                    "2019",
                    "13",
                    "BBM",
                    "5400000",
                ],
                ["1002", "bifold", "2019", "12", "MMPC", "3600000"],
                ["5", "bifold stretch", "2019", "13", "MMPC", "3800000"],
                ["6", "bifold stretch", "2019", "13", "MMPC", "3800000"],
                [
                    "10",
                    "BBM16: seasonal and trip driving offers",
                    "2019",
                    "16",
                    "BBM",
                    "5400000",
                ],
                [
                    "11",
                    "BBM17: seasonal and trip driving offers plus inside"
                    " cover",
                    "2019",
                    "17",
                    "BBM",
                    "5500000",
                ],
            ],
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.campaign_check.read_s3_to_local"
    )
    def test_valid_campaign(self, read_s3_to_local):
        read_s3_to_local.return_value = self.valid_campaigns
        job = Mock(
            config=Mock(
                paths={"CAMPAIGN": "/path/to/campaign.csv"},
                params={"experiment": 1002},
            )
        )

        actual_checks = check_campaign(job)
        expected_checks = [
            Check("check_experiment_id", "campaign.csv", True),
            Check(
                "check_distribution_between_min_max",
                "campaign.csv",
                True,
                "Channel {channel} Min expected dist. {min_dist}. Max"
                " expected dist. {max_dist}. Current expected dist.:"
                " {current_dist}".format(
                    channel="MMPC",
                    min_dist=2500000.00001,
                    max_dist=3800000,
                    current_dist=3600000,
                ),
            ),
        ]

        self.assertEqual(actual_checks, expected_checks)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.campaign_check.read_s3_to_local"
    )
    def test_duplicate_experiment_id(self, read_s3_to_local):
        duplicate_campaigns = self.valid_campaigns.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    ["1002", "raw_cf_2slot", "2019", "7", "MMPC", "2500000"]
                ],
            )
        )
        read_s3_to_local.return_value = duplicate_campaigns
        job = Mock(
            config=Mock(
                paths={"CAMPAIGN": "/path/to/campaign.csv"},
                params={"experiment": 1002},
            )
        )
        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "The current experiment_id is duplicated" " in the campaign.csv",
            check_campaign,
            job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.campaign_check.read_s3_to_local"
    )
    def test_no_previous_experiments(self, read_s3_to_local):
        no_previous_experiments = self.valid_campaigns.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    ["1000", "raw_cf_2slot", "2019", "7", "EMAIL", "2500000"]
                ],
            )
        )
        read_s3_to_local.return_value = no_previous_experiments
        job = Mock(
            config=Mock(
                paths={"CAMPAIGN": "/path/to/campaign.csv"},
                params={"experiment": 1000},
            )
        )

        actual_checks = check_campaign(job)

        expected_checks = [
            Check("check_experiment_id", "campaign.csv", True),
            Check(
                "check_distribution_between_min_max",
                "campaign.csv",
                True,
                "No previous value. Current expected dist. "
                "{current_dist}:".format(current_dist=2500000),
            ),
        ]

        self.assertEqual(actual_checks, expected_checks)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.campaign_check.read_s3_to_local"
    )
    def test_expected_dist_min_max_is_the_same(self, read_s3_to_local):
        min_max_the_same = self.valid_campaigns.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    ["1000", "raw_cf_2slot", "2019", "7", "EMAIL", "2600000"],
                    ["1003", "raw_cf_2slot", "2019", "7", "EMAIL", "2600000"],
                ],
            )
        )
        read_s3_to_local.return_value = min_max_the_same
        job = Mock(
            config=Mock(
                paths={"CAMPAIGN": "/path/to/campaign.csv"},
                params={"experiment": 1003},
            )
        )

        actual_checks = check_campaign(job)

        expected_checks = [
            Check("check_experiment_id", "campaign.csv", True),
            Check(
                "check_distribution_between_min_max",
                "campaign.csv",
                True,
                "Channel {channel} Min expected dist. {min_dist}. Max"
                " expected dist. {max_dist}. Current expected dist.:"
                " {current_dist}".format(
                    channel="EMAIL",
                    min_dist=2600000,
                    max_dist=2600000,
                    current_dist=2600000,
                ),
            ),
        ]

        self.assertEqual(actual_checks, expected_checks)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.campaign_check.warnings.warn"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.campaign_check.read_s3_to_local"
    )
    def test_expected_dist_not_between_min_max(self, read_s3_to_local, warn):
        not_between_min_max = self.valid_campaigns.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[["1000", "raw_cf_2slot", "2019", "7", "MMPC", "250000"]],
            )
        )
        read_s3_to_local.return_value = not_between_min_max
        job = Mock(
            config=Mock(
                paths={"CAMPAIGN": "/path/to/campaign.csv"},
                params={"experiment": 1000},
            )
        )

        actual_checks = check_campaign(job)
        expected_checks = [
            Check("check_experiment_id", "campaign.csv", True),
            Check(
                "check_distribution_between_min_max",
                "campaign.csv",
                False,
                "Channel {channel} Min expected dist. {min_dist}. Max"
                " expected dist. {max_dist}. Current expected dist.:"
                " {current_dist}".format(
                    channel="MMPC",
                    min_dist=2500000.00001,
                    max_dist=3800000,
                    current_dist=250000,
                ),
            ),
        ]

        self.assertEqual(actual_checks, expected_checks)
        warn.assert_called_once()
        warn.assert_called_with(
            "Channel {channel} Min expected dist."
            " {min_dist}. Max"
            " expected dist. {max_dist}. Current expected"
            " dist.:"
            " {current_dist}".format(
                channel="MMPC",
                min_dist=2500000.00001,
                max_dist=3800000,
                current_dist=250000,
            ),
            excp.AssignmentInputWarning,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.campaign_check.read_s3_to_local"
    )
    def test_missing_experiment_id(self, read_s3_to_local):
        read_s3_to_local.return_value = self.valid_campaigns
        job = Mock(
            config=Mock(
                paths={"CAMPAIGN": "/path/to/campaign.csv"},
                params={"experiment": 1000},
            )
        )

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "The current experiment_id is not present" " in the campaign.csv",
            check_campaign,
            job,
        )


class CheckJSONSTestCase(unittest.TestCase):
    """Unit tests for check_jsons"""

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_no_jsons_present_in_experiment(self, get_jsons_mock):
        get_jsons_mock.return_value = ([], [])
        job = Mock()
        expected_result = []
        actual_result = check_jsons(job)

        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.has_correct_static_data"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.name_matches_id"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.converts_to_json"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_construct_is_ok(
        self,
        get_jsons_mock,
        converts_to_json_mock,
        name_matches_id_mock,
        has_correct_static_data_mock,
    ):
        get_jsons_mock.return_value = (["1"], [])
        converts_to_json_mock.return_value = (True, None)
        name_matches_id_mock.return_value = (True, None)
        has_correct_static_data_mock.return_value = (True, "test value")

        job = Mock(config=Mock(paths={"CONSTRUCT_BANK": "construct_path"}))
        expected_result = [
            Check(
                name="converts_to_json",
                file="CONSTRUCTS/1.json",
                status=True,
                details=None,
            ),
            Check(
                name="name_matches_id",
                file="CONSTRUCTS/1.json",
                status=True,
                details=None,
            ),
            Check(
                name="has_correct_static_data",
                file="CONSTRUCTS/1.json",
                status=True,
                details="test value",
            ),
        ]
        actual_result = check_jsons(job)

        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.converts_to_json"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_construct_is_not_valid_json(
        self, get_jsons_mock, converts_to_json_mock
    ):
        get_jsons_mock.return_value = (["1"], [])
        converts_to_json_mock.return_value = (False, ValueError())

        job = Mock(config=Mock(paths={"CONSTRUCT_BANK": "construct_path"}))

        self.assertRaises(excp.AssignmentInputError, check_jsons, job)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.name_matches_id"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.converts_to_json"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_construct_id_mismatch(
        self, get_jsons_mock, converts_to_json_mock, name_matches_id_mock
    ):
        get_jsons_mock.return_value = (["1"], [])
        converts_to_json_mock.return_value = (True, None)
        name_matches_id_mock.return_value = (False, "test message")

        job = Mock(config=Mock(paths={"CONSTRUCT_BANK": "construct_path"}))

        self.assertRaises(excp.AssignmentInputError, check_jsons, job)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.warnings.warn"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.has_correct_static_data"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.name_matches_id"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.converts_to_json"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_construct_has_incorrect_static_data(
        self,
        get_jsons_mock,
        converts_to_json_mock,
        name_matches_id_mock,
        has_correct_static_data_mock,
        warn,
    ):
        get_jsons_mock.return_value = (["1"], [])
        converts_to_json_mock.return_value = (True, None)
        name_matches_id_mock.return_value = (True, None)
        has_correct_static_data_mock.return_value = (False, "test message")

        job = Mock(config=Mock(paths={"CONSTRUCT_BANK": "construct_path"}))

        actual_checks = check_jsons(job)
        expected_checks = [
            Check("converts_to_json", "CONSTRUCTS/1.json", True, None),
            Check("name_matches_id", "CONSTRUCTS/1.json", True, None),
            Check(
                "has_correct_static_data",
                "CONSTRUCTS/1.json",
                False,
                "test message",
            ),
        ]

        self.assertEqual(actual_checks, expected_checks)
        warn.assert_called_once()
        warn.assert_called_with(
            "CONSTRUCTS/1.json: test message", excp.AssignmentInputWarning
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.name_matches_id"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.converts_to_json"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_segment_is_ok(
        self, get_jsons_mock, converts_to_json_mock, name_matches_id_mock
    ):
        get_jsons_mock.return_value = ([], ["1"])
        converts_to_json_mock.return_value = (True, None)
        name_matches_id_mock.return_value = (True, None)

        job = Mock(config=Mock(paths={"SEGMENT_BANK": "segment_path"}))
        expected_result = [
            Check(
                name="converts_to_json",
                file="SEGMENTS/1.json",
                status=True,
                details=None,
            ),
            Check(
                name="name_matches_id",
                file="SEGMENTS/1.json",
                status=True,
                details=None,
            ),
        ]
        actual_result = check_jsons(job)

        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.converts_to_json"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_segment_is_not_valid_json(
        self, get_jsons_mock, converts_to_json_mock
    ):
        get_jsons_mock.return_value = ([], ["1"])
        converts_to_json_mock.return_value = (False, ValueError())

        job = Mock(config=Mock(paths={"SEGMENT_BANK": "segment_path"}))
        self.assertRaises(excp.AssignmentInputError, check_jsons, job)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.name_matches_id"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.converts_to_json"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_segment_id_mismatch(
        self, get_jsons_mock, converts_to_json_mock, name_matches_id_mock
    ):
        get_jsons_mock.return_value = ([], ["1"])
        converts_to_json_mock.return_value = (True, None)
        name_matches_id_mock.return_value = (False, "test message")

        job = Mock(config=Mock(paths={"SEGMENT_BANK": "segment_path"}))
        self.assertRaises(excp.AssignmentInputError, check_jsons, job)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.has_correct_static_data"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.name_matches_id"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.converts_to_json"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.get_jsons"
    )
    def test_json_check_stops_at_first_error(
        self,
        get_jsons_mock,
        converts_to_json_mock,
        name_matches_id_mock,
        has_correct_static_data_mock,
    ):
        get_jsons_mock.return_value = (["1"], [])

        converts_to_json_mock.return_value = (False, ValueError())

        job = Mock(config=Mock(paths={"CONSTRUCT_BANK": "construct_path"}))
        self.assertRaises(excp.AssignmentInputError, check_jsons, job)

        converts_to_json_mock.assert_called_once()
        name_matches_id_mock.assert_not_called()
        has_correct_static_data_mock.assert_not_called()


class ConvertsToJSONTestCase(unittest.TestCase):
    """Unit tests for converts_to_json"""

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_converts_ok_to_json(self, read_s3_to_local):
        read_s3_to_local.return_value = None
        expected_result = (True, None)

        actual_result = converts_to_json('{"test_id": "test_value"}')

        self.assertEqual(actual_result, expected_result)

        actual_result = converts_to_json(
            "/path/to/json/json.json", is_file=True
        )

        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_value_error(self, read_s3_to_local):
        read_s3_to_local.side_effect = ValueError(
            "Expecting value: line 1 column 13 (char 12)"
        )
        expected_result = (
            False,
            ValueError("Expecting value: line 1 column 13 (char 12)"),
        )

        actual_result = converts_to_json('{"test_id": test_value}')

        self.assertEqual(actual_result[0], expected_result[0])
        self.assertEqual(str(actual_result[1]), str(expected_result[1]))
        self.assertTrue(isinstance(expected_result[1], ValueError))

        actual_result = converts_to_json(
            "/path/to/json/json.json", is_file=True
        )

        self.assertEqual(actual_result[0], expected_result[0])
        self.assertEqual(str(actual_result[1]), str(expected_result[1]))
        self.assertTrue(isinstance(expected_result[1], ValueError))


class NameMatchesIDTestCase(unittest.TestCase):
    """Unit tests for name_matches_id"""

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_name_matches_id(self, read_s3_to_local):
        read_s3_to_local.side_effect = [
            {"construct_id": "1"},
            {"segment_id": "2"},
        ]
        expected_result = (True, None)
        actual_result = name_matches_id("/path/to/1.json")
        self.assertEqual(actual_result, expected_result)

        actual_result = name_matches_id("/path/to/2.json")
        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_name_does_not_match_id(self, read_s3_to_local):
        read_s3_to_local.side_effect = [
            {"construct_id": "3"},
            {"segment_id": "5"},
        ]
        expected_result = (False, "File name 1 does not match content id 3")
        actual_result = name_matches_id("/path/to/1.json")
        self.assertEqual(actual_result, expected_result)

        expected_result = (False, "File name 2 does not match content id 5")
        actual_result = name_matches_id("/path/to/2.json")
        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_key_not_present_in_json(self, read_s3_to_local):
        read_s3_to_local.side_effect = [{"invalid_key": "3"}]
        expected_result = (
            False,
            "File contains neither construct_id" " nor segment_id",
        )
        actual_result = name_matches_id("/path/to/1.json")
        self.assertEqual(actual_result, expected_result)


class HasCorrectStaticDataTestCase(unittest.TestCase):
    """Unit tests for has_correct_static_data"""

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_static_slot_is_using_dummy_data(self, read_s3_to_local):
        to_check = """{
            "construct_id": 4,
            "slots": [
                {
                    "slot_num": 0,
                    "slot_size": 1,
                    "slot_type": "static",
                    "offer_data": "dummy"
                }
            ]
        }"""

        read_s3_to_local.return_value = eval(to_check)

        expected_result = (True, "Static slot(s) is using dummy data")
        actual_result = has_correct_static_data(
            "/path/to/construct/1.json", is_file=True
        )

        self.assertEqual(actual_result, expected_result)

        expected_result = (True, "Static slot(s) is using dummy data")
        actual_result = has_correct_static_data(to_check, is_file=False)

        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_static_slot_is_not_using_dummy_data(self, read_s3_to_local):
        to_check = """{
            "construct_id": 4,
            "slots": [
                {
                    "slot_num": 0,
                    "slot_size": 1,
                    "slot_type": "static",
                    "offer_data": "not dummy"
                }
            ]
        }"""
        read_s3_to_local.return_value = eval(to_check)

        expected_result = (False, "not dummy data being used with static slot")
        actual_result = has_correct_static_data(
            "/path/to/construct/1.json", is_file=True
        )

        self.assertEqual(actual_result, expected_result)

        expected_result = (False, "not dummy data being used with static slot")
        actual_result = has_correct_static_data(to_check, is_file=False)

        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_static_slot_not_present(self, read_s3_to_local):
        to_check = """{
            "construct_id": 4,
            "slots": [
                {
                  "slot_num": 4,
                  "slot_size": 1,
                  "slot_type": "rank_by_col",
                  "slot_parameters": {
                    "rank_col": "TRIPS",
                    "rank": 1
                  }
                }
            ]
        }"""
        read_s3_to_local.return_value = eval(to_check)

        expected_result = (None, "no static slot")
        actual_result = has_correct_static_data(
            "/path/to/construct/1.json", is_file=True
        )

        self.assertEqual(actual_result, expected_result)

        expected_result = (None, "no static slot")
        actual_result = has_correct_static_data(to_check, is_file=False)

        self.assertEqual(actual_result, expected_result)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_mixture_of_valid_invalid_static_slots(self, read_s3_to_local):
        to_check = """{
            "construct_id": 4,
            "slots": [
                {
                    "slot_num": 0,
                    "slot_size": 1,
                    "slot_type": "static",
                    "offer_data": "dummy"
                },
                {
                    "slot_num": 0,
                    "slot_size": 1,
                    "slot_type": "static",
                    "offer_data": "not dummy"
                }
            ]
        }"""

        read_s3_to_local.return_value = eval(to_check)

        expected_result = (False, "not dummy data being used with static slot")
        actual_result = has_correct_static_data(to_check, is_file=False)

        self.assertEqual(actual_result, expected_result)


class GetJSONSTestCase(unittest.TestCase):
    """Unit tests for get_jsons"""

    def setUp(self):
        ok_csv = io.StringIO(
            "cell_id,experiment_id,cell_name,cell_desc,inhome_date,"
            + "cell_start,cell_end,construct_id,bf_construct_id,ctrl_flag,"
            + "test_flag,sorting_order,cell_size,segment_id,longitudinal_id,"
            + "is_primary\n"
            + "0,1,Sumerville,NO TEST: sumerville,9 / 6 / 2018,9 / 7 / 2018,"
            + "9 / 19 / 2019,11,3,0,0,1,,6,,1\n"
            + "14,1,PAGINATION_TEST_control,CONROL: pagination,9/6/2018,"
            + "9/7/2018,9/19/2019,12,,1,0,2,400000,,,1\n"
            + "83,1,5 slots decile 1,5 slots decile 1,9/26/2018,9/20/2018,"
            + "10/10/2018,,,0,1,,,,,0\n"
            + "19,5,PAGINATION_STRETCH_CONTROL,CONTOL: 5fold,9/26/2018,"
            + "9/20/2018,10/10/2018,12,,1,0,4,571000,8,,1\n"
            + "20,1,PAGINATION_STRETCH_CONTROL,CONTOL: 5fold,9/26/2018,"
            + "9/20/2018,10/10/2018,0,,1,0,4,571000,0,,1\n"
            + "21,1,PAGINATION_STRETCH_CONTROL,CONTOL: 5fold,9/26/2018,"
            + "9/20/2018,10/10/2018,1_2,1_3,1,0,4,571000,0,,1\n"
            + "80,1,5 slots decile 1,5 slots decile 1,9/26/2018,9/20/2018,"
            + "10/10/2018,,,0,1,,,1_2_3,,0\n"
            + "79,1,5 slots decile 1,5 slots decile 1,9/26/2018,9/20/2018,"
            + "10/10/2018,,,0,1,,,1|2|3,,0\n"
        )
        self.ok_cells = pandas.read_csv(ok_csv, sep=",")

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.json_check.read_s3_to_local"
    )
    def test_return_ids(self, read_s3_to_local):
        read_s3_to_local.return_value = self.ok_cells

        job = Mock(
            config=Mock(
                paths={"CELL": "/path/to/cell"}, params={"experiment": 1}
            )
        )

        expected_results = (
            {"11", "12", "0", "1", "2", "3"},
            {"6", "0", "1", "2", "3"},
        )
        actual_results = get_jsons(job)

        self.assertEqual(actual_results, expected_results)


class CheckCellsCSV(unittest.TestCase):
    """Unit tests for check_cells_csv"""

    def setUp(self):
        self.columns = [
            "cell_id",
            "experiment_id",
            "cell_name",
            "cell_desc",
            "inhome_date",
            "cell_start",
            "cell_end",
            "construct_id",
            "bf_construct_id",
            "ctrl_flag",
            "test_flag",
            "sorting_order",
            "cell_size",
            "estimated_size",
            "segment_id",
            "longitudinal_id",
            "is_primary",
        ]
        self.valid_cells_block = pandas.DataFrame(
            columns=self.columns,
            data=[
                [
                    "0",
                    "4",
                    "Sumerville",
                    "NO TEST: sumerville",
                    "9/6/2018",
                    "9/7/2018",
                    "9/19/2019",
                    "11",
                    "",
                    "0",
                    "0",
                    "1",
                    "100",
                    "100",
                    "6",
                    "",
                    "1",
                ],
                [
                    "5",
                    "4",
                    "ROI_control_bopic",
                    "CONTROL: ROI+ members get no offer",
                    "9/19/2018",
                    "9/20/2018",
                    "10/10/2018",
                    "4",
                    "",
                    "1",
                    "0",
                    "3",
                    "200000",
                    "2",
                    "",
                    "1",
                ],
                [
                    "5",
                    "3",
                    "test name",
                    "test description",
                    "9/19/2018",
                    "9/20/2019",
                    "10/10/2018",
                    "4",
                    "",
                    "1",
                    "0",
                    "3",
                    "200000",
                    "2",
                    "",
                    "1",
                ],
                [
                    "5",
                    "3",
                    "test name",
                    "test description",
                    "200/19/2018",
                    "9/20/2018",
                    "10/10/2018",
                    "4",
                    "",
                    "1",
                    "0",
                    "3",
                    "200000",
                    "2",
                    "",
                    "1",
                ],
            ],
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.cells_check.read_s3_to_local"
    )
    def test_valid_cells_csv(self, read_s3_to_local):
        read_s3_to_local.return_value = self.valid_cells_block

        job = Mock(
            config=Mock(
                params={"experiment": 4}, paths={"CELL": "/path/to/cells.csv"}
            )
        )
        actual_checks = check_cells_csv(job)
        expected_checks = [
            Check("check_experiment_id", "cells.csv", True),
            Check("check_duplicate_entries", "cells.csv", True),
            Check(
                "check_date_format",
                "cells.csv",
                True,
                "All below columns have the correct date format"
                " (e.g. 1/3/2019): {}".format(
                    ["cell_start", "cell_end", "inhome_date"]
                ),
            ),
            Check(
                "check_cell_start_before_cell_end",
                "cells.csv",
                True,
                "All dates in cell_start precedes cell_end",
            ),
            Check(
                "check_inhome_before_cell_end",
                "cells.csv",
                True,
                "All dates in inhome_date precedes cell_end",
            ),
            Check(
                "check_header_cells.csv",
                "cells.csv",
                True,
                "The headers of this file match the specified schema",
            ),
        ]

        self.assertEqual(actual_checks, expected_checks)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.cells_check.read_s3_to_local"
    )
    def test_experiment_id_mismatch(self, read_s3_to_local):
        read_s3_to_local.return_value = self.valid_cells_block
        job = Mock(
            config=Mock(
                params={"experiment": 100},
                paths={"CELL": "/path/to/cells.csv"},
            )
        )

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "The current experiment_id is not present" "in the cells.csv",
            check_cells_csv,
            job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.cells_check.read_s3_to_local"
    )
    def test_cell_experiment_entry_duplicates(self, read_s3_to_local):
        duplicate_entries = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "0",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "9/6/2018",
                        "9/7/2018",
                        "9/19/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )
        read_s3_to_local.return_value = duplicate_entries
        job = Mock(
            config=Mock(
                params={"experiment": 4}, paths={"CELL": "/path/to/cells.csv"}
            )
        )

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape(
                "Cell/Experiment pair duplicates"
                " in the cells.csv((cell_id, exp_id):"
                " times): ('0', '4'): 2x"
            ),
            check_cells_csv,
            job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.cells_check.read_s3_to_local"
    )
    def test_cell_start_greater_than_cell_end(self, read_s3_to_local):
        greater_start_date = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "1",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "9/6/2018",
                        "10/7/2019",
                        "9/19/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )
        read_s3_to_local.return_value = greater_start_date
        job = Mock(
            config=Mock(
                params={"experiment": 4}, paths={"CELL": "/path/to/cells.csv"}
            )
        )

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape("cell_start has date after cell_end"),
            check_cells_csv,
            job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.cells_check.read_s3_to_local"
    )
    def test_inhome_date_greater_than_cell_end(self, read_s3_to_local):
        greater_inhome_date = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "1",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "10/6/2019",
                        "9/7/2018",
                        "9/19/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )
        read_s3_to_local.return_value = greater_inhome_date
        job = Mock(
            config=Mock(
                params={"experiment": 4}, paths={"CELL": "/path/to/cells.csv"}
            )
        )

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape("inhome_date has date after cell_end"),
            check_cells_csv,
            job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.cells_check.read_s3_to_local"
    )
    def test_date_format(self, read_s3_to_local):
        invalid_month_inhome_date_format = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "1",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "20/6/2018",
                        "9/7/2018",
                        "9/19/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )
        invalid_month_start_date_format = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "1",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "9/6/2018",
                        "20/7/2018",
                        "9/19/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )
        invalid_month_end_date_format = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "1",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "9/6/2018",
                        "9/7/2018",
                        "20/19/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )
        invalid_zero_pad_inhome_date_format = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "1",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "02/06/2018",
                        "9/7/2018",
                        "9/19/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )
        invalid_zero_pad_start_date_format = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "1",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "9/6/2018",
                        "02/07/2018",
                        "9/19/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )
        invalid_zero_pad_end_date_format = self.valid_cells_block.append(
            pandas.DataFrame(
                columns=self.columns,
                data=[
                    [
                        "1",
                        "4",
                        "Sumerville",
                        "NO TEST: sumerville",
                        "9/6/2018",
                        "9/7/2018",
                        "2/09/2019",
                        "11",
                        "",
                        "0",
                        "0",
                        "1",
                        "100",
                        "100",
                        "6",
                        "1",
                        "",
                    ]
                ],
            ),
            ignore_index=True,
        )

        read_s3_to_local.side_effect = [
            invalid_month_inhome_date_format,
            invalid_month_start_date_format,
            invalid_month_end_date_format,
            invalid_zero_pad_inhome_date_format,
            invalid_zero_pad_start_date_format,
            invalid_zero_pad_end_date_format,
        ]
        job = Mock(
            config=Mock(
                params={"experiment": 4}, paths={"CELL": "/path/to/cells.csv"}
            )
        )
        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape(
                "inhome_date column does not have the"
                " correct dateformat, correct format"
                " should be like 1/3/2019"
            ),
            check_cells_csv,
            job,
        )
        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape(
                "cell_start column does not have the"
                " correct dateformat, correct format"
                " should be like 1/3/2019"
            ),
            check_cells_csv,
            job,
        )
        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape(
                "cell_end column does not have the"
                " correct dateformat, correct format"
                " should be like 1/3/2019"
            ),
            check_cells_csv,
            job,
        )
        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape(
                "inhome_date column does not have the"
                " correct dateformat, correct format"
                " should be like 1/3/2019"
            ),
            check_cells_csv,
            job,
        )
        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape(
                "cell_start column does not have the"
                " correct dateformat, correct format"
                " should be like 1/3/2019"
            ),
            check_cells_csv,
            job,
        )
        self.assertRaisesRegex(
            excp.AssignmentInputError,
            re.escape(
                "cell_end column does not have the"
                " correct dateformat, correct format"
                " should be like 1/3/2019"
            ),
            check_cells_csv,
            job,
        )


class CheckPathTestCase(unittest.TestCase):
    """Unit tests for check_paths"""

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.job = JobManager(
            "check_paths",
            "check_paths",
            conf_path_in=os.path.join(
                os.path.dirname(dir_path), "conf/config_template.yml"
            ),
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.path_check.warnings.warn"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.path_check.is_s3_path"
    )
    def test_all_paths_are_valid(self, is_s3_path, warn):
        is_s3_path.return_value = True

        actual_checks = check_paths(self.job)
        self.expected_checks = [
            Check("paths_exist", self.job.config.cfg_path, True),
            Check("optional_matches_precalculated", "INPUT_ASSIGNMENTS", True),
            Check("optional_matches_precalculated", "INPUT_CONSTRUCTS", True),
            Check("optional_matches_precalculated", "INPUT_MAILHOUSE", True),
            Check(
                "optional_matches_precalculated",
                "MAIL_POPULATION_ASSIGNMENT",
                True,
            ),
        ]
        self.assertEqual(actual_checks, self.expected_checks)
        self.assertEqual(warn.call_count, 0)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.path_check.warnings.warn"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.path_check.is_s3_path"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.path_check.is_s3_file"
    )
    def test_path_does_not_exist(self, is_s3_path, is_s3_file, warn):
        is_s3_path.return_value = False
        is_s3_file.return_value = True

        actual_checks = check_paths(self.job)
        self.expected_checks = [
            Check("paths_exist", self.job.config.cfg_path, True),
            Check("optional_matches_precalculated", "INPUT_ASSIGNMENTS", True),
            Check("optional_matches_precalculated", "INPUT_CONSTRUCTS", True),
            Check("optional_matches_precalculated", "INPUT_MAILHOUSE", True),
            Check(
                "optional_matches_precalculated",
                "MAIL_POPULATION_ASSIGNMENT",
                True,
            ),
        ]
        self.assertEqual(actual_checks, self.expected_checks)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.path_check.is_s3_path"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.path_check.is_s3_file"
    )
    def test_file_does_not_exist(self, is_s3_path, is_s3_file):
        is_s3_path.return_value = False
        is_s3_file.return_value = False

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "Parameter TRANSACTIONS_PATH has invalid path"
            " assigned: s3://memberanalytics-data-out-prod"
            "/pipelined_intermediates/transaction_fiscal/detail",
            check_paths,
            self.job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks.path_check.is_s3_path"
    )
    def test_warnings_for_optional_paths(self, is_s3_path):
        is_s3_path.return_value = True

        with warnings.catch_warnings(record=True) as w:
            self.job.config.paths["INPUT_ASSIGNMENTS"] = "wrong_path/"
            self.job.config.paths["INPUT_CONSTRUCTS"] = "wrong_path/"
            self.job.config.paths["MAIL_POPULATION_ASSIGNMENT"] = "wrong_path/"
            self.job.config.paths["INPUT_MAILHOUSE"] = "wrong_path/"
            self.job.config.params["campaign"] = "MMPC"
            check_paths(self.job)
            self.assertEqual(len(w), 4)
            self.assertTrue(
                issubclass(w[0].category, excp.AssignmentInputWarning)
            )
            self.assertTrue("Pre-calculated path" in str(w[0].message))
            self.assertTrue(
                issubclass(w[1].category, excp.AssignmentInputWarning)
            )
            self.assertTrue("Pre-calculated path" in str(w[1].message))
            self.assertTrue(
                issubclass(w[2].category, excp.AssignmentInputWarning)
            )
            self.assertTrue("Pre-calculated path" in str(w[2].message))
            self.assertTrue(
                issubclass(w[3].category, excp.AssignmentInputWarning)
            )
            self.assertTrue("Pre-calculated path" in str(w[3].message))


class DupsCheckTestCase(unittest.TestCase):
    """Unit tests for duplicate check"""

    def setUp(self):
        self.columns = ["col1", "col2"]
        self.test_data = pandas.DataFrame(
            columns=self.columns, data=[["A", "1"], ["B", "3"]]
        )

    def test_dups_columns(self):
        dups_test_data = self.test_data.append(
            pandas.DataFrame(columns=self.columns, data=[["A", "2"]])
        )
        with self.assertRaisesRegex(
            excp.AssignmentInputError,
            "Duplicate values are present in the following column.*col1.*",
        ):
            validators.check_column_duplicates(dups_test_data, ["col1"])

    def test_non_dups_columns(self):
        status, details = validators.check_column_duplicates(self.test_data)
        actual_check = Check("duplicate", "input_file.csv", status, details)
        expected_check = Check(
            "duplicate", "input_file.csv", True, "No duplicates found"
        )
        self.assertEqual(actual_check, expected_check)


class LongitudinalDesignCheckTestCase(unittest.TestCase):
    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.job = JobManager(
            "check_longitudinal_design",
            "check_longitudinal_design",
            conf_path_in=os.path.join(
                os.path.dirname(dir_path), "conf/config_template.yml"
            ),
        )

    @patch("memberdna.pipelines.assignment.lib.assn_io.read_s3_to_local")
    def test_longitudinal_design_fails_with_dups(self, data_read):
        data_read.return_value = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, None, 1, 1], [1, 2, 2, None, 2, 1]],
        )

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "Duplicate values are present in the following column\(s\):"
            " \['longitudinal_id'\]",
            ldc.check_longitudinal_design,
            self.job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks."
        "longitudinal_design_check.iotools.read_s3_to_local"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_io.read_s3_to_local")
    def test_longitudinal_invalid_segment_config(
        self, data_read, read_s3_to_local
    ):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, None, 1, 1], [1, 2, 2, None, 2, 2]],
        )
        segments = [
            {
                "segment_id": 1,
                "filters": [
                    {
                        "filter_num": 1,
                        "filter_type": "longitudinal",
                        "filter_parameters": {"filters": []},
                    }
                ],
            },
            {
                "segment_id": 2,
                "filters": [
                    {
                        "filter_num": 1,
                        "filter_type": "longitudinal",
                        "filter_parameters": {"filters": []},
                    }
                ],
            },
        ]
        data_read.return_value = cells
        read_s3_to_local.side_effect = segments

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "mbr_type is not set for segment 1",
            ldc.check_longitudinal_design,
            self.job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks."
        "longitudinal_design_check.iotools.read_s3_to_local"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_io.read_s3_to_local")
    def test_longitudinal_cell_without_longitudinal_segment(
        self, data_read, read_s3_to_local
    ):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, None, 1, 1], [1, 2, 2, None, 2, 2]],
        )
        segments = [
            {
                "segment_id": 1,
                "filters": [
                    {
                        "filter_num": 1,
                        "filter_type": "longitudinal",
                        "filter_parameters": {
                            "mbr_type": "new",
                            "filters": [],
                        },
                    }
                ],
            },
            {
                "segment_id": 2,
                "filters": [
                    {
                        "filter_num": 1,
                        "filter_type": "construct_satisfied",
                        "filter_parameters": {},
                    }
                ],
            },
        ]

        data_read.return_value = cells
        read_s3_to_local.side_effect = segments

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "Longitudinal cell {} does not have a longitudinal segment"
            " assigned".format(2),
            ldc.check_longitudinal_design,
            self.job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks."
        "longitudinal_design_check.iotools.read_s3_to_local"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_io.read_s3_to_local")
    def test_longitudinal_segment_without_long_id(
        self, data_read, read_s3_to_local
    ):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, None, 1, 1], [1, 2, 2, None, 2, None]],
        )
        segments = [
            {
                "segment_id": 1,
                "filters": [
                    {
                        "filter_num": 1,
                        "filter_type": "longitudinal",
                        "filter_parameters": {
                            "mbr_type": "new",
                            "filters": [],
                        },
                    }
                ],
            },
            {
                "segment_id": 2,
                "filters": [
                    {
                        "filter_num": 1,
                        "filter_type": "longitudinal",
                        "filter_parameters": {
                            "mbr_type": "new",
                            "filters": [],
                        },
                    }
                ],
            },
        ]

        data_read.return_value = cells
        read_s3_to_local.side_effect = segments

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "Longitudinal cell \[2\] has a longitudinal segment set, but no"
            " longitudinal id",
            ldc.check_longitudinal_design,
            self.job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks."
        "longitudinal_design_check.iotools.read_s3_to_local"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_io.read_s3_to_local")
    def test_longitudinal_design_fails_with_invalid_handshakes(
        self, data_read, read_s3_to_local
    ):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[
                [1, 1, 1, None, 1, 1],
                [1, 2, 2, None, 2, 2],
                [2, 3, 3, None, 3, 2],
                [2, 4, 4, None, 4, 1],
            ],
        )
        third_segment = {
            "segment_id": 3,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "mbr_type": "both",
                        "filters": [],
                        "filter_past_mbrs": False,
                    },
                }
            ],
        }
        forth_segment = {
            "segment_id": 4,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "mbr_type": "both",
                        "filters": [],
                        "filter_past_mbrs": False,
                    },
                }
            ],
        }

        handshakes = pandas.DataFrame(
            columns=[
                "comparison_id",
                "cell_id",
                "comparison_desc",
                "comparison_base_flag",
            ],
            data=[
                [1, 1, "cell 1 vs cell 2", 1],
                [1, 2, "cell 1 vs cell 2", 0],
                [2, 3, "cell 3 vs cell 4", 1],
                [2, 4, "cell 3 vs cell 4", 0],
            ],
        )
        data_read.side_effect = [cells, handshakes]
        read_s3_to_local.side_effect = [third_segment, forth_segment]

        self.job.config.params["experiment"] = 2
        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "The following longitudinal_ids have different historical"
            " handshakes than in the current campaign: \[1, 2\]",
            ldc.check_longitudinal_design,
            self.job,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks."
        "longitudinal_design_check.warnings.warn"
    )
    @patch(
        "memberdna.pipelines.assignment.lib.input_checks."
        "longitudinal_design_check.iotools.read_s3_to_local"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_io.read_s3_to_local")
    def test_experiment_executed_in_different_order(
        self, data_read, read_s3_to_local, warn
    ):
        warn.return_value = None
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[
                [1, 1, 1, None, 1, 1],
                [1, 2, 2, None, 2, 2],
                [2, 3, 3, None, 3, 1],
                [2, 4, 4, None, 4, 2],
            ],
        )
        first_segment = {
            "segment_id": 1,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "mbr_type": "both",
                        "filters": [],
                        "filter_past_mbrs": False,
                    },
                }
            ],
        }
        second_segment = {
            "segment_id": 2,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "mbr_type": "both",
                        "filters": [],
                        "filter_past_mbrs": False,
                    },
                }
            ],
        }

        handshakes = pandas.DataFrame(
            columns=[
                "comparison_id",
                "cell_id",
                "comparison_desc",
                "comparison_base_flag",
            ],
            data=[
                [1, 1, "cell 1 vs cell 2", 1],
                [1, 2, "cell 1 vs cell 2", 0],
                [2, 3, "cell 3 vs cell 4", 1],
                [2, 4, "cell 3 vs cell 4", 0],
            ],
        )
        data_read.side_effect = [cells, handshakes]
        read_s3_to_local.side_effect = [first_segment, second_segment]

        self.job.config.params["experiment"] = 1
        checks = ldc.check_longitudinal_design(self.job)

        self.assertEqual(
            checks[-1],
            Check(
                "check_longitudinal_experiment_order",
                "cells.csv",
                False,
                "Experiments have been executed in the order they were created",
            ),
        )
        warn.assert_called_with(
            "There is at least one campaign with a greater experiment id which"
            " uses the current longitudinal ids",
            excp.AssignmentInputWarning,
        )

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks."
        "longitudinal_design_check.iotools.read_s3_to_local"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_io.read_s3_to_local")
    def test_longitudinal_design_checks_pass(
        self, data_read, read_s3_to_local
    ):
        """
        This test makes sure the longitudinal design passes with valid
        data, which now includes a cell without longitudinal id but with
        a composite segment.
        """
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[
                [1, 1, 1, None, 1, 1],
                [1, 2, 2, None, 2, 2],
                [2, 3, 3, None, 3, 1],
                [2, 4, 4, None, 4, 2],
                [3, 5, 5, None, "5_6", None],
            ],
        )
        third_segment = {
            "segment_id": 3,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "mbr_type": "both",
                        "filters": [],
                        "filter_past_mbrs": False,
                    },
                }
            ],
        }
        forth_segment = {
            "segment_id": 4,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "mbr_type": "both",
                        "filters": [],
                        "filter_past_mbrs": False,
                    },
                }
            ],
        }

        handshakes = pandas.DataFrame(
            columns=[
                "comparison_id",
                "cell_id",
                "comparison_desc",
                "comparison_base_flag",
            ],
            data=[
                [1, 1, "cell 1 vs cell 2", 1],
                [1, 2, "cell 1 vs cell 2", 0],
                [2, 3, "cell 3 vs cell 4", 1],
                [2, 4, "cell 3 vs cell 4", 0],
            ],
        )
        data_read.side_effect = [cells, handshakes]
        read_s3_to_local.side_effect = [third_segment, forth_segment]

        self.job.config.params["experiment"] = 2
        checks = ldc.check_longitudinal_design(self.job)
        self.assertEquals(len(checks), 7)

        expected_checks = [
            Check(
                "check_longitudinal_id_duplicates",
                "cells.csv",
                True,
                "No duplicates found",
            ),
            Check(
                "check_longitudinal_cells_segment_type",
                "cells.csv",
                True,
                "All cell have valid longitudinal segments assigned",
            ),
            Check(
                "check_longitudinal_segment_longitudinal_id",
                "cells.csv",
                True,
                "All longitudinal segments have a longitudinal id",
            ),
            Check(
                "check_segment_configuration",
                "3.json",
                True,
                "Longitudinal settings are not missing or inconsistent",
            ),
            Check(
                "check_segment_configuration",
                "4.json",
                True,
                "Longitudinal settings are not missing or inconsistent",
            ),
            Check(
                "check_longitudinal_handshakes_consistency",
                "handshakes.csv and cells.csv",
                True,
                "Longitudinal handshakes consistent with historical campaigns",
            ),
            Check(
                "check_longitudinal_experiment_order",
                "cells.csv",
                True,
                "Experiments have been executed in the order they were created",
            ),
        ]
        for idx, check in enumerate(checks):
            self.assertEqual(expected_checks[idx], check)

    @patch(
        "memberdna.pipelines.assignment.lib.input_checks."
        "longitudinal_design_check.iotools.read_s3_to_local"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_io.read_s3_to_local")
    def test_longitudinal_segment_with_composite_segment(
        self, data_read, read_s3_to_local
    ):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "construct_id",
                "bf_construct_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, None, "1_2", 1], [1, 2, 2, None, "2", None]],
        )
        segments = [
            {
                "segment_id": 1,
                "filters": [
                    {
                        "filter_num": 1,
                        "filter_type": "longitudinal",
                        "filter_parameters": {
                            "mbr_type": "new",
                            "filters": [],
                        },
                    }
                ],
            },
            {
                "segment_id": 2,
                "filters": [
                    {
                        "filter_num": 1,
                        "filter_type": "construct_satisfied",
                        "filter_parameters": {},
                    }
                ],
            },
        ]

        data_read.return_value = cells
        read_s3_to_local.side_effect = segments

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "Longitudinal cell [1] cannot have composed segments" " assigned",
            ldc.check_longitudinal_design,
            self.job,
        )


class SegmentConfigurationCheckTestCase(unittest.TestCase):
    def test_longitudinal_segment_mbr_type_not_set(self):
        cells = (
            pandas.DataFrame(
                columns=[
                    "experiment_id",
                    "cell_id",
                    "segment_id",
                    "longitudinal_id",
                ],
                data=[[1, 1, 1, 1]],
            ),
        )
        segment = {
            "segment_id": 1,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {"filters": []},
                }
            ],
        }

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "mbr_type is not set for segment 1",
            validators.check_segment_configuration,
            1,
            segment,
            cells,
        )

    def test_longitudinal_segment_mbr_type_incorrect(self):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1]],
        )
        segment = {
            "segment_id": 1,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "filters": [],
                        "mbr_type": "incorrect_value",
                    },
                }
            ],
        }

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "Please set mbr type to 'new', 'past', or 'both' in filter "
            "parameters",
            validators.check_segment_configuration,
            1,
            segment,
            cells,
        )

    def test_longitudinal_segment_filter_past_mbrs_not_set_with_past_mbrs(
        self,
    ):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1]],
        )
        segment = {
            "segment_id": 1,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {"filters": [], "mbr_type": "past"},
                }
            ],
        }

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "If using past mbrs, please set filter past mbrs to True or False",
            validators.check_segment_configuration,
            1,
            segment,
            cells,
        )

    def test_longitudinal_segment_filter_past_mbrs_on_new_members(self):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1]],
        )
        segment = {
            "segment_id": 1,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "filters": [],
                        "mbr_type": "new",
                        "filter_past_mbrs": True,
                    },
                }
            ],
        }

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "If you want to filter past mbrs, please set mbr type to 'both' or"
            " 'past'",
            validators.check_segment_configuration,
            1,
            segment,
            cells,
        )

    def test_use_past_members_with_no_past_long_ids(self):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1]],
        )
        segment = {
            "segment_id": 1,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "filters": [],
                        "mbr_type": "both",
                        "filter_past_mbrs": True,
                    },
                }
            ],
        }

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "The following longitudinal ids have not been used before, "
            "but are set to load past mbrs: {1}",
            validators.check_segment_configuration,
            1,
            segment,
            cells,
        )

    def test_past_long_ids_present_but_use_past_members_not_set(self):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1], [2, 2, 2, 1]],
        )
        segment = {
            "segment_id": 2,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {"filters": [], "mbr_type": "new"},
                }
            ],
        }

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "The following longitudinal ids have been used before, but "
            "are not set to load past mbrs: \[1\]",
            validators.check_segment_configuration,
            2,
            segment,
            cells,
        )

    def test_logitudinal_segment_is_ok(self):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1], [2, 2, 2, 1]],
        )
        segment = {
            "segment_id": 2,
            "filters": [
                {
                    "filter_num": 1,
                    "filter_type": "longitudinal",
                    "filter_parameters": {
                        "filters": [],
                        "mbr_type": "both",
                        "filter_past_mbrs": True,
                    },
                }
            ],
        }

        status, details = validators.check_segment_configuration(
            2, segment, cells
        )

        self.assertTrue(status)
        self.assertEqual(
            details, "Longitudinal settings are not missing or inconsistent"
        )


class CheckLongitudinalHandshakesTestCase(unittest.TestCase):
    def test_longitudinal_id_with_incorrect_handshakes(self):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1], [1, 2, 2, 2], [2, 3, 3, 2], [2, 4, 4, 1]],
        )

        handshakes = pandas.DataFrame(
            columns=[
                "comparison_id",
                "cell_id",
                "comparison_desc",
                "comparison_base_flag",
            ],
            data=[
                [1, 1, "cell 1 vs cell 2", 1],
                [1, 2, "cell 1 vs cell 2", 0],
                [2, 3, "cell 3 vs cell 4", 1],
                [2, 4, "cell 3 vs cell 4", 0],
            ],
        )

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "The following longitudinal_ids have different historical "
            "handshakes than in the current campaign: \[1, 2\]",
            validators.check_longitudinal_handshakes,
            cells,
            handshakes,
            2,
        )

    def test_longitudinal_id_with_incomplete_handshakes(self):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1], [1, 2, 2, 2], [2, 3, 3, 2], [2, 4, 4, 1]],
        )

        handshakes = pandas.DataFrame(
            columns=[
                "comparison_id",
                "cell_id",
                "comparison_desc",
                "comparison_base_flag",
            ],
            data=[
                [1, 1, "cell 1 vs cell 2", 1],
                [1, 2, "cell 1 vs cell 2", 0],
                [2, 3, "cell 3 vs cell 4", 1],
            ],
        )

        self.assertRaisesRegex(
            excp.AssignmentInputError,
            "The following longitudinal_ids have different historical "
            "handshakes than in the current campaign: \[1, 2\]",
            validators.check_longitudinal_handshakes,
            cells,
            handshakes,
            2,
        )

    def test_longitudinal_id_with_correct_handshakes(self):
        cells = pandas.DataFrame(
            columns=[
                "experiment_id",
                "cell_id",
                "segment_id",
                "longitudinal_id",
            ],
            data=[[1, 1, 1, 1], [1, 2, 2, 2], [2, 3, 3, 1], [2, 4, 4, 2]],
        )

        handshakes = pandas.DataFrame(
            columns=[
                "comparison_id",
                "cell_id",
                "comparison_desc",
                "comparison_base_flag",
            ],
            data=[
                [1, 1, "cell 1 vs cell 2", 1],
                [1, 2, "cell 1 vs cell 2", 0],
                [2, 3, "cell 3 vs cell 4", 1],
                [2, 4, "cell 3 vs cell 4", 0],
            ],
        )

        status, details = validators.check_longitudinal_handshakes(
            cells, handshakes, 2
        )

        self.assertTrue(status)
        self.assertEqual(
            details,
            "Longitudinal handshakes consistent with historical campaigns",
        )


if __name__ == "__main__":
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
