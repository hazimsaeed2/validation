"""Unit Tests for offer assignment."""


import unittest

import pandas as pd
import xmlrunner
from mock import Mock, patch

from pe_memberdna.assignment.lib.checks import check_execution_overwrite


class TestUtils2(unittest.TestCase):
    """Unit tests for util functions."""

    def test_downsample_rows(self):

        data = spark.sparkContext.parallelize(
            map(lambda x: [0], range(0, 100))
        ).toDF()
        ds = downsample_rows(data, "_1", 0, 0.4, 5)
        ds.count()
        self.assertEqual(ds.count() == 40, True)

        data = spark.sparkContext.parallelize(
            map(lambda x: ["hat"], range(0, 100))
        ).toDF()
        ds = downsample_rows(data, "_1", "hat", 0.4, 5)
        self.assertEqual(ds.count() == 40, True)

    def test_cap_top_percentile(self):
        data = (
            spark.sparkContext.parallelize(map(lambda x: [x], range(0, 101)))
            .toDF(["column"])
            .cache()
        )
        fixed = cap_top_percentile(data, "column").cache()

        self.assertEqual(fixed.groupBy().max().collect()[0][0], 99.0)

    def test_get_all_of_key(self):

        deep_map = {
            "k1": "v1",
            "hs_ind_lambda": "v3",
            "k2": {
                "hs_ind_lambda": "v3",
                "k4": [{"k5": {"hs_ind_lambda": "v5"}}],
            },
        }

        self.assertEqual(get_all_of_key(deep_map), ["v3", "v3", "v5"])

    def test_get_key_values(self):
        deep_map = {
            "k1": "v1",
            "hs_ind_lambda": "v3",
            "k2": {
                "hs_ind_lambda": "v3",
                "k4": [{"k5": {"hs_ind_lambda": "v5"}}],
            },
        }
        print(get_key_values(deep_map))
        self.assertEqual(
            [kv for kv in get_key_values(deep_map)],
            [
                ("k1", "v1"),
                ("hs_ind_lambda", "v3"),
                ("hs_ind_lambda", "v3"),
                ("hs_ind_lambda", "v5"),
            ],
        )

    def test_mapping(self):
        """Test assn_utils.mapping()."""
        CATEGORY_LOOKUP = {
            "IN-HSE BAKERY": "BAKERY",
            "COMMERCIAL BAKERY": "BAKERY",
            "DEFAULT": "default",
        }
        vals = [["IN-HSE BAKERY"], ["COMMERCIAL BAKERY"], ["HAHA"]]
        test = sc.parallelize(vals).toDF(["category"])

        test_after = mapping(test, "category", "category", CATEGORY_LOOKUP)
        val_count = test_after.filter(
            test_after.category.isin(list(CATEGORY_LOOKUP.keys()))
        ).count()
        map_val_count = test_after.filter(
            test_after.category.isin(list(CATEGORY_LOOKUP.values()))
        ).count()
        default_val_count = test_after.filter(
            test_after.category == "default"
        ).count()
        other_val_count = test_after.filter(
            (~test_after.category.isin(list(CATEGORY_LOOKUP.values())))
            & (~test_after.category.isin(list(CATEGORY_LOOKUP.keys())))
        ).count()
        self.assertEqual(
            [val_count, map_val_count, default_val_count, other_val_count],
            [0, 3, 1, 0],
        )

    def test_map_under_threshold(self):
        """Test assn_utils.map_under_threshold()."""
        threshold = [50, 100, 400]
        vals = [[30], [50], [90], [380], [600]]
        df = sc.parallelize(vals).toDF(["val_before_mapping"])
        df_mapped = map_under_threshold(
            df, "val_before_mapping", "val_after_mapping", threshold
        )
        len_tot = df_mapped.count()
        len_50 = df_mapped.filter(df_mapped.val_after_mapping == 50).count()
        len_100 = df_mapped.filter(df_mapped.val_after_mapping == 100).count()
        len_400 = df_mapped.filter(df_mapped.val_after_mapping == 400).count()
        self.assertEqual(len_50, 1)
        self.assertEqual(len_100, 2)
        self.assertEqual(len_400, 1)
        self.assertEqual(len_50 + len_100 + len_400 + 1, len_tot)

    def test_calc_avg_basket(self):
        """Test assn_utils.calc_avg_basket()."""
        vals = [[1, 100, 2, 0.5], [2, 100, 0, 0.5], [3, 100, 2, 0.1]]
        df = sc.parallelize(vals).toDF(
            ["ID", "spend_col", "trips_col", "probability_making_a_trip"]
        )
        # scenario 1
        test_df_1 = calc_avg_basket(df, "spend_col", "trips_col", 0)
        test_df_1 = test_df_1.select("ID", "avg_basket")
        vals_1 = [[1, 50], [2, 0], [3, 50]]
        exp_df_1 = sc.parallelize(vals_1).toDF(["ID", "avg_basket"])
        join_df_1 = test_df_1.join(exp_df_1, ["ID", "avg_basket"], "inner")
        self.assertEqual(join_df_1.count(), 3)
        # scenario 2
        test_df_2 = calc_avg_basket(df, "spend_col", "trips_col", 0.2)
        vals_2 = [[1, 50], [2, 0], [3, 0]]
        exp_df_2 = sc.parallelize(vals_2).toDF(["ID", "avg_basket"])
        join_df_2 = test_df_2.join(exp_df_2, ["ID", "avg_basket"], "inner")
        self.assertEqual(join_df_2.count(), 3)

    def test_calc_overlapping_columns(self):
        df1 = spark.sparkContext.parallelize([[1, 2]]).toDF(["a", "b"])
        df2 = spark.sparkContext.parallelize([[2, 3]]).toDF(["c", "B"])
        df3 = spark.sparkContext.parallelize([[2, 3]]).toDF(["b", "c"])
        df4 = spark.sparkContext.parallelize([[2, 3]]).toDF(["B", "c"])

        self.assertEqual(calc_overlapping_cols(df1, df2), ["b"])
        self.assertEqual(calc_overlapping_cols(df1, df3), ["b"])
        self.assertEqual(calc_overlapping_cols(df2, df1), ["B"])
        self.assertEqual(calc_overlapping_cols(df2, df4), ["c", "B"])

    def test_fill_with_average(self):
        """Test assn_utils.fill_with_average()."""
        vals = [[1, 0], [2, None], [3, 2], [4, 4]]
        df = sc.parallelize(vals).toDF(["ID", "score"])
        # scenario 1
        test_df_1 = fill_with_average(df, "score", None)
        exp_vals_1 = [[1, 0], [2, 2], [3, 2], [4, 4]]
        exp_df_1 = sc.parallelize(exp_vals_1).toDF(["ID", "score"])
        exp_len_1 = test_df_1.join(exp_df_1, ["ID", "score"], "inner").count()
        self.assertEqual(exp_len_1, 4)
        # scenario 2
        test_df_2 = fill_with_average(df, "score", 0)
        exp_vals_2 = [[1, 3], [2, 3], [3, 2], [4, 4]]
        exp_df_2 = sc.parallelize(exp_vals_2).toDF(["ID", "score"])
        exp_len_2 = test_df_2.join(exp_df_2, ["ID", "score"], "inner").count()
        self.assertEqual(exp_len_2, 4)

    def test_flag_rows(self):
        """Test assn_utils.flag_rows()."""
        l = [
            ("Ankit", 0.3),
            ("Jalfaizy", None),
            ("saurabh", 0.5),
            ("Bala", 0.9),
            ("Greg", 0.7),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], score=x[1]))
        df = spark.createDataFrame(data)

        df = flag_rows(df, "filt_out", "score", ">=", 0.5)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 3)
        self.assertEqual(len(failed), 2)

        df = flag_rows(df, "filt_out", "score", "<", 0.5)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 1)
        self.assertEqual(len(failed), 4)

        df = flag_rows(df, "filt_out", "score", "=", None)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 1)
        self.assertEqual(len(failed), 4)

        df = flag_rows(df, "filt_out", "score", "!=", None)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 4)
        self.assertEqual(len(failed), 1)

    def test_filter_rows(self):
        """Test assn_utils.filter_rows()."""
        df = sc.parallelize(
            [
                ["A", 1.0, 1],
                ["D", 2.22, 2],
                ["F", 3.23, 3],
                ["B", 4.45, 3],
                ["B", 5.56, 7],
                ["B", 6.67, 9],
            ]
        ).toDF(("MBRSHP_SID", "ID", "AMT"))

        filter_cols = {"column": "AMT", "relation": "<=", "threshold": 7}
        print(filter_cols["relation"])
        print(filter_cols["threshold"])
        df_test = filter_rows(df, filter_cols)
        amt_max = df_test.agg({"AMT": "max"}).collect()[0][0]
        self.assertEqual(amt_max, 7)

    def test_check_cpn_nbr_or_version(self):
        """Test assn_utils.check_cpn_nbr_or_version()."""
        df = sc.parallelize([[1, "800000"], [2, "A"], [3, None]]).toDF(
            ["ID", "cpn_nbr"]
        )
        test_df = check_cpn_nbr_or_version(df, "cpn_nbr")
        test_df = test_df.dropna()
        exp_df = sc.parallelize([[1, "800000"], [2, None], [3, None]]).toDF(
            ["ID", "cpn_nbr"]
        )
        exp_df = exp_df.dropna()
        join_df = test_df.join(exp_df, ["ID", "cpn_nbr"], "inner")
        self.assertEqual(join_df.count(), 1)

    def test_cap_value(self):
        """Test assn_utils.cap_value()."""
        df = sc.parallelize(
            [
                ["A", 1.0, 1],
                ["D", 2.22, 2],
                ["F", 3.23, 3],
                ["B", 4.45, 3],
                ["B", 5.56, 7],
                ["B", 6.67, 9],
            ]
        ).toDF(("MBRSHP_SID", "ID", "AMT"))
        df = cap_value(df, "AMT", cap_lb=3, cap_ub=7)
        df_lb = df.filter(df.AMT == 3)
        df_ub = df.filter(df.AMT == 7)
        self.assertEqual(df_lb.count(), 4)
        self.assertEqual(df_ub.count(), 2)

    def test_create_equal_size_bucket(self):
        """Test assn_utils.reate_equal_size_bucket()."""
        df = sc.parallelize(
            [
                ["A", 1.0, 1],
                ["B", 2.22, 2],
                ["C", 3.23, 3],
                ["D", 4.45, 3],
                ["E", 5.56, 7],
                ["F", 6.67, 9],
            ]
        ).toDF(("MBRSHP_SID", "ID", "AMT"))
        df = create_equal_size_bucket(df, 2, "ID", "bucket")
        self.assertEqual(df.select("bucket").distinct().count(), 2)

    def test_calc_sampling_value(self):
        """Test assn_utils.calc_sampling_value()."""
        df = sc.parallelize(
            [
                ["A", 0, 0],
                ["B", 26, 3],
                ["C", 20, 7],
                ["D", 10, 99],
                ["E", 2626, 2],
                ["F", 2651, 100],
                ["G", 2652, 2],
                ["H", 2653, 100],
            ]
        ).toDF(
            (
                "MBRSHP_SID",
                "LAST_TWENTY-SIX_WEEK_SPEND",
                "DAYS_SINCE_LAST_TRIP",
            )
        )
        df = calc_sampling_value(df, "LAST_TWENTY-SIX_WEEK_SPEND")
        exp_df = sc.parallelize(
            [
                ["A", 0.0],
                ["B", 25.991780821917807],
                ["C", 19.980821917808218],
                ["D", 9.728767123287671],
                ["E", 2625.9945205479453],
                ["F", 2650.72602739726],
                ["G", 2651.9945205479453],
                ["H", 2652.72602739726],
            ]
        ).toDF(("MBRSHP_SID", "sampling_value"))
        overlap = df.join(exp_df, ["MBRSHP_SID", "sampling_value"], "inner")
        self.assertEqual(overlap.count(), 8)

    def test_calc_sampling_seg(self):
        """Test assn_utils.calc_sampling_seg()."""
        df = sc.parallelize(
            [
                ["A", 0, 0],
                ["B", 26, 1],
                ["C", 20, 1],
                ["D", 10, 2],
                ["E", 2626, 2],
                ["F", 2651, 3],
                ["G", 2652, 3],
                ["H", 2653, None],
            ]
        ).toDF(("MBRSHP_SID", "TENURE", "decile"))
        df = calc_sampling_seg(df)
        exp_df = sc.parallelize(
            [
                ["A", 0],
                ["B", 0],
                ["C", 0],
                ["D", 0],
                ["E", 2],
                ["F", 3],
                ["G", 3],
                ["H", 10],
            ]
        ).toDF(("MBRSHP_SID", "sampling_seg"))
        overlap = df.join(exp_df, ["MBRSHP_SID", "sampling_seg"], "inner")
        self.assertEqual(overlap.count(), 8)

    def test_rdd_rank_by_col(self):
        """Test assn_utils.rdd_rank_by_col()."""
        df = sc.parallelize(
            [
                ["A", 0, 0, 0, 0],
                ["B", 26, 3, 26, 3],
                ["C", 20, 7, 20, 7],
                ["D", 10, 99, 10, 99],
                ["E", 2626, 2, 2626, 2],
                ["F", 2651, 100, 2651, 100],
                ["G", 2652, 2, 2652, 2],
                ["H", 2653, 100, 2653, 100],
            ]
        ).toDF(
            (
                "MBRSHP_SID",
                "LAST_TWENTY-SIX_WEEK_SPEND",
                "DAYS_SINCE_LAST_TRIP",
                "TENURE",
                "decile",
            )
        )
        df_val = calc_sampling_value(df, "LAST_TWENTY-SIX_WEEK_SPEND")
        df_seg = calc_sampling_seg(df)
        df = df.join(df_val, "MBRSHP_SID", "inner")
        df = df.join(df_seg, "MBRSHP_SID", "inner")
        df = rdd_rank_by_col(
            df, "sampling_seg", "sampling_value", "sampling_rank"
        )
        exp_df = sc.parallelize(
            [
                ["B", 0],
                ["C", 1],
                ["D", 2],
                ["A", 3],
                ["G", 4],
                ["E", 5],
                ["H", 6],
                ["F", 7],
            ]
        ).toDF(("MBRSHP_SID", "sampling_rank"))
        overlap = df.join(exp_df, ["MBRSHP_SID", "sampling_rank"], "inner")
        self.assertEqual(overlap.count(), 8)

    def test_palindrome_rank_group(self):
        """Test assn_utils.palindrome_rank_group()."""
        df = sc.parallelize(
            [
                ["A", 0, 0, 0, 0],
                ["B", 26, 3, 26, 3],
                ["C", 20, 7, 20, 7],
                ["D", 10, 99, 10, 99],
                ["E", 2626, 2, 2626, 2],
                ["F", 2651, 100, 2651, 100],
                ["G", 2652, 2, 2652, 2],
                ["H", 2653, 100, 2653, 100],
            ]
        ).toDF(
            (
                "MBRSHP_SID",
                "LAST_TWENTY-SIX_WEEK_SPEND",
                "DAYS_SINCE_LAST_TRIP",
                "TENURE",
                "decile",
            )
        )
        df_val = calc_sampling_value(df, "LAST_TWENTY-SIX_WEEK_SPEND")
        df_seg = calc_sampling_seg(df)
        df = df.join(df_val, "MBRSHP_SID", "inner")
        df = df.join(df_seg, "MBRSHP_SID", "inner")
        df = rdd_rank_by_col(
            df, "sampling_seg", "sampling_value", "sampling_rank"
        )
        df = palindrome_rank_group(df, 3, "sampling_rank", "sampling_group")
        exp_df = sc.parallelize(
            [
                ["B", 1],
                ["C", 2],
                ["D", 3],
                ["A", 3],
                ["G", 2],
                ["E", 1],
                ["H", 1],
                ["F", 2],
            ]
        ).toDF(("MBRSHP_SID", "sampling_group"))
        overlap = df.join(exp_df, ["MBRSHP_SID", "sampling_group"], "inner")
        self.assertEqual(overlap.count(), 8)

    def test_palindrome_sample(self):
        """Test assn_utils.palindrome_sampling()."""
        df = sc.parallelize(
            [
                ["A", 0, 0, 0, 0],
                ["B", 26, 3, 26, 3],
                ["C", 20, 7, 20, 7],
                ["D", 10, 99, 10, 99],
                ["E", 2626, 2, 2626, 2],
                ["F", 2651, 100, 2651, 100],
                ["G", 2652, 2, 2652, 2],
                ["H", 2653, 100, 2653, 100],
                ["I", 2653, 100, 2653, 100],
                ["J", 2653, 100, 2653, 100],
            ]
        ).toDF(
            (
                "MBRSHP_SID",
                "LAST_TWENTY-SIX_WEEK_SPEND",
                "DAYS_SINCE_LAST_TRIP",
                "TENURE",
                "decile",
            )
        )
        df_val = calc_sampling_value(df, "LAST_TWENTY-SIX_WEEK_SPEND")
        df_seg = calc_sampling_seg(df)
        df = df.join(df_val, "MBRSHP_SID", "inner")
        df = df.join(df_seg, "MBRSHP_SID", "inner")
        df = palindrome_sample(df, 0.7, 10)
        self.assertEqual(df.count(), 7)

    def test_deterministic_sample(self):
        columns = ["column"]
        sample_size = 10
        data = map(lambda x: [x], range(0, 100))
        df = spark.sparkContext.parallelize(data).toDF(columns)
        try_1 = deterministic_sample(df, sample_size, ["column"]).take(
            sample_size
        )
        data = map(lambda x: [x], range(0, 100))
        df = spark.sparkContext.parallelize(data).toDF(columns)
        try_2 = deterministic_sample(df, sample_size).take(sample_size)
        data = map(lambda x: [x], range(0, 100))
        df = spark.sparkContext.parallelize(data).toDF(columns)
        try_3 = deterministic_sample(df, sample_size * 1.0 / df.count()).take(
            sample_size
        )

        self.assertEqual(try_1, try_2)
        self.assertEqual(try_1, try_3)

    def test_apply_offer_recency(self):
        """Test assn_utils.apply_offer_recencyg()."""
        assign = sc.parallelize(
            [
                [1, 1, 1],
                [1, 2, 2],
                [1, 3, 3],
                [1, 4, 1],
                [1, 5, 1],
                [2, 1, 1],
                [2, 2, 3],
                [2, 3, 2],
                [2, 4, 3],
                [2, 5, 1],
            ]
        ).toDF(("mbrshp_sid", "experiment_id", "cpn_nbr"))

        channel = sc.parallelize(
            [[1, "bbm"], [2, "email"], [3, "mmpc"], [4, "mmpc"], [5, "mmpc"]]
        ).toDF(("experiment_id", "channel"))

        coupon = sc.parallelize([[1, 1], [2, 2], [3, 1]]).toDF(
            ("cpn_nbr", "offer_id")
        )

        cells = sc.parallelize(
            [
                [1, "06/14/2018"],
                [2, "07/14/2018"],
                [3, "06/14/2018"],
                [4, "06/13/2018"],
                [5, "06/14/2019"],
            ]
        ).toDF(("experiment_id", "inhome_date"))

        inhome_date = "07/14/2018"

        df = apply_offer_recency(
            assign, channel, coupon, cells, inhome_date, "offer_id"
        )

        exp_df = sc.parallelize(
            [
                [1, 1, 30, 30],
                [2, 1, 30, 31],
                [2, 2, None, 30],
                [1, 2, 0, 0],
            ]
        ).toDF(("mbrshp_sid", "offer_id", "BBM", "MMPC"))

        df = df.fillna(0)
        exp_df = exp_df.fillna(0)

        df_match = df.join(exp_df, exp_df.columns, "inner")
        self.assertEqual(df_match.count(), df.count())
        self.assertEqual(df_match.count(), exp_df.count())

    @patch("memberdna.pipelines.assignment.lib.assn_utils.spark")
    def test_check_cast_type(self, spark_mock):
        spark_mock.read.csv.return_value = sc.parallelize(
            [
                (None, 0, 0.0, 0),
                (0, None, 0.0, 0),
                (0, 0, None, 0),
                (0, 0, 0.0, None),
            ]
        ).toDF(["offer_id", "cpn_nbr", "cpn_dollar_threshold", "cpn_class_id"])
        df = read_subset_and_cast("dummy_path", "csv")
        types = df.dtypes
        self.assertEqual(types[0][1], "int")
        self.assertEqual(types[2][1], "double")
        self.assertEqual(types[3][1], "int")

    @patch("memberdna.pipelines.assignment.lib.assn_utils.read_s3_to_local")
    def test_load_past_longitudinal_mbrs_with_no_past_cells(
        self, read_s3_to_local
    ):
        read_s3_to_local.return_value = pd.DataFrame(
            [["1", "1", ""], ["2", "2", "1"]],
            columns=[
                "cell_id",
                "experiment_id",
                "longitudinal_id",
            ],
        )

        mbrs = load_past_longitudinal_mbrs(
            longitudinal_id="2",
            experiment_id=2,
            assignment_path="dummy_path",
            cells_path="dummy_path",
        )

        self.assertEqual(mbrs.count(), 0)

    @patch(
        "memberdna.pipelines.assignment.lib.assn_utils.read_subset_and_cast"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_utils.read_s3_to_local")
    def test_load_past_longitudinal_mbrs_with_past_members(
        self, read_s3_to_local, read_subset_and_cast
    ):
        read_subset_and_cast.return_value = spark.createDataFrame(
            [
                ["001", 2, 1, "12345", 4],
                ["001", 2, 2, "12346", 4],
                ["002", 2, 1, "12345", 3],
                ["002", 2, 2, "12346", 3],
                ["003", 1, 1, "22345", 2],
                ["003", 1, 2, "22346", 2],
                ["001", 1, 1, "12345", 1],
                ["001", 1, 2, "12346", 1],
            ],
            ["mbrshp_sid", "experiment_id", "slot_nbr", "cpn_nbr", "cell_id"],
        )

        read_s3_to_local.return_value = pd.DataFrame(
            [["1", "1", "1"], ["2", "2", "1"]],
            columns=["experiment_id", "cell_id", "longitudinal_id"],
        )

        mbrs = load_past_longitudinal_mbrs(
            longitudinal_id="1",
            experiment_id=2,
            assignment_path="dummy_path",
            cells_path="dummy_path",
        )

        self.assertEqual(mbrs.count(), 1)
        self.assertEqual(mbrs.collect()[0], Row(mbrshp_sid="001"))

    @patch(
        "memberdna.pipelines.assignment.lib.assn_utils.checks."
        "check_loaded_long_cells"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_utils.log")
    @patch(
        "memberdna.pipelines.assignment.lib.assn_utils.read_subset_and_cast"
    )
    @patch("memberdna.pipelines.assignment.lib.assn_utils.read_s3_to_local")
    def test_load_past_longitudinal_mbrs_with_no_past_members(
        self,
        read_s3_to_local,
        read_subset_and_cast,
        log,
        check_loaded_long_cells,
    ):
        past_assignments = spark.createDataFrame(
            [
                ["003", 1, 1, "22345", 2],
                ["003", 1, 2, "22346", 2],
                ["001", 1, 1, "12345", 2],
                ["001", 1, 2, "12346", 2],
            ],
            ["mbrshp_sid", "experiment_id", "slot_nbr", "cpn_nbr", "cell_id"],
        )
        read_subset_and_cast.return_value = past_assignments

        cells = pd.DataFrame(
            [
                ["1", "1", "1"],
                ["1", "2", ""],
                ["2", "3", "1"],
                ["2", "4", ""],
            ],
            columns=["experiment_id", "cell_id", "longitudinal_id"],
        )
        read_s3_to_local.return_value = cells

        load_past_longitudinal_mbrs(
            longitudinal_id="1",
            experiment_id=2,
            assignment_path="dummy_path",
            cells_path="dummy_path",
        )

        log.warn.assert_called_with("longitudinal id 1 has no past members")

    def test_update_categories(self):
        """Test assn_utils.update_categories()."""
        transactions = sc.parallelize(
            [
                [10001, 1001, 101, "12/31/2020"],
                [10002, 1002, 102, "12/31/2020"],
                [10003, 1002, 102, "12/31/2020"],
                [10004, 1003, 103, "12/31/2020"],
                [10005, 1004, 104, "12/31/2020"],
            ]
        ).toDF(("ARTICLE_NBR", "AH4_CD", "AH5_CD", "EXP_DT"))
        item = sc.parallelize(
            [
                [10003, 1003, 103, "12/31/2020"],
                [10004, 1003, 103, "12/31/2020"],
                [10005, 1005, 105, "12/31/2020"],
            ]
        ).toDF(("ARTICLE_NBR", "AH4_CD", "AH5_CD", "EXP_DT"))

        transactions_updated = update_categories(transactions, item)
        transactions_expected = sc.parallelize(
            [
                [10001, 1001, 101, "12/31/2020"],
                [10002, 1002, 102, "12/31/2020"],
                [10003, 1003, 103, "12/31/2020"],
                [10004, 1003, 103, "12/31/2020"],
                [10005, 1005, 105, "12/31/2020"],
            ]
        ).toDF(("ARTICLE_NBR", "AH4_CD", "AH5_CD", "EXP_DT"))
        transactions_updated = transactions_updated.toPandas()
        transactions_updated.sort_values("ARTICLE_NBR", inplace=True)
        transactions_updated.reset_index(inplace=True, drop=True)
        transactions_expected = transactions_expected.toPandas()
        transactions_expected.reset_index(inplace=True, drop=True)
        pd.testing.assert_frame_equal(
            transactions_updated, transactions_expected
        )

        transactions_updated = update_categories(transactions, item, "AH4_CD")
        transactions_expected = sc.parallelize(
            [
                [10001, 1001, 101, "12/31/2020"],
                [10002, 1002, 102, "12/31/2020"],
                [10003, 1003, 102, "12/31/2020"],
                [10004, 1003, 103, "12/31/2020"],
                [10005, 1005, 104, "12/31/2020"],
            ]
        ).toDF(("ARTICLE_NBR", "AH4_CD", "AH5_CD", "EXP_DT"))
        transactions_updated = transactions_updated.toPandas()
        transactions_updated.sort_values("ARTICLE_NBR", inplace=True)
        transactions_updated.reset_index(inplace=True, drop=True)
        transactions_expected = transactions_expected.toPandas()
        transactions_expected.reset_index(inplace=True, drop=True)
        pd.testing.assert_frame_equal(
            transactions_updated, transactions_expected
        )

        transactions_updated = update_categories(transactions, item, "AH5_CD")
        transactions_expected = sc.parallelize(
            [
                [10001, 1001, 101, "12/31/2020"],
                [10002, 1002, 102, "12/31/2020"],
                [10003, 1002, 103, "12/31/2020"],
                [10004, 1003, 103, "12/31/2020"],
                [10005, 1004, 105, "12/31/2020"],
            ]
        ).toDF(("ARTICLE_NBR", "AH4_CD", "AH5_CD", "EXP_DT"))
        transactions_updated = transactions_updated.toPandas()
        transactions_updated.sort_values("ARTICLE_NBR", inplace=True)
        transactions_updated.reset_index(inplace=True, drop=True)
        transactions_expected = transactions_expected.toPandas()
        transactions_expected.reset_index(inplace=True, drop=True)
        pd.testing.assert_frame_equal(
            transactions_updated, transactions_expected
        )


class TestFilters2(unittest.TestCase):
    """Unit tests for filter functions."""

    def test_special_club(self):
        """Test filters.special_club()."""
        l = [
            ("Ankit", "3780001"),
            ("Jalfaizy", "3780002"),
            ("saurabh", "003780001"),
            ("Bala", "21"),
            ("Greg", "1"),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], MBRSHP_NBR=x[1]))
        df = spark.createDataFrame(data)
        df = special_club(df, "filt_out", ["378", "003"])
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 3)
        self.assertEqual(len(failed), 2)

    def test_special_zipcode(self):
        """Test filters.special_zipcode()."""
        l = [
            ("Ankit", "02142"),
            ("Jalfaizy", "02139"),
            ("saurabh", "02215"),
            ("Bala", "02216"),
            ("Greg", "02210"),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], LATEST_HOME_ZIP_CD=x[1]))
        df = spark.createDataFrame(data)
        df = special_zipcode(df, "filt_out", ["02142", "02139"])
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 2)
        self.assertEqual(len(failed), 3)

    def test_expired(self):
        """Test filters.expired()."""
        l = [
            ("Ankit", "2016-01-01"),
            ("Jalfaizy", "2016-01-01"),
            ("saurabh", "2017-01-01"),
            ("Bala", "2018-01-01"),
            ("Greg", "2019-01-01"),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], LATEST_MBRSHP_EXP_DT=x[1]))
        df = spark.createDataFrame(data)
        df = expired(df, "filt_out", "2017-01-02")
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 3)
        self.assertEqual(len(failed), 2)

    def test_tenure(self):
        """Test filters.tenure()."""
        l = [
            ("Ankit", 100),
            ("Jalfaizy", 22),
            ("saurabh", 50),
            ("Bala", -26),
            ("Greg", None),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], TENURE=x[1]))
        df = spark.createDataFrame(data)
        df = tenure(df, "filt_out", 35)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 2)
        self.assertEqual(len(failed), 3)

    def test_new(self):
        """Test filters.new()."""
        l = [
            ("Ankit", 100),
            ("Jalfaizy", 22),
            ("saurabh", 50),
            ("Bala", -26),
            ("Greg", None),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], TENURE=x[1]))
        df = spark.createDataFrame(data)
        df = new(df, "filt_out", 35)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 2)
        self.assertEqual(len(failed), 3)

    def test_trial(self):
        """Test filters.trial()."""
        l = [
            ("Ankit", "i am trial"),
            ("Jalfaizy", "i am trial too"),
            ("saurabh", "i am not trial"),
            ("Bala", "i am not trial"),
            ("Greg", "i am not trial"),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], Trial=x[1]))
        df = spark.createDataFrame(data)
        df = trial(df, "filt_out", "Trial", ["i am trial", "i am trial too"])
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 2)
        self.assertEqual(len(failed), 3)

    def test_gas(self):
        """Test filters.gas()."""
        l = [
            ("Ankit", 1, 0),
            ("Jalfaizy", 1, 1),
            ("saurabh", 1, 1),
            ("Bala", 0, 0),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(
            lambda x: Row(name=x[0], gas_club=x[1], gas_purchase=x[2])
        )
        df = spark.createDataFrame(data)
        df_1 = gas(df, "filt_out", ["gas_club"], 1)
        collected = df_1.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 3)
        self.assertEqual(len(failed), 1)
        df_2 = gas(df, "filt_out", ["gas_purchase"], 1)
        collected = df_2.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 2)
        self.assertEqual(len(failed), 2)
        df_3 = gas(df, "filt_out", ["gas_purchase", "gas_club"], 1)
        collected = df_3.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 3)
        self.assertEqual(len(failed), 1)

    def test_avg_basket(self):
        """Test filters.avg_basket()."""
        vals = [
            ["A", 0, 0, 0.02],
            ["B", 1000, 4, 0.25],
            ["C", 300, 1, 0.9],
            ["D", 2000, 10, 0.9],
            ["E", 150, 1, 0.2],
            ["F", 1000, 40, 0.5],
        ]
        df = sc.parallelize(vals).toDF(
            (
                "ID",
                "FIFTY-TWOW_SPEND_IN_STORE",
                "FIFTY-TWO_WEEK_TRIPS",
                "probability_making_a_trip",
            )
        )
        test_df = avg_basket(
            df,
            "is_basket",
            spend_col="FIFTY-TWOW_SPEND_IN_STORE",
            trips_col="FIFTY-TWO_WEEK_TRIPS",
            limit=400,
            propensity_threshold=0.3,
        )
        test_df = test_df.filter(test_df.is_basket == 1)
        len_tot = test_df.count()
        len_match = test_df.filter(
            test_df.ID.isin(["A", "B", "C", "E"])
        ).count()
        self.assertEqual(len_tot, 6)
        self.assertEqual(len_match, 4)

    def test_ROI_positive(self):
        """Test filters.ROI_positive()."""
        vals = [
            ["A", 0, 0, 0.02],
            ["B", 1000, 4, 0.25],
            ["C", 300, 1, 0.9],
            ["D", 2000, 10, 0.9],
            ["E", 150, 1, 0.2],
            ["F", 1000, 40, 0.5],
        ]
        df = sc.parallelize(vals).toDF(
            (
                "ID",
                "FIFTY-TWOW_SPEND_IN_STORE",
                "FIFTY-TWO_WEEK_TRIPS",
                "probability_making_a_trip",
            )
        )
        basket_offer_discount = "{50: 5, 100: 10, 400: 10}"
        basket_offer_threshold = "{50: 50, 100: 100, 400: 200}"
        test_df = ROI_positive(
            df,
            "is_basket",
            basket_offer_discount,
            basket_offer_threshold,
            spend_col="FIFTY-TWOW_SPEND_IN_STORE",
            trips_col="FIFTY-TWO_WEEK_TRIPS",
            trips_weeks=52,
            incr_weeks=3,
            margin_rate=0.175,
            cannibalization_rate=0.6,
            incr_margin_lb=-1,
            incr_sales_lb=0.01,
        )
        test_df = test_df.filter(test_df.is_basket == 1)
        len_tot = test_df.count()
        len_match = test_df.filter(
            test_df.ID.isin(["A", "B", "C", "E"])
        ).count()
        self.assertEqual(len_tot, 4)
        self.assertEqual(len_match, 4)

        test_df_2 = ROI_positive(
            df,
            "is_basket",
            basket_offer_discount,
            basket_offer_threshold,
            spend_col="FIFTY-TWOW_SPEND_IN_STORE",
            trips_col="FIFTY-TWO_WEEK_TRIPS",
            trips_weeks=52,
            incr_weeks=3,
            margin_rate=0.175,
            cannibalization_rate=0.6,
            incr_margin_lb=-1,
            incr_sales_lb=0.01,
            propensity_threshold=0.33,
        )
        test_df_2 = test_df_2.filter(test_df_2.is_basket == 1)
        len_tot_2 = test_df_2.count()
        len_match_2 = test_df_2.filter(
            test_df_2.ID.isin(["A", "B", "C", "E"])
        ).count()
        self.assertEqual(len_tot_2, 4)
        self.assertEqual(len_match_2, 4)

    def test_constructsatisfied(self):
        """Test filters.construct_satisfied()."""
        l = [
            ("Ankit", 1, 2, 3, 4, None),
            ("Jalfaizy", 1, 2, 2, 3, 5),
            ("saurabh", 2, 4, 3, 7, 1),
            ("Bala", None, 1, 3, 4, 6),
            ("Greg", 0, 0, 0, 0, 0),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(
            lambda x: Row(
                name=x[0],
                c3__3s0g0p3=x[1],
                c3__3s1g0p3=x[2],
                c3__3s2g0p3=x[3],
                c3__3s3g0p3=x[4],
                c3__3s4g0p3=x[5],
            )
        )
        df = spark.createDataFrame(data)
        df = construct_satisfied(df, "filt_out", 3)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 3)
        self.assertEqual(len(failed), 2)

        df = spark.createDataFrame(data)
        df = construct_satisfied(df, "filt_out", 3, [0, 1, 2, 3])
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 4)
        self.assertEqual(len(failed), 1)

    def test_general_filter(self):
        """Test filters.general_filter()."""
        l = [
            ("Ankit", 0.3),
            ("Jalfaizy", None),
            ("saurabh", 0.5),
            ("Bala", 0.9),
            ("Greg", 0.7),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], score=x[1]))
        df = spark.createDataFrame(data)
        df = general_filter(df, "filt_out", "score", ">=", 0.5)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 3)
        self.assertEqual(len(failed), 2)

        df = general_filter(df, "filt_out", "score", "<", 0.5)
        collected = df.toPandas()
        passed = collected[collected.filt_out == 1]
        failed = collected[collected.filt_out == 0]
        self.assertEqual((len(passed) + len(failed)), len(collected))
        self.assertEqual(len(passed), 1)
        self.assertEqual(len(failed), 4)

    def test_run_sub_filters(self):
        """Test filters.run_sub_filters()."""
        l = [("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5), ("F", 6)]
        rdd = sc.parallelize(l)
        data = rdd.map(lambda x: Row(name=x[0], score=x[1]))
        df = spark.createDataFrame(data)
        return_df = run_sub_filters(
            df,
            "pass",
            [
                {
                    "filter_num": 0,
                    "filter_type": "general_filter",
                    "filter_parameters": {
                        "column": "score",
                        "relation": ">",
                        "threshold": "2",
                    },
                },
                {
                    "filter_num": 1,
                    "filter_type": "general_filter",
                    "filter_parameters": {
                        "column": "score",
                        "relation": "<",
                        "threshold": "5",
                    },
                },
            ],
        )
        self.assertEqual(
            return_df.where(F.col("name") == "A")
            .select("pass")
            .collect()[0][0],
            0,
        )
        self.assertEqual(
            return_df.where(F.col("name") == "B")
            .select("pass")
            .collect()[0][0],
            0,
        )
        self.assertEqual(
            return_df.where(F.col("name") == "C")
            .select("pass")
            .collect()[0][0],
            1,
        )
        self.assertEqual(
            return_df.where(F.col("name") == "D")
            .select("pass")
            .collect()[0][0],
            1,
        )
        self.assertEqual(
            return_df.where(F.col("name") == "E")
            .select("pass")
            .collect()[0][0],
            0,
        )
        self.assertEqual(
            return_df.where(F.col("name") == "F")
            .select("pass")
            .collect()[0][0],
            0,
        )

    def test_longitudinal_new_members_only(self):
        memberdata = spark.createDataFrame(
            [
                ["001", 1, 0, 0],
                ["006", 1, 0, 0],
                ["003", 1, 1, 0],
                ["005", 1, 1, 0],
                ["008", 0, 0, 0],
                ["002", 1, 0, 0],
                ["004", 1, 1, 0],
                ["009", 0, 0, 0],
                ["007", 0, 1, 0],
            ],
            [
                "mbrshp_sid",
                "l1_past_mbr",
                "l2_past_mbr",
                "l3_past_mbr",
            ],
        )

        df = longitudinal(
            memberdata,
            "s1f1",
            [3],
            "new",
            [
                {
                    "filter_num": 1,
                    "filter_type": "sql",
                    "filter_parameters": {
                        "sql_string": "select *, if(mbrshp_sid in"
                        " ('001', '006'), 1, 0) as {} from df"
                    },
                }
            ],
        )

        actual = df.filter(df["s1f1"] == 1).toPandas().mbrshp_sid.tolist()
        self.assertEqual(sorted(actual), ["001", "006"])

    def test_longitudinal_old_members_only(self):
        memberdata = spark.createDataFrame(
            [
                ["001", 1, 0],
                ["006", 1, 0],
                ["003", 1, 1],
                ["005", 1, 1],
                ["008", 0, 0],
                ["002", 1, 0],
                ["004", 1, 1],
                ["009", 0, 0],
                ["007", 0, 1],
            ],
            ["mbrshp_sid", "l1_past_mbr", "l2_past_mbr"],
        )

        df = longitudinal(
            memberdata,
            "s1f1",
            [1],
            "past",
            [
                {
                    "filter_num": 1,
                    "filter_type": "sql",
                    "filter_parameters": {
                        "sql_string": "select *, if(mbrshp_sid in"
                        " ('001', '006', '003', '005'), 1, 0) as"
                        " {} from df"
                    },
                }
            ],
            filter_past_mbrs=True,
        )

        actual = df.filter(df["s1f1"] == 1).toPandas().mbrshp_sid.tolist()
        self.assertEqual(sorted(actual), ["001", "003", "005", "006"])

    def test_longitudinal_both_members(self):
        memberdata = spark.createDataFrame(
            [
                ["001", 1, 0],
                ["006", 1, 0],
                ["003", 1, 1],
                ["005", 1, 1],
                ["008", 0, 0],
                ["002", 1, 0],
                ["004", 1, 1],
                ["009", 0, 0],
                ["007", 0, 1],
            ],
            ["mbrshp_sid", "l1_past_mbr", "l2_past_mbr"],
        )

        df = longitudinal(
            memberdata,
            "s1f1",
            [1, 2],
            "both",
            [
                {
                    "filter_num": 1,
                    "filter_type": "sql",
                    "filter_parameters": {
                        "sql_string": "select *, if(mbrshp_sid in"
                        " ('001', '006', '009', '008'), 1, 0)"
                        " as {} from df"
                    },
                }
            ],
            filter_past_mbrs=True,
        )

        actual = df.filter(df["s1f1"] == 1).toPandas().mbrshp_sid.tolist()
        self.assertEqual(sorted(actual), ["001", "006", "008", "009"])


class TestSlots(unittest.TestCase):
    """Unit tests for slot functions."""

    def test_sql(self):
        data = [("hat", 1), ("cat", 2)]
        df = (
            spark.sparkContext.parallelize(data)
            .toDF(["MBRSHP_SID", "cpn_nbr"])
            .cache()
        )
        output = sql(
            None, df, None, sql_string="select * , 'pi' as constant from df"
        )
        self.assertEqual(output.count(), 2)
        self.assertEqual(output.columns[2], "constant")
        self.assertEqual(output.first()["constant"], "pi")
        df.unpersist()

    def test_compose_slots(self):
        l = [
            ("Ankit", None, 2, 1),
            ("Ankit", None, 2, 2),
            ("Ankit", 5, 2, 3),
            ("Ankit", 5, 1, 4),
            ("Bala", 0, 2, 1),
            ("Bala", 9, 0, 2),
            ("Bala", 6, 7, 3),
            ("Bala", 6, 12, 4),
        ]
        rdd = sc.parallelize(l)
        data = rdd.toDF(
            ["MBRSHP_SID", "prediction", "TRIPS", "cpn_nbr"]
        ).cache()
        slot_0 = {
            "slot_type": "sql",
            "slot_size": "1",
            "slot_parameters": {
                "sql_string": "select * from df where TRIPS > 2"
            },
        }

        slot_1 = {
            "slot_type": "rank_by_col",
            "slot_size": "1",
            "slot_parameters": {
                "rank_col": "TRIPS",
                "is_ascending": True,
                "secondary_sort": "prediction",
            },
        }
        slots = [slot_0, slot_1]
        output = compose_slots(
            None, data, None, 1, slots, is_backfill=False, seed=1
        ).cache()
        self.assertEqual(output.count(), 1)
        self.assertEqual(output.first()["MBRSHP_SID"], "Bala")
        self.assertEqual(output.first()["TRIPS"], 7)
        output.unpersist()

    def test_waterfall_slots(self):
        l = [
            ("Ankit", "dummy", 2, "top_pick"),
            ("Ankit", "dummy", 2, ""),
            ("Ankit", "dummy", 2, ""),
            ("Ankit", "dummy", 1, "second_pick"),
            ("Bala", "dummy", 2, ""),
            ("Bala", "dummy", 0, "second_pick"),
            ("Bala", "dummy", 7, "top_pick"),
            ("Bala", "dummy", 12, ""),
        ]
        rdd = sc.parallelize(l)
        data = rdd.toDF(["MBRSHP_SID", "cpn_type", "TRIPS", "cpn_nbr"]).cache()
        slot_0 = {
            "slot_type": "sql",
            "slot_size": "1",
            "offer_data": "dummy",
            "slot_parameters": {
                "sql_string": "select * , 1 as rank from df where cpn_nbr == 'top_pick'"
            },
        }

        slot_1 = {
            "slot_type": "rank_by_col",
            "slot_size": "1",
            "offer_data": "dummy",
            "slot_parameters": {"rank_col": "TRIPS", "is_ascending": True},
        }
        slots = [slot_0, slot_1]
        output = waterfall_slots(None, data, None, 2, slots, seed=1).cache()
        self.assertEqual(
            output.filter("mbrshp_sid == 'Bala' AND rank == 1").first()[
                "cpn_nbr"
            ],
            "top_pick",
        )
        self.assertEqual(
            output.filter("mbrshp_sid == 'Bala' AND rank == 2").first()[
                "cpn_nbr"
            ],
            "second_pick",
        )
        self.assertEqual(
            output.filter("mbrshp_sid == 'Ankit' AND rank == 1").first()[
                "cpn_nbr"
            ],
            "top_pick",
        )
        self.assertEqual(
            output.filter("mbrshp_sid == 'Ankit' AND rank == 2").first()[
                "cpn_nbr"
            ],
            "second_pick",
        )
        output.unpersist()

    def test_rankbycol(self):
        """Test slots.rank_by_col()."""
        l = [
            ("Ankit", 1, 2, 1),
            ("Ankit", 7, 2, 2),
            ("Ankit", 5, 2, 3),
            ("Ankit", 5, 1, 4),
            ("Bala", 0, 2, 1),
            ("Bala", 9, 0, 2),
            ("Bala", 6, 7, 3),
            ("Bala", 6, 12, 4),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(
            lambda x: Row(
                MBRSHP_SID=x[0], prediction=x[1], TRIPS=x[2], cpn_nbr=x[3]
            )
        )
        df = spark.createDataFrame(data)
        df = rank_by_col(
            None,
            df,
            None,
            total_coupons=1,
            rank_col="prediction",
            is_ascending=False,
            secondary_sort="TRIPS",
            seed=1,
        )
        collected = df.toPandas()
        self.assertEqual(len(collected), 2)
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Ankit"].cpn_nbr.tolist(), [2]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Bala"].cpn_nbr.tolist(), [2]
        )
        df = spark.createDataFrame(data)
        df = rank_by_col(
            None,
            df,
            None,
            total_coupons=1,
            rank_col="prediction",
            is_ascending=True,
            seed=1,
        )
        collected = df.toPandas()
        self.assertEqual(len(collected), 2)
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Ankit"].cpn_nbr.tolist(), [1]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Bala"].cpn_nbr.tolist(), [1]
        )
        df = spark.createDataFrame(data)
        df = rank_by_col(
            None,
            df,
            None,
            total_coupons=1,
            rank_col="prediction",
            is_ascending=False,
            secondary_sort="TRIPS",
            filter={"column": "TRIPS", "relation": ">", "threshold": 0},
            seed=1,
        )
        collected = df.toPandas()
        self.assertEqual(len(collected), 2)
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Ankit"].cpn_nbr.tolist(), [2]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Bala"].cpn_nbr.tolist(), [4]
        )

    def test_limit_to_cf_match(self):
        """Test slots.limit_to_cf_match()."""
        l = [
            ("Ankit", 1, "stretch", 1, 1, 1),
            ("Ankit", 7, "hook", 2, 2, 0),
            ("Ankit", 5, "hook", 3, 3, 0),
            ("Ankit", 5, "hook", 4, 4, 0),
            ("Carlos", 5, "hook", 1, 1, 0),
            ("Carlos", 4, "stretch", 1, 2, 0),
            ("Carlos", 10, "stretch", 1, 3, 1),
            ("Carlos", 6, "stretch", 3, 4, 2),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(
            lambda x: Row(
                MBRSHP_SID=x[0],
                prediction=x[1],
                hs_ind_lambda0=x[2],
                CATEGORY_ID=x[3],
                cpn_nbr=x[4],
                rank=x[5],
            )
        )
        df = spark.createDataFrame(data)
        df_mbr = df.select(
            "MBRSHP_SID",
            "prediction",
            "hs_ind_lambda0",
            "CATEGORY_ID",
            "cpn_nbr",
        )
        df_full = df.filter(df.hs_ind_lambda0 == "stretch").select(
            "MBRSHP_SID", "cpn_nbr", "rank"
        )

        df1 = limit_to_cf_match(None, df_mbr, None, df_full, 4, seed=1)
        collected = df1.toPandas()
        self.assertEqual(collected.MBRSHP_SID.tolist(), ["Carlos", "Carlos"])
        self.assertEqual(collected.cpn_nbr.tolist(), [3, 4])
        self.assertEqual(collected["rank"].tolist(), [1, 2])

    def test_cf(self):
        """Test slots.cf()."""
        l = [
            ("Ankit", 1, "stretch", 1, 1),
            ("Ankit", 7, "hook", 2, 2),
            ("Ankit", 5, "hook", 3, 3),
            ("Ankit", 5, "hook", 4, 4),
            ("Bala", 0, "stretch", 1, 1),
            ("Bala", 9, "stretch", 2, 2),
            ("Bala", 6, "stretch", 3, 3),
            ("Bala", 6, "stretch", 4, 4),
            ("Charlie", 7, "hook", 1, 1),
            ("Charlie", 9, "hook", 2, 2),
            ("Charlie", 6, "stretch", 3, 3),
            ("Charlie", 6, "stretch", 4, 4),
            ("Brad", 7, "hook", 1, 1),
            ("Brad", 9, "hook", 1, 2),
            ("Brad", 6.5, "stretch", 1, 3),
            ("Brad", 6, "stretch", 3, 4),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(
            lambda x: Row(
                MBRSHP_SID=x[0],
                prediction=x[1],
                hs_ind_lambda0=x[2],
                CATEGORY_ID=x[3],
                cpn_nbr=x[4],
            )
        )
        df = spark.createDataFrame(data)
        df1 = cf(
            None,
            df,
            None,
            total_coupons=1,
            hs_ind_lambda=0,
            hs_ind="stretch",
            offer_band=None,
            rank_limit=None,
            score_limit=8,
            filter=None,
            seed=1,
        )
        collected = df1.toPandas()
        self.assertEqual(len(collected), 1)
        self.assertEqual(collected.MBRSHP_SID.tolist(), ["Bala"])
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Bala"].CATEGORY_ID.tolist(), [2]
        )

        filter_col = {"column": "prediction", "relation": ">=", "threshold": 9}
        df2 = cf(
            None,
            df,
            None,
            total_coupons=1,
            hs_ind_lambda=0,
            hs_ind=None,
            offer_band=None,
            rank_limit=None,
            score_limit=None,
            filter=filter_col,
            seed=1,
        )
        collected = df2.toPandas()
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Charlie"].prediction.tolist(),
            [9],
        )

        df3 = cf(
            None,
            df,
            None,
            total_coupons=1,
            hs_ind_lambda=0,
            hs_ind=None,
            offer_band=6,
            rank_limit=None,
            score_limit=None,
            filter=None,
            seed=1,
        )
        self.assertEqual(df3.count(), 4)

        df4 = cf(
            None,
            df,
            None,
            total_coupons=1,
            hs_ind_lambda=0,
            hs_ind=None,
            offer_band=1,
            rank_limit=None,
            score_limit=None,
            filter=filter_col,
            seed=1,
        )
        collected = df4.toPandas()
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Charlie"].prediction.tolist(),
            [9],
        )

        df5 = cf(
            None,
            df,
            None,
            total_coupons=3,
            hs_ind_lambda=0,
            hs_ind=None,
            offer_band=None,
            rank_limit=None,
            score_limit=None,
            filter=None,
            seed=1,
        )
        collected = df5.toPandas()
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Brad"].cpn_nbr.tolist(),
            [2, 1, 4],
        )

        df6 = cf(
            None,
            df,
            None,
            total_coupons=4,
            hs_ind_lambda=0,
            hs_ind=None,
            offer_band=None,
            rank_limit=None,
            score_limit=None,
            filter=None,
            cf_thres=0.1,
            seed=1,
        )
        collected = df6.toPandas()
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Brad"].prediction.tolist(), [9]
        )

        df7 = cf(
            None,
            df,
            None,
            total_coupons=1,
            hs_ind_lambda=0,
            hs_ind="stretch",
            offer_band=None,
            rank_limit=None,
            score_limit=None,
            filter=None,
            cf_thres=0.1,
            limit_match=True,
            seed=1,
        )
        collected = df7.toPandas()
        self.assertEqual(collected.MBRSHP_SID.tolist(), ["Bala"])
        self.assertEqual(collected.cpn_nbr.tolist(), [2])
        self.assertEqual(collected["rank"].tolist(), [1])

    def test_cf_combined(self):
        """Test slots.cf_combined()."""
        l = [
            ("Ankit", 1, "stretch", 1, 1, 100),
            ("Ankit", 7, "hook", 2, 2, 100),
            ("Ankit", 5, "hook", 3, 3, 100),
            ("Ankit", 5, "hook", 4, 4, 100),
            ("Bala", 0, "stretch", 1, 1, 100),
            ("Bala", 9, "stretch", 2, 2, 100),
            ("Bala", 6, "stretch", 3, 3, 100),
            ("Bala", 6, "stretch", 4, 4, 100),
            ("Charlie", 7, "hook", 1, 1, 200),
            ("Charlie", 9, "hook", 2, 2, 200),
            ("Charlie", 6, "stretch", 3, 3, 200),
            ("Charlie", 6, "stretch", 4, 4, 200),
            ("Brad", 7, "hook", 1, 1, 300),
            ("Brad", 9, "hook", 1, 2, 300),
            ("Brad", 6.5, "stretch", 1, 3, 300),
            ("Brad", 6, "stretch", 3, 4, 300),
        ]
        rdd = sc.parallelize(l)
        data = rdd.map(
            lambda x: Row(
                MBRSHP_SID=x[0],
                prediction=x[1],
                hs_ind_lambda0=x[2],
                CATEGORY_ID=x[3],
                cpn_nbr=x[4],
                TENURE=x[5],
            )
        )
        df = spark.createDataFrame(data)
        df1 = cf_combined(
            None,
            df,
            None,
            total_coupons=1,
            tenure=150,
            hs_ind_lambda=0,
            hs_ind="stretch",
            offer_band=None,
            rank_limit=None,
            score_limit=None,
            filter=None,
            seed=1,
        )
        collected = df1.toPandas()
        self.assertEqual(len(collected), 4)
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Ankit"].CATEGORY_ID.tolist(),
            [2],
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Bala"].CATEGORY_ID.tolist(), [2]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "Brad"].CATEGORY_ID.tolist(), [3]
        )

        df2 = cf_combined(
            None,
            df,
            None,
            total_coupons=1,
            tenure=50,
            hs_ind_lambda=0,
            hs_ind="stretch",
            offer_band=None,
            rank_limit=None,
            score_limit=None,
            filter=None,
            cf_thres=0.1,
            limit_match=True,
            seed=1,
        )
        collected = df2.toPandas()
        self.assertEqual(collected.MBRSHP_SID.tolist(), ["Bala"])
        self.assertEqual(collected.cpn_nbr.tolist(), [2])
        self.assertEqual(collected["rank"].tolist(), [1])

    def test_basket(self):
        """Test slots.basket()."""
        basket_offer_threshold = "{50: 50, 100: 100, 400: 200}"
        vals = [
            ["A", 0, 0, 1, 50, 0.4],
            ["A", 0, 0, 2, 100, 0.4],
            ["A", 0, 0, 3, 200, 0.4],
            ["B", 1000, 4, 1, 50, 0.5],
            ["B", 1000, 4, 2, 100, 0.5],
            ["B", 1000, 4, 3, 200, 0.5],
            ["C", 4000, 10, 1, 50, 0.8],
            ["C", 4000, 10, 2, 100, 0.8],
            ["C", 4000, 10, 3, 200, 0.8],
            ["D", 1000, 4, 1, 50, 0.25],
            ["D", 1000, 4, 2, 100, 0.25],
            ["D", 1000, 4, 3, 200, 0.25],
        ]
        df = sc.parallelize(vals).toDF(
            (
                "MBRSHP_SID",
                "LFIFTY-TWOW_SPEND_IN_STORE",
                "LAST_FIFTY-TWO_WEEK_TRIPS",
                "cpn_nbr",
                "cpn_dollar_threshold",
                "probability_making_a_trip",
            )
        )
        df1 = basket(
            None,
            df,
            None,
            1,
            "LFIFTY-TWOW_SPEND_IN_STORE",
            "LAST_FIFTY-TWO_WEEK_TRIPS",
            basket_offer_threshold,
            seed=1,
        )
        collected = df1.toPandas()
        self.assertEqual(len(collected), 3)
        self.assertEqual(set(collected.MBRSHP_SID.tolist()), {"A", "B", "D"})
        self.assertEqual(
            collected[collected.MBRSHP_SID == "A"].cpn_nbr.tolist(), [1]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "B"].cpn_nbr.tolist(), [3]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "D"].cpn_nbr.tolist(), [3]
        )

        df2 = basket(
            None,
            df,
            None,
            1,
            "LFIFTY-TWOW_SPEND_IN_STORE",
            "LAST_FIFTY-TWO_WEEK_TRIPS",
            basket_offer_threshold,
            propensity_threshold=0.33,
            seed=1,
        )
        collected = df2.toPandas()
        self.assertEqual(len(collected), 3)
        self.assertEqual(set(collected.MBRSHP_SID.tolist()), {"A", "B", "D"})
        self.assertEqual(
            collected[collected.MBRSHP_SID == "A"].cpn_nbr.tolist(), [1]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "B"].cpn_nbr.tolist(), [3]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "D"].cpn_nbr.tolist(), [1]
        )

    def test_static(self):
        """Test slots.static()."""
        vals = [
            ["A", 1],
            ["A", 2],
            ["A", 3],
            ["B", 1],
            ["B", 2],
            ["B", 3],
            ["C", 1],
            ["C", 2],
            ["C", 3],
        ]
        df = sc.parallelize(vals).toDF(("MBRSHP_SID", "cpn_nbr"))

        df1 = static(None, df, None, 1, seed=1)
        collected = df1.toPandas()
        self.assertEqual(len(collected), 3)

    def test_random(self):
        """Test slots.random()."""
        vals = [
            ["A", 1],
            ["A", 2],
            ["A", 3],
            ["B", 1],
            ["B", 2],
            ["B", 3],
            ["C", 1],
            ["C", 2],
            ["C", 3],
        ]
        df = sc.parallelize(vals).toDF(("MBRSHP_SID", "cpn_nbr"))

        df1 = random(None, df, None, is_backfill=False, seed=1)
        collected = df1.toPandas()
        self.assertEqual(len(collected), 3)

    def test_stretch_spend(self):
        """Test slots.stretch_spend()."""
        vals = [
            ["A", 10, 1, 1, 5],
            ["A", 10, 1, 2, 15],
            ["A", 10, 1, 3, 25],
            ["B", None, None, 1, 5],
            ["B", None, None, 2, 15],
            ["B", None, None, 3, 25],
            ["C", 20, 1, 1, 5],
            ["C", 15, 2, 1, 5],
            ["C", 20, 1, 2, 20],
            ["C", 15, 2, 2, 20],
            ["C", 20, 1, 3, 25],
            ["C", 15, 2, 3, 25],
            ["D", 28, 1, 1, 5],
            ["D", None, None, 1, 5],
            ["D", 28, 1, 2, 15],
            ["D", None, None, 2, 15],
            ["D", 28, 1, 3, 25],
            ["D", None, None, 3, 25],
        ]
        df = sc.parallelize(vals).toDF(
            (
                "MBRSHP_SID",
                "SPEND",
                "PURCH_HDR_ID",
                "cpn_nbr",
                "cpn_dollar_threshold",
            )
        )
        df = stretch_spend(
            None, df, None, total_coupons=1, cpn_list=["1", "2", "3"], seed=1
        )
        len_tot = df.count()
        collected = df.orderBy("MBRSHP_SID").toPandas()
        self.assertEqual(len(collected), 4)
        self.assertEqual(collected.MBRSHP_SID.tolist(), ["A", "B", "C", "D"])
        self.assertEqual(
            collected[collected.MBRSHP_SID == "A"].cpn_nbr.tolist(), [2]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "B"].cpn_nbr.tolist(), [1]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "C"].cpn_nbr.tolist(), [2]
        )
        self.assertEqual(
            collected[collected.MBRSHP_SID == "D"].cpn_nbr.tolist(), [3]
        )


class CalculateFilepathsTestCase(unittest.TestCase):
    """Test assn_io.calculate_filepaths()."""

    def setUp(self):
        cnf, cfg_path = load_config()
        self.params = cnf["shared"]
        self.paths = cnf["paths"]
        self.optional_mmpc_paths = {
            "INPUT_ASSIGNMENTS": "s3://memberanalytics-data-out-prod/ASSIGNMENTS/cdsa/assn_output/test_campaign_path/assignments",
            "INPUT_CONSTRUCTS": "s3://memberanalytics-data-out-prod/ASSIGNMENTS/cdsa/assn_output/test_campaign_path/assignments_allconstructs",
            "MAIL_POPULATION_ASSIGNMENT": "s3://memberanalytics-data-out-prod/ASSIGNMENTS/cdsa/assn_output/test_campaign_path/mail_subset",
            "INPUT_MAILHOUSE": "s3://memberanalytics-data-out-prod/ASSIGNMENTS/cdsa/assn_output/test_campaign_path/final_mailhouse",
        }
        self.optional_bbm_paths = {
            "INPUT_ASSIGNMENTS": "s3://memberanalytics-data-out-prod/ASSIGNMENTS/cdsa/assn_output/test_campaign_path/assignments",
            "INPUT_CONSTRUCTS": "s3://memberanalytics-data-out-prod/ASSIGNMENTS/cdsa/assn_output/test_campaign_path/assignments_allconstructs",
            "MAIL_POPULATION_ASSIGNMENT": "s3://memberanalytics-data-out-prod/ASSIGNMENTS/cdsa/assn_output/test_campaign_path/assignments",
            "INPUT_MAILHOUSE": "s3://memberanalytics-data-out-prod/ASSIGNMENTS/cdsa/assn_output/test_campaign_path/final_mailhouse",
        }

    def update_path(self, paths_dict, path_name, path):
        """
        Updates the path in the paths dictionary.
        """
        if path == "pop":
            paths_dict.pop(path_name)
        else:
            paths_dict[path_name] = path

        return paths_dict

    def check_calc_path(
        self, target, input_path, correct_path, path_updates={}
    ):
        """
        Checks if the calculated filepath for the target given an input_path
        and correct_path for the target.

        Parameters
        ----------
        target (string): the path name from the config that you are checking \n
        input_path (string): the path for the target in the config \n
        correct_path (string): the path that should be returned when
        calculating the file path for the target \n
        path_updates (dictionary): a dictionary of path names and input paths
        to update the paths dictionary with before calculating the filepath for
        the target path name.
        """
        cnf_params = copy.deepcopy(self.params)
        cnf_paths = copy.deepcopy(self.paths)
        cnf_paths = self.update_path(cnf_paths, target, input_path)
        for path_name, updated_path in path_updates.items():
            cnf_paths = self.update_path(cnf_paths, path_name, updated_path)
        actual_params, actual_paths = calculate_filepaths(
            cnf_params, cnf_paths
        )
        self.assertEqual(actual_paths[target], correct_path)

    def determine_correct_path(self, input_path, default_path):
        """
        Determines whether to use the input_path or the default_path as the
        correct output when checking calculated paths. Returns the input_path
        if it is a custom path. Otherwise, it returns the default_path.
        """
        if input_path not in ("", None, "pop"):
            correct_path = input_path
        else:
            correct_path = default_path

        return correct_path

    def check_optional_paths(self, input_path):
        """
        Runs through all optional paths for the MMPC and BBM, and checks to see
        if they were calculated correctly given the input_path.
        """
        self.params["campaign"] = "MMPC"
        for target, default_path in self.optional_mmpc_paths.items():
            correct_path = self.determine_correct_path(
                input_path, default_path
            )
            self.check_calc_path(target, input_path, correct_path)
        self.params["campaign"] = "BBM"
        for target, default_path in self.optional_bbm_paths.items():
            correct_path = self.determine_correct_path(
                input_path, default_path
            )
            self.check_calc_path(target, input_path, correct_path)
        target = "MAIL_POPULATION_ASSIGNMENT"
        default_path = self.optional_bbm_paths[target]
        for input_assignments_path in [
            "custom_input_assignments_path",
            "",
            "pop",
        ]:
            path_updates = {"INPUT_ASSIGNMENTS": input_assignments_path}
            if input_path == "custom_path":
                correct_path = "custom_path"
            else:
                correct_path = self.determine_correct_path(
                    input_assignments_path, default_path
                )
            self.check_calc_path(
                target, input_path, correct_path, path_updates
            )

    @patch("memberdna.pipelines.assignment.lib.assn_io.generate_campaign_path")
    def test_optional_path(self, generate_campaign_path):
        """
        Looks through the four possible inputs for optional inputs: missing the
        input altogether ("pop"), an empty string (""), blank (None), and a
        custom path ("custom_path").
        """
        generate_campaign_path.return_value = "test_campaign_path/"

        self.check_optional_paths(input_path="pop")
        self.check_optional_paths(input_path=None)
        self.check_optional_paths(input_path="")
        self.check_optional_paths(input_path="custom_path")

    def test_input_mail_path_with_commented_output_path(self):
        campaign = "MMPC15FY20"
        self.params["campaign"] = campaign
        self.paths["MAIL_LIST"] = ""

        expected_path = f"s3://memberanalytics-data-out-prod/ASSIGNMENTS/campaigns/FY20/{campaign}/input_mail_list"
        cnf_params = copy.deepcopy(self.params)
        cnf_paths = copy.deepcopy(self.paths)
        actual_params, actual_paths = calculate_filepaths(
            cnf_params, cnf_paths
        )
        self.assertEqual(actual_paths["MAIL_LIST"], expected_path)

    def test_input_mail_path_with_another_cdsa_loc(self):
        campaign = "MMPC15FY20"
        self.params["campaign"] = campaign
        self.paths["MAIL_LIST"] = ""
        self.paths[
            "CDSA_LOC"
        ] = "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/assignment/GenerateInputMail/ASSIGNMENTS/cdsa/"

        expected_base_path = "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/assignment/GenerateInputMail/ASSIGNMENTS/"
        expected_path = (
            f"{expected_base_path}campaigns/FY20/{campaign}/input_mail_list"
        )

        cnf_params = copy.deepcopy(self.params)
        cnf_paths = copy.deepcopy(self.paths)
        actual_params, actual_paths = calculate_filepaths(
            cnf_params, cnf_paths
        )
        self.assertEqual(actual_paths["MAIL_LIST"], expected_path)

    def test_invalid_campaign_name(self):
        self.params["campaign"] = "MMPC"
        self.paths["MAIL_LIST"] = ""
        cnf_params = copy.deepcopy(self.params)
        cnf_paths = copy.deepcopy(self.paths)
        with self.assertRaises(ValueError):
            calculate_filepaths(cnf_params, cnf_paths)

    def test_invalid_cdsa_loc(self):
        self.params["campaign"] = "MMPC15FY20"
        self.paths["MAIL_LIST"] = ""
        self.paths["CDSA_LOC"] = ""
        cnf_params = copy.deepcopy(self.params)
        cnf_paths = copy.deepcopy(self.paths)
        with self.assertRaises(ValueError):
            calculate_filepaths(cnf_params, cnf_paths)


class CheckExecutionOverwrite(unittest.TestCase):
    def setUp(self):
        self.cnf, cfg_path = load_config()
        self.cnf["shared"]["experiment"] = "30"

        self.params = dict()
        self.params.update(self.cnf["shared"])
        self.params.update(self.cnf["assignment"])
        self.paths = self.cnf["paths"]

    @patch("memberdna.pipelines.assignment.lib.checks.is_s3_file")
    @patch("memberdna.pipelines.assignment.lib.checks.is_s3_path")
    @patch("memberdna.pipelines.assignment.lib.checks.get_run_args")
    def test_force(self, get_run_args, is_s3_path, is_s3_file):
        get_run_args.return_value = Mock(force=True)

        check_execution_overwrite(paths_to_check=self.paths)

        is_s3_path.assert_not_called()
        is_s3_file.assert_not_called()

    @patch("memberdna.pipelines.assignment.lib.checks.is_s3_file")
    @patch("memberdna.pipelines.assignment.lib.checks.is_s3_path")
    @patch("memberdna.pipelines.assignment.lib.checks.get_run_args")
    def test_first_execution(self, get_run_args, is_s3_path, is_s3_file):
        get_run_args.return_value = Mock(force=False)
        is_s3_path.return_value = False
        is_s3_file.return_value = False

        check_execution_overwrite(paths_to_check=self.paths)

        is_s3_path.assert_called()
        is_s3_file.assert_called()

    @patch("memberdna.pipelines.assignment.lib.checks.is_s3_file")
    @patch("memberdna.pipelines.assignment.lib.checks.is_s3_path")
    @patch("memberdna.pipelines.assignment.lib.checks.get_run_args")
    def test_second_execution(self, get_run_args, is_s3_path, is_s3_file):
        get_run_args.return_value = Mock(force=False)
        is_s3_path.return_value = True
        is_s3_file.return_value = False

        self.assertRaisesRegex(
            Exception,
            "There already has been a campaign executed with the same"
            " campaign and run name. Change the names in the config or"
            " use --force in order to overwrite the last execution.",
            check_execution_overwrite,
            self.paths,
        )


class DumpConfigTestCase(unittest.TestCase):
    def setUp(self):
        self.cnf, cfg_path = load_config()
        self.cnf["shared"]["experiment"] = "30"

        self.params = dict()
        self.params.update(self.cnf["shared"])
        self.params.update(self.cnf["assignment"])
        self.paths = self.cnf["paths"]

    @patch("memberdna.pipelines.assignment.lib.assn_io.write_yaml_to_s3")
    def test_path_provided(self, write_yaml_to_s3):
        test_path = "s3://test_bucket/test_key"

        actual_path = dump_config(self.cnf, dict(), dict(), test_path)

        self.assertEqual(actual_path, test_path)
        write_yaml_to_s3.assert_called_with(
            "test_bucket", "test_key", self.cnf
        )

    @patch("memberdna.pipelines.assignment.lib.assn_io.write_yaml_to_s3")
    def test_path_not_provided(self, write_yaml_to_s3):
        output_dir = "s3://test_bucket/test_output_dir"
        expected_path = (
            output_dir
            + "/"
            + "full_configuration"
            + "/"
            + "config_full"
            + self.params["campaign"]
            + ""
            + self.params["run_name"]
            + ""
            + self.params["run_type"]
            + ".yml"
        )

        actual_path = dump_config(self.cnf, {"OUTPUT_DIR": output_dir}, dict())

        self.assertEqual(actual_path, expected_path)
        write_yaml_to_s3.assert_called_with(
            "test_bucket",
            "test_output_dir/full_configuration/"
            "config_fullmmpc_bbmtesttest.yml",
            self.cnf,
        )

    @patch("memberdna.pipelines.assignment.lib.assn_io.write_yaml_to_s3")
    def test_paths_and_params_not_provided(self, write_yaml_to_s3):
        dump_config(self.cnf, cfg_path="s3://test_bucket/test_key")
        write_yaml_to_s3.assert_called_with(
            "test_bucket", "test_key", self.cnf
        )

    @patch("memberdna.pipelines.assignment.lib.assn_io.write_yaml_to_s3")
    def test_skip_unknown_parameters(self, write_yaml_to_s3):
        self.params["coupon_inhome_date"] = "coupon_creation param"
        self.params["test_unknown"] = "value"
        self.paths["unknown_path"] = "value"

        dump_config(
            self.cnf,
            self.paths,
            self.params,
            cfg_path="s3://test_bucket/test_key",
        )
        write_yaml_to_s3.assert_called_with(
            "test_bucket", "test_key", self.cnf
        )

    @patch("memberdna.pipelines.assignment.lib.assn_io.write_yaml_to_s3")
    def test_parameters_are_updated(self, write_yaml_to_s3):
        self.params["experiment"] = "50"
        self.params["backfill_method"] = {"basket": "test"}
        self.paths["INPUT_ASSIGNMENTS"] = "s3://test_bucket/test_key"

        expected_cnf = dict(self.cnf)
        expected_cnf["shared"]["experiment"] = "50"
        expected_cnf["assignment"]["backfill_method"] = {"basket": "test"}
        expected_cnf["paths"][
            "INPUT_ASSIGNMENTS"
        ] = "s3://test_bucket/test_key"

        dump_config(
            self.cnf,
            self.paths,
            self.params,
            cfg_path="s3://test_bucket/test_key",
        )
        write_yaml_to_s3.assert_called_with(
            "test_bucket", "test_key", expected_cnf
        )


if __name__ == "__main__":
    import findspark

    findspark.init()
    import copy

    from pyspark.sql import Row, SparkSession
    from pyspark.sql import functions as F

    from pe_memberdna.assignment.lib.assn_io import (
        calculate_filepaths,
        dump_config,
        load_config,
    )
    from pe_memberdna.assignment.lib.assn_utils import (
        apply_offer_recency,
        calc_avg_basket,
        calc_overlapping_cols,
        calc_sampling_seg,
        calc_sampling_value,
        cap_top_percentile,
        cap_value,
        check_cpn_nbr_or_version,
        create_equal_size_bucket,
        deterministic_sample,
        downsample_rows,
        fill_with_average,
        filter_rows,
        flag_rows,
        get_all_of_key,
        get_key_values,
        load_past_longitudinal_mbrs,
        map_under_threshold,
        mapping,
        palindrome_rank_group,
        palindrome_sample,
        rdd_rank_by_col,
        read_subset_and_cast,
        update_categories,
    )
    from pe_memberdna.assignment.lib.filters import (
        ROI_positive,
        avg_basket,
        construct_satisfied,
        expired,
        gas,
        general_filter,
        longitudinal,
        new,
        run_sub_filters,
        special_club,
        special_zipcode,
        tenure,
        trial,
    )
    from pe_memberdna.assignment.lib.slots import (
        basket,
        cf,
        cf_combined,
        compose_slots,
        limit_to_cf_match,
        random,
        rank_by_col,
        sql,
        static,
        stretch_spend,
        waterfall_slots,
    )
    from pe_memberdna.lib.spark_util import union_with_mismatched_columns

    name = "assign_unit_test"

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
