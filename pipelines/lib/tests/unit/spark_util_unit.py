import unittest

import xmlrunner
from pyspark.sql import SparkSession

from memberdna.pipelines.lib.spark_util import (
    count_nulls,
    crosstab_pct,
    grouped_percentiles,
    join_all,
    pct_flagged,
    union_with_mismatched_columns,
)

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")


class TestFilters(unittest.TestCase):
    """Unit tests for filter functions."""

    def test_union_with_mismatched_columns(self):
        df1 = spark.sparkContext.parallelize([[0, "b1", "c1"]]).toDF(
            ["col1", "col2", "col3"]
        )
        df2 = spark.sparkContext.parallelize([["b2", "c2", True]]).toDF(
            ["col2", "col3", "col4"]
        )
        output = union_with_mismatched_columns(df1, df2).cache()
        self.assertEqual(output.count(), 2)
        self.assertEqual(output.columns, ["col1", "col2", "col3", "col4"])
        self.assertEqual(output.filter("col1 == 0").first()["col4"], None)
        self.assertEqual(output.filter("col4 == true").first()["col1"], None)
        output.unpersist()

    def test_join_all(self):
        _dfs = list(
            map(
                lambda x: spark.sparkContext.parallelize([[1, x]]).toDF(
                    ["id", "score_{}".format(x)]
                ),
                list(range(0, 8)),
            )
        )

        out = join_all(_dfs, ["id"])

        self.assertEqual(out.count(), 1)
        self.assertEqual(
            out.columns,
            [
                "id",
                "score_0",
                "score_1",
                "score_2",
                "score_3",
                "score_4",
                "score_5",
                "score_6",
                "score_7",
            ],
        )


class TestCountNulls(unittest.TestCase):
    """Unit tests for count_nulls()."""

    def setUp(self):
        self.df = spark.sparkContext.parallelize(
            [
                ["A", 1],
                ["A", 2],
                ["A", None],
                ["B", 1],
                ["B", None],
                ["B", None],
            ]
        ).toDF(["key", "val"])

    def test_count_nulls_key(self):
        res = count_nulls(self.df, "val", "key")
        res.columns.sort()
        self.assertEqual(res.columns, ["key", "nulls"])
        self.assertEqual(res.count(), 2)

        a = res.filter(res.key == "A").select("nulls").collect()[0][0]
        self.assertEqual(a, 1)
        b = res.filter(res.key == "B").select("nulls").collect()[0][0]
        self.assertEqual(b, 2)

    def test_count_nulls_no_key(self):
        res = count_nulls(self.df, "val")
        self.assertEqual(res, 3)


class TestGroupPercentiles(unittest.TestCase):
    """Unit test for grouped_percentiles()."""

    def setUp(self):
        self.df = spark.sparkContext.parallelize(
            [
                ["A", 1],
                ["A", 2],
                ["A", 3],
                ["A", 4],
                ["A", None],
                ["B", 1],
                ["B", 2],
                ["B", None],
                ["B", None],
            ]
        ).toDF(["key", "val"])

    def test_grouped_percentiles(self):
        res = grouped_percentiles(self.df, "key", "val", pcts=[0.1, 0.5])
        res.columns.sort()
        self.assertEqual(res.columns, ["key", "pct_10", "pct_50"])
        self.assertEqual(res.count(), 2)

        a = res.filter(res.key == "A")
        self.assertEqual(a.select("pct_10").collect()[0][0], 1.3)
        self.assertEqual(a.select("pct_50").collect()[0][0], 2.5)
        b = res.filter(res.key == "B")
        self.assertEqual(b.select("pct_10").collect()[0][0], 1.1)
        self.assertEqual(b.select("pct_50").collect()[0][0], 1.5)


class TestPctFlagged(unittest.TestCase):
    """Unit test for pct_flagged()"""

    def setUp(self):
        self.df = spark.sparkContext.parallelize(
            [["A", 1], ["A", 0], ["B", 0], ["B", 0], ["C", 1]]
        ).toDF(["key", "flag"])

    def test_pct_flagged_key(self):
        res = pct_flagged(self.df, "flag", "pct_flagged", "key")
        self.assertSetEqual(set(res.columns), set(("key", "pct_flagged")))
        self.assertEqual(res.count(), 3)

        a = res.filter(res.key == "A").select("pct_flagged").collect()[0][0]
        self.assertEqual(a, 0.5)
        b = res.filter(res.key == "B").select("pct_flagged").collect()[0][0]
        self.assertEqual(b, 0)
        c = res.filter(res.key == "C").select("pct_flagged").collect()[0][0]
        self.assertEqual(c, 1)

    def test_pct_flagged_no_key(self):
        res = pct_flagged(self.df, "flag", "pct_flagged")
        self.assertEqual(res.columns, ["pct_flagged"])
        self.assertEqual(res.count(), 1)

        pct = res.select("pct_flagged").collect()[0][0]
        self.assertEqual(pct, 0.4)


class TestCrossTabPct(unittest.TestCase):
    """Unit test for crosstab_pct()"""

    def setUp(self):
        self.df = spark.sparkContext.parallelize(
            [
                ["k1", "a", 1],
                ["k1", "b", 1],
                ["k1", "b", 1],
                ["k1", "b", 1],
                ["k2", "a", 1],
            ]
        ).toDF(["key", "col", "val"])

    def test_crosstab_pct_key(self):
        res = crosstab_pct(self.df, "col", "val", "key")
        self.assertSetEqual(set(res.columns), set(("key", "a", "b")))
        self.assertEqual(res.count(), 2)

        k1 = res.filter(res.key == "k1")
        a = k1.select("a").collect()[0][0]
        self.assertEqual(a, 0.25)
        b = k1.select("b").collect()[0][0]
        self.assertEqual(b, 0.75)

        k2 = res.filter(res.key == "k2")
        a = k2.select("a").collect()[0][0]
        self.assertEqual(a, 1)
        b = k2.select("b").collect()[0][0]
        self.assertEqual(b, 0)

    def test_crosstab_pct_no_key(self):
        res = crosstab_pct(self.df, "col", "val")
        self.assertSetEqual(set(res.columns), set(("a", "b")))
        self.assertEqual(res.count(), 1)

        a = res.select("a").collect()[0][0]
        self.assertEqual(a, 0.4)
        b = res.select("b").collect()[0][0]
        self.assertEqual(b, 0.6)


if __name__ == "__main__":
    import findspark

    findspark.init()

    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
