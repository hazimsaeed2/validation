"""Unit Tests for offer assignment."""


import unittest

import xmlrunner
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

import memberdna.testing_support.regression_framework.regression as regression


class TestRegression(unittest.TestCase):
    """Unit tests for Regression functions."""

    def test_create_modulo_when_statement(self):
        df = spark.sparkContext.parallelize(
            map(lambda x: [x], range(0, 6)), 10
        ).toDF(["hat"])
        options = ["-1", "-2", "-3"]
        statement = regression.RegressionTest.create_modulo_when_statement(
            "hat",
            options
        )

        result = df.withColumn("sample", statement).collect()
        self.assertTrue(result[0]["sample"] == "-1")
        self.assertTrue(result[1]["sample"] == "-2")
        self.assertTrue(result[2]["sample"] == "-3")
        self.assertTrue(result[3]["sample"] == "-1")
        self.assertTrue(result[4]["sample"] == "-2")
        self.assertTrue(result[5]["sample"] == "-3")

    def test_apply_schema_hard(self):
        columns = ["double_col", "long_col"]
        schema = StructType(
            [
                StructField("long_col", LongType(), False),
                StructField("double_col", DoubleType(), False),
            ]
        )
        df = spark.sparkContext.parallelize(
            [["10", ".1"], [".1", "100"]], 10
        ).toDF(columns)
        typed = regression.RegressionTest.apply_schema_hard(df, schema)

        self.assertTrue(typed.dtypes[0][0] == "long_col")
        self.assertTrue(typed.dtypes[0][1] == "bigint")
        self.assertTrue(typed.dtypes[1][0] == "double_col")
        self.assertTrue(typed.dtypes[1][1] == "double")


if __name__ == "__main__":
    import findspark

    findspark.init()

    name = "regression_test"

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
