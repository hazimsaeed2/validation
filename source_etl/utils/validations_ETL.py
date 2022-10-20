import logging
import subprocess

import pyspark.sql.functions as sqlf
import yaml
from memberdna.lib.data_quality import *
from pyspark.sql.types import FloatType


class TestControlTable(DQ_check):
    """
    Verifiy that the control file produced from redshift matches the
    control file created based on the pipelined_intermediates data.
    """

    def __init__(
        self,
        spark,
        tabletype,
        tablename,
        df,
        config_validation,
        s3_stat_path="",
        throw_errors=True,
    ):
        """
        Constructor
        """

        super(TestControlTable, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )

        self.max_rows = (config_validation.get("TestControlTable") or {}).get(
            "toleranceRows"
        ) or 0

    def test_body(self):
        """
        The structure of the table produced by the etl_tasks.control_files is
        such that there are columns starting with redshift_ and columns starting
        with engine_.

        This method verifies that if the columns are of numeric type the relative
        difference between the coresponding columns is less than tolerance specified
        in the config file.

        If the columns are of string or date type the method checks for hard
        equivalence.
        """

        orig_col_list = set(col.split("_", 1)[1] for col in self.df.columns)

        for col in orig_col_list:
            if self.df.select("engine_" + col).dtypes[0][1] in [
                "date",
                "string",
                "timestamp",
            ]:

                df_comparison = self.df.withColumn(
                    "comparison_" + col,
                    sqlf.when(
                        (
                            sqlf.isnull(self.df["engine_" + col])
                            & sqlf.isnull(self.df["redshift_" + col])
                        )
                        | (
                            (
                                self.df["engine_" + col]
                                == self.df["redshift_" + col]
                            )
                        ),
                        0.0,
                    ).otherwise(sqlf.lit(1.0)),
                )
            else:
                df_comparison = self.df.withColumn(
                    "comparison_" + col,
                    sqlf.when(
                        (
                            sqlf.isnull(self.df["engine_" + col])
                            & sqlf.isnull(self.df["redshift_" + col])
                        )
                        | (
                            sqlf.isnan(self.df["engine_" + col])
                            & sqlf.isnan(self.df["redshift_" + col])
                        )
                        | (
                            (self.df["engine_" + col] == 0)
                            & (self.df["redshift_" + col] == 0)
                        ),
                        0.0,
                    ).otherwise(
                        sqlf.abs(
                            (
                                self.df["engine_" + col].cast(FloatType())
                                - self.df["redshift_" + col].cast(FloatType())
                            )
                            / (
                                self.df["redshift_" + col].cast(FloatType())
                                + self.df["engine_" + col].cast(FloatType())
                            )
                            * 2
                        )
                    ),
                )

            df_comparison = df_comparison.where(
                df_comparison["comparison_" + col] > self.tolerance
            )

            deviant_count = df_comparison.count()

            if deviant_count > self.max_rows:

                problematic_lines = (
                    df_comparison.limit(10).toPandas().to_dict(orient="list")
                )

                self.fail_test(
                    col,
                    self.generate_error_msg(
                        col, problematic_lines, deviant_count
                    ),
                    deviant_count,
                )
            else:
                self.pass_test(col)

    def generate_error_msg(self, col_name, problematic_lines, deviant_count):
        """
        Generate an error message
        """

        fail_message = (
            "Non-matching value between redshift and the engine found for %(col_name)s"
            + " in the %(tabletype)s table %(tablename)s . Overall count of errorneous rows"
            + " = %(deviant_count)s \n"
        ) % {
            "col_name": col_name,
            "tabletype": self.tabletype,
            "tablename": self.tablename,
            "deviant_count": deviant_count,
        }

        fail_message += (
            "Example of problematic lines: " + str(problematic_lines) + " \n"
        )

        return fail_message


class TestOutlierDays(DQ_check):
    """
    Tests for outlying daily mean values when compared to
    the annual mean values. This test only makes sense for tables
    that have the column PURCH_DT. Note that this is done for every day of the
    week separately to take into account that average
    sales on Monday may be higher than Friday, for example.

    The following series of daily means should pass the test:
    10 11 10 12 13 10 9 12 10

    the following series should fail:
    10 11 10 12654 13 10 9 12 10

    The name of the column to be checked and the check tolerance are
    passed as arguments of the constructor.
    """

    def __init__(
        self,
        spark,
        tabletype,
        tablename,
        df,
        config_validation,
        s3_stat_path="",
        throw_errors=True,
    ):
        """
        Args: Same as parent class.

        Extract the following keys from config_validation:
            amnt_check_cols (list): aggregate SQL expessions that are to be
                checked. e.g. queries might include 'Count(*)' or SUM(payment)
            TestOutlierDays.tolerance: tolerance
        """
        super(TestOutlierDays, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )

        self.amnt_check_cols = config_validation["amnt_check_cols"]

    def test_body(self):
        """
        The method operates as follows:
            1.) Calculate daily aggregates
            2.) Calculate the annual mean of daily aggregates
            3.) Calculates the relative difference between the annual
                aggregates and the daily means, looks for large relative
                differences
        """

        def _tmp_tbl(sql, name):
            """
            Register a spark temp table

            Args:
                sql (str): sql string to execute to obtain table contents
                name (str): name of registered temp table to create
            Returns:
                Nothing. Creates registered temp table <name>
            """
            df = self.spark.sql(sql)
            df.registerTempTable(name)

        if "PURCH_DT" not in self.df.columns:
            logging.info("--> No PURCH_DT column found.")
            return

        self.df.registerTempTable("df")

        qry_daily_aggs = """
            SELECT
                {},
                PURCH_DT
            FROM df
            GROUP BY PURCH_DT
            """.format(
            ", ".join(
                [
                    col + " as col" + str(i)
                    for i, col in enumerate(self.amnt_check_cols)
                ]
            )
        )

        aggs_df = self.spark.sql(qry_daily_aggs)
        aggs_df.registerTempTable("df")

        _tmp_tbl("SELECT MAX(PURCH_DT) AS max_dt FROM df", "max_dt_tab")

        for col_id, col in enumerate(self.amnt_check_cols):

            d = aggs_df.select("col" + str(col_id), "PURCH_DT")
            d = d.withColumnRenamed("col" + str(col_id), "agg_sum")
            d.registerTempTable("daily_aggregates")

            qry_avg_weekday = """
                SELECT
                    datediff(dt.max_dt, agg.PURCH_DT) % 7 as dow,
                    avg(agg.agg_sum) as avg_weekday
                FROM
                    daily_aggregates as agg
                CROSS JOIN
                    max_dt_tab as dt
                WHERE
                    datediff(dt.max_dt, agg.PURCH_DT) < 365
                GROUP BY
                    dow
                """

            _tmp_tbl(qry_avg_weekday, "avg_weekday_tab")

            qry_avg_weekday = """
                SELECT
                    datediff(dt.max_dt, agg.PURCH_DT)
                    % 7 as dow,
                    avg(agg.agg_sum) as avg_weekday
                FROM daily_aggregates as agg
                CROSS JOIN max_dt_tab as dt
                WHERE
                    datediff(dt.max_dt, agg.PURCH_DT) < 365
                GROUP BY
                    dow
                """

            _tmp_tbl(qry_avg_weekday, "avg_weekday_tab")

            qry_avg_agg_comp = """
                SELECT
                    *
                FROM
                    (
                    select
                        wd_tab.dow,
                        wd_tab.avg_weekday,
                        agg.agg_sum,
                        agg.PURCH_DT,
                        dt.max_dt,
                        abs(wd_tab.avg_weekday - agg.agg_sum)
                            / wd_tab.avg_weekday as abs_rel_dev
                    FROM daily_aggregates as agg
                    CROSS JOIN max_dt_tab as dt
                    JOIN avg_weekday_tab as wd_tab
                    ON
                        wd_tab.dow =
                        datediff(dt.max_dt, agg.PURCH_DT)
                    WHERE
                        datediff(dt.max_dt, agg.PURCH_DT) < 7
                   ) AS A
                WHERE
                    abs_rel_dev > %(tolerance)s
                """ % {
                "tolerance": self.tolerance
            }

            avg_agg_comp_df = self.spark.sql(qry_avg_agg_comp)

            deviant_count = avg_agg_comp_df.count()

            logging.info(
                "-->"
                + str(deviant_count)
                + " days with Outlier dates for "
                + col
                + "."
            )

            if deviant_count != 0:

                self.fail_test(
                    col,
                    self.generate_error_msg(col, deviant_count),
                    deviant_count,
                )
            else:
                self.pass_test(col)

    def generate_error_msg(self, col_name, deviant_count):
        fail_message = (
            "Outlier value of column(s) %(col_name)s"
            + " in the %(tabletype)s table %(tablename)s . \n"
        ) % {
            "col_name": col_name,
            "tabletype": self.tabletype,
            "tablename": self.tablename,
        }
        fail_message += (
            "Number of Outliers detected: " + str(deviant_count) + " \n"
        )

        return fail_message


class _CompareColAggregates(DQ_check):
    """
    Look for match between aggregate values such as sums and counts.
    """

    def __init__(
        self,
        spark,
        tabletype,
        tablename,
        df,
        config_validation,
        s3_stat_path="",
        throw_errors=True,
        type="source",
    ):
        super(_CompareColAggregates, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )
        """
        Args:
            Inherited from parent
            type (str): 'source', 'prior', 'both'
                source: compare current versus source
                prior: compare current versus prior run
                both: compare current versus source and prior

        Extract the following keys from the config_validation:
            amnt_check_cols (list): aggregate expressions to be checked
            CompareColAggregates.tolerance
        """
        self.amnt_check_cols = config_validation["amnt_check_cols"]
        self.type = type
        if type == "both":
            if throw_errors:
                self.tolerance_source = config_validation.get(
                    "CompareColAggregatesSource", {}
                ).get("tolerance", 0.05)
                self.tolerance_prior = config_validation.get(
                    "CompareColAggregatesPrior", {}
                ).get("tolerance", 0.05)
            else:
                self.tolerance_source = 0
                self.tolerance_prior = 0

    def test_body(self):
        """
        Calculate agreggate expressions defined in amnt_check_cols
        and compare according to type (source, prior, or both).
        """
        if not (self.amnt_check_cols):
            logging.info(
                "No columns to check, not running" + self.__class__.__name__
            )
            return

        self.df.registerTempTable("df")
        sql = "SELECT {} FROM df".format(", ".join(self.amnt_check_cols))
        col_sum_df = self.spark.sql(sql)

        col_sums = {}
        for col_id, amnt_col in enumerate(self.amnt_check_cols):
            col_sums[amnt_col] = col_sum_df.collect()[0][col_id]

        if self.type == "both":
            checks = ["source", "prior"]
        else:
            checks = [self.type]

        for check in checks:
            suffix = check[0].upper() + check[1:]
            self.testname = "CompareColAggregates{}".format(suffix)
            if check == "source":
                col_sum_required = self.get_stat(
                    "source", self.tablename, self.testname
                )
                if self.type == "both":
                    tolerance = self.tolerance_source
                else:
                    tolerance = self.tolerance
            else:
                col_sum_required = self.get_stat()
                if self.type == "both":
                    tolerance = self.tolerance_prior
                else:
                    tolerance = self.tolerance

            if len(col_sum_required) == 0:
                if check == "prior":
                    self.add_stat(col_sums)
                logging.info(
                    "DQ check {} cancelled: \n".format(self.testname)
                    + "Cannot find saved aggregates.\nSaving currente values."
                )
                continue

            for amnt_col in self.amnt_check_cols:
                amt = col_sum_required.get(amnt_col)
                if amt is not None:
                    amt = float(amt)
                    rel_diff = abs((float(col_sums[amnt_col]) - amt) / amt)
                    logging.info(
                        "--> Relative difference = "
                        + str(rel_diff)
                        + " in "
                        + amnt_col
                    )

                    if rel_diff > tolerance:
                        self.fail_test(
                            amnt_col,
                            self.generate_error_msg(
                                col_sums, col_sum_required, amnt_col
                            ),
                            rel_diff,
                        )
                    else:
                        self.pass_test(amnt_col)
                else:
                    self.fail_test(
                        amnt_col,
                        " Cannot find saved aggregate value for " + amnt_col,
                        1,
                    )

            if check == "prior":
                self.add_stat(col_sums)

    def generate_error_msg(self, col_sums, col_sum_required, amnt_col):
        fail_message = (
            "The value of aggregate %(amnt_col)s "
            + "in %(tabletype)s table %(tablename)s "
            + "is inconsistent with the %(type)s values \n "
            + "The expected value of %(amnt_col)s "
            + "was %(prev_col_sums)s \n "
            + "the current value of %(amnt_col)s "
            + "is %(curr_col_sums)s \n"
        ) % {
            "curr_col_sums": col_sums[amnt_col],
            "prev_col_sums": col_sum_required[amnt_col],
            "amnt_col": amnt_col,
            "tabletype": self.tabletype,
            "tablename": self.tablename,
            "type": self.type,
        }
        return fail_message


class CompareColAggregatesSource(_CompareColAggregates):
    """
    Look for match between aggregate values such as sums and counts between
    current and source.
    """

    def __init__(
        self,
        spark,
        tabletype,
        tablename,
        df,
        config_validation,
        s3_stat_path="",
        throw_errors=True,
    ):
        super(CompareColAggregatesSource, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
            type="source",
        )


class CompareColAggregatesPrior(_CompareColAggregates):
    """
    Look for match between aggregate values such as sums and counts between
    current and prior.
    """

    def __init__(
        self,
        spark,
        tabletype,
        tablename,
        df,
        config_validation,
        s3_stat_path="",
        throw_errors=True,
    ):
        super(CompareColAggregatesPrior, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
            type="prior",
        )


class CompareColAggregatesBoth(_CompareColAggregates):
    """
    Look for match between aggregate values such as sums and counts between
    current and source as well as current and prior.
    """

    def __init__(
        self,
        spark,
        tabletype,
        tablename,
        df,
        config_validation,
        s3_stat_path="",
        throw_errors=True,
    ):
        super(CompareColAggregatesBoth, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
            type="both",
        )
