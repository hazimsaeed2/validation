import logging
import pandas as pd

from memberdna.lib.data_quality import *


def get_numeric_columns(df):
    """
    Returns the non-numeric columns present in the
    input dataframe df.
    This excludes the columns MBRSHP_SID and FISCAL_WEEK_END.
    """

    col_list_numeric = [
        col
        for col in df.columns
        if (
            (not (str(df.schema[col].dataType) in ["DateType", "StringType"]))
            & (not (col in ["MBRSHP_SID", "FISCAL_WEEK_END"]))
        )
    ]
    return col_list_numeric


class TestOldDNAvsNewDNAColAggregates(DQ_check):
    """
    This class calculates aggregates such as sum(X), count(X)
    or SUM(CASE WHEN x IS NULL THEN 1 ELSE 0 END)
    for every week in the DNA table from this fiscal week as well as
    and compares them to aggregates from the DNA table from
    the previous week. This is done for every fiscal week for
    every column.
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

        super(TestOldDNAvsNewDNAColAggregates, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )

        self.aggregate_expressions = config_validation.get(self.testname, {}).get(
            "aggregates", {}
        )

        if not (self.aggregate_expressions):
            raise ValueError(
                "the key aggregates is missing for tabletype = "
                + tabletype
                + " tablename = "
                + tablename
            )

    def test_body(self):
        """
        Reads the new DNA table, reads the old dna table and compares
        the aggregates for all weeks
        """

        self.df.registerTempTable("df_TestOldDNAvsNewDNAColAggregates_new")

        col_list_new = get_numeric_columns(self.df)

        result_list = []

        for col_name in col_list_new:
            logging.info("processing column " + col_name)

            sql_statement = (
                " UNION ALL ".join(
                    [
                        (
                            """
                        SELECT
                            FISCAL_WEEK_END,
                            \'{expr_name}\' as aggregate_name,
                            {agg_expr} as aggregate_value
                        FROM
                            df_TestOldDNAvsNewDNAColAggregates_new
                        GROUP BY
                            FISCAL_WEEK_END
                        """.format(
                                expr_name=expr_name,
                                agg_expr=self.aggregate_expressions[expr_name],
                            )
                        )
                        for expr_name in self.aggregate_expressions.keys()
                    ]
                )
            ).format(col_name=col_name)

            df_weekly_aggregates = self.spark.sql(sql_statement)
            df_weekly_aggregates.registerTempTable("df_weekly_aggregates_new")

            loc_dict_old_dna = self.get_stat(
                self.tabletype, self.tablename, self.testname
            ).get(col_name, {})

            if not (loc_dict_old_dna):
                pandas_df = df_weekly_aggregates.toPandas()
                my_list_out = pandas_df.to_dict("list")
                loc_dict_old_dna[col_name] = my_list_out
                self.add_stat(loc_dict_old_dna)
                self.pass_test(col_name)
            else:

                loc_df_old_dna = pd.DataFrame.from_dict(loc_dict_old_dna)
                df_old_dna = self.spark.createDataFrame(loc_df_old_dna)
                df_old_dna.registerTempTable("df_weekly_aggregates_old")

                sql_statement = (
                    """
                    SELECT
                        agg_new.aggregate_value as aggregate_value_new,
                        agg_old.aggregate_value as aggregate_value_old,
                        agg_old.aggregate_name,
                        agg_old.FISCAL_WEEK_END
                    FROM
                        df_weekly_aggregates_old as agg_old
                    INNER JOIN
                        df_weekly_aggregates_new as agg_new
                    ON 1=1
                        AND agg_old.aggregate_name = agg_new.aggregate_name
                        AND agg_old.FISCAL_WEEK_END = agg_new.FISCAL_WEEK_END
                    WHERE
                        abs((
                            agg_new.aggregate_value/agg_old.aggregate_value
                        )-1)>{tolerance}
                    """
                ).format(tolerance=self.tolerance)

                df_out = self.spark.sql(sql_statement)

                error_count = df_out.count()

                if error_count > 0:

                    error_msg = " \n ".join(
                        [
                            self.generate_error_msg(col_name, row)
                            for row in df_out.collect()
                        ]
                    )

                    self.fail_test(col_name, error_msg, error_count)
                else:
                    pandas_df = df_weekly_aggregates.toPandas()
                    my_list_out = pandas_df.to_dict("list")
                    loc_dict_old_dna[col_name] = my_list_out
                    self.add_stat(loc_dict_old_dna)
                    self.pass_test(col_name)

    def generate_error_msg(self, col_name, row):
        """
        Parses togather the error message
        """

        err_aggregate_value_new = row[0]
        err_aggregate_value_old = row[1]
        err_aggregate_name = row[2]
        err_fiscal_week_end = row[3]

        fail_message = (
            "Mismatch between the old and "
            + "the new value of the column %(col_name)s "
            + "was detected in the DNA table . "
        ) % {"col_name": col_name}

        fail_message += (
            " aggregate:"
            + err_aggregate_name
            + " outlier week:"
            + str(err_fiscal_week_end)
            + " err_aggregate_value_new:"
            + str(err_aggregate_value_new)
            + " err_aggregate_value_old:"
            + str(err_aggregate_value_old)
        )

        return fail_message


class TestOutlierWeeks(DQ_check):
    """
    This check tests for weeks where a value of one of the DNA columns is
    significantly larger than average
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

        Loads the seciton aggregate_expressions from the config file
        """
        super(TestOutlierWeeks, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )

        self.aggregate_expressions = config_validation.get(self.testname, {}).get(
            "aggregates", {}
        )

        if not (self.aggregate_expressions):
            raise ValueError(
                "the key aggregates is missing for tabletype = "
                + tabletype
                + " tablename = "
                + tablename
            )

    def test_body(self):
        """
        calculates aggregate values for the entire data and then looks at
        individual weeks that significantly deviate from this value
        """

        self.df.registerTempTable("df_TestOutlierWeeks")

        col_list = get_numeric_columns(self.df)

        result_list = []

        for col_name in col_list:
            logging.info("Processing column " + col_name)

            sql_statement = (
                " UNION ALL ".join(
                    [
                        (
                            """
                        SELECT
                            FISCAL_WEEK_END,
                            \'%(expr_name)s\' as aggregate_name,
                            %(agg_expr)s as aggregate_value
                        FROM
                            df_TestOutlierWeeks
                        GROUP BY
                            FISCAL_WEEK_END
                        """
                            % {
                                "expr_name": expr_name,
                                "agg_expr": self.aggregate_expressions[expr_name],
                            }
                        )
                        for expr_name in self.aggregate_expressions
                    ]
                )
            ).format(col_name=col_name)

            df_weekly_aggregates = self.spark.sql(sql_statement)
            df_weekly_aggregates.registerTempTable("df_weekly_aggregates")

            df_out = self.spark.sql(
                (
                    """
                        SELECT
                            df_w_a.FISCAL_WEEK_END,
                            overall_aggregates.aggregate_name,
                            overall_aggregates.overall_mean
                                as overall_aggregate_mean_value,
                            df_w_a.aggregate_value
                                as weekly_aggregate_value,
                                abs(overall_aggregates.overall_mean
                                - df_w_a.aggregate_value)
                                 / overall_aggregates.overall_std
                                 as relative_difference
                        FROM
                            df_weekly_aggregates as df_w_a
                        INNER JOIN
                            (
                            SELECT
                                mean(aggregate_value) as overall_mean,
                                std(aggregate_value) as overall_std,
                                aggregate_name
                            FROM
                                df_weekly_aggregates
                            GROUP BY
                                aggregate_name
                            ) as overall_aggregates
                        ON 1=1
                            AND df_w_a.aggregate_name
                                = overall_aggregates.aggregate_name
                            AND NOT(
                                overall_aggregates.overall_mean IS NULL
                                )
                            AND NOT(
                                overall_aggregates.overall_std IS NULL
                                )
                            AND NOT(
                                df_w_a.aggregate_value IS NULL
                                )
                            AND abs(
                                overall_aggregates.overall_mean
                                - df_w_a.aggregate_value)
                            / (
                                CASE WHEN
                                    overall_aggregates.overall_std
                                    < 0.001
                                THEN
                                    0.001
                                ELSE
                                    overall_aggregates.overall_std
                                END
                            ) > {tolerance}

                        """
                ).format(tolerance=self.tolerance)
            )

            error_count = df_out.count()

            if error_count > 0:

                error_msg = " \n ".join(
                    [
                        self.generate_error_msg(col_name, error_count, row)
                        for row in df_out.collect()
                    ]
                )

                self.fail_test(col_name, error_msg, error_count)
            else:
                self.pass_test(col_name)

    def generate_error_msg(self, col_name, bad_row_count, row):
        """
        This function generates the error message displayed when the test fails
        """

        err_FISCAL_WEEK_END = row[0]
        err_aggregate_name = row[1]
        err_overall_aggregate_mean_value = row[2]
        err_weekly_aggregate_value = row[3]
        err_relative_difference = row[4]

        fail_message = (
            "outlier value of column %(col_name)s " + "was detected in the DNA table . "
        ) % {"col_name": col_name}

        fail_message += "Number of outlayer rows detected: " + str(bad_row_count)

        fail_message += (
            " aggregate:"
            + err_aggregate_name
            + " outlier week:"
            + str(err_FISCAL_WEEK_END)
            + " err_overall_aggregate_mean_value:"
            + str(err_overall_aggregate_mean_value)
            + " err_weekly_aggregate_value:"
            + str(err_weekly_aggregate_value)
            + " err_relative_difference:"
            + str(err_relative_difference)
        )

        return fail_message


class TestCountsPerKey(DQ_check):
    """
    Checks that the number of fiscal weeks per MBRSHP_SID is always the same
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

        super(TestCountsPerKey, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )

        self.group_by_key = config_validation["group_by_key"]

    def test_body(self):
        """
        Caluclates the counts per key and looks for
        keys whose count in different than the average
        """

        self.df.registerTempTable("df_TestCountsPerKey")

        df_weekly_counts = self.spark.sql(
            (
                """
                        SELECT
                            COUNT(*) as cnt,
                            {group_by_key}
                        FROM
                            df_TestCountsPerKey
                        GROUP BY
                            {group_by_key}
                        """
            ).format(group_by_key=self.group_by_key)
        )

        df_weekly_counts.registerTempTable("df_counts_per_key")

        df_outliers = self.spark.sql(
            (
                """
                    SELECT
                        df_cps.cnt,
                        {group_by_key}
                    FROM
                        df_counts_per_key as df_cps
                    CROSS JOIN
                        (
                        SELECT
                            mean(cnt) as my_avg
                        FROM
                            df_counts_per_key
                        ) AS avg_tab
                    WHERE
                        abs(avg_tab.my_avg - df_cps.cnt) > {tolerance}
                    """
            ).format(group_by_key=self.group_by_key, tolerance=self.tolerance)
        )

        error_count = df_outliers.count()

        if error_count > 0:

            self.fail_test(
                "", self.generate_error_msg(error_count, df_outliers), error_count
            )
        else:
            self.pass_test()

    def generate_error_msg(self, bad_row_count, df_outliers):
        """
        Parses an error message in case the test fails
        """

        df_outliers.registerTempTable("df_outliers")

        df_outliers_max = (
            self.spark.sql(
                (
                    """
                select
                    df_o.{group_by_key},
                    df_o.cnt
                FROM
                    df_counts_per_key as df_o
                INNER JOIN
                    (
                    SELECT
                        MAX(cnt) as max_cnt
                    FROM
                        df_counts_per_key
                    ) as ext_tab
                ON
                    df_o.cnt = ext_tab.max_cnt
                """
                ).format(group_by_key=self.group_by_key)
            )
            .limit(2)
            .collect()
        )

        df_outliers_min = (
            self.spark.sql(
                (
                    """
                    select
                        df_o.{group_by_key},
                        df_o.cnt
                    FROM
                        df_counts_per_key as df_o
                    INNER JOIN
                        (
                        SELECT
                            MIN(cnt) as min_cnt
                        FROM
                            df_counts_per_key
                        ) as ext_tab
                    ON
                        df_o.cnt = ext_tab.min_cnt
                    """
                ).format(group_by_key=self.group_by_key)
            )
            .limit(2)
            .collect()
        )

        fail_message = (
            "Inconsistent number of users per week "
            + "was detected in the DNA table . "
        )

        fail_message += "Number of inconsistent users detected: " + str(bad_row_count)

        fail_message += (
            (" problematic {} 1: ").format(self.group_by_key)
            + str(df_outliers_min[0][0])
            + " cnt="
            + str(df_outliers_min[0][1])
            + (" problematic {} 2: ").format(self.group_by_key)
            + str(df_outliers_max[0][0])
            + " cnt="
            + str(df_outliers_max[0][1])
        )

        return fail_message


class TestCountsPerMbrshpSid(TestCountsPerKey):
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

        configure the parent class TestCountsPerKey to use MBRSHP_SID
        as group_by_key.
        """
        config_validation["group_by_key"] = "MBRSHP_SID"

        super(TestCountsPerMbrshpSid, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )


class TestCountsPerFiscalWeekEnd(TestCountsPerKey):
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

        configure the parent class TestCountsPerKey to
        use FISCAL_WEEK_END as group_by_key.
        """
        config_validation["group_by_key"] = "FISCAL_WEEK_END"

        super(TestCountsPerFiscalWeekEnd, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )
