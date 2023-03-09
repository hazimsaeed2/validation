from datetime import datetime as dt
import io
import logging
import subprocess
from urllib.parse import urlparse

import boto3
import pyspark.sql.functions as sqlf
import yaml


from pe_memberdna.lib.iotools import s3_copy, split_path_bucket_key


class DataQualityException(Exception):
    """
    This is to distinquish DQ exceptions form other exceptions that may
    be due to typos in the code etc.
    """

    pass


def _key_exists(path):
    """Verify key exists on s3"""
    try:
        out = subprocess.check_output(["aws", "s3", "ls", path])
        return len(out) > 0
    except subprocess.CalledProcessError:
        return False


def get_archive_path():
    archive = "ETL/stats/archive/stats_{}".format(
        dt.strftime(dt.today(), "%Y%m%d")
    )
    return archive


def archive_stats(s3_stat_path):
    """
    Save contents of s3_stat_path to the archive.
    """

    bucket, source = split_path_bucket_key(s3_stat_path)
    s3_copy(bucket, source, get_archive_path())


def get_s3_object(path):
    """
    Reads the path and returns data from S3

        Args:
            path: contains bucket name and key

        Returns:
            Body of the object containing actual data
    """
    s3_client = boto3.client("s3")
    parse_file = urlparse(path)
    parse_bucket = parse_file.netloc
    parse_path = parse_file.path
    response = s3_client.get_object(Bucket=parse_bucket, Key=parse_path[1:])
    return response["Body"]


class DQ_check(object):
    """
    A parent class for all DQ checks. Does not do anything except
    for taking in a list of arguments.
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
        Set up a DQ check.

        Args:
            spark (spark object): spark object
            tabletype (str): type of the passed df - source, intermediate,
                or dna
            tablename (str): name of the passed df - e.g. brand, item
            df {spark.sql.Dataframe}: table to be checked
            config_validation (dict): validation section from the config file
                associated with table to be tested
            s3_stat_path (str): location to store the DQ stats
            default_tolerance (float): tolerance to be used when not specified
                in the config
        """
        self.spark = spark
        self.df = df
        self.tabletype = tabletype
        self.tablename = tablename
        self.s3_stat_path = s3_stat_path
        self.testname = self.__class__.__name__
        self.DQ_error_buffer = []
        self.throw_errors = throw_errors

        if self.throw_errors:
            self.tolerance = config_validation.get(self.testname, {}).get(
                "tolerance", 0.05
            )
        else:
            self.tolerance = 0.0

    def run_test(self):
        """
        Run the test, unless it has been turned off via setting
        tolerance='OFF'.

        This allows tests to be independently turned off.
        """
        logging.info("Running DQ test " + self.testname + "...")

        if self.tolerance == "OFF":
            logging.info("--> The test is OFF")
        else:
            self.test_body()

        logging.info("--> OK")

        return self.DQ_error_buffer

    def test_body(self):
        """
        Handle problems with the method not being present
        """
        raise Exception("run_test not defined")

    def generate_error_msg(test_body, *args):
        """
        Handle problems with the method not being present
        """
        raise Exception("generate_error_msg not defined")

    def get_old_stats(self):
        """
        Load the stat_data structure from a yaml file.

        This is neccessary in order to the DQ check classes to be able to
        remember different information from the previous run of checks
        such as the list of columns a specific table is required to
        have.

        Args:

        Returns:
            (dict): previous DQ test results
        """
        if _key_exists(self.s3_stat_path):
            s3_response = get_s3_object(self.s3_stat_path)
            return yaml.load(s3_response, Loader=yaml.Loader)
        return {}

    def get_stat(self, tabletype="", tablename="", testname=""):
        """
        Extract a value from the stat_data dictionary structure.
        Returns [] if the key does not exist in the structure

        Args:
            tabletype (str): 'source', 'intermediate', etc.
            tablename (str): can be 'club','member', etc.
            testname (str):

        Returns:
         (list): results from previous run of DQ check, empty if not present
        """
        stats_data = self.get_old_stats()

        if len(tabletype) > 0:
            return (
                stats_data.get(tabletype, {})
                .get(tablename, {})
                .get(testname, {})
            )

        else:
            return (
                stats_data.get(self.tabletype, {})
                .get(self.tablename, {})
                .get(self.testname, {})
            )

    def add_stat(self, val):
        """
        Add a new value into stat_data. If the neccessary keys don't exist in
        the structure they are created.

        Args:
            val: value to be added to the results

        Returns:
            Modifies self.s3_stat_path contents
        """
        stat_data = self.get_old_stats()

        stat_data.setdefault(self.tabletype, {}).setdefault(
            self.tablename, {}
        )[self.testname] = val

        self.update_old_stats(stat_data)

    def update_old_stats(self, stat_data, encryption="aws:kms"):
        """
        Rewrite the file storing the DQ check statistics

        Args:
            stat_data (dict): updated stats to write to s3

        Returns:
            Modifies self.s3_stat_path contents
        """

        parsed = urlparse(self.s3_stat_path)
        bucket = parsed.netloc
        key = parsed.path[1:]

        yaml_buffer = io.BytesIO()

        yaml.dump(
            stat_data, yaml_buffer, encoding="UTF-8", default_flow_style=False
        )

        s3_resource = boto3.resource("s3")
        if encryption is None:
            s3_resource.Object(bucket, key).put(Body=yaml_buffer.getvalue())
        else:
            s3_resource.Object(bucket, key).put(
                Body=yaml_buffer.getvalue(), ServerSideEncryption=encryption
            )

    def append_error_buffer(self, subtest_name, cnt=0):
        """
        Appends a test result to self.DQ_error_buffer

        Args:
            subtest_name: name of the subtest - can be a column name
                for example
            cnt: number of errors
        """
        self.DQ_error_buffer.append(
            {
                "tabletype": self.tabletype,
                "testname": self.testname,
                "tablename": self.tablename,
                "subtest_name": subtest_name,
                "error_count": cnt,
            }
        )

    def pass_test(self, subtest_name="", cnt=0):
        """
        Add a record to a DQ error buffer if throw erros is disabled.
        This funciton allows for DQ errors to be either just
        recorded into DQ_error_buffer or raised as
        a Data_Quality_exception.

        Args:
            subtest_name: name of the subtest - can be a column name
                for example
            cnt: number of errors
        """
        if not self.throw_errors:
            self.append_error_buffer(subtest_name, cnt)

    def fail_test(self, subtest_name, msg, cnt=1):
        """
        Similarily to pass_test this either throws an error or reconrds
        the error into DQ_error_buffer.

        Args:
            subtest_name: name of the subtest - can be a column name
                for example
            msg: Text describing the error
            cnt: number of errors
        """
        if self.throw_errors:
            raise DataQualityException(
                "DQ check " + self.testname + " failed: \n" + msg
            )

        logging.info("DQ check " + self.testname + " failed: \n" + msg)

        self.append_error_buffer(subtest_name, cnt)


class TestColNames(DQ_check):
    """
    Compare the current list of column names against the previous list.

    Throw an error if the current list does not match the previous list.

    The class uses a yaml file in order to store the lists of columns
    for every table.
    """

    def test_body(self):
        """
        Look for match between the list of columns in table under test and the
        last saved list of columns for given table.
        """
        cols_present = self.df.columns
        cols_required = self.get_stat()

        if cols_required:
            missing_cols = list(set(cols_present) ^ set(cols_required))
            logging.info("-->" + str(len(missing_cols)) + " missing columns")

            if len(missing_cols) > 0:
                self.fail_test(
                    "",
                    self.generate_error_msg(
                        missing_cols, cols_required, cols_present
                    ),
                    len(missing_cols),
                )
            else:
                self.pass_test()
        else:
            logging.info("--> Nothing to check.")

        self.add_stat(cols_present)

    def generate_error_msg(self, missing_cols, cols_required, cols_present):
        """
        Parses the text to be displayed when a DQ issue is detected
        """
        fail_message = """
            Column missmatch in %(tabletype)s table %(tablename)s . \n
            Missing columns %(missing_cols)s. \n
            The previous import had columns %(cols_required)s \n
            the current import has columns %(cols_present)s \n
            """ % {
            "tabletype": self.tabletype,
            "tablename": self.tablename,
            "missing_cols": str(missing_cols),
            "cols_required": str(cols_required),
            "cols_present": str(cols_present),
        }

        return fail_message


class TestDuplicates(DQ_check):
    """
    Test for duplicate prime key.
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

        Extract the list under key_columns and TestDuplicates.tolerance from
        config_validations.

        Use 'key_column_1','key_column_1','key_column_3' to define
        multiple keys
        and  'key_column_1, key_column_1, key_column_3' to define a
        compound key.
        """

        super(TestDuplicates, self).__init__(
            spark,
            tabletype,
            tablename,
            df,
            config_validation,
            s3_stat_path,
            throw_errors=throw_errors,
        )

        self.unique_keys = config_validation["key_columns"]

    def test_body(self):
        """
        Execute an SQL statement grouping the rows by the prime key.

        Check for prime key values associated with more than one row.
        """
        self.df.registerTempTable("df_duplicate_check")
        total_row_count = self.df.count()

        if total_row_count > 0:
            if self.unique_keys is None:
                logging.info("--> Nothing to check. The table has zero rows. ")
            else:
                for unique_key in self.unique_keys:
                    check_sql = """
                        SELECT
                             %(col_name)s,
                             COUNT(*)
                        FROM df_duplicate_check
                        GROUP BY %(col_name)s
                        HAVING COUNT(*) > 1
                        """ % {
                        "col_name": unique_key
                    }
                    duplicates_df = self.spark.sql(check_sql)
                    duplicates_cnt = duplicates_df.count()

                    logging.info(
                        "--> Found "
                        + str(duplicates_cnt)
                        + " duplicates with respect to "
                        + unique_key
                    )

                    dup_fraction = float(duplicates_cnt) / float(
                        total_row_count
                    )

                    if dup_fraction > self.tolerance:
                        self.fail_test(
                            "",
                            self.generate_error_msg(unique_key, dup_fraction),
                            dup_fraction,
                        )
                    else:
                        self.pass_test()

    def generate_error_msg(self, unique_key, duplicate_fraction):
        """
        Parses the test to be displayed when a DQ issue is detected
        """
        fail_message = (
            "Duplicates in the column(s) %(col_name)s "
            "in the %(tabletype)s table %(tablename)s. \n"
        ) % {
            "col_name": unique_key,
            "tablename": self.tablename,
            "tabletype": self.tabletype,
        }

        fail_message += (
            "Percentage of duplicates detected: "
            + str(duplicate_fraction)
            + " \n"
        )

        return fail_message


def validate_table(
    spark,
    tabletype,
    tablename,
    config_validation,
    df,
    check_list=[],
    archive=False,
    throw_errors=True,
):
    """
    Run a list of data quality checks for a given table.

    Args:
        spark (spark object)
        tabletype (str): type of the table - source, intermediate, or dna
        tablename (str): name of the table under test - e.g. club, member
        config_validation (dict) - configuration of the tests
        df (spark.sql.dataframe) - table under test
        check_list (list): class names of different tests that are to be run
        archive (bool): whether to archive stats to s3
        '...ETL/archive/stats_{dt}'
    """

    logging.info("Checking " + tabletype + " table " + tablename + "...")

    if len(check_list) == 0:
        if tabletype == "source":
            import pe_memberdna.etl.lib.validations_ETL as validations

            check_list = [
                validations.CompareColAggregatesPrior,
                TestColNames,
                TestDuplicates,
            ]

        if tabletype == "intermediate":
            import pe_memberdna.etl.lib.validations_ETL as validations

            check_list = [
                validations.CompareColAggregatesBoth,
                TestColNames,
                TestDuplicates,
            ]

    params = config_validation.get("table_params")
    if params is not None:
        err_report = sum(
            [
                check_class(
                    spark,
                    tabletype,
                    tablename,
                    df,
                    params[tabletype][tablename],
                    config_validation["s3_stat_path"],
                    throw_errors=throw_errors,
                ).run_test()
                for check_class in check_list
            ],
            [],
        )
        archive_stats(config_validation["s3_stat_path"])
        return err_report
    else:
        logging.info("Skipping validation")
        return []
