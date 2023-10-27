import argparse
import copy
import inspect
import os
import re
import time
import unittest
import xmlrunner

from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta

from pyspark.sql.functions import col, concat
from pyspark.sql.functions import hash as fhash
from pyspark.sql.functions import lit, lpad
from pyspark.sql.functions import max as fmax
from pyspark.sql.functions import rpad, when
from pyspark.sql.types import IntegerType, StructField, StructType

from pe_memberdna.pipelines.assignment.lib.assn_io import JobManager
from pe_memberdna.pipelines.assignment.lib.assn_utils import (
    deterministic_sample,
    rdd_rank_by_col,
)
from pe_memberdna.pipelines.assignment.lib.schemas.campaign_schemas import (
    INPUT_MAIL_FILE_SCHEMA,
)
from pe_memberdna.pipelines.assignment.lib.schemas.coupon_schemas import (
    ARTICLE_COUPON_SCHEMA,
    BASKET_COUPON_SCHEMA,
    CAT_COUPON_SCHEMA,
    VERSION_MAP_SCHEMA,
)
from pe_memberdna.pipelines.lib.iotools import s3_delete, split_path_bucket_key
from pe_memberdna.pipelines.lib.spark_util import get_logger


class RegressionTest(unittest.TestCase, JobManager, metaclass=ABCMeta):
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.test_name = type(self).__name__

        current_file_dir = os.path.dirname(inspect.getfile(self.__class__))

        self.config_file_path = "{}/conf/{}_config.yml".format(
            current_file_dir, self.test_name
        )

        JobManager.__init__(
            self, "regression_test", conf_path_in=self.config_file_path
        )

        test_filename = os.path.basename(
            inspect.getfile(self.__class__)
        ).replace(".py", "")

        self.test_name = test_filename

        self.additional_paths_to_clean = None

    def wasSuccessful(self):
        """Tells whether or not this test was a success.
        Parameters:
            None
        Returns:
             (boolean): whether the test was a success or not
        """
        errors = []
        for test_method, exception in self._outcome.errors:
            if exception is not None:
                errors.append((test_method, exception))

        return len(errors) == 0 or self._outcome.expecting_failure == True

    def tearDown(self):
        """Clean the output directories for assignment.

        The directories for create_coupons.py are done granulary, because we
        have to make create_closures.py part of the regression tests. Currently
        the output for it is hardcoded. We cannot clean it up.

        Parameters:
            None
        Returns:
             None
        """
        self.print_results()

        if not self.wasSuccessful():
            return

        allowed_to_delete = [
            "REGRESSION_TESTS/assignment/{}".format(self.test_name)
        ]

        paths_to_clean = [
            "OUTPUT_DIR",
            "COUPON_BANK",
            "COUPON_QUALS",
            "COUPON_MAP",
            "COUPON_MEMTRIP",
        ]

        if self.additional_paths_to_clean:
            paths_to_clean.extend(self.additional_paths_to_clean)

        for path in paths_to_clean:
            self.log.info("Deleting {}".format(self.config.paths[path]))
            bucket, dest_key = split_path_bucket_key(self.config.paths[path])
            s3_delete(bucket, dest_key, allowed_paths=allowed_to_delete)

    @abstractmethod
    def test_regression(self):
        """
        This is a placeholder to be implemented by the implementing class

        :return:
        """
        pass

    def execute_assignment_scripts(
        self,
        _scripts=[
            "create_coupons.py",
            "assign_offers.py",
            "subsample_population.py",
            "backtest_sizing.py",
            "generate_output_file.py",
            "qc_assignments.py",
        ],
    ):
        scripts_run = copy.deepcopy(_scripts)
        self.log.info("Running the following scripts: {}".format(scripts_run))

        for script in scripts_run:

            outcome = [script, 1]
            self.outcomes.append(outcome)

            if script == "create_coupons.py":
                # This import was added here instead of at the top to make
                # sure that the syntax and errors are correctly caught
                # and recorded by the TestCase class
                # see PR #267 for context
                import pe_memberdna.pipelines.assignment.scripts.create_coupons as create_coupons

                create_coupons.main(self.config_file_path)
            elif script == "assign_offers.py":
                # see comment for case create_coupons.py
                import pe_memberdna.pipelines.assignment.scripts.assign_offers as assign_offers

                assign_offers.main(self.config_file_path)
            elif script == "subsample_population.py":
                # see comment for case create_coupons.py
                import pe_memberdna.pipelines.assignment.scripts.subsample_population as subsample_population

                subsample_population.main(self.config_file_path)
            elif script == "backtest_sizing.py":
                # see comment for case create_coupons.py
                import pe_memberdna.pipelines.assignment.scripts.backtest_sizing as backtest_sizing

                backtest_sizing.main(self.config_file_path)
            elif script == "qc_assignments.py":
                # see comment for case create_coupons.py
                import pe_memberdna.pipelines.assignment.scripts.qc_assignments as qc_assignments

                qc_assignments.main(self.config_file_path)
            elif script == "generate_output_file.py":
                # see comment for case create_coupons.py
                import pe_memberdna.pipelines.assignment.scripts.generate_output_file as generate_output_file

                generate_output_file.main(self.config_file_path)
            elif script == "generate_input_mail.py":
                import pe_memberdna.pipelines.assignment.scripts.generate_input_mail as generate_input_mail

                generate_input_mail.main(self.config_file_path)
            elif script.startswith("assign_closure.py"):
                # see comment for case create_coupons.py
                import pe_memberdna.pipelines.assignment.scripts.assign_closure as assign_closure

                cells, iterate, seed, ahcd_dedup = script.split(" ")[1:]
                cells = int(cells)
                iterate = False if iterate == "False" else True
                seed = seed if seed != "None" else None
                ahcd_dedup = False if ahcd_dedup == "False" else True

                assign_closure.main(self, cells, iterate, seed, ahcd_dedup)
            elif script == "generate_msmt_input.py":
                # see comment for case create_coupons.py
                import pe_memberdna.pipelines.assignment.scripts.generate_msmt_input as generate_msmt_input

                generate_msmt_input.main(self.config_file_path)
            else:
                raise Exception('Unknown script file "{}".'.format(script))

            self.outcomes[-1][1] = 0

            self.log.info(outcome)

    def print_results(self):
        """
        This function prints the outcomes of the regression test in a format
        that looks something like this
            TEST_OUTCOME - DummyCoupon - 0 - DummyCoupon
            TEST_OUTCOME - DummyCoupon - 1 - ['create_coupons.py', 0]
            TEST_OUTCOME - DummyCoupon - 2 - ['assign_offers.py', 0]
            TEST_OUTCOME - DummyCoupon - 3 - ['subsample_population.py', 0]
            TEST_OUTCOME - DummyCoupon - 4 - ['backtest_sizing.py', 0]
            TEST_OUTCOME - DummyCoupon - 5 - ['qc_assignments.py', 0]
            TEST_OUTCOME - DummyCoupon - 6 - ['generate_output_file.py', 0]
            TEST_OUTCOME - DummyCoupon - 7 - ['Test', 0]

        This function accesses the private member variable _feedErrorsToResult
        from unittest.TestCase in order to obtain the information on whether
        the test passed or failed. The information about the successfull
        execution of the assignemnt scripts is obtained from self.outcomes

        :return:
        """

        if len(self.outcomes) == 1:
            self.outcomes.append(["PlaceHolder", 0])

        result = self.defaultTestResult()
        self._feedErrorsToResult(result, self._outcome.errors)

        error = len(result.errors) > 0
        failure = len(result.failures) > 0
        test_ok = not error and not failure

        outcome = ["Test", int(not (test_ok))]
        self.outcomes.append(outcome)
        self.log.info(outcome)

        i = 0
        for outcome in self.outcomes:
            self.log.info(
                "TEST_OUTCOME - {} - {} - {}".format(
                    self.test_name, i, outcome
                )
            )
            i = i + 1

    def run_test_scripts_and_prepare_data(self):

        self.template = ""

        self.log = get_logger(self.test_name)

        self.test_dir = "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/assignment/{}".format(
            self.test_name
        )
        self.outcomes = [self.test_name]
        data_exists = self._key_exists(self.test_dir)
        self.create_test_config()
        self.hash_seed = hash(
            str(self.config.params["experiment"])
            + str(self.config.params["creation"]["sample_members"])
        )

        if data_exists:
            self.log.info("Test data already exists, running test.")
            self.execute_assignment_scripts()
            return

        self.log.info("Creating data and cdsa template.")

        self.raw = [
            DataMeta("transactions", "TRANSACTIONS_PATH", filetype="parquet"),
            DataMeta("article_dna", "ARTICLE_DNA_PATH", filetype="parquet"),
            DataMeta("ah5_dna", "AH5_DNA_PATH", filetype="parquet"),
            DataMeta("ah4_dna", "AH4_DNA_PATH", filetype="parquet"),
            DataMeta(
                "article_category_map",
                "ARTICLE_AH4_AH5_MAP",
                filetype="parquet",
            ),
            DataMeta("raw_member", "RAW_MEMBER", filetype="parquet"),
            DataMeta("cube", "CUBE", filetype="parquet"),
            DataMeta("item_master", "ITEM_MASTER", filetype="parquet"),
            DataMeta("trip_propensity", "PROPENSITY", filetype="csv"),
            DataMeta("cf", "PRED_LIST", filetype="parquet"),
        ]

        self.rest = [
            DataMeta("msmt_assignments", None, "CDSA_ASSGN", "parquet"),
            DataMeta("mail_list", None, "MAIL_LIST", "csv"),
            DataMeta("mail_list", "MAIL_LIST", "FHH", "csv"),
            DataMeta("backtest_mail_list", None, "BACKTEST_MAIL_LIST", "csv"),
            DataMeta("version_map", None, "VERSION_MAP", "csv"),
        ]

        for meta in self.raw:
            self.log.info("Loading {}".format(meta.path_read_key))
            self.data.read(
                meta.name, meta.path_read_key, filetype=meta.filetype
            )

        # Filter to one fiscal week for efficiency

        date = datetime.now() - timedelta(days=365)
        # No, Leap years don't matter because they don't exist
        year_ago = "{}-{}-{}".format(date.year, date.month, date.day)
        self.data.tables["transactions"] = self.data.tables[
            "transactions"
        ].filter("FISCAL_WEEK_END > '{}'".format(year_ago))

        self.create_constructs_segments()
        self.create_core_csv()
        self.gen_data()
        self.data.tables["cpn_article"].show()
        self._write_as_file(
            self.data.tables["cpn_article"],
            self.config.test_paths["CPG_COUPON_LIST_PATH"],
            "article.csv",
        )
        self._write_as_file(
            self.data.tables["cpn_category"],
            self.config.test_paths["CATEGORY_COUPON_PATH"],
            "category.csv",
        )
        self._write_as_file(
            self.data.tables["cpn_basket"],
            self.config.test_paths["BASKET_COUPON_PATH"],
            "basket.csv",
        )

        for meta in self.raw + self.rest:
            self.log.info("Writing {}...".format(meta.path_write_key))
            self.data.tables[meta.name].repartition(
                self.config.params["creation"]["output_partitions"]
            ).write.mode("overwrite").format(meta.filetype).option(
                "header", "true"
            ).save(
                self.config.test_paths[meta.path_write_key]
            )

        self.log.info("Done instantiating RegressionTest base class.")

    def create_constructs_segments(self):
        """
        Creates the place holders for constructs/segments

        :return:
        """
        segment_path = self.config.test_paths["SEGMENT_BANK"] + "{}.json"
        construct_path = self.config.test_paths["CONSTRUCT_BANK"] + "{}.json"

        num_segments = self.config.params["creation"]["csv_rows"][
            "segments.csv"
        ]
        num_constructs = self.config.params["creation"]["csv_rows"][
            "constructs.csv"
        ]

        self.log.info("Creating {} segments.".format(num_segments))
        self.log.info("Creating {} constructs.".format(num_constructs))

        for i in range(0, num_segments):
            self.touch_s3_and_log(segment_path.format(i))

        for i in range(0, num_constructs):
            self.touch_s3_and_log(construct_path.format(i))

    def create_core_csv(self):
        """
        Create the csvs that are needed for cdsa

        :return:
        """
        self._read_write_csv("CAMPAIGN", "campaign.csv")
        self._read_write_csv("CONSTRUCT", "constructs.csv")
        self._read_write_csv("CELL", "cells.csv")
        self._read_write_csv("SEGMENT", "segments.csv")
        self._read_write_csv("HANDSHAKES", "handshakes.csv")
        # self._write_csv('MSMT_CELL', 'msmt_cells.csv')
        # 'MSMT_CELL_ARCHIVE'

    def _read_write_csv(self, path_key, csv):
        """
        Reads in a folder (path_key) with a name something.csv and puts the first part

        :param path_key: the path to write to
        :param csv: the name of the csv file you want to write
        :return:
        """
        paths = self.config.paths
        test_paths = self.config.test_paths
        csv_rows = self.config.params["creation"]["csv_rows"]
        self.log.info("Writing {}".format(csv))
        df = (
            self.spark.read.option("header", "true")
            .csv(paths[path_key])
            .limit(csv_rows[csv])
        )
        self._write_as_file(df, test_paths[path_key], csv)

    def _write_as_file(self, df, path, filename, options={"header": "true"}):
        """

        :param df:
        :param path:
        :param filename:
        :return:
        """
        writer = df.repartition(1).write
        for k in options:
            writer = writer.option(k, options[k])
        writer.mode("overwrite").csv(path)

        self.move_folder_to_file(path, filename)

    def _key_exists(self, path):
        """
        Function to verify key exists on s3

        :param path:
        :return:
        """
        ls_command = "aws s3 ls {}/".format(path)
        self.log.info("ls command: {}".format(ls_command))
        output = os.popen(ls_command).readlines()
        return len(output) > 0

    def move_folder_to_file(self, path, csv):
        """
        Moves a single part file into a file named csv

        :param path:
        :param csv:
        :return:
        """
        ls_command = "aws s3 ls {}/".format(path)
        self.log.info("ls command: {}".format(ls_command))

        secs = 5
        self.log.info("Waiting {} seconds for s3.".format(secs))
        time.sleep(secs)

        output = os.popen(ls_command).readlines()
        part = [x for x in output if "part" in x][0]
        cleaned_part = re.sub(".*(?=part)", "", part).replace("\n", "")
        part_path = "{}/{}".format(path, cleaned_part)

        self.log.info("Moving {} to a single file.".format(csv))
        move_command = 'aws s3 mv {} {} --sse "aws:kms"'.format(
            part_path, path
        )

        self.log.info("Moving {} to {}".format(csv, move_command))

        os.popen(move_command).readlines()
        os.popen("aws s3 rm --recursive {}".format(path)).readlines()

    @staticmethod
    def touch_s3(path):
        """

        :return:
        """
        os.popen('echo "" | aws s3 cp - {} --sse "aws:kms"'.format(path))

    def touch_s3_and_log(self, path):
        """
        Create empty file at specified s3 path

        :param path:
        :return:
        """
        self.log.info("Touching {}".format(path))
        RegressionTest.touch_s3(path)

    def create_test_config(self):
        """
            Using the existing paths, modify for tests and fill in missing paths
        :return:
        """
        self.config.test_paths = copy.deepcopy(self.config.paths)

        with open(self.config.cfg_path) as f:
            self.template = f.read()

        input_paths = self.test_dir + "/ASSIGNMENTS/campaigns/campaign/{}"
        for k in self.config.test_paths:
            if self.test_dir not in self.config.test_paths[k]:
                self.config.test_paths[k] = self.config.test_paths[k].replace(
                    "s3://memberanalytics-data-out-prod", self.test_dir
                )

        self.config.test_paths["MAIL_LIST"] = input_paths.format(
            "input_mail_list"
        )
        self.config.test_paths["FHH"] = self.config.test_paths["MAIL_LIST"]
        self.config.test_paths["CATEGORY_COUPON_PATH"] = input_paths.format(
            "input_coupon_list/category.csv"
        )
        self.config.test_paths["BASKET_COUPON_PATH"] = input_paths.format(
            "input_coupon_list/basket.csv"
        )
        self.config.test_paths["CPG_COUPON_LIST_PATH"] = input_paths.format(
            "input_coupon_list/article.csv"
        )
        self.config.test_paths["BACKTEST_MAIL_LIST"] = self.config.test_paths[
            "MAIL_LIST"
        ]
        self.config.test_paths["VERSION_MAP"] = input_paths.format(
            "input_version_map"
        )

        self.config.test_paths["INPUT_ASSIGNMENTS"] = self.config.test_paths[
            "ASSIGNMENT_PATH"
        ]
        self.config.test_paths["INPUT_CONSTRUCTS"] = self.config.test_paths[
            "CONSTRUCTS_PATH"
        ]
        self.config.test_paths[
            "MAIL_POPULATION_ASSIGNMENT"
        ] = self.config.test_paths["SUBSET"]

        for k in self.config.test_paths:
            self.template = re.sub(
                "{}:.*".format(k),
                "{}: '{}'".format(k, self.config.test_paths[k]),
                self.template,
            )

        filename = "{}_config.yml".format(self.test_name)

        self._write_as_file(
            self.spark.sparkContext.parallelize(
                [[x] for x in self.template.split("\n")], 1
            )
            .toDF()
            .filter(col("_1") != ""),
            "{}/{}".format(self.test_dir, filename),
            filename,
            {
                "header": "false",
                "ignoreLeadingWhiteSpace": "false",
                "escapeQuotes": "false",
                "sep": "|",
            },
        )
        print(self.template)

    def gen_data(self):
        """
        Creates the data for the regression test and saves to the job

        :return: modifies the state of the job instead of returning
        """
        sample_members = self.config.params["creation"]["sample_members"]
        if isinstance(sample_members, int):
            self.log.info(
                "Creating a sample of size {} members for testing.".format(
                    sample_members
                )
            )
            member_list = self._gen_member_sample(sample_members)
        elif isinstance(sample_members, list):
            for_rdd = [[x] for x in sample_members]

            self.log.info(
                "Using provided member list of size {} as sample for testing.".format(
                    len(sample_members)
                )
            )
            member_list = self.sc.parallelize(for_rdd).toDF(["mbrshp_sid"])
        else:
            self.log.error(
                "Sample members but either be an integer or a list."
            )
            return
        self.data.tables["member_list"] = member_list

        self._filter_to_column("transactions", "member_list", ["mbrshp_sid"])

        self._filter_to_column("article_dna", "transactions", ["article_nbr"])
        self._filter_to_column("item_master", "transactions", ["article_nbr"])
        self._filter_to_column(
            "article_category_map", "transactions", ["article_nbr"]
        )

        self.gen_cpg_coupon_list(
            self.config.params["creation"]["article_coupons"]["num_coupons"]
        )
        self.gen_category_coupon_list(
            self.config.params["creation"]["category_coupons"]["num_coupons"]
        )
        self.gen_basket_coupon(
            self.config.params["creation"]["basket_coupons"]["num_coupons"]
        )
        self.gen_version_map()

        self._filter_to_column("ah5_dna", "article_category_map", ["ah5_cd"])
        self._filter_to_column("ah4_dna", "article_category_map", ["ah4_cd"])
        self._filter_to_column("raw_member", "member_list", ["mbrshp_sid"])
        self._filter_to_column("cube", "member_list", ["mbrshp_sid"])

        self._filter_to_column(
            "trip_propensity", "member_list", ["mbrshp_sid"]
        )
        self._filter_to_column("cf", "member_list", ["mbrshp_sid"])

        self.generate_msmt_input_assignments()
        self.gen_mail_list()
        self.data.tables["backtest_mail_list"] = self.data.tables["mail_list"]

    # Coupon Inputs
    def gen_cpg_coupon_list(self, num_coupons=3):
        """

        Up to 10000 coupons
        All article coupon numbers end with a '1'

        :param num_coupons:
        :return: article coupons added to job
        """
        self.log.info("Generating {} article coupons.".format(num_coupons))
        num_coupons = self._guard_too_many_coupons(num_coupons)
        art_cat_lkup = self.data.tables["article_category_map"]
        articles = (
            art_cat_lkup.select("article_nbr")
            .distinct()
            .orderBy(fhash(col("article_nbr")))
            .cache()
        )
        articles = rdd_rank_by_col(
            articles, "article_nbr", "article_nbr", "row_num", "article_nbr"
        )
        if num_coupons > articles.count():
            self.log.warn(
                "Not enough articles found for the specified number of coupons {}.  Creating {} coupons instead".format(
                    num_coupons, articles.count()
                )
            )
            num_coupons = articles.count()

        promo_types = ["Paper Coupon", "Clipless Coupon"]
        quantity_thresholds = [1, 2]
        discount_values = [5.0, 2.0, 1.0]
        eligibility_for_mmpc = [0, 1, 1]
        self_funded_flags = [0, 0, 1]

        cpn_list = (
            articles.limit(num_coupons)
            .withColumn("Promo #", col("article_nbr"))
            .withColumn(
                "PMR Offer ID",
                concat(lit("10"), lpad(col("row_num"), 4, "0"), lit("1")),
            )
            .withColumn(
                "Promo Description",
                concat(lit("Article Coupon "), col("article_nbr")),
            )
            .withColumn(
                "Promo Type",
                self.create_modulo_when_statement("row_num", promo_types),
            )
            .withColumn("Valid From", lit("3/14/2010"))
            .withColumn("Valid To", lit("3/14/2028"))
            .withColumn("Article Number", col("article_nbr"))
            .withColumn(
                "Quantity Threshold",
                self.create_modulo_when_statement(
                    "row_num", quantity_thresholds
                ),
            )
            .withColumn(
                "Discount Value",
                self.create_modulo_when_statement("row_num", discount_values),
            )
            .withColumn(
                "Eligible for MMPC",
                self.create_modulo_when_statement(
                    "row_num", eligibility_for_mmpc
                ),
            )
            .withColumn(
                "self_funded_flag",
                self.create_modulo_when_statement(
                    "row_num", self_funded_flags
                ),
            )
        )

        self.data.tables["cpn_article"] = self.apply_schema_hard(
            cpn_list, ARTICLE_COUPON_SCHEMA
        )

    def gen_category_coupon_list(self, num_coupons=1):
        """

        Up to 10000 coupons
        All category coupon numbers end with a '2'

        :param num_coupons:
        :return: category coupons added to job
        """
        self.log.info("Generating {} category coupons.".format(num_coupons))
        num_coupons = self._guard_too_many_coupons(num_coupons)
        art_cat_lkup = self.data.tables["article_category_map"]
        ah4 = (
            art_cat_lkup.select("ah4_cd")
            .distinct()
            .orderBy(fhash(col("ah4_cd")))
            .withColumnRenamed("ah4_cd", "cpn_ah4_cd")
            .cache()
        )
        ah4 = rdd_rank_by_col(
            ah4, "cpn_ah4_cd", "cpn_ah4_cd", "row_num", "cpn_ah4_cd"
        )

        ah5 = (
            art_cat_lkup.select("ah5_cd")
            .distinct()
            .orderBy(fhash(col("ah5_cd")))
            .withColumnRenamed("ah5_cd", "cpn_ah5_cd")
            .cache()
        )
        ah5 = rdd_rank_by_col(
            ah5, "cpn_ah5_cd", "cpn_ah5_cd", "row_num", "cpn_ah5_cd"
        )

        if num_coupons > (ah4.count() + ah5.count()):
            self.log.warn(
                "Not enough categories found for the specified number of coupons {}.  Creating {} coupons instead".format(
                    num_coupons, (ah4.count() + ah5.count())
                )
            )
            num_coupons = ah4.count() + ah5.count()

        cpn_dollar_off = [1, 2.5]
        cpn_dollar_threshold = [2, 5]
        self_funded_flag = [1, 0, 1, 1]

        ah4_cpn_list = (
            ah4.withColumn(
                "cpn_nbr",
                concat(lit("1"), lpad(col("row_num"), 4, "0"), lit("2")),
            )
            .withColumn("offer_id", lit(1000))
            .withColumn("cpn_class_id", rpad(col("row_num"), 4, "0"))
            .withColumn("cpn_ah5_cd", lit(""))
            .withColumn(
                "cpn_desc", concat(lit("Category coupon "), col("cpn_nbr"))
            )
            .withColumn(
                "cpn_dollar_off",
                self.create_modulo_when_statement("row_num", cpn_dollar_off),
            )
            .withColumn(
                "cpn_dollar_threshold",
                self.create_modulo_when_statement(
                    "row_num", cpn_dollar_threshold
                ),
            )
            .withColumn(
                "self_funded_flag",
                self.create_modulo_when_statement("row_num", self_funded_flag),
            )
            .withColumn("cpn_start", lit("3/14/2010"))
            .withColumn("cpn_end", lit("3/14/2028"))
        )

        ah5_cpn_list = (
            ah5.withColumn(
                "cpn_nbr",
                concat(
                    lit("1"),
                    lpad((ah4.count() + col("row_num")), 4, "0"),
                    lit("2"),
                ),
            )
            .withColumn("offer_id", lit(1000))
            .withColumn(
                "cpn_class_id", concat(lit("1"), rpad(col("row_num"), 3, "0"))
            )
            .withColumn("cpn_ah4_cd", lit(""))
            .withColumn(
                "cpn_desc", concat(lit("Category coupon "), col("cpn_nbr"))
            )
            .withColumn(
                "cpn_dollar_off",
                self.create_modulo_when_statement("row_num", cpn_dollar_off),
            )
            .withColumn(
                "cpn_dollar_threshold",
                self.create_modulo_when_statement(
                    "row_num", cpn_dollar_threshold
                ),
            )
            .withColumn(
                "self_funded_flag",
                self.create_modulo_when_statement("row_num", self_funded_flag),
            )
            .withColumn("cpn_start", lit("3/14/2010"))
            .withColumn("cpn_end", lit("3/14/2028"))
        )

        # cpn_nbr's should be unique
        cpn_list = (
            self.apply_schema_hard(ah4_cpn_list, CAT_COUPON_SCHEMA)
            .union(self.apply_schema_hard(ah5_cpn_list, CAT_COUPON_SCHEMA))
            .orderBy(fhash(col("cpn_nbr")))
            .limit(num_coupons)
        )

        self.data.tables["cpn_category"] = cpn_list

    def gen_basket_coupon(self, num_coupons=2):
        """

        Up to 10000 coupons
        All basket coupon numbers end with a '3'

        :param num_coupons:
        :return: basket coupons added to job
        """
        self.log.info("Generating {} basket coupons".format(num_coupons))
        num_coupons = self._guard_too_many_coupons(num_coupons)
        basket_count = self.sc.parallelize(
            [[x] for x in range(0, num_coupons)]
        ).toDF(StructType([StructField("row_num", IntegerType(), True)]))

        cpn_dollar_off = [10, 15, 20]
        cpn_dollar_threshold = [100, 150]
        self_funded_flag = [1, 0, 1, 1]

        basket_cpn_list = (
            basket_count.withColumn(
                "cpn_nbr",
                concat(lit("1"), lpad(col("row_num"), 4, "0"), lit("3")),
            )
            .withColumn("offer_id", lit(1000))
            .withColumn("cpn_class_id", rpad(col("row_num") + lit(1), 4, "0"))
            .withColumn(
                "cpn_desc", concat(lit("Basket coupon "), col("cpn_nbr"))
            )
            .withColumn(
                "cpn_dollar_off",
                self.create_modulo_when_statement("row_num", cpn_dollar_off),
            )
            .withColumn(
                "cpn_dollar_threshold",
                self.create_modulo_when_statement(
                    "row_num", cpn_dollar_threshold
                ),
            )
            .withColumn("cpn_start", lit("3/14/2010"))
            .withColumn("cpn_end", lit("3/14/2028"))
            .withColumn(
                "self_funded_flag",
                self.create_modulo_when_statement("row_num", self_funded_flag),
            )
        )

        self.data.tables["cpn_basket"] = self.apply_schema_hard(
            basket_cpn_list, BASKET_COUPON_SCHEMA
        )

    def generate_msmt_input_assignments(self):
        """
        Creates a dummy assignment table for previous coupon assignments

        :return: Updates the job with the table
        """
        dummy = [[-1, -1, "0", "123456", 0.0]]
        columns = [
            "mbrshp_sid",
            "experiment_id",
            "slot_nbr",
            "cpn_nbr",
            "cell_id",
        ]
        assignments = (
            self.spark.sparkContext.parallelize(dummy)
            .toDF(columns)
            .withColumn("mbrshp_sid", col("mbrshp_sid").cast("int"))
            .withColumn("experiment_id", col("experiment_id").cast("int"))
            .select(columns)
        )
        self.data.tables["msmt_assignments"] = assignments

    def _guard_too_many_coupons(self, num_coupons, limit=10000):
        """

        :param num_coupons:
        :param limit:
        :return: a safe number of coupons
        """
        if num_coupons > limit:
            self.log.warn(
                "More than {} coupons is not supported.".format(num_coupons)
            )
            return limit
        return num_coupons

    @staticmethod
    def create_modulo_when_statement(column, options):
        """
        Takes a list of options, then based on the value of column % len(column) create an if/then statement
        assigning the corresponding index of the passed in list

        :param column: Column to perform mod on
        :param options: list of options to use as values
        :return:
        """
        spark_column = col(column).cast("string").cast("long")
        statement = when(
            spark_column % lit(len(options)) == lit(0), lit(options[0])
        )
        for index in range(1, len(options)):
            statement = when(
                spark_column % lit(len(options)) == lit(index),
                lit(options[index]),
            ).otherwise(statement)

        return statement

    def gen_version_map(self):
        """
        Creates a version letter for all basket and category coupons

        :return:
        """
        letters = [
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H",
            "I",
            "J",
            "K",
            "L",
            "M",
            "N",
            "O",
            "P",
            "Q",
            "R",
            "S",
            "T",
            "U",
            "V",
            "W",
            "X",
            "Y",
            "Z",
        ]
        all_coupons = (
            self.data.tables["cpn_category"]
            .select("cpn_nbr")
            .union(self.data.tables["cpn_basket"].select("cpn_nbr"))
            .distinct()
            .cache()
        )
        self.log.info(
            "Creating version map for {} category and basket coupons".format(
                all_coupons.count()
            )
        )

        cpn_list = (
            all_coupons.orderBy(fhash(col("cpn_nbr")))
            .withColumn(
                "VERSION",
                self.create_modulo_when_statement("cpn_nbr", letters),
            )
            .withColumnRenamed("cpn_nbr", "CPN1")
        )

        self.data.tables["version_map"] = self.apply_schema_hard(
            cpn_list, VERSION_MAP_SCHEMA
        )

    def gen_mail_list(self, pct_fhh=0.1):
        """
        Generates the mail list
        Randomly assigns score and decile with decile being uniformily distributed from 0 to 1

        :param pct_fhh: Percentage of members that are free house holders
        :return:
        """
        self.log.info("Creating Mail input file")
        raw_member = self.data.tables["raw_member"].select(
            "MBRSHP_NBR", "MBRSHP_SID"
        )
        member_list = self.data.tables["member_list"]

        core_mail_list = member_list.join(raw_member, "MBRSHP_SID").withColumn(
            "CellName", lit("Test Cell")
        )
        member_count = member_list.count()
        scored_mail_list = (
            rdd_rank_by_col(
                core_mail_list, "MBRSHP_SID", "CellName", "row_num"
            )
            .withColumn(
                "score", col("row_num").cast("double") / lit(member_count)
            )
            .withColumn(
                "decile", lit(1) + col("row_num") / lit(member_count / 10)
            )
            .withColumn(
                "FHH_IND",
                when(col("score") < lit(pct_fhh), "Y").otherwise("N"),
            )
        )

        self.data.tables["mail_list"] = self.apply_schema_hard(
            scored_mail_list, INPUT_MAIL_FILE_SCHEMA
        )

    def _gen_member_sample(self, num_members):
        """
        Creates a random, determinstic sample based on the number of members.
        Random is seeded on experiment and number of members

        :param num_members: The number of members to sample
        :return: the sample
        """

        active_members = (
            self.data.tables["transactions"].select("mbrshp_sid").distinct()
        )
        return deterministic_sample(
            active_members, num_members, ["mbrshp_sid"], self.hash_seed
        )

    def _filter_to_column(self, df_name, subset_name, columns):
        """
        A function to subset df to the intersection it has with subset.

        Also logs if resulting dataframe is not a superset of of subset.  This can be OK but
        not what I would expect in general.

        :param df_name(Dataframe): name of large dataset to subset'
        :param subset(Dataframe): Dataframe to subset to
        :param columns(list(string)): The columns to use in join
        :return: No return, updates a table in job.data.tables
        """
        df = self.data.tables[df_name]
        subset = self.data.tables[subset_name]

        self.log.info(
            "Subsetting {} dataset to only include rows subsetted by {} based on columns {}.".format(
                df_name, subset_name, columns
            )
        )

        distinct_subset = subset.select(columns).distinct().cache()
        result = df.join(distinct_subset, columns).cache()

        self.log.info(
            "Distinct rows by {}: {}".format(columns, distinct_subset.count())
        )

        if (
            result.select(columns).distinct().count()
            != distinct_subset.count()
        ):
            self.log.warn(
                "{} data source does not have full data for subset specified.".format(
                    df_name
                )
            )
            self.log.info(
                "Distinct rows found: {}".format(
                    result.select(columns).distinct().count()
                )
            )

        self.data.tables[df_name] = result

    @staticmethod
    def apply_schema_hard(df, schema):
        """
        Takes a dataframe, subsets to the columns in the schema, then casts each column to match the schema.
        :param df: the dataframe
        :param schema: the schema
        :return:
        """
        df = df.select(schema.names)
        for field in schema.fields:
            df = df.withColumn(
                field.name, col(field.name).cast(field.dataType.simpleString())
            )

        return df

    @classmethod
    def execute_test(cls):
        suite = unittest.TestLoader().loadTestsFromTestCase(cls)
        suite = unittest.TestSuite([suite])

        runner = xmlrunner.XMLTestRunner(output="regression-test-reports")
        runner.run(suite)


class DataMeta(object):
    def __init__(
        self, name, path_read_key, path_write_key=None, filetype="csv"
    ):
        self.name = name
        if path_read_key is None:
            path_read_key = path_write_key
        self.path_read_key = path_read_key

        if path_write_key is None:
            path_write_key = path_read_key
        self.path_write_key = path_write_key
        self.filetype = filetype
