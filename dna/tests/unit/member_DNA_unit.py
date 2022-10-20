import logging
import os
import sys
import unittest
import unittest.mock as mock

import findspark
import pyspark.sql as sql
import pyspark.sql.functions as sqlf
import xmlrunner
import yaml

import memberdna.dna.lib.utils as utils
import memberdna.dna.lib.managers as managers
import memberdna.dna.scripts.generate_coupon_and_digital_1 as gen_cpn_and_dig_1
import memberdna.dna.scripts.generate_coupon_and_digital_2 as gen_cpn_and_dig_2
import memberdna.dna.scripts.generate_member as gen_member
import memberdna.dna.scripts.generate_most_shopped as gen_most_shopped
import memberdna.dna.scripts.generate_transaction_1 as gen_trans_1
import memberdna.dna.scripts.generate_transaction_2 as gen_trans_2
import memberdna.dna.scripts.generate_transaction_3 as gen_trans_3
import memberdna.dna.scripts.generate_misc as gen_misc
import memberdna.dna.scripts.integrate_acquisition as integrate_acquisition
import memberdna.dna.scripts.merge as gen_merge
import memberdna.category_square.unittests.category_DNA_unit as cat_dna_unit
import memberdna.source_etl.unittests.source_etl_unit as source_etl_unit
import memberdna.testing_support.lib.utility as test_utils


def prepare_member_dna_data(
    spark_in,
    unittest_config_path,
    unittest_source_etl_config_path,
    simulated_source_etl_config_path,
):
    """
    Copy the supporting text files from local directory to
    an S3 folder and convert them to the parquet format.
    The S3 folder later simulates the memberdna-data-out bucket.

    Parameters:
        spark_in (SparkSession)- spark session object
        unittest_config_path (str) - config file used by the test to setup dna
        unittest_source_etl_config_path (str) - config file used by the test to
            setup source etl
        simulated_source_etl_config_path (str) - source etl test config
    Returns:
        None
    """

    with open(unittest_config_path) as config_file:
        unittest_config = yaml.load(config_file, Loader=yaml.FullLoader)

    source_etl_unit.prepare_etl_data(spark_in, unittest_source_etl_config_path)
    source_etl_unit.run_source_etl(simulated_source_etl_config_path)

    for _, paths in unittest_config["additional_files"].items():
        if paths["csv"].startswith(unittest_config["root_s3_path"]):
            destination_path_csv = paths["csv"]
            destination_path_parquet = paths["parquet"]
        else:
            destination_path_csv = (
                unittest_config["root_s3_path"] + paths["csv"]
            )
            destination_path_parquet = (
                unittest_config["root_s3_path"] + paths["parquet"]
            )

        local_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), paths["local_file"]
        )

        test_utils.copy_local_to_s3(local_path, destination_path_csv)

        df = spark_in.read.csv(
            destination_path_csv, sep=",", header=True, quote='"'
        )

        df.write.parquet(destination_path_parquet, mode="overwrite")


def run_member_dna(simulated_config_path):
    """
    Execute member DNA using a config
    file path passed as an input argument referencing the
    files prepared in S3 by prepare_member_dna_data

    Args:
        simulated_config_path: path to the config file that
            simulates the real memebr DNA config file but
            points to a test S3 folder

    Returns:
    """

    with open(simulated_config_path) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    sys.argv.extend(["--config", simulated_config_path])
    gen_cpn_and_dig_1.main()
    gen_cpn_and_dig_2.main()
    gen_member.main()
    gen_most_shopped.main()
    gen_trans_1.main()
    gen_trans_2.main()
    gen_trans_3.main()
    gen_misc.main()
    integrate_acquisition.main()

    gen_merge.main()


class TestVectorVsMemberDna(unittest.TestCase):
    """
    Unit tests associated with member DNA

    Generate experimental data containing 4 test members,
    execute cube.py from member DNA and then verify
    that the output data is correct.

    The 4 test members have the following atributes:
        member 100001 - used 0 coupons
        member 100002 - used 1 regural coupon
        member 100003 - used 1 vector coupons
        member 100004 - used 1 vector + 1 regural coupon

    The text files in the source_ETL folder are artificial but follow 100%
    the same logic as the real input source files
    """

    @classmethod
    def setUpClass(cls):
        """
        Create pipelined intermediate files and loads the extected result
        file
        """
        test_name = cls.__name__
        unique_id = test_utils.get_unique_id(test_name)

        cls.simulated_config_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "data/simulated_config.yaml.{}".format(unique_id),
        )

        cls.simulated_source_etl_config_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "data/simulated_config_source_ETL.yaml.{}".format(unique_id),
        )

        cls.unittest_config_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "data/unittest_config.yaml.{}".format(unique_id),
        )

        cls.unittest_source_etl_config_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "data/unittest_config_source_ETL.yaml.{}".format(unique_id),
        )

        cls.desired_results_path = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            "data/desired_results.yaml",
        )

        templates = [
            cls.simulated_config_path,
            cls.simulated_source_etl_config_path,
            cls.unittest_config_path,
            cls.unittest_source_etl_config_path,
        ]
        for template in templates:
            original = template.replace(".{}".format(unique_id), "")
            with open(original, "r") as config_file:
                config = config_file.read()
            with open(template, "w") as config_file:
                config = config.replace("UNIQUE_ID", unique_id)
                config_file.write(config)

        with open(cls.unittest_config_path) as config_file:
            unittest_config = yaml.load(config_file, Loader=yaml.FullLoader)

        test_utils.remove_from_s3(
            unittest_config["root_s3_path"]
            + "data_out/"
        )

        prepare_member_dna_data(
            spark,
            cls.unittest_config_path,
            cls.unittest_source_etl_config_path,
            cls.simulated_source_etl_config_path,
        )
        run_member_dna(cls.simulated_config_path)

        with open(cls.unittest_config_path) as config_file:
            unittest_config = yaml.load(config_file, Loader=yaml.FullLoader)

        cls.cube_df = spark.read.parquet(
            unittest_config["root_s3_path"]
            + "data_out/customer_cube_full",
        )

        with open(cls.desired_results_path) as config_file:
            cls.desired_results = yaml.load(
                config_file, Loader=yaml.FullLoader
            )

    def test_memeber_dna_coupon(self):
        """
        Verify that the member DNA table generated
        for 4 different members match the results defined in
        desired_results.yaml
        """

        for (
            curr_mbrshp_sid,
            desired_vals_per_mem,
        ) in self.desired_results.items():

            df_loc = (
                self.cube_df.filter(
                    self.cube_df.MBRSHP_SID.isin([curr_mbrshp_sid])
                )
                .select(*[list(desired_vals_per_mem.keys())])
                .collect()[0]
            )

            for i, (feature_name, desired_val) in enumerate(
                desired_vals_per_mem.items()
            ):

                self.assertEqual(
                    str(df_loc[i]),
                    str(desired_val),
                    'For member "'
                    + str(curr_mbrshp_sid)
                    + '" The coolumn "'
                    + feature_name
                    + " for member "
                    + str(curr_mbrshp_sid)
                    + '" does not match when comparing '
                    + "the expected value to the real value. "
                    + " expected value = "
                    + str(desired_val)
                    + " real value = "
                    + str(df_loc[i]),
                )

    def test_duplicity_generation(self):
        """
        Verify that generated member DNA table has no duplicates
        """

        df_grouped = self.cube_df.groupBy(
            *["MBRSHP_SID", "FISCAL_WEEK_END"]
        ).agg(sqlf.count(sqlf.col("MBRSHP_SID")).alias("cnt"))

        dupe_cnt = df_grouped.where(df_grouped["cnt"] > 1).count()

        if dupe_cnt > 0:
            print("The following problematic columns were detected:")
            df_grouped.where(df_grouped["cnt"] > 1).show()

        self.assertEqual(
            dupe_cnt,
            0,
            "Found {} duplicates with respect to MBRSHP_SID and FISCAL_WEEK_END".format(
                str(dupe_cnt)
            ),
        )

    def test_dna_columns(self):
        expected_columns_input = open(
            os.path.join(
                os.path.abspath(os.path.dirname(__file__)),
                "data/dna_columns.txt",
            ),
            "r"
        )
        expected_columns = expected_columns_input.readlines()
        expected_columns = [
            column.replace("\n", "")
            for column in expected_columns
        ]
        current_columns = self.cube_df.columns

        self.assertEqual(sorted(expected_columns), sorted(current_columns))


    @classmethod
    def tearDownClass(cls):
        """
        Delete all files generated by this unit test
        """

        with open(cls.unittest_config_path) as config_file:
            unittest_config = yaml.load(config_file, Loader=yaml.FullLoader)

        test_utils.remove_from_s3(unittest_config["root_s3_path"] + "data_out/")


class TestMemberDnaAutoDate(cat_dna_unit.TestAutoDate, unittest.TestCase):
    """
    Verify that the start_date and the end_date is calculated
    correctly after the introduction of the feature that calculates them
    automaticaly
    """

    def setUp(self):
        """
        Read the simulated config file
        """
        test_name = self.__class__.__name__
        unique_id = test_utils.get_unique_id(test_name)
        parent_dir = os.path.abspath(os.path.dirname(__file__))

        self.simulated_config_path = os.path.join(
            parent_dir, "data/simulated_config.yaml.{}".format(unique_id)
        )

        unittest_config_path = os.path.join(
            parent_dir, "data/unittest_config.yaml.{}".format(unique_id)
        )

        templates = [
            self.simulated_config_path,
            unittest_config_path,
        ]
        for template in templates:
            original = template.replace(".{}".format(unique_id), "")
            with open(original, "r") as config_file:
                config = config_file.read()
            with open(template, "w") as config_file:
                config = config.replace("UNIQUE_ID", unique_id)
                config_file.write(config)

        with open(self.simulated_config_path) as config_file:
            self.config = yaml.load(config_file, Loader=yaml.FullLoader)

        with open(unittest_config_path) as config_file:
            unittest_config = yaml.load(config_file, Loader=yaml.FullLoader)

        for _, paths in unittest_config["additional_files"].items():
            local_path = os.path.join(parent_dir, paths["local_file"])
            print("copy %s to %s" % (local_path, paths["csv"]))
            if paths["csv"].startswith(unittest_config["root_s3_path"]):
                test_utils.copy_local_to_s3(local_path, paths["csv"])
            else:
                test_utils.copy_local_to_s3(
                    local_path, unittest_config["root_s3_path"] + paths["csv"]
                )

        self.job = managers.JobManager(
            "test_dates",
            conf_path_in=self.simulated_config_path
        )

    def del_cfg(self, key_in):
        """
        Delete the start_date or end_date from
        the config
        """
        del self.job.config.params["params"][key_in + "_date"]

    def get_cfg(self, key_in, config_in=None):
        """
        Return the value of start_date or end_date from
        the config
        """
        if config_in:
            if key_in == "start":
                return config_in.start_date
            elif key_in == "end":
                return config_in.end_date
            else:
                raise Exception('key_in must be either "start" or "end"')
        else:
            return self.job.config.params["params"][key_in + "_date"]

    def set_cfg(self, key_in, val_in, config_in=None):
        """
        Set the value of start_date or end_date in
        the config
        """
        if config_in:
            setattr(config_in, key_in + "_date", val_in)
        else:
            self.job.config.params["params"][key_in + "_date"] = val_in

    def run_dna(self):
        """
        Execute the member DNA
        """

        start_date, end_date, lookback_start_date = utils.window_dates(
            self.job
        )

        return mock.MagicMock(
            start_date=start_date,
            end_date=end_date,
            transaction_start_filter=lookback_start_date,
        )


if __name__ == "__main__":
    findspark.init()
    name = "Member DNA - unittests"
    spark = sql.SparkSession.builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logging.getLogger().setLevel(logging.INFO)

    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
