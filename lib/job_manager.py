"""
Contains the JobManager class
"""

import logging
import os

import yaml
from pe_memberdna.lib.misc import (
    get_last_fiscal_weekend,
    today_helper,
    get_latest_path,
)
from pyspark import SparkContext
from pyspark.sql import SparkSession


class JobManager(object):
    """
    This is a class to be used in all of the module to interact with
    SPARK and to perform read and writes.
    """

    def __init__(self, app_name, config_path=None, log_level="WARN"):
        """
        Set up spark session, spark context and the class member variables.

        Args:
            app_name (str) - name of the SPARK application to be started
            config_path (str) - path pointing to the config file
                containing paths and parameters
            log_level (str) - logging level to be used - INFO, WARN, DEBUG
        """
        self.app_name = app_name

        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.conf.set(
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED"
        )
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel(log_level)

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        if not self.logger.hasHandlers():
            streamHandler = logging.StreamHandler()
            streamHandler.setFormatter(formatter)
            self.logger.addHandler(streamHandler)

    def load_config(self, args):
        """
        Read the config file, replace date placeholders in paths
        and check if this is a single task run

        Args:
            args - object containing the command line arguments as attrubutes

        Returns:
            config - dictionary structure containign the config file
        """
        with open(args.config_path) as config_file:
            config = yaml.load(config_file, Loader=yaml.FullLoader)

        end_date_str = config["club_square_config"].get("max_date")

        if not end_date_str:
            _, end_date_str = get_last_fiscal_weekend()

        end_date_str_no_dashes = end_date_str.replace("-", "")

        # Quotient ID uses the latest data instead of last fiscal weekend
        try:
            config["data_paths"]["source"]["quotient_id"] = get_latest_path(
                config["data_paths"]["source"]["quotient_id"]
            )
        except ValueError:
            # For tests, the get_latest_path should raise an exception because the
            # file will not have the variable date portion, which is ignored here
            pass

        for data_type in [
            "source",
            "intermediate",
            "sampled",
        ]:
            for data_name, data_path in (
                config["data_paths"].get(data_type, {}).items()
            ):
                format_dict = {
                    "curr_date": end_date_str_no_dashes,
                    "16,17,18,19,20,21,22,23": "{16,17,18,19,20,21,22,23}",
                }
                config["data_paths"][data_type][data_name] = data_path.format(
                    **format_dict
                )

        return config

    def split_config(self, config):
        """
        Read the config file and split into each indiviual sections
        Args:
            args - object containing the command line arguments as attrubutes

        Returns:
            config - returns the indivial sections of the config.
        """

        data_paths = config["data_paths"]
        club_square_config = config["club_square_config"]
        config_validation = config["validation"]

        data_paths["archived"] = (
            data_paths["archived"] + "/" + "{:%Y-%m-%d}".format(today_helper())
        )
        return data_paths, club_square_config, config_validation

    def initialize_logging(self):
        """
        Creates logger instance which logs the log message
        Returns:
            returns a reference to a logger instance with the specified
            logging level
        """
        logger = logging.getLogger().setLevel(logging.INFO)
        logger.basicConfig(
            level=logger.INFO,
            format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        return logger
