"""
Contains JobManager, DataManager and ConfigManager
"""
import yaml

import findspark

findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession


class JobManager:
    """
    This is the class to be used for all of modules interacting with SPARK.
    Handles SPARK, configruation, and data management for pipeline jobs
    """

    def __init__(self, app_name, config_path=None, log_level="WARN"):
        """
        Set up SPARK session, context and Manager classes

        Args:
            app_name (str) - name of SPARK application
            config_path (str) - path pointing to the config file
                containing paths and parameters
            log_level (str) - logging level to be used - INFO, WARN, DEBUG

        Returns:
        """
        self.app_name = app_name

        self.sc = SparkContext.getOrCreate()
        log4j_logger = self.sc._jvm.org.apache.log4j
        self.logger = log4j_logger.LogManager.getLogger(self.app_name)

        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()

        self.sc.setLogLevel(log_level)

        self.config = ConfigManager(config_path=config_path)
        self.data = DataManager(self.spark, self.config.paths)


class ConfigManager:
    """
    The class for configuration management
    """

    def __init__(self, config_path=None):
        """
        Initialize self.config via reading the file config_path

        Args:
            config_path (str) - path to the yaml config file that
                is to be read

        Returns:
        """
        if config_path:
            with open(config_path, "r") as ymlfile:
                config = yaml.load(ymlfile, Loader=yaml.FullLoader)

            self.config = config

        self.params = self.config["params"]
        self.paths = self.config["paths"]


class DataManager:
    """
    Manager for pipeline job data.
    Read, write, and manage tables as specified by paths.
    """

    def __init__(self, spark, paths):
        """
        Initialize DataManager

        Args:
            paths (dict) - paths dictionary for input/output locations
            SPARK (spark.SparkSession): spark session for data

        Returns:
        """
        self.spark = spark
        self.paths = paths
        self.tables = {}

    def add(self, table_name, table):
        """
        Add one table to DataManagers tables attribute

        Args:
            table_name (str) - name of the table to be read
            table (dataframe) - dataframe representing the table

        Returns:
        """
        self.tables[table_name] = table

    def remove(self, table_name):
        """
        Remove a table from DataManagers tables attribute

        Args:
            table_name (str) - name of the table to be read

        Returns:
        """
        del self.tables[table_name]

    def read(
        self, table_name, path_name=None, file_type="parquet", columns=None
    ):
        """
        Read a table from file, handles primary reads from s3

        Args:
            table_name (str) - name of the table to be read
            path_name (str) - name of the path for the table,
                default same as table_name
            file_type (str) - either csv or parquet,
                default as parquet
            columns (list) - list of columns to be read,
                default as None, read all columns

        Returns:
        """
        if not path_name:
            path_name = table_name

        if path_name not in self.paths:
            raise Exception(
                " Cannot read table "
                + table_name
                + ". This table name not defined in paths."
                + " Defined table names: "
                + str(self.paths.keys())
            )

        path = self.paths[path_name]["path"]

        if file_type == "csv":
            table = self.spark.read.csv(path, header=True)

        elif file_type == "parquet":
            table = self.spark.read.parquet(path)

        else:
            raise Exception(
                "Incorrect or missing data source format "
                + "specified for table '"
                + table_name
                + "'. Data source details: "
                + str(self.paths[path_name])
            )

        if columns:
            table = table.select(*columns)

        self.add(table_name, table)

    def write(
        self,
        table_name,
        path_name=None,
        df=None,
        mode="overwrite",
        file_type="parquet",
    ):
        """
        write a table into a file on s3

        Args:
            table_name (str) - name of the table to be written
            path_name (str) - name of the path for the table,
                default same as table_name
            df (SPARK dataframe) - dataframe to be written,
                if None, search from tables attribute
            mode (str) - writting mode to be passed to write function
            file_type (str) - file type to be written into

        Returns:
        """
        if not path_name:
            path_name = table_name
        path = self.paths[path_name]["path"]

        if not df:
            df = self.tables[table_name]

        writer = df.write

        if file_type == "csv":
            writer.csv(
                path,
                header=True,
                sep=",",
                mode=mode,
                escape="",
                emptyValue="",
                nullValue=None,
            )

        elif file_type == "parquet":
            writer.mode(mode).parquet(path)
