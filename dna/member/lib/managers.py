"""
Helper functions and classes for managing the Spark Job.
"""

import argparse
import copy
import datetime as dt
import os
import re
import socket
import sys

import pe_memberdna.dna.member.lib.utils as utils
import pe_memberdna.lib.iotools as iotools
import pe_memberdna.lib.spark_util as spark_util
import yaml

log = spark_util.get_logger("dna")


def get_run_args():
    """
    Return the command line arguments used to start the script:
        --config_path file_name: specifies the config file to use

    Parameters:

    Returns:
        (argparse.Namespace): object containing the command line args
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_path",
        type=str,
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../configs/config.yaml",
        ),
        help=(
            """
            path to the config file
            """
        ),
    )

    args, _ = parser.parse_known_args()

    return args


def load_config(conf_path_in=None):
    """
    Read in configuration file.

    Reads in confuguration file given path
    and retuns dictionary of full config.

    Parameters:
        conf_path_in (str): local path to config file

    Returns:
        (dict): dictionary representation of config
    """
    if not conf_path_in:
        args = get_run_args()
        cfg_path = args.config_path
    else:
        cfg_path = conf_path_in

    with open(cfg_path, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    return cfg, cfg_path


def calculate_filepaths(config):
    """
    Calculate additional derived parameters and filepaths.

    Parameters:
        config (dict): dictionary of raw config
    Returns:
        (dict): dna config params
        (dict): paths used by dna
    """
    params = copy.deepcopy(config)
    del params["paths"]

    paths = {}
    bucket = config["paths"]["bucket"]

    for path_type in ("input", "output"):
        dir = config["paths"][path_type]["dir"]
        for key, path in config["paths"][path_type].items():
            if key == "dir":
                continue

            if path.startswith("s3://"):
                paths[key] = path
            else:
                paths[key] = f"s3://{bucket}/{dir}/{path}"

    paths["mbr_basic_path"] = utils.get_latest_path(paths["mbr_basic_path"])

    s3_stat_input = config["paths"]["input"]["s3_stat_path"]
    paths["s3_stat_input"] = f"s3://{bucket}/{s3_stat_input}"

    s3_stat_output = config["paths"]["output"]["s3_stat_path"]
    paths["s3_stat_output"] = f"s3://{bucket}/{s3_stat_output}"

    del paths["s3_stat_path"]

    if params["params"]["archive"]:
        archive_path = (
            paths["archive_base_path"]
            if paths["archive_base_path"].endswith("/")
            else "{}/".format(paths["archive_base_path"])
        )
        archive_path = "{}customer_cube_{}".format(
            archive_path, dt.datetime.now().strftime("%Y-%m-%d")
        )
        paths["archive_base_path"] = archive_path

    return params, paths


class ConfigManager:
    """
    Manager for pipeline config.

    Loads and parses config, calculates
    additional paths for use in scripts.
    """

    def __init__(self, job_name, conf_path_in=None):
        """
        Initialize Config Manager.
        Parameters:
            job_name (str): name of relevant script in configuration file.
            conf_path_in (str): path to the local config file if not
                supplied as a command line argument (optional)

        Returns:
            None
        """
        self.cnf, self.cfg_path = load_config(conf_path_in=conf_path_in)
        self.params, self.paths = calculate_filepaths(self.cnf)


class DataManager:
    """
    Manager for pipeline job data.

    Reads, Writes, and manages tables as specified
    by paths.
    """

    def __init__(self, paths, params, spark):
        """
        Initialize DataManager.

        Parameters:
            paths (dict): paths dictionary for input/output locations
            spark (spark.SparkSession): spark session for data

        Returns:
            None
        """

        self.paths = paths
        self.params = params
        self.tables = {}
        self.schemas = self._parse_schemas()
        self.spark = spark

    def _parse_schemas(self):
        """
        Parse schema files for DataManager

        This will put all schemas by name into the
        DataManagers schemas attribute.

        Parameters:
            None

        Returns:
            None
        """
        pass

    def add(self, name, obj):
        """
        Add table to DataManager.

        Parameters:
            name (str): name (key) to associate with object
            obj (object): object to add
        """
        self.tables[name] = obj

    def remove(self, name):
        """
        Remove table from DataManager.

        Parameters:
            name (str): name of obect to remove

        Returns:
            None
        """
        del self.tables[name]

    def read(
        self,
        name,
        pathname,
        readtype="distributed",
        filetype="parquet",
        required=True,
        schema=None,
        cols=None,
    ):
        """
        Read a table from file into the DataManager.

        Handles primary reads from s3 for pipeline jobs
        Schemas are optional, and can be grabbed from the DataManger
        with DataManger.schemas[schemaname]. Detault is
        to read 'distributed' into a Spark DataFrame, but also
        supports reading into a Pandas Dataframe (or local object) by passing in the
        'local' option. Handles parquet and csv files (regardless of extension)
        for distributed reads, and a variety of types for local reads.

        Parameters:
            name (str): name of table (key)
            pathname (str): name of path from config
            readtype (str): type of read (local or distributed)

        Returns:
            None
        """
        readtype = readtype.lower()
        filetype = filetype.lower()
        table = None
        if not required and pathname not in list(self.paths.keys()):
            log.warn(
                "{} is not a key in config but it is also not requried".format(
                    pathname
                )
            )
        else:
            path = self.paths[pathname]
            if readtype == "distributed":
                if filetype == "parquet":
                    table = self.spark.read.parquet(path)
                elif filetype == "csv":
                    table = self.spark.read.csv(
                        path, header="true", schema=schema
                    )
                else:
                    ValueError("invalid distributed filetype specified!")
            elif readtype == "local":
                table = iotools.read_s3_to_local(path, ftype=filetype)
            else:
                ValueError("invalid read type specified!")

            if cols:
                table = table.select(*cols)

            self.add(name, table)

    def write(
        self,
        name,
        pathname,
        mode="overwrite",
        singlefile=False,
        writetype="s3",
        ftype="dual",
        partitionby=None,
    ):
        """
        Write a table to file from DataManager.

        Handles primary writes to s3 and local for
        DataManager items.

        Parameters:
            name (str): name of table to write (key)
            pathname (str): name of path to write to from config
            mode (str): write mode (overwrite or append)
            singlefile (bool): whether to write as single file or not
            writetype (str): type of write (local or s3)
            ftype (str): 'csv', 'parquet', or 'dual' for both. Only applies to
                         spark df's written to s3 (excludes local tables)
            partitionby (str, number or list): the output partitioning to be used

        Returns:
            None
        """
        from pyspark.sql import DataFrame

        table = self.tables[name]
        path = self.paths[pathname]
        writetype = writetype.lower()
        if ftype == "dual":
            csvpath = path + "/CSV"
            pqpath = path + "/PARQUET"
        elif ftype == "csv":
            csvpath = path
        elif ftype == "parquet":
            pqpath = path

        if singlefile:
            table = table.repartition(1)
        elif isinstance(partitionby, list):
            table = table.repartition(*partitionby)
        else:
            table = table.repartition(partitionby)

        if writetype == "s3":
            if isinstance(table, DataFrame):
                writer = table.write
                if isinstance(partitionby, str):
                    writer = writer.partitionBy(partitionby)

                if ftype in ("csv", "dual"):
                    log.info("writing csv...")
                    writer.csv(csvpath, mode=mode, header=True)

                if ftype in ("parquet", "dual"):
                    log.info("writing parquet..")
                    writer.parquet(pqpath, mode=mode)

            else:
                iotools.write_local_to_s3(table, path, mode=mode)

        elif writetype == "local":
            if isinstance(table, DataFrame):
                table.toPandas().to_csv(path, index=False)
            else:
                with open(path, "w+") as f:
                    f.write(table)

    def checkpoint(self, name):
        """
        Checkpoint an object.

        Parameters:
            name (str): object to persist

        Returns:
            None
        """
        pass

    def persist(self, name):
        """
        Persist and object.

        Parameters:
            name (str): object to persist

        Returns:
            None
        """
        self.tables[name].persist(StorageLevel.DISK_ONLY)


class JobManager(object):
    """
    Primary Manager Class for pipeline Jobs

    Handles spark, configruation, and data management
    for pipeline jobs.
    """

    def __init__(self, jobname, appname=None, conf_path_in=None):
        """
        Initialize pipeline job.

        Parameter
            jobname (str): name of job
            appname (str, opt): optional name of app (defaults to jobname)
            conf_path_in (str): path to the local config file if not
                supplied as a command line argument (optional)
        Returns:
            None
        """
        if appname is None:
            appname = jobname

        try:
            pyspark
        except NameError:
            import findspark

            findspark.init()
        from pyspark.sql import SparkSession

        self.spark = SparkSession.builder.appName(appname).getOrCreate()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        self.spark.conf.set(
            "spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED"
        )
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel("WARN")
        self.log = spark_util.get_logger(appname)
        self.config = ConfigManager(jobname, conf_path_in=conf_path_in)
        self.data = DataManager(
            self.config.paths, self.config.params, self.spark
        )
