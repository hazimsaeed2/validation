"""Helper functions for reading/writing in the assignment pipeline.

TODO:
    - Convert all assignment jobs to new API and pull global helpers inside appropriate classes
    - Add checkpointing logic
    - Add production overwrite logic to writer (same filename)
    - Make read/write more robust
    - Remove hardcoded depencency on argparse to allow use with jupyter/spark shell easily
"""

import argparse
import copy
import os
import random
import re
import socket
import sys
from datetime import datetime

import yaml
from lib_assignment.assn_utils import env_path

from lib.iotools_assignment import (
    is_s3_file, is_volume_file, 
    list_s3_dir, list_volume_dir,
    read_s3_to_local,
    s3_copy,
    split_path_bucket_key,
    write_local_to_s3,
    write_yaml_to_s3,
)
# from pe_memberdna.lib.spark_util import get_logger

# log = get_logger("assn_io")

CUR_FILE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTBOUND_FILE_DIR = "s3://memberanalytics-data-out-prod/dbx_test/ASSIGNMENTS/outbound/"
OUTBOUND_FILES = [
    "final_mailhouse",
    "cpn_redemption",
    "club_redemption",
    "cell_redemption",
    "qc",
    "cpn_impression",
]


#-------------------------------------------------------------------------------------------------------------------------
def _is_spark_dataframe(obj):
    """Return True for both classic PySpark and Spark Connect dataframes."""
    return (
        hasattr(obj, "write")
        and hasattr(obj, "columns")
        and callable(getattr(obj, "toPandas", None))
    )


#-------------------------------------------------------------------------------------------------------------------------
def get_run_args():
    """Return the command line arguments used to start the script:
        --config file_name: specifies the config file to use
        --test: marks the run as a test and loads a test config
        --force: forces the run to overwrite existing files
        --allow-multi-long-tests: forces the run to allow members which are
            eligible for multiple longitudinal tests.

    Parameters:

    Returns:
        args (argparse.Namespace): object containing the command line args
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        nargs="?",
        default="{}/conf/config_template.yml".format(CUR_FILE_DIR),
    )
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-multi-long-tests", action="store_true")

    args, _ = parser.parse_known_args()

    return args

#-------------------------------------------------------------------------------------------------------------------------
def load_config(conf_path_in=None):
    """Read in configuration file.

    Reads in confuguration file given path
    and retuns dictionary of full config.

    Parameters:
        conf_path_in (str): local path to config file

    Returns:
        cfg (dict): dictionary representation of config
    """
    # # Args are not used, must to explicitly declare the config path
    # # if not conf_path_in:
    # #     args = get_run_args()
    # #     if args.test:
    # #         cfg_path = "{}/conf/test_conf.yml".format(CUR_FILE_DIR)
    # #     else:
    # #         cfg_path = args.config
    # # else:
    cfg_path = conf_path_in

    with open(cfg_path, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    return cfg, cfg_path

#-------------------------------------------------------------------------------------------------------------------------
def dump_config(cfg, paths=None, params=None, cfg_path=None):
    """Dump config object.

    Dump config object to S3 with the updates for
    shared and assignment configuration parameters provided in params
    argument and for paths parameter in paths argument

    Parameters:
        cfg (dict): representing a config object
        paths (dict): representing paths updates
        params (dict): representing shared and assignment updates
        cfg_path (str): representing the path where to dump the config

    Returns:
        cfg_path (str): the path where the config is being dumped
    """

    if not paths:
        paths = dict()

    if not params:
        params = dict()

    out_cfg = copy.deepcopy(cfg)
    for path, value in paths.items():
        if path in out_cfg["paths"]:
            out_cfg["paths"][path] = value

    for param, value in params.items():
        if param in out_cfg["shared"]:
            out_cfg["shared"][param] = value
        if param in out_cfg["assignment"]:
            out_cfg["assignment"][param] = value

    if not cfg_path:
        output_dir = paths["OUTPUT_DIR"]
        cfg_path = (
            output_dir
            + "/"
            + "full_configuration"
            + "/"
            + "config_full"
            + out_cfg["shared"]["campaign"]
            + out_cfg["shared"]["run_name"]
            + out_cfg["shared"]["run_type"]
            + ".yml"
        )

    bucket, key = split_path_bucket_key(cfg_path)
    #write_yaml_to_s3(bucket, key, out_cfg)
    print("should have invoked write_yaml_to_s3, IMPORTANTE, review")
    return cfg_path

#-------------------------------------------------------------------------------------------------------------------------
def _get_identifier(params):
    """
    Creates a 'unique' identifier for this run.  Can be changed to be more/less unique if necessary.
    :return:
    """

    if params["run_type"].lower() == "test":
        cwd = os.getcwd()
        person = re.search("(?<=/home/hadoop/).*", cwd).group(0).split("/")[0]
        ip = socket.gethostbyname(socket.gethostname())
        return "<{}-{}>".format(person, ip)
    else:
        return ""

#-------------------------------------------------------------------------------------------------------------------------
def make_assign_filenames(base_path):
    """Convert assignment parameters to filenames.

    Takes the bounds and parameters that define
    a set of assignments, and converts them to filenames
    following conventions for storing.

    Parameters:
        base_path (str): base path to append file to

    Returns:
        assn_path (str): assignments_path
        const_path (str): all-construct assignments path
    """
    assn_pth = "assignments"
    const_pth = "assignments_allconstructs"
    subset_pth = "mail_subset"
    output_file_pth = "final_mailhouse"
    qcfile_pth = "qcfile"
    qcreport_pth = "qc"
    sensqc_pth = "assigned_sensitive_backfill"
    imp_pth = "cpn_impression"
    red_pth = "cpn_redemption"
    cell_red_pth = "cell_redemption"
    club_red_pth = "club_redemption"
    savings_pth = "savings"
    paths_to_create = (
        assn_pth,
        const_pth,
        subset_pth,
        qcfile_pth,
        qcreport_pth,
        sensqc_pth,
        output_file_pth,
        imp_pth,
        red_pth,
        cell_red_pth,
        club_red_pth,
        savings_pth,
    )

    output_tuple = ()
    for path in paths_to_create:
        fullpath = base_path + path
        output_tuple += (fullpath,)

    return output_tuple

#-------------------------------------------------------------------------------------------------------------------------
def generate_qc_report_name(params):
    """Adds a suffix to the qc_report name.
    E.g: The full name of the report will have the following format:
        qc_{campaign}_{run_name}_{run_type}.xlsx
    The suffix is {campaign}_{run_name}_{run_type} .

    Parameters:
        params (dict): dictionary of baseline parameters
    Returns:
        qc_report_name (string): string of qc_report suffix
    """

    qc_report_name = (
        "_"
        + params["campaign"]
        + "_"
        + params["run_name"]
        + "_"
        + params["run_type"]
    )
    return qc_report_name

#-------------------------------------------------------------------------------------------------------------------------
def generate_campaign_path(params):
    """Calculate campaign specific path for subfolder within output directory
    E.g: all assignment output will output to
         assn_output/{run_type}/{campaign_name}/{run_name}-{assignment_date}/{output_file_name}

    Parameters:
        params (dict): dictionary of baseline parameters
    Returns:
        paths (string): string of calculated campaign path
    """
    campaign_path = (
        params["campaign"]
        + "/"
        + _get_identifier(params)
        + "/"
        + params["run_name"]
        + "_"
        + params["assignment_date"]
        + "/"
    )
    return campaign_path.replace("//", "/")

#-------------------------------------------------------------------------------------------------------------------------
def calculate_filepaths(params, paths):
    """Calculate additional derived parameters and filepaths.

    Takes the bounds and parameters that define a pipeline
    run and calculates derived parameters for use throughout
    the run.

    Parameters:
        params (dict): dictionary of baseline parameters
        paths (dict): dictionary of baseline filepaths
    Returns:
        params (dict): extended dictionary of parameters
        paths (dict): extended dictionary of paths
    """
    campaign_path = generate_campaign_path(params)
    time = datetime.now().strftime("%Y%m%d_%H%M")

    # dynamic paths
    if params["run_type"].lower() == "prod":
        bank_path = paths["CDSA_LOC"] + "coupons/PROD/"
        assign_path = paths["ASSN_LOC"] + "PROD/" + campaign_path
        params["ftype"] = "parquet"
    elif params["run_type"].lower() == "test":
        bank_path = paths["CDSA_LOC"] + "coupons/TEST/" + campaign_path
        assign_path = paths["ASSN_LOC"] + campaign_path
        params["ftype"] = "parquet"
    else:
        bank_path = paths["CDSA_LOC"] + "coupons/DEV/" + campaign_path
        assign_path = paths["ASSN_LOC"] + "DEV/" + campaign_path
        params["ftype"] = "parquet"

    if not paths["MAIL_LIST"]:
        campaign = params["campaign"].upper()
        campaign_pattern = "[A-Z]+[0-9]+FY[0-9]+"
        campaign_match = re.search(campaign_pattern, campaign)

        if campaign_match is None:
            raise ValueError(
                "Invalid campaign name! A campaign name should be something like MMPC14FY21."
            )

        if "cdsa" not in paths["CDSA_LOC"]:
            raise ValueError(
                "Invalid CDSA_LOC! The CDSA_LOC should contain cdsa"
            )

        cdsa_loc_split = paths["CDSA_LOC"].split("cdsa")

        campaign_base_path = cdsa_loc_split[0]
        fiscal_year = campaign[-4:]

        base_path = f"{campaign_base_path}campaigns/{fiscal_year}/{campaign}"
        paths["MAIL_LIST"] = base_path + "/input_mail_list"

    if params["run_size"].lower() == "full":
        params["run_size"] = sys.maxsize
    else:
        params["run_size"] = int(params["run_size"])
        print("Running with sample size of: {}".format(params["run_size"]))

    # base tables
    paths["CAMPAIGN"] = paths["CDSA_LOC"] + "campaign.csv"
    paths["CELL"] = paths["CDSA_LOC"] + "cells.csv"
    paths["CONSTRUCT"] = paths["CDSA_LOC"] + "constructs.csv"
    paths["SEGMENT"] = paths["CDSA_LOC"] + "segments.csv"
    paths["CONSTRUCT_BANK"] = paths["CDSA_LOC"] + "CONSTRUCTS/"
    paths["SEGMENT_BANK"] = paths["CDSA_LOC"] + "SEGMENTS/"
    paths["HANDSHAKES"] = paths["CDSA_LOC"] + "handshakes.csv"

    # base coupon tables
    paths["COUPON_BANK"] = bank_path + "coupon_bank"
    paths["COUPON_QUALS"] = bank_path + "coupon_quals"
    paths["COUPON_MAP"] = bank_path + "coupon_map"
    # dynamic coupon tables
    paths["COUPON_MEMTRIP"] = (
        bank_path + "member_trips_coupon/" + params["campaign"]
    )
    paths["COUPON_MEMUSAGE"] = (
        bank_path + "member_coupon_usage/" + params["campaign"]
    )

    paths["COUPON_DISCOUNT"] = (
        bank_path + "coupon_discounts/" + params["campaign"]
    )

    paths["COUPON_CLOSURE"] = (
        bank_path + "coupon_closure/" + params["campaign"]
    )

    # dynamic assignment tables
    (
        assn,
        const,
        subset,
        qc,
        qc_rep,
        sensitive_qc,
        output,
        impr,
        red,
        cell_red,
        club_red,
        svgs,
    ) = make_assign_filenames(assign_path)
    paths["OUTPUT_DIR"] = assign_path
    paths["ASSIGNMENT_PATH"] = assn
    paths["CONSTRUCTS_PATH"] = const
    paths["MAILFILE"] = output
    paths["QCFILE"] = qc
    paths["QC_REPORT"] = qc_rep + generate_qc_report_name(params) + ".xlsx"
    paths["SENSITIVE_QC"] = sensitive_qc
    paths["SUBSET"] = subset
    paths["SIZING_IMP"] = impr
    paths["SIZING_BUDGET"] = red
    paths["SIZING_CELL"] = cell_red
    paths["SIZING_CLUB"] = club_red
    paths["SAVINGS"] = svgs
    # other output locations
    paths["ASSIGN_LOG"] = paths[
        "LOG_LOC"
    ] + "assign_offers/experiment={}_{}.csv".format(
        str(params["experiment"]), time
    )
    # for read in full folder
    paths["COUPON_LOG_FOLDER"] = paths["LOG_LOC"] + "create_coupons/"
    # for write out seperate csv
    paths["COUPON_LOG"] = paths[
        "COUPON_LOG_FOLDER"
    ] + "experiment={}_{}.csv".format(str(params["experiment"]), time)
    paths["COUPON_EXCLUSION_LOG"] = assign_path + "cpn_exclusion_log/"
    paths["COUPON_BACKFILL_ELIGIBILITY_LOG"] = (
        assign_path + "cpn_backfill_eligibility_log/"
    )
    paths["INTERMEDIATE_CONSTRUCT"] = (
        assign_path + "intermediate/" + "member_construct/"
    )
    paths["INTERMEDIATE_SEGMENT"] = (
        assign_path + "intermediate/" + "member_construct_segments/"
    )

    if params["run_type"].lower() == "dev":
        paths["MSMT_ASSGN"] = assign_path + "assignments"
        paths["MSMT_AFEYN"] = assign_path + "afeynmants"
        paths["MSMT_CELL"] = assign_path + "msmt_cells/msmt_cells.csv"
        paths["MSMT_CELL_ARCHIVE"] = (
            assign_path + "msmt_cells/msmt_cells_archive/msmt_cells.csv"
        )
        paths["CDSA_ASSGN"] = paths["CDSA_LOC"] + "assignments"
        paths["CDSA_CPN_BANK"] = paths["CDSA_LOC"] + "coupons/PROD/coupon_bank"
    elif params["run_type"].lower() == "adhoc":
        paths["MSMT_ASSGN"] = paths[
            "CDSA_LOC"
        ] + "AD_HOC/{}/assignments/".format(time)
        paths["MSMT_AFEYN"] = paths[
            "CDSA_LOC"
        ] + "AD_HOC/{}/afeynmants/".format(time)
        paths["MSMT_CELL"] = paths["CDSA_LOC"] + "msmt_cells/msmt_cells.csv"
        paths["MSMT_CELL_ARCHIVE"] = paths[
            "CDSA_LOC"
        ] + "msmt_cells/msmt_cells_archive/msmt_cells_{}.csv".format(time)
    elif params["run_type"].lower() in ["prod", "test"]:
        paths["MSMT_ASSGN"] = paths["CDSA_LOC"] + "assignments"
        paths["MSMT_AFEYN"] = paths["CDSA_LOC"] + "afeynmants"
        paths["MSMT_CELL"] = paths["CDSA_LOC"] + "msmt_cells/msmt_cells.csv"
        paths["MSMT_CELL_ARCHIVE"] = paths[
            "CDSA_LOC"
        ] + "msmt_cells/msmt_cells_archive/msmt_cells_{}.csv".format(time)
        paths["CDSA_ASSGN"] = paths["MSMT_ASSGN"]
        paths["CDSA_CPN_BANK"] = paths["COUPON_BANK"]
    # make sure all run type are intended
    else:
        raise ValueError("Invalid run type!!!")
    params["SEED"] = int(params["experiment"])

    optional_file_paths = {
        "INPUT_ASSIGNMENTS": paths["ASSIGNMENT_PATH"],
        "INPUT_CONSTRUCTS": paths["CONSTRUCTS_PATH"],
        "INPUT_MAILHOUSE": paths["MAILFILE"],
        "EXCLUSIONS": paths["EXCLUSIONS"] or "",
    }
    if "BBM" in params["campaign"].upper():
        input_assignments_path = paths.get("INPUT_ASSIGNMENTS")
        if input_assignments_path == "" or input_assignments_path is None:
            input_assignments_path = optional_file_paths["INPUT_ASSIGNMENTS"]
        optional_file_paths[
            "MAIL_POPULATION_ASSIGNMENT"
        ] = input_assignments_path
    elif "MMPC" in params["campaign"].upper():
        optional_file_paths["MAIL_POPULATION_ASSIGNMENT"] = paths["SUBSET"]

    random.seed(params["SEED"])
    if params["run_type"].lower() == "test":
        for key, path in paths.items():
            if path is None and key in optional_file_paths:
                pass
            else:
                paths[key] = re.sub("<.+>", _get_identifier(params), path)
    for key, path in paths.items():
        if path is None and key in optional_file_paths:
            pass
        else:
            if isinstance(path, str):
                paths[key] = re.sub(r"(?<!s3:)\/\/", "/", path)
            else:
                for i, p in enumerate(path):
                    paths[key][i] = re.sub(r"(?<!s3:)\/\/", "/", p)

    # inserting missing optional config file paths
    for config_path, default_path in list(optional_file_paths.items()):
        file_path = paths.get(config_path)
        if file_path == "" or file_path is None:
            paths[config_path] = default_path

    inputs = (
        "INPUT_ASSIGNMENTS",
        "INPUT_CONSTRUCTS",
        "MAIL_POPULATION_ASSIGNMENT",
    )

    for path in inputs:
        path_list = paths[path].rstrip("/").split("/")
        paths["CAP_ORIGINAL_" + path] = "/".join(
            path_list[:-1] + ["cap_original_backups"] + [path_list[-1]]
        )

    paths["OUTBOUND"] = OUTBOUND_FILE_DIR

    return params, paths


#-------------------------------------------------------------------------------------------------------------------------
def generate_outbound_path(file_name, file_type, params, paths):
    """Generates new file path for outbound files
    Parameters:
        file_name (str): Name of the file
        file_type (str): csv/parquet/xlsx (file extension)
        params (dict): Job parameters
        paths (dict): Job paths
    Returns:
        outbound_path (str): New path for outbound file
    """

    campaign = "BBM" if "BBM" in params["campaign"].upper() else "MMPC"
    
    cpgn = read_s3_to_local(paths["CAMPAIGN"], ftype="csv")
    cpgn = cpgn[cpgn.experiment_id == params["experiment"]]

    campaign_nbr = cpgn.fiscal_num.iloc[0]
    year = cpgn.fiscal_year.iloc[0]
    year = str(year)[-2:]

    dest_bucket, dest_key = split_path_bucket_key(paths["OUTBOUND"])

    dest_key = "{dest_key}{file_name}/FY{year}_{campaign}{campaign_nbr}_{file_name}.{file_type}".format(
        dest_key=dest_key,
        file_name=file_name,
        year=year,
        campaign=campaign,
        campaign_nbr=campaign_nbr,
        file_type=file_type,
    )

    return "s3://{bucket}/{key}".format(bucket=dest_bucket, key=dest_key)


def generate_outbound_path_dbx(file_name, file_type, params, paths, spark, vol_base, env):
    """
    Databricks Volume version of generate_outbound_path.
    
    Generates the destination path inside a Databricks Volume.

    Parameters:
        file_name (str)
        file_type (str): csv/parquet/xlsx
        params (dict)
        paths (dict)
        spark: SparkSession (used to read CSV)

    Returns:
        outbound_path (str): Full Databricks Volume path for the output file
    """

    # --- Determine MMPC / BBM campaign ---
    campaign = "BBM" if "BBM" in params["campaign"].upper() else "MMPC"

    # --- Read campaign file from Volumes ---
    cpgn_path = env_path(paths["CAMPAIGN"],  vol_base, env)

    cpgn = spark.read.csv(cpgn_path, header=True, inferSchema=True)

    cpgn = cpgn.filter(cpgn.experiment_id == params["experiment"])

    campaign_nbr = cpgn.select("fiscal_num").collect()[0][0]
    year_full = cpgn.select("fiscal_year").collect()[0][0]
    year = str(year_full)[-2:]   # Last 2 digits

    # --- Build outbound folder path (remove trailing slash) ---
    outbound_base = env_path(  paths["OUTBOUND"].rstrip("/") + "/" ,  vol_base, env)

    # --- New filename ---
    new_filename = f"FY{year}_{campaign}{campaign_nbr}_{file_name}.{file_type}"

    # --- Final outbound file path ---
    outbound_path = f"{outbound_base}{file_name}/{new_filename}"

    return outbound_path

#-------------------------------------------------------------------------------------------------------------------------
def move_to_outbound(file_path, file_name, file_type, params, paths):
    """Move outbound files to new path
    Parameters
        file_path (str): current file path
        file_name (str): Name of the file
        file_type (str): csv/parquet/xlsx (file extension)
        params (dict): Job parameters
        paths (dict): Job paths
    Returns:
        Nothing
    """
    src_bucket, src_key = split_path_bucket_key(file_path)

    file_found = False

    if ( is_s3_file(src_bucket, src_key) and src_key.endswith(file_type)):

        src_key_with_file = src_key
        file_found = True

    else:

        dir_objs = list_s3_dir(src_bucket, src_key)
        for obj in dir_objs:
            _, src_key_with_file = split_path_bucket_key(obj)
            if src_key_with_file.endswith(file_type):
                file_found = True
                break

    dest_path = generate_outbound_path(file_name, file_type, params, paths)

    _, dest_key = split_path_bucket_key(dest_path)

    if file_found:     
        s3_copy(src_bucket, src_key_with_file, dest_key)
    else:
        raise Exception(
            "No file found in {file_path} of type {file_type}. Transfer of"
            " {file_name} failed".format(
                file_path=file_path, file_type=file_type, file_name=file_name
            )
        )


    
#-------------------------------------------------------------------------------------------------------------------------
def move_to_outbound_dbx(file_path, file_type, file_name, params, paths, spark, vol_base, env,  dbutils):
    """
    Databricks Volume equivalent of the S3 logic:
    - If file_path is a file and ends with file_type → use it
    - Else treat it as a directory and search for matching file_type
    - Copy selected file to outbound destination
    
    Move outbound files to new path
    Parameters
        file_path (str): current file path
        file_name (str): Name of the file
        file_type (str): csv/parquet/xlsx (file extension)
        params (dict): Job parameters
        paths (dict): Job paths
    Returns:
        Nothing
    """

    # Normalize (Volumes paths should NOT end with "/")
    file_path = file_path.rstrip("/")

    file_found = False
    src_file_path = None

    # ---------------------------------------
    # CASE 1: file_path IS ALREADY A FILE
    # ---------------------------------------
    if is_volume_file(file_path, dbutils) and file_path.endswith(file_type):
        src_file_path = file_path
        file_found = True

    else:
        # ---------------------------------------
        # CASE 2: treat as directory and list files
        # ---------------------------------------
        dir_files = list_volume_dir(file_path, dbutils)

        for child in dir_files:
            if child.endswith(file_type):
                src_file_path = child
                file_found = True
                print(f'Found child with file type: {child}')
                break


    # ---------------------------------------
    # Build destination path in Volumes
    # ---------------------------------------
    
    dest_path = generate_outbound_path_dbx(file_name, file_type, params, paths, spark, vol_base, env).rstrip("/")

    # ---------------------------------------
    # Perform copy
    # ---------------------------------------
    if file_found:
        print(f"Copying:\n- src: {src_file_path}\n- to dest: {dest_path}\n")
        dbutils.fs.cp(src_file_path, dest_path)
    else:
        raise Exception(
            f"No file found in {file_path} of type {file_type}. "
            f"Transfer of {file_name} failed."
        )
        
        
    



class ConfigManager:
    """Manager for pipeline config.

    Loads and parses config, calculates
    additional paths for use in scripts.
    """

    def __init__(self, job_name, conf_path_in=None):
        """Initialize Config Manager.
        Parameters:
            job_name (str): name of relevant script in configuration file.
            conf_path_in (str): path to the local config file if not
                supplied as a command line argument (optional)

        Returns:
            None
        """
        self.cnf, self.cfg_path = load_config(conf_path_in=conf_path_in)
        if job_name in list(self.cnf.keys()):
            self.params = dict(
                list(self.cnf["shared"].items())
                + list(self.cnf[job_name].items())
            )
        else:
            self.params = self.cnf["shared"]
        self.original_paths = self.cnf["paths"]
        self.params, self.paths = calculate_filepaths(
            self.params, copy.deepcopy(self.original_paths)
        )


class DataManager:
    """Manager for pipeline job data.

    Reads, Writes, and manages tables as specified
    by paths.
    """

    def __init__(self, paths, params, spark):
        """Initialize DataManager.

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


    #-------------------------------------------------------------------------------------------------------------------------
    def _parse_schemas(self):
        """Parse schema files for DataManager

        This will put all schemas by name into the
        DataManagers schemas attribute.
        """
        from lib_assignment.schemas.assn_schemas import (
            ASSIGNMENTS,
        )
        from lib_assignment.schemas.cdsa_schemas import CDSA
        from lib_assignment.schemas.coupon_schemas import COUPONS

        schemas = CDSA.copy()
        schemas.update(ASSIGNMENTS)
        schemas.update(COUPONS)
        return schemas
    

    #-------------------------------------------------------------------------------------------------------------------------
    def add(self, name, obj):
        """Add table to DataManager.

        Parameters:
            name (str): name (key) to associate with object
            obj (object): object to add
        """
        self.tables[name] = obj



    #-------------------------------------------------------------------------------------------------------------------------
    def remove(self, name):
        """Remove table from DataManager.

        Parameters:
            name (str): name of obect to remove
        """
        del self.tables[name]



    #-------------------------------------------------------------------------------------------------------------------------
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
        """Read a table from file into the DataManager.

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
            print(
                "{} is not a key in config but it is also not requried".format(
                    pathname
                )
            )
        else:
            if (
                self.params["run_type"].lower() == "prod"
                and name in OUTBOUND_FILES
            ):
                # if self.env == "prod":
                #     path = generate_outbound_path(name, filetype, self.params, self.paths)
                # else:
                path = generate_outbound_path_dbx(
                            file_name=name, 
                            file_type=filetype, 
                            params=self.params,
                            paths=self.paths,
                            spark=self.spark, 
                            vol_base=self.vol_base, 
                            env=self.env
                        )
            else:
                path = self.paths.get(pathname,None)
            
            if path:
                path = env_path(path, self.vol_base, self.env)
            
            print(f'- Path read:{path}')


            if readtype == "distributed":
                if filetype == "parquet":
                    table = self.spark.read.parquet(path)
                elif filetype == "csv":
                    table = self.spark.read.csv(path, header="true", schema=schema)
                elif filetype == "table":
                    table = self.spark.table(pathname)
                else:
                    ValueError("invalid distributed filetype specified!")
            elif readtype == "local":
                if filetype == "parquet":
                    table = self.spark.read.parquet(path).toPandas()
                elif filetype == "csv":
                    table = self.spark.read.csv(path, header="true", inferSchema=True).toPandas() 
            else:
                ValueError("invalid read type specified!")

            if cols:
                table = table.select(*cols)

            self.add(name, table)


    #-------------------------------------------------------------------------------------------------------------------------
    def write(
        self,
        name,
        pathname,
        mode="overwrite",
        singlefile=False,
        writetype="s3",
        ftype="dual",
        partitionby=None,
        dbutils = None, # only used in dbx
    ):
        """Write a table to file from DataManager.

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
        writetype   = writetype.lower()
        table       = self.tables[name]
        base_path   = self.paths[pathname]

        # Use env_path to resolve the actual path for writing
        path = env_path(base_path, self.vol_base, self.env)
            

        if ftype == "dual":
            csvpath = path + "/CSV"
            pqpath = path + "/PARQUET"
        elif ftype == "csv":
            csvpath = path
        elif ftype == "parquet":
            pqpath = path




        if singlefile:
            table = table.repartition(1)
        elif partitionby:
            table = table.repartition(partitionby)


        if writetype == "s3":

            if _is_spark_dataframe(table):
                writer = table.write
                if isinstance(partitionby, str):
                    writer = writer.partitionBy(partitionby)

                if ftype in ("csv", "dual"):
                    writer.csv(csvpath, mode=mode, header=True)

                if ftype in ("parquet", "dual"):
                    writer.parquet(pqpath, mode=mode)

            else:
                write_local_to_s3(table, path, mode=mode)

        elif writetype == "local":

            if _is_spark_dataframe(table):
                table.toPandas().to_csv(path, index=False)
            else:
                with open(path, "w+") as f:
                    f.write(table)

        print(f"Initial writing successful..")

        if (
            self.params["run_type"].lower() == "prod"
            and name in OUTBOUND_FILES
        ):
            print(f"We are also copying the file to the outbound folder with a particular file name.\n")

            # if self.env == "prod":
            #     move_to_outbound(path, name, ftype, self.params, self.paths)
            # else:
            move_to_outbound_dbx(
                file_path=path, 
                file_name=name, 
                file_type=ftype, 
                params=self.params,
                paths=self.paths,
                spark=self.spark, 
                vol_base=self.vol_base, 
                env=self.env, 
                dbutils=dbutils)



    #-------------------------------------------------------------------------------------------------------------------------
    def checkpoint(self, name):
        """Checkpoint an object.

        Parameters:
            name (str): object to persist

        Returns:
            None
        """
        pass

    def persist(self, name):
        """Persist and object.

        Parameters:
            name (str): object to persist

        Returns:
            None
        """
        self.tables[name].persist(StorageLevel.DISK_ONLY)



#-------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------
class JobManager(object):
    """Primary Manager Class for pipeline Jobs

    Handles spark, configruation, and data management
    for pipeline jobs.
    """

    def __init__(self, jobname, conf_path_in=None, spark=None, vol_base="/Volumes/datascience_ea_dev/pe/outputs_for_s3", env="dev"): ## Add spark to arguments
        """Initialize pipeline job.

        Parameter
            jobname (str): name of job
            appname (str, opt): optional name of app (defaults to jobname)
            conf_path_in (str): path to the local config file if not
                supplied as a command line argument (optional)
        """
        # if appname is None:
        #     appname = jobname

        # try:
        #     pyspark
        # except NameError:
        #     import findspark

        #     findspark.init()
        # from pyspark.sql import SparkSession

        self.spark = spark # SparkSession.builder.appName(appname).getOrCreate()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # self.sc = self.spark.sparkContext
        # self.sc.setLogLevel("WARN")
        # self.log = get_logger(appname)
        self.vol_base = vol_base
        self.env = env.lower()
        self.config = ConfigManager(jobname, conf_path_in=conf_path_in)
        self.data = DataManager(
            self.config.paths, self.config.params, self.spark
        )
        self.data.env = env
        self.data.vol_base = vol_base
