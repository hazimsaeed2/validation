from pemember_dna.pipelines.assignment.lib.assn_io import (
    calculate_filepaths,
    dump_config,
    load_config,
)
from pemember_dna.pipelines.assignment.lib.checks import (
    check_execution_overwrite,
)
from pemember_dna.pipelines.lib.iotools import (
    get_filename,
    s3_copy,
    split_path_bucket_key,
)
from pemember_dna.pipelines.lib.spark_util import get_logger

log = get_logger("create_debug_files")


def create_debug_file(file_prefix, source_path):
    """
        Create a single debug file.
    Parameters:
        file_prefix (str): the prefix under which to create the file
        source_path (str): the source file used
    Returns:
        path (str): the path to the created debug file
    """

    path = file_prefix + get_filename(source_path)
    bucket, source_key = split_path_bucket_key(source_path)
    _, destination_key = split_path_bucket_key(path)
    s3_copy(bucket, source_key, destination_key)

    return path


def create_debug_files():
    """
    Copies main files involved in assignment job execution to a
    separate directory for debugging purposes.
    """

    log.info("Creating debug files...")

    cnf, cnf_path = load_config()

    PARAMS = dict(
        list(cnf["shared"].items()) + list(cnf["assignment"].items())
    )
    PATHS = cnf["paths"]
    PARAMS, PATHS = calculate_filepaths(PARAMS, PATHS)

    file_prefix = PATHS["OUTPUT_DIR"] + "full_configuration" + "/"

    if PARAMS["run_type"].lower() == "prod":
        check_execution_overwrite(paths_to_check=[file_prefix])

    config_dump_path = dump_config(cnf, PATHS, PARAMS)
    log.info("Config dumped at {path}".format(path=config_dump_path))

    file_path = create_debug_file(file_prefix, PATHS["CAMPAIGN"])
    log.info("Debug file {path} created".format(path=file_path))

    file_path = create_debug_file(file_prefix, PATHS["CELL"])
    log.info("Debug file {path} created".format(path=file_path))

    file_path = create_debug_file(file_prefix, PATHS["HANDSHAKES"])
    log.info("Debug file {path} created".format(path=file_path))

    file_path = create_debug_file(file_prefix, PATHS["CONSTRUCT"])
    log.info("Debug file {path} created".format(path=file_path))

    file_path = create_debug_file(file_prefix, PATHS["SEGMENT"])
    log.info("Debug file {path} created".format(path=file_path))

    file_path = create_debug_file(
        file_prefix + "constructs/", PATHS["CONSTRUCT_BANK"]
    )
    log.info("Debug file {path} created".format(path=file_path))

    file_path = create_debug_file(
        file_prefix + "segments/", PATHS["SEGMENT_BANK"]
    )
    log.info("Debug file {path} created".format(path=file_path))

    log.info("Debug files created")


if __name__ == "__main__":
    create_debug_files()
