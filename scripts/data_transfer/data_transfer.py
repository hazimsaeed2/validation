"""
This script handles Personalization Engine's files/data transfer to
Membership Foundation data lake. All future data requests to Membership Foundation
should be implemented in this script after adding corresponding input and output
paths to config.yaml

"""
import argparse
import logging

import yaml

import pe_memberdna.lib.iotools as iotools
import pe_memberdna.model.cf_model.lib.cf_utils as utils


def parse():
    """
    Parse command line arguments

    Args:

    Returns:
        args - object containing the command line arguments as attributes
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", nargs="?", default="config.yaml")

    return parser.parse_args()


def load_config(args):
    """
    Reads the config file

    Args:
        args - object containing the command line arguments as attributes

    Returns:
        config - dictionary structure containing the config file
    """

    with open(args.config_path, "r") as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    return config


def main(config):
    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Transfer the latest Trip Propensity predictions file
    logging.info("Starting Trip Propensity file transfer...")

    bucket_src, key_src = iotools.split_path_bucket_key(
        config["data_paths"]["input"]["trip_prop_path"]
    )
    trip_prop_input_path = utils.get_latest_prop_path(bucket_src, key_src)

    if trip_prop_input_path is not None:
        bucket_dest, key_dest = iotools.split_path_bucket_key(
            config["data_paths"]["output"]["edw_trip_prop_path"]
        )
        iotools.s3_copy_managed(
            bucket_src,
            trip_prop_input_path,
            key_dest,
            bucket_dest,
        )

        logging.info("Completed Trip Propensity file transfer.")
    else:
        logging.warn(
            "Source Trip Propensity file not found for bucket: {} and key: {}".format(
                bucket_src, key_src
            )
        )

    # Transfer Member DNA output files
    logging.info("Starting Member DNA file transfer...")

    bucket_src, key_src = iotools.split_path_bucket_key(
        config["data_paths"]["input"]["dna_path"]
    )
    bucket_dest, key_dest = iotools.split_path_bucket_key(
        config["data_paths"]["output"]["edw_dna_path"]
    )
    iotools.s3_copy_managed(
        bucket_src,
        key_src,
        key_dest,
        bucket_dest,
    )

    logging.info("Completed Member DNA file transfer.")


if __name__ == "__main__":
    main(load_config(parse()))
