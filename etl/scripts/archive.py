import argparse
import datetime
import logging
import os
import subprocess
from urllib.parse import urlparse

from pe_memberdna.lib.job_manager import JobManager


def main(job, data_paths):
    """
    Creates a dated copy of pipeline intermediates into the specified
    archive directory.

    Args:
        data_paths - dictionary structure of yarn config file containing
        the paths
    """

    if "archived" in data_paths:
        logging.info("Archiving pipelined intermediates")
        for item_name, item_path in data_paths["intermediate"].items():
            logging.info("Archiving " + item_name)
            parsed_s3_path = urlparse(item_path)
            key = parsed_s3_path.path
            archive_path = data_paths["archived"] + key
            logging.info(f"Archiving to {archive_path}")
            subprocess.check_call(
                [
                    "aws",
                    "s3",
                    "cp",
                    item_path,
                    archive_path,
                    "--recursive",
                    "--sse",
                    "aws:kms",
                ],
                stderr=subprocess.STDOUT,
            )
    else:
        logging.info("Archiving is turned OFF")


job = JobManager("Archive")
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
args = parser.parse_args()
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths)
