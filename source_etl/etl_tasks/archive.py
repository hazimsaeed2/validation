import datetime
import logging
import subprocess
from urllib.parse import urlparse


def main(data_paths):
    """
    Creates a dated copy of pipelined intermediates into the specified
    archive directryself.

    Args:
        data_paths - dictionary structure of yarm config file containing
        the paths
    """

    if "archived" in data_paths:
        logging.info("Archiving pipelined intermediates")
        for item_name, item_path in data_paths["intermediate"].items():
            logging.info("Archiving " + item_name)
            parsed_s3_path = urlparse(item_path)
            key = parsed_s3_path.path
            archive_path = data_paths["archived"] + key
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
