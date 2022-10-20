import argparse
import logging
import os

from memberdna.lib.job_manager import JobManager
from memberdna.source_etl.utils.utility import *


def main(job, data_paths, config_validation):
    # job.
    # logging.info("Starting processing table brand")

    source_path = data_paths["source"]["brand"]

    cast_sql = """
    select
         _c0 as ARTICLE_NBR
        ,_c1 as CASE_EXPRESSION
        ,_c2 as BRAND
    from df
    """

    dest_path = data_paths["intermediate"]["brand"]

    repartition_val = 1

    createSchemaParquet(
        job.spark,
        source_path,
        config_validation,
        "brand",
        cast_sql,
        dest_path,
        repartition_val,
        sep=",",
        header=False,
    )
    logging.info("DONE.")


parser = argparse.ArgumentParser()


parser.add_argument("--prod-mode", dest="prod_mode", action="store_true")
parser.add_argument(
    "config_path",
    nargs="?",
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../configs/config.yaml",
    ),
)
parser.set_defaults(prod_mode=True)
args = parser.parse_args()
job = JobManager("brand")
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths, config_validation)
