import argparse
import logging
import os

from memberdna.lib.job_manager import JobManager
from memberdna.source_etl.utils.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table item_cost")

    source_path = data_paths["source"]["item_cost"]

    cast_sql = """
    select
         ARTICLE_NBR
        ,cast(SITE_NBR as int) as SITE_NBR
        ,cast(DIST_CHANNEL as int) as DIST_CHANNEL
        ,cast(SITE_LANDED_COST as double) as SITE_LANDED_COST
        ,cast(EFF_DT as date) as EFF_DT
        ,cast(EXP_DT as date) as EXP_DT
    from df
    """
    dest_path = data_paths["intermediate"]["item_cost"]
    repartition_val = 1

    createSchemaParquet(
        job.spark,
        source_path,
        config_validation,
        "item_cost",
        cast_sql,
        dest_path,
        repartition_val,
        sep=",",
        quote="'",
    )


job = JobManager("item_cost")
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
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths, config_validation)
