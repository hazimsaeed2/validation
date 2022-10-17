import argparse
import logging
import pprint

from pyspark.sql import SparkSession
import yaml

import memberdna.source_etl.etl_tasks.archive as archive
import memberdna.source_etl.etl_tasks.awards as awards
import memberdna.source_etl.etl_tasks.awards_fiscal as awards_fiscal
import memberdna.source_etl.etl_tasks.brand as brand
import memberdna.source_etl.etl_tasks.census_tract as census_tract
import memberdna.source_etl.etl_tasks.club as club
import memberdna.source_etl.etl_tasks.club_square as club_square
import memberdna.source_etl.etl_tasks.control_files as control_files
import memberdna.source_etl.etl_tasks.coupon_clip as coupon_clip
import memberdna.source_etl.etl_tasks.coupon_clip_fiscal as coupon_clip_fiscal
import memberdna.source_etl.etl_tasks.detail as detail
import memberdna.source_etl.etl_tasks.detail_isnr_fiscal as detail_isnr_fiscal
import memberdna.source_etl.etl_tasks.detail_gas_nr_fiscal as detail_gas_nr_fiscal
import memberdna.source_etl.etl_tasks.detail_fiscal as detail_fiscal
import memberdna.source_etl.etl_tasks.email as email
import memberdna.source_etl.etl_tasks.email_fiscal as email_fiscal
import memberdna.source_etl.etl_tasks.fiscal_days as fiscal_days
import memberdna.source_etl.etl_tasks.header as header
import memberdna.source_etl.etl_tasks.header_fiscal as header_fiscal
import memberdna.source_etl.etl_tasks.item as item
import memberdna.source_etl.etl_tasks.item_add_brand as item_add_brand
import memberdna.source_etl.etl_tasks.member_extended as member_extended
import memberdna.source_etl.etl_tasks.member as member
import memberdna.source_etl.etl_tasks.member_history as member_history
import memberdna.source_etl.etl_tasks.payment as payment
import memberdna.source_etl.etl_tasks.payment_fiscal as payment_fiscal
import memberdna.source_etl.etl_tasks.quotient_id as quotient_id
import memberdna.source_etl.etl_tasks.sampling as sampling
import memberdna.source_etl.etl_tasks.skeleton as skeleton
from memberdna.dna.lib.utils import get_latest_path

from memberdna.lib.misc import get_last_fiscal_weekend
from memberdna.lib.misc import today_helper


def parse():
    """
    Parse command line arguments

    Args:

    Returns:
        args - object containing the command line arguments as attrubutes
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--single-task", dest="single_task", nargs="?")
    parser.add_argument("--prod-mode", dest="prod_mode", action="store_true")
    parser.add_argument(
        "config_path", nargs="?", default="configs/config.yaml"
    )
    parser.set_defaults(prod_mode=True)
    args = parser.parse_args()

    return args


def load_config(args):
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

    for data_name, data_path in config["data_paths"]["source"].items():
        config["data_paths"]["source"][data_name] = data_path % {
            "curr_date": end_date_str_no_dashes
        }

    config["single_task"] = args.single_task

    return config


def main(config):
    """
    ETL pipeline

    Generate intermediates which support rest of segmented marketing
    process (incl. DNAs, modeling, offer build, assignment). Input/output S3 locations
    dictated by configs/data_paths.yaml content.

    Run via spark-submit:
        spark-submit --conf spark.sql.shuffle.partitions=800 --conf spark.dynamicAllocation.enabled=true --executor-memory 80g --executor-cores 5 run.py

    """

    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    spark = SparkSession.builder.appName("source_etl").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite","CORRECTED")
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    data_paths = config["data_paths"]
    club_square_config = config["club_square_config"]
    config_validation = config["validation"]
    pprint.pprint(config)

    data_paths["archived"] = (
        data_paths["archived"] + "/" + "{:%Y-%m-%d}".format(today_helper())
    )

    if config["single_task"]:
        feature_list = __import__("etl_tasks")
        feature_test = getattr(feature_list, config["single_task"])
        feature_test.main(spark, data_paths)
    else:
        item.main(spark, data_paths, config_validation)
        brand.main(spark, data_paths, config_validation)
        # depends on item, brand, AH5, item_with_brand
        item_add_brand.main(spark, data_paths, config_validation)
        header.main(spark, data_paths, config_validation)
        detail.main(spark, data_paths, config_validation)
        payment.main(spark, data_paths, config_validation)

        fiscal_days.main(spark, data_paths, config_validation)

        # depends on header, fiscal_days
        header_fiscal.main(spark, data_paths, config_validation)
        # depends on detail, header_fiscal, item
        detail_fiscal.main(spark, data_paths, config_validation)
        # depends on detail_fiscal, requires 20G exec mem
        detail_isnr_fiscal.main(spark, data_paths, config_validation)
        # depends on detail_fiscal
        detail_gas_nr_fiscal.main(spark, data_paths, config_validation)
        # depends on payment, header_fiscal
        payment_fiscal.main(spark, data_paths, config_validation)

        member.main(spark, data_paths, config_validation)
        member_extended.main(spark, data_paths, config_validation)
        census_tract.main(spark, data_paths, config_validation)
        member_history.main(spark, data_paths, config_validation)
        # depends on header_fiscal, fiscal_days
        skeleton.main(spark, data_paths, config_validation)

        # depends on member extended
        email.main(spark, data_paths, config_validation)
        email_fiscal.main(spark, data_paths, config_validation)
        # depends on detail_fiscal
        club.main(spark, data_paths, config_validation)
        # depends on header_fiscal, detail_fiscal, club
        club_square.main(
            spark, data_paths, club_square_config, config_validation
        )

        coupon_clip.main(spark, data_paths, config_validation)
        coupon_clip_fiscal.main(spark, data_paths, config_validation)

        awards.main(spark, data_paths, config_validation)
        awards_fiscal.main(spark, data_paths, config_validation)
        control_files.main(
            spark, data_paths, club_square_config, config_validation
        )

        # depends on member extended
        quotient_id.main(spark, data_paths, config_validation)

        sampling.main(spark, data_paths)
        archive.main(data_paths)


if __name__ == "__main__":
    main(load_config(parse()))
