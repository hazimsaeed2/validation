"""
Contains the BBM propensity model ETL
"""

import argparse
import os

from pe_memberdna.model.bbm_propensity_model.lib import bbm_etl_utils
from pe_memberdna.model.bbm_propensity_model.lib.managers import JobManager


def run(job):
    """
    Calculate and write the training data for the BBM propensity models

    Args:
        job (JobManager) - an active JobManager instance

    Returns:
    """

    bbm_etl_utils.create_datasets(job)

    print("ETL Done.")


if __name__ == "__main__":

    PARSER = argparse.ArgumentParser(
        description=(
            """
            ETL script for the BBM propensity model
            """
        )
    )

    PARSER.add_argument(
        "--config",
        type=str,
        default=os.path.normpath(
            os.path.join(
                os.path.dirname(__file__), "../conf/config_train.yaml"
            )
        ),
        help=(
            """
            path to the config file
            """
        ),
    )

    PARSER.add_argument(
        "--train_inference_choice",
        type=str,
        default="train",
        help=(
            """
            path to the config file
            """
        ),
    )

    ARGS = PARSER.parse_args()

    JOB = JobManager("BBM_propensity_model_etl", config_path=ARGS.config)

    if ARGS.train_inference_choice == "inference":
        JOB.config.params["train"] = False

    run(JOB)
