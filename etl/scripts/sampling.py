import argparse
import logging
import os

from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.s3 import input_data_validator
from pe_memberdna.etl.lib.utility import *


def selectSampledMembers(job, data_paths):
    out_path = data_paths["sampled"]["member"]

    member = job.spark.read.parquet(data_paths["intermediate"]["member"])
    print("member count: ", member.count())

    sampled_member = member.sample(False, 0.05, 3)
    print("sampled member count: ", sampled_member.count())

    print(f"Saving sample to {out_path}")
    sampled_member.write.parquet(
        data_paths["sampled"]["member"], mode="overwrite"
    )


def readSampledMemberIDs(job, data_paths):
    sampled_member = job.spark.read.parquet(data_paths["sampled"]["member"])
    sampled_member = sampled_member.select("MBRSHP_SID")
    sampled_member.cache()
    return sampled_member


def sampleIntermediate(job, intermediate_name, sampled_member, data_paths):
    df = job.spark.read.parquet(data_paths["intermediate"][intermediate_name])
    sampled_df = df.join(sampled_member, "MBRSHP_SID")
    print(intermediate_name, df.count(), sampled_df.count())

    if "FISCAL_WEEK_END" in sampled_df.schema.names:
        sampled_df.repartition("FISCAL_WEEK_END").write.parquet(
            data_paths["sampled"][intermediate_name],
            partitionBy="FISCAL_WEEK_END",
            mode="overwrite",
        )
    else:
        sampled_df.write.parquet(
            data_paths["sampled"][intermediate_name], mode="overwrite"
        )


def main(job, data_paths):

    # running the line directly below gets a new sample of members. if you want continuity in the sample (same members) comment out next line
    selectSampledMembers(job, data_paths)

    sampled_member = readSampledMemberIDs(job, data_paths)

    non_member_intermediates = {
        intermediate_name
        for intermediate_name in data_paths["sampled"]
        if intermediate_name != "member"
    }
    for intermediate_name in non_member_intermediates:
        print("sampling ", intermediate_name)
        sampleIntermediate(job, intermediate_name, sampled_member, data_paths)


job = JobManager("sampling")
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
