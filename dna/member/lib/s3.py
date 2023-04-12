"""
Generic S3 helper functions for dna.
input_data_validator()
"""
from pe_memberdna.lib.s3 import input_data_validator


def member_dna_input_data_validator(
    job, recency_lookback_duration, data_paths
):
    """
    Check for existence and last Modified timestamp of
    file on s3 location against the threshold

    Args:
        job (JobManager): JobManager instance to run.
        recency_lookback_duration (int) - recency threshold in number of days
        data_paths - dict structure containing the source and intermediate path
    """
    global_lookback_duration = recency_lookback_duration.get("global", 0)
    dataset_local_recency_duration = recency_lookback_duration.get("local", {})
    for data_path in data_paths:
        final_recency_duration = dataset_local_recency_duration.get(
            data_path, global_lookback_duration
        )
        data_path = job.config.paths[data_path]
        input_data_validator(final_recency_duration, data_path)
