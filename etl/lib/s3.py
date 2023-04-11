"""
Generic S3 helper functions for etl.
input_data_validator()
"""
from pe_memberdna.lib.s3 import input_data_validator


def etl_input_data_validator(
    path_dir, recency_lookback_duration, data_paths, recency_paths
):
    """
    Check for existence and last Modified timestamp of
    file on s3 location against the threshold

    Args:
        job (JobManager): JobManager instance to run.
        recency_lookback_duration (int) - recency threshold in number of days
        data_paths - dict structure containing the source and intermediate paths
        recency_paths(list) - list of paths which are subjected to existence
                            and freshness check; can contain one of more elements
        path_dir (str) - path dir
    """
    global_lookback_duration = recency_lookback_duration.get("global", 0)
    dataset_local_recency_duration = recency_lookback_duration.get("local", {})
    for data_path in recency_paths:
        final_recency_duration = dataset_local_recency_duration.get(
            data_path, global_lookback_duration
        )
        data_path = data_paths[path_dir][data_path]
        input_data_validator(final_recency_duration, data_path)
