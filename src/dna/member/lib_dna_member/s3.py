"""
Generic S3 helper functions for etl.
input_data_validator()
"""

from lib.s3 import etl_input_table_validator


def member_dna_input_data_validator(
    *tables_list,
    recency_lookback_duration,
    spark
):
    etl_input_table_validator(
        *tables_list,
        recency_lookback_duration=recency_lookback_duration,
        spark=spark
    )
