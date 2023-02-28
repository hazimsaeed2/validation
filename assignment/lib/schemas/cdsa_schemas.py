"""CDSA schemas."""

from pyspark.sql.types import (
    StringType,
    StructField,
    LongType,
    StructType,
    DateType,
    DoubleType,
)


CAMPAIGNS_SCHEMA = StructType(
    [
        StructField("experiment_id", LongType(), False),
        StructField("experiment_desc", StringType(), True),
        StructField("fiscal_year", LongType(), True),
        StructField("fiscal_num", LongType(), True),
        StructField("channel", StringType(), True),
        StructField("expected_distribution", LongType(), True),
    ]
)


CELLS_SCHEMA = StructType(
    [
        StructField("cell_id", LongType(), False),
        StructField("experiment_id", LongType(), True),
        StructField("cell_name", StringType(), True),
        StructField("cell_desc", StringType(), True),
        StructField("inhome_date", StringType(), True),
        StructField("cell_start", StringType(), True),
        StructField("cell_end", StringType(), True),
        StructField("construct_id", StringType(), True),
        StructField("bf_construct_id", StringType(), True),
        StructField("ctrl_flag", LongType(), True),
        StructField("test_flag", LongType(), True),
        StructField("sorting_order", LongType(), True),
        StructField("cell_size", DoubleType(), True),
        StructField("estimated_size", DoubleType(), True),
        StructField("segment_id", StringType(), True),
        StructField("longitudinal_id", LongType(), True),
        StructField("is_primary", LongType(), True),
    ]
)


CONSTRUCTS_SCHEMA = StructType(
    [
        StructField("construct_id", LongType(), False),
        StructField("construct_desc", StringType(), True),
        StructField("num_offers", LongType(), True),
        StructField("construct_json", StringType(), True),
    ]
)


SEGMENTS_SCHEMA = StructType(
    [
        StructField("segment_id", LongType(), False),
        StructField("segment_desc", StringType(), True),
        StructField("segment_json", StringType(), True),
    ]
)


HANDSHAKES_SCHEMA = StructType(
    [
        StructField("comparison_id", LongType(), False),
        StructField("cell_id", LongType(), True),
        StructField("comparison_desc", StringType(), True),
        StructField("comparison_base_flag", LongType(), True),
    ]
)


CDSA = {
    "campaigns": CAMPAIGNS_SCHEMA,
    "cells": CELLS_SCHEMA,
    "constructs": CONSTRUCTS_SCHEMA,
    "segments": SEGMENTS_SCHEMA,
    "handshakes": HANDSHAKES_SCHEMA,
}
