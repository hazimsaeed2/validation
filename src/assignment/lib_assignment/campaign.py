"""Campaign object for handling experimental input to offer assignment.

TODO: Refactor parsing and ingestion along construct/segment lines (i.e. ingest/parse)?
      abstract ingestion to subclass?
      confirm that coupons are valid for length of experiment
      handle basket coupons
      support variable filters on CF (==, >=, <=)
      Better find fiscal week end, handle sparse data

"""
import copy
import functools
import itertools
import re
from collections import OrderedDict

import numpy as np
import pandas as pd
import pyspark.sql.functions as sqlf
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W

import lib_assignment.checks as checks
import lib_assignment.ingest as ig
import lib_assignment.schemas.cdsa_schemas as cdsa_schemas
from lib_assignment.assn_utils import (
    CONSTRUCT_COLUMN_EXT,
    CONSTRUCT_COLUMN_EXT_BACKFILL,
    LAYOUT_ID_MATCH,
    apply_offer_recency,
    at_least_one_filter,
    calc_overlapping_cols,
    calc_sampling_seg,
    calc_sampling_value,
    decompose_construct,
    decompose_segment,
    deterministic_sample,
    downsample_rows,
    get_all_of_key,
    group_filters_by_segment,
    load_past_longitudinal_mbrs,
    palindrome_sample,
    read_subset_and_cast,
    env_path
)
from lib_assignment.filters import filter_functions
from lib_assignment.ingest import (
    broadcast_filter,
    join_category_agnostic,
)
from lib_assignment.slots import fill_slot, offer_data_to_list
from lib.iotools import (
    read_s3_to_local,
    split_path_bucket_key,
    write_json_to_s3,
    load_json_as_dict
)
from lib.spark_util import (
    get_logger,
    truncate_history,
    union_with_mismatched_columns,
)
from lib.utils import (
    apply_unionall,
    convert_id_cols_to_str,
    stack,
)

spark = SparkSession.builder.getOrCreate()
# sparkContext = spark.sparkContext
# log = get_logger("campaign")

# ---- HELPERS ---- #


def global_min(df_cols, missing_value_exclude=True):
    """
    find the minimun value of each rows from the input columns.
    if there is null value in input columns,
        if missing_value_exclude == True: exclude null and find the min value of the
                                            rest of the columns for that row.
                                          if all columns are null, return null

        if missing_value_exclude == False: return null for that row

    Parameters:
        df_cols: a pyspark dataframe with targeted columns
        missing_value_exclude: True or false

    Returns:
        a column that contains the min value of the input columns
    """

    if missing_value_exclude:
        return functools.reduce(
            lambda x, y: sqlf.when(x.isNull() & y.isNull(), x)
            .when(x.isNull(), y)
            .when(y.isNull(), x)
            .when(x < y, x)
            .otherwise(y),
            [sqlf.col(c) if isinstance(c, str) else c for c in df_cols],
        )
    else:
        return functools.reduce(
            lambda x, y: sqlf.when(x < y, x).otherwise(y),
            [sqlf.col(c) if isinstance(c, str) else c for c in df_cols],
        )


def _dedupe_sources(source_list):
    """Deduplicate data sources to prevent extra computation.

    For when multiple constructs or segments need the same data!

    Parameters:
        source_list (list[obj]): list of data source objects

    Returns:
        clean_list (list[obj]): list of clean data source objects
    """
    clean_list = [source_list[0]]
    for obj1 in source_list[1:]:
        match = False
        for obj2 in clean_list:
            name = obj1["name"] == obj2["name"]
            ftype = obj1["ftype"] == obj2["ftype"]
            path = obj1["path"] == obj2["path"]
            condition = name and ftype and path
            # check if sources are the same, join columns if the same
            if condition:
                match = True
                all_cols = list(set(obj1["cols"] + obj2["cols"]))
                obj2["cols"] = all_cols
        # append if no match to anything
        if not match:
            clean_list.append(obj1)
    return clean_list


def _exclude_exp(df, exld_exp_channel, exld_exp_days):
    """exclude the coupons if the same offer exposure within x days from a list of channels

    Parameters:
        df (pyspark.sql.DataFrame): member offer data for filtering
        exld_exp_channel (list[str]): a list of channels for excluding exposures, collate to the channels in campaigns.csv
        exld_exp_days (int): # days for controlling exposures

    Returns:
        df (pyspark.sql.DataFrame): member offer data after filtering
    """
    exld_exp_channel = list(
        set(df.columns) & set([x.upper() for x in exld_exp_channel])
    )

    if (len(exld_exp_channel) > 0) & (exld_exp_days is not None):
        df = df.withColumn("last_exp_days", global_min(df[exld_exp_channel]))
        no_exp = df.last_exp_days.isNull()
        beyond_threshold_exp = df.last_exp_days > exld_exp_days
        df = df.filter(no_exp | beyond_threshold_exp)

    return df


def _run_slot(
    memberdata,
    assignment_pools,
    coupon_pools,
    slot_group,
    total_coupons,
    backfill=False,
    seed=0,
):
    """Run a given slot.

    Fill member_data with a slot given the slot JSON parameters.

    Parameters:
        memberdata (pyspark.sql.DataFrame): member data for joining to
        assignment_pools (dict): assignment after ingestion
        coupon_pools (dict): coupons after ingestion
        slot_group (dict): the slot group configuration which will be used
        total_coupons (int): number of coupons to consider
        backfill (bool): whether the is backfill slot or not
    Returns:
        assignments (pyspark.sql.DataFrame): members with ranked coupons
    """

    slot_offer_data_type = offer_data_to_list(slot_group)
    print("Calculating offer data for slot group.")
    if backfill:
        # Backfill functions expect one type of offer data
        slot_offer_data_type = [slot_offer_data_type[0]]

    slot_subset_data = copy.copy(assignment_pools)
    if isinstance(slot_group.get("exclude_exp_channel"), dict):
        print("need to avoid back to back for specific offer data: ")
        slot_subset_data = dict()
        for offer_type in slot_offer_data_type:
            if offer_type in slot_group.get("exclude_exp_channel").keys():
                slot_subset_data[offer_type] = _exclude_exp(
                    assignment_pools[offer_type],
                    slot_group.get("exclude_exp_channel")[offer_type],
                    slot_group.get("exclude_exp_days"),
                )
            else:
                slot_subset_data[offer_type] = assignment_pools[offer_type]
    elif slot_group.get("exclude_exp_channel") is not None:
        print("need to avoid back to back for all offer data: ")
        slot_subset_data = dict()
        for offer_type in slot_offer_data_type:
            slot_subset_data[offer_type] = _exclude_exp(
                assignment_pools[offer_type],
                slot_group.get("exclude_exp_channel"),
                slot_group.get("exclude_exp_days"),
            )

    slot_subset_data = functools.reduce(
        lambda x, y: union_with_mismatched_columns(x, y),
        [slot_subset_data[data_type] for data_type in slot_offer_data_type],
    )

    coupon_subset = functools.reduce(
        lambda x, y: union_with_mismatched_columns(x, y),
        [coupon_pools[data_type] for data_type in slot_offer_data_type],
    )

    print(
        "Calculating {} with {}...".format(
            slot_group["offer_data"], slot_group["slot_type"]
        )
    )

    filled_slot = fill_slot(
        memberdata,
        slot_subset_data,
        coupon_subset,
        slot_group,
        total_coupons,
        backfill,
        seed,
        dedupe=(slot_group["pool_type"] == Campaign.PoolType.LAYOUT),
    )
    filled_slot = filled_slot.withColumn("cpn_nbr", filled_slot.cpn_nbr.cast("string"))

    if "is_backfill" not in filled_slot.columns:
        if not backfill:
            slot_structure = Campaign.SLOT_STRUCTURE
        else:
            slot_structure = Campaign.BF_SLOT_STRUCTURE

        filled_slot = filled_slot.withColumn(
            "slot_structure",
            sqlf.format_string(
                slot_structure,
                sqlf.lit(slot_group["layout_id"]),
                filled_slot["rank"] - sqlf.lit(1),
                sqlf.lit(slot_group["slot_num"]),
                sqlf.lit(slot_group["parent_construct"]),
            ),
        )
    else:
        # check note 1000001 to understand
        # why is_backfill exists
        filled_slot = filled_slot.withColumn(
            "slot_structure",
            sqlf.when(
                (filled_slot.is_backfill == 0),
                sqlf.format_string(
                    Campaign.SLOT_STRUCTURE,
                    sqlf.lit(slot_group["layout_id"]),
                    filled_slot["rank"] - sqlf.lit(1),
                    sqlf.lit(slot_group["slot_num"]),
                    sqlf.lit(slot_group["parent_construct"]),
                ),
            ).otherwise(
                sqlf.format_string(
                    Campaign.BF_SLOT_STRUCTURE,
                    sqlf.lit(slot_group["layout_id"]),
                    filled_slot["rank"] - sqlf.lit(1),
                    sqlf.lit(slot_group["slot_num"]),
                    sqlf.lit(slot_group["parent_construct"]),
                )
            ),
        )

    filled_slot = filled_slot.select("MBRSHP_SID", "cpn_nbr", "slot_structure")

    assignments = memberdata.select("MBRSHP_SID").join(
        filled_slot, "MBRSHP_SID", "left"
    )
    print("Finished calculating offer for slot group: %s" % slot_group)

    return assignments


def _read_and_subset_exp_csv(experiment_name, experiment_path, columns=None, vol_base=None, env="dev"):
    """Read in master campaign csv and subset to relevant campaign.

    Parameters:
        experiment_name(str): name of experiment for filtering
        experiment_path(str): path to read from

    Returns:
        subset (pd.DataFrame): subsetted DataFrame
    """
    df = read_s3_to_local(env_path(experiment_path, vol_base, env), ftype="csv", columns=columns)
    rel_df = df[df.experiment_id == experiment_name]
    return rel_df


def _parse_campaign_details(experiment_name, experiment_path, vol_base=None, env="dev"):
    """Parse experiment detail table.

    Parse input campaign input into useful
    parameters for sharing.

    Parameters:
        experiment_name(str): name of experiment for filtering
        experiment_path(str): path of experiments table

    Returns:
        description (str): campaign description
        year (int): fiscal year for campaign
        number (int): campaign number for fiscal year
        channel (str): channel description
        size (int): total size of campaign
    """
    rel_dtls = _read_and_subset_exp_csv(experiment_name, experiment_path, vol_base=vol_base, env=env)
    description = rel_dtls.experiment_desc.iloc[0]
    year = int(rel_dtls.fiscal_year.iloc[0])
    number = int(rel_dtls.fiscal_num.iloc[0])
    channel = rel_dtls.channel.iloc[0]
    size = int(rel_dtls.expected_distribution.iloc[0])
    return (description, year, number, channel, size)


def _slot_size_error_catch(slot, str_construct):
    """Check if the given slot contains a slot_size parameter.

    Parameters:
        slot(dict): slot dictionary to check
        str_construct(str): construct_id from which the slot is from
                            (to aid the user in debugging)

    Returns:
        Nothing. Raises an error if slot does not exist or does not
        contain a slot_size parameter.
    """
    try:
        slot["slot_size"]
    except KeyError as e:
        print(
            ("Construct_id {0} does not have a specified {1}".format(str_construct, e))
        )
        raise
    if slot is None:
        print(
            (
                "Construct_id {0} does not have a specified slot_size".format(
                    str_construct
                )
            )
        )
        raise


def _build_layout_id(construct_id, bf_construct_id):
    """Builds a str object which represents a unique identifier for a layout
    (construct + bf construct).

    Parameters:
        construct(str): a simple or multi construct
        bf_construct_id(str): a simple or multi construct

    Returns:
        layout_id (str): a string representing the combination of the 2
                        constructs
    """
    construct_ids = decompose_construct(str(construct_id))
    if construct_ids[0] is None:
        raise Exception("Error while decomposing construct ids")

    construct_ids = "_".join(construct_ids)
    bf_construct_ids = decompose_construct(str(bf_construct_id))
    if bf_construct_ids[0] is None:
        bf_construct_ids = construct_ids
    else:
        bf_construct_ids = "_".join(bf_construct_ids)

    layout_id = "{construct_id}__{bf_construct_id}".format(
        construct_id=construct_ids, bf_construct_id=bf_construct_ids
    )
    return layout_id


def _build_construct_structure(construct, paths, vol_base=None, env="dev"):
    """Builds a dict object composed of all information read from all ids found
    in construct.

    Parameters:
        construct(str): a simple or multi construct
        paths(dict): a dict containing the campaign main paths

    Returns:
        construct_structure (dict): a dict object representing the construct
    """
    construct_ids = decompose_construct(str(construct))

    construct_structure = None
    for construct_id in construct_ids:
        fpath = paths["CONSTRUCT_BANK"] + construct_id + ".json"
        print("parsing construct {}".format(construct_id))
        single_json_data = load_json_as_dict(env_path(fpath, vol_base, env))

        for slot in single_json_data["slots"]:
            _slot_size_error_catch(slot, construct_id)

        data_source = single_json_data.get("data_sources", [])
        slots = sorted(
            single_json_data["slots"],
            key=lambda slot_group: slot_group["slot_num"],
        )
        for group in slots:
            group["pool_type"] = group.get("pool_type", Campaign.PoolType.LAYOUT)
            group["parent_construct"] = int(construct_id)

        # using both eval and str the map can be passed
        # as both a string or a dict object
        # OrderedDict is imported at the top because it is used by eval
        slot_map = eval(str(single_json_data.get("slot_map", "{}")))
        slot_map = {int(key): int(value) for key, value in slot_map.items()}
        sort = single_json_data.get("sort", [])

        if construct_structure is None:
            construct_structure = single_json_data
            construct_structure["slot_map"] = slot_map
            construct_structure["sort"] = sort
            construct_structure["data_sources"] = data_source
        else:
            construct_structure["slot_map"] = slot_map

            for column in sort:
                if column not in construct_structure["sort"]:
                    construct_structure["sort"].append(column)

            construct_structure["slots"].extend(slots)
            construct_structure["data_sources"].extend(data_source)

    if len(construct_structure["data_sources"]) > 1:
        construct_structure["data_sources"] = _dedupe_sources(
            construct_structure["data_sources"]
        )

    # update user input into the default slot mapping
    number_of_slots = sum([int(x["slot_size"]) for x in construct_structure["slots"]])
    default_slot_map = {slot: slot for slot in range(1, number_of_slots + 1)}
    slot_map = dict(default_slot_map)
    slot_map.update(construct_structure["slot_map"])

    if (
        max(slot_map.keys()) > number_of_slots
        or max(slot_map.values()) > number_of_slots
    ):
        raise Exception(
            "Construct {construct} slot_map has slot numbers greater than the"
            " slot set size.".format(construct=construct)
        )

    if set(slot_map.keys()) != set(slot_map.values()):
        raise Exception(
            "Construct {construct} slot_map is incomplete. Each mapping must"
            " be specified as a pair: {{a:b, b:a}}.".format(construct=construct)
        )

    is_default_mapping = slot_map == default_slot_map
    construct_structure["slot_map"] = slot_map

    # if the current construct is a multi construct
    # save it to disc for debugging
    if len(construct_ids) > 1:
        construct_structure["construct_id"] = construct
        slotnum_spot = 0
        for slot in construct_structure["slots"]:
            slot["slot_num"] = slotnum_spot
            slotnum_spot += int(slot["slot_size"])

        write_path = "{construct_bank}{construct_name}.json".format(
            construct_bank=paths["CONSTRUCT_BANK"], construct_name=construct
        )
        # bucket, key = split_path_bucket_key(write_path)
        write_json_to_s3(write_path, construct_structure, vol_base, env)
    else:
        construct_structure["construct_id"] = construct_ids[0]

    # setting is_default_mapping after write to disc since this is not
    # part of the construct keys
    construct_structure["is_default_mapping"] = is_default_mapping

    if not is_default_mapping and construct_structure.get("sort"):
        print(
            "WARNING: Construct {construct} has both mapping and sort"
            " specified".format(construct=construct)
        )

    return construct_structure


def _check_constructs(constructs_to_check, master_constructs_list):
    """Check if constructs exist in construct table.

    Checking if the construct_id's exist in the construct table.

    Parameters:
        constructs_to_check(set): constructs to be assigned
        master_constructs_list(list): master table of constructs

    Returns:
        Nothing!
    """
    check_list = [
        int(construct_id)
        for construct in constructs_to_check
        for construct_id in decompose_construct(construct)
    ]

    for constr_id in check_list:
        if constr_id in master_constructs_list:
            continue
        else:
            raise ValueError("{0} is not an existing construct_id!".format(constr_id))
    print("All construct_id's exist")


def _clean_constructs(cell_constructs):
    """Reformat a list of tuples containing cell and multi constructs to a list
    of tuples containing the cell id and the constructs split into
    individual ids.
    If one id from the multi construct is invalid return None for that
    construct.

    Parameters:
        cell_constructs (list(tuple)): list of (cell, frontfill_construct,
                                       backfill_construct)

    Returns:
        clean_constr_list (list(tuple)): list of (cell,
                                         (frontfill_construct,
                                          backfill_construct))
    """
    clean_constr_list = list()

    for cell_id, construct_id, bf_construct_id in cell_constructs:
        construct_ids = decompose_construct(str(construct_id))
        bf_construct_ids = decompose_construct(str(bf_construct_id))

        if len(construct_ids) > 1:
            construct = construct_id
        else:
            construct = construct_ids[0]

        if len(bf_construct_ids) > 1:
            bf_construct = bf_construct_id
        else:
            bf_construct = bf_construct_ids[0]

        clean_constr_list.append((cell_id, (construct, bf_construct)))

    return clean_constr_list


def _parse_constructs(cells, paths, backfill_default_constructs, vol_base=None, env="dev"):
    """Parse construct data.

    Parse input tables into relevant construct
    data for application to member dataframe.

    Parameters:
        paths (dict[str]): paths object for experiment paths
        cells (pandas.DataFrame): dataframe object describing experiment cells

    Returns:
        construct_list (list[list[dict]]): list of relevant constructs for application
        source_list (list[obj]): list of sources for joining to offers
        slot_list (list[obj]): list of slots to fill for all constructs
    """
    constr_list = cells[["cell_id", "construct_id", "bf_construct_id"]].values.tolist()

    clean_constr_list = _clean_constructs(constr_list)

    all_constructs = read_s3_to_local(env_path(paths["CONSTRUCT"], vol_base, env), ftype="csv")
    all_constructs_list = all_constructs.construct_id.astype(int).tolist()

    unique_constructs = set()
    for cell_id, construct_pair in clean_constr_list:
        for construct in construct_pair:
            if construct is not None:
                unique_constructs.add(construct)

    _check_constructs(unique_constructs, all_constructs_list)

    source_list = []
    slot_set_list = []
    lambda_list = []
    constructs = dict()

    for cell_id, construct_pair in clean_constr_list:
        for construct in construct_pair:
            if construct and construct not in constructs:
                constructs[construct] = _build_construct_structure(construct, paths, vol_base, env)
                print("added construct {} to construct_list".format(construct))
                construct_structure = constructs[construct]
                if len(construct_structure["data_sources"]) > 0:
                    source_list += construct_structure["data_sources"]

    unique_construct_pair = set()
    for cell_id, (construct, bf_construct) in clean_constr_list:
        construct_structure = constructs[construct]
        bf_construct_structure = constructs.get(bf_construct)

        if bf_construct_structure is None:
            # set default backfill construct
            bf_construct_structure = copy.deepcopy(construct_structure)
            for bf_slot_group in bf_construct_structure["slots"]:
                offer_data = bf_slot_group.get("offer_data")
                if isinstance(offer_data, list):
                    offer_data = offer_data[0]
                bf_slot_group["slot_type"] = backfill_default_constructs[offer_data]

        construct_pair = (
            construct_structure["construct_id"],
            bf_construct_structure["construct_id"],
        )

        if construct_pair not in unique_construct_pair:
            unique_construct_pair.add(construct_pair)
        else:
            continue

        slot_group_pair = itertools.zip_longest(
            construct_structure["slots"],
            bf_construct_structure["slots"],
            fillvalue=None,
        )

        slot_set = list()
        for slot_group, bf_slot_group in slot_group_pair:
            groups_match = (
                slot_group is not None
                and bf_slot_group is not None
                and slot_group["slot_num"] == bf_slot_group["slot_num"]
                and slot_group["slot_size"] == bf_slot_group["slot_size"]
            )
            if not groups_match:
                raise Exception("Constructs do not match")

            layout_id = _build_layout_id(
                construct_structure["construct_id"],
                bf_construct_structure["construct_id"],
            )

            parsed_group = copy.deepcopy(slot_group)
            parsed_bf_group = copy.deepcopy(bf_slot_group)

            lambda_list += get_all_of_key(parsed_group)
            lambda_list += get_all_of_key(parsed_bf_group)

            parsed_group["construct_id"] = str(construct_structure["construct_id"])
            parsed_group["layout_id"] = layout_id

            parsed_bf_group["construct_id"] = str(
                bf_construct_structure["construct_id"]
            )
            parsed_bf_group["layout_id"] = layout_id

            if (
                isinstance(slot_group["offer_data"], list)
                and len(slot_group["offer_data"]) > 1
            ):
                # check note 1000001 to understand
                # why parsed_bf_group can be empty
                slot_set.append(
                    {
                        Campaign.FillType.FF: parsed_group,
                        Campaign.FillType.BF: dict(),
                    }
                )
            else:
                slot_set.append(
                    {
                        Campaign.FillType.FF: parsed_group,
                        Campaign.FillType.BF: parsed_bf_group,
                    }
                )

        slot_set_list.append(
            {
                "groups": slot_set,
                "sort": construct_structure["sort"],
                "slot_map": construct_structure["slot_map"],
                "is_default_mapping": construct_structure["is_default_mapping"],
            }
        )

    if len(source_list) > 1:
        source_list = _dedupe_sources(source_list)

    return source_list, slot_set_list, list(set(lambda_list))


def _build_segment_structure(segment, paths, vol_base=None, env="dev"):
    """Builds a dict object composed of all information read from all ids found
    in segment.

    Parameters:
        segment(str): a simple or multi segment
        paths(dict): a dict containing the campaign main paths

    Returns:
        segment_structure (dict): a dict object representing the segment
    """
    segment_ids = decompose_segment(str(segment))

    segment_structure = None
    for segment_id in segment_ids:
        fpath = paths["SEGMENT_BANK"] + str(segment_id) + ".json"
        print("parsing segment {}".format(segment_id))
        single_json_data = load_json_as_dict(env_path(fpath, vol_base, env))

        data_source = single_json_data.get("data_sources", [])
        filters = single_json_data.get("filters", [])

        for filt in filters:
            filt["segment_id"] = str(segment_id)
            filt["filter_id"] = "s" + filt["segment_id"] + "f" + str(filt["filter_num"])

        if segment_structure is None:
            segment_structure = single_json_data
            segment_structure["data_sources"] = data_source
            segment_structure["filters"] = filters
        else:
            segment_structure["filters"].extend(filters)
            segment_structure["data_sources"].extend(data_source)

    if len(segment_structure["data_sources"]) > 1:
        segment_structure["data_sources"] = _dedupe_sources(
            segment_structure["data_sources"]
        )

    # if the current segment is a multi segment
    # save it to disc for debugging
    if len(segment_ids) > 1:
        segment_structure["segment_id"] = segment

        segment_to_save = copy.deepcopy(segment_structure)

        # clean filters
        filter_num = 0
        for filt in segment_to_save["filters"]:
            filt["filter_num"] = filter_num
            filter_num += 1
            del filt["filter_id"]
            del filt["segment_id"]

        write_path = "{segment_bank}{segment_name}.json".format(
            segment_bank=paths["SEGMENT_BANK"],
            segment_name=segment_to_save["segment_id"],
        )
        # bucket, key = split_path_bucket_key(write_path)
        write_json_to_s3(write_path, segment_to_save, vol_base, env)
    else:
        segment_structure["segment_id"] = segment_ids[0]

    return segment_structure


def _check_segments(segments_to_check, master_segments_list):
    """Check if segments exist in segments table.

    Checking if the segment_id's exist in the segments table.

    Parameters:
        segments_to_check(set): segments to be assigned
        master_segments_list(list): master table of segments

    Returns: None
    """
    check_list = set()
    for segment in segments_to_check:
        for segment_id in decompose_segment(str(segment)):
            if segment_id is not None:
                check_list.add(int(segment_id))

    for segment_id in check_list:
        if segment_id in master_segments_list:
            continue
        else:
            raise ValueError("{0} is not an existing segment_id!".format(segment_id))
    print("All segments_id's exist")


def _parse_segments(cells, paths, vol_base=None, env="dev"):
    """Parse segment data.

    Parse input tables into relevant segment
    data for application to member dataframe.

    Parameters:
        paths (dict[str]): paths object for experiment paths
        cells (pandas.DataFrame): dataframe object describing experiment cells

    Returns:
        segment_list (list[list[dict]]): list of relevant segments for
                                        application
        source_list (list[obj]): list of sources for member data
        filter_list (list[obj]): list of filters to apply for all segments
    """
    seg_list = cells.segment_id.dropna().tolist()

    all_segments = read_s3_to_local(env_path(paths["SEGMENT"], vol_base, env), ftype="csv")
    all_segments_list = all_segments.segment_id.astype(int).tolist()

    unique_segments = {str(segment) for segment in seg_list}

    _check_segments(unique_segments, all_segments_list)

    segments = dict()
    segment_list = []
    source_list = []

    for segment in unique_segments:
        if segment and segment not in segments:
            segments[segment] = _build_segment_structure(segment, paths, vol_base, env)
            print("added segment {} to segment_list".format(segment))

    unique_filters = dict()
    for segment in unique_segments:
        segment_structure = segments[segment]
        segment_list.append(segment_structure)
        data_source = segment_structure.get("data_sources")
        if data_source is not None:
            source_list += data_source

        for filt in segment_structure["filters"]:
            unique_filters[filt["filter_id"]] = filt

    if len(source_list) > 1:
        source_list = _dedupe_sources(source_list)

    filter_list = list(unique_filters.values())

    return segment_list, source_list, filter_list


def _append_source_data(offer_data, source, self, join_not_unique_key=None):
    """append extra source data to base offer data

    Append user specified source data to base offer table

    Parameters:
        offer_data (pyspark.sql.DataFrame): base form offer data to append to
        source (dict): dictionary of source data
        self (dict): dictionary of paths and parameters
        join_not_unique_key (int): when this sets to be 1, will skip the drop duplicate on joint key step

    Returns:
        offer_data (pyspark.sql.DataFrame): base table with extra cols from source data appended
    """
    self.offer_sources_df[source["name"]] = {}

    self.offer_sources_df[source["name"]]["data"] = read_subset_and_cast(
        env_path(self.paths[source["path"]], self.vol_base, self.env),
        source["ftype"],
        source.get("cols"),
        source.get("time_partition"),
        self.parameters["assignment_date"],
    )
    # apply relevant data source transforms and filters
    self.offer_sources_df[source["name"]]["merge_col"] = calc_overlapping_cols(
        self.offer_sources_df[source["name"]]["data"], offer_data
    )
    if join_not_unique_key == 1:
        pass
    else:
        merge_col = self.offer_sources_df[source["name"]]["merge_col"]
        data = self.offer_sources_df[source["name"]]["data"]
        hash_cols = [sqlf.coalesce(sqlf.col(c).cast("string"), sqlf.lit("__null__"))
                    for c in data.columns]
        data = data.withColumn("_dedup_hash", sqlf.sha2(sqlf.concat_ws("||", *hash_cols), 256))
        w = W.partitionBy(*merge_col).orderBy("_dedup_hash")
        data = (data.withColumn("_dedup_rank", sqlf.row_number().over(w))
                .filter(sqlf.col("_dedup_rank") == 1)
                .drop("_dedup_rank", "_dedup_hash"))
        self.offer_sources_df[source["name"]]["data"] = data

    self.offer_sources_df[source["name"]]["data"] = truncate_history(
        self.offer_sources_df[source["name"]]["data"], cache=True
    )

    offer_data = offer_data.join(
        self.offer_sources_df[source["name"]]["data"],
        self.offer_sources_df[source["name"]]["merge_col"],
        "left",
    )
    fillna = source.get("fillna")
    if fillna is not None:
        offer_data = offer_data.fillna(fillna, subset=source["cols"])

    return offer_data


def _assign_cell_sample(cell_sample, memberdata, cell):
    """
    Assigns the cell sample to the cell and removes it from the memberdata.

    Parameters:
        cell_sample (spark.sql.DataFrame): a portion of memberdata that will be
                                           assigned to the cell
        memberdata (spark.sql.DataFrame): data for members yet to be assigned
        cell (dict): dictionary of campaign information for the cell

    Returns:
        cell_sample (spark.sql.DataFrame): data for just assigned members
        memberdata (spark.sql.DataFrame): data for members w/o cell sample
    """

    cell_sample = cell_sample.withColumn("CELL_ID", sqlf.lit(cell["cell_id"]))

    layout_id = _build_layout_id(cell["construct_id"], cell["bf_construct_id"])

    cell_sample = cell_sample.withColumn("CONSTRUCT_ID", sqlf.lit(layout_id))

    cell_sample = truncate_history(cell_sample, True)
    memberdata = truncate_history(memberdata, True)

    memberdata = memberdata.join(cell_sample, "MBRSHP_SID", "leftanti")
    memberdata = truncate_history(memberdata, True)

    return cell_sample, memberdata


# ---- MAIN CLASS ---- #


class Campaign:
    """Parse experimental input into campaign object.

    Take experimental input tables and parse
    into relevant filters, constructs, and cells
    for campaign, as well as other campaign parameters,
    to be applied to member data table.
    """

    class FillType:
        FF = "frontfill"
        BF = "backfill"

    class FillTypeValue:
        FF = 0
        BF = 1

    class BackfillPriority:
        HIGH = "high"
        LOW = "low"

    SLOT_STRUCTURE = "c%ss%dg%dp%d"
    BF_SLOT_STRUCTURE = "c%ss%dg%dp%db"

    class PoolType:
        LAYOUT = "layout"
        UNIVERSAL = "universal"

    def __init__(self, parameters, paths, vol_base, env):
        """Initialize parser.

        Parameters:
            parameters (dict): parameters dictionary for campaign
            paths (dict): paths dictionary for campaign
        """
        self.parameters = parameters
        self.paths = paths
        self.vol_base = vol_base
        self.env = env.lower()
        # separate out the experiment parameter for user reference / readability
        self.experiment = self.parameters["experiment"]
        self.replace_with_null = self.parameters["replace_with_null"]
        self.backfill_method = self.parameters["backfill_method"]
        self.backfill_priority = self.parameters["backfill_priority"]
        (
            self.description,
            self.year,
            self.number,
            self.channel,
            self.size,
        ) = _parse_campaign_details(self.experiment, self.paths["CAMPAIGN"], self.vol_base, self.env)

        self.cells = _read_and_subset_exp_csv(
            self.experiment,
            self.paths["CELL"],
            columns=cdsa_schemas.CELLS_SCHEMA.names,
            vol_base = self.vol_base,
            env= self.env
        )

        if self.cells.construct_id.dtype == np.float64:
            self.cells.construct_id = self.cells.construct_id.astype(int)

        self.cells = convert_id_cols_to_str(
            self.cells, ["segment_id", "longitudinal_id"]
        )

        self.backfill_default_constructs = self.parameters["backfill_method"]

        self.offer_sources, self.slots, self.lambdas = _parse_constructs(
            self.cells, self.paths, self.backfill_default_constructs, self.vol_base, self.env
        )
        self.segments, self.member_sources, self.filters = _parse_segments(
            self.cells, self.paths, self.vol_base, self.env
        )
        self.offer_list = [source.get("offer_data") for source in self.offer_sources]

        self.offer_list += [
            slot_type.get("offer_data")
            for slot_set in self.slots
            for slot_group in slot_set["groups"]
            for slot_type in slot_group.values()
        ]

        self.offer_list = functools.reduce(
            list.__add__,
            [
                offer if isinstance(offer, list) else [offer]
                for offer in self.offer_list
            ],
        )

        self.offer_list = filter(lambda x: x is not None, self.offer_list)
        self.offer_sources_df = {}

        self.mbr_lkup = read_subset_and_cast(
            self.paths["RAW_MEMBER"],
            "table",
            subset_cols=["MBRSHP_NBR", "MBRSHP_SID"],
        )
        self.mbr_lkup.persist(StorageLevel.DISK_ONLY)

        mbr_data = read_subset_and_cast(self.paths["MAIL_LIST"], "csv")
        mbr_data.persist(StorageLevel.DISK_ONLY)

        # read in base data
        mbr_data = deterministic_sample(
            mbr_data, self.parameters["run_size"], ["mbrshp_sid"]
        ).cache()

        join_col = calc_overlapping_cols(mbr_data, self.mbr_lkup)
        self.mbr_data = mbr_data.join(self.mbr_lkup, join_col, "left").cache()

    def ingest_member_data(self):
        """Dynamic member data ingestion.

        Different campaigns require different initial datasources
            in order to calculate the relevant segments.
        This function takes a parameter/path set from configuration and uses
        it to dynamically read and ingest the desired datasources, joining
        into a member indexed table for application of filter assignment
        rules.

        Parameters:

        Returns:
            data (pyspark.sql.DataFrame): ingested aand joined member data for assignment
        """
        # read in ID lookup table to prep for join
        dna_cols = ["MBRSHP_SID", "DAYS_SINCE_LAST_TRIP", "TENURE"] + [
            self.parameters["sampling_weeks_rev_columns"]
        ]
        dna = read_subset_and_cast(
            self.paths["CUBE"],
            "table",
            dna_cols,
            "fiscal_week",
            self.parameters["assignment_date"],
        )
        # read in bbm score to prep for sampling
        bbmprop_cols = ["MBRSHP_NBR", "decile"]
        bbmprop = read_subset_and_cast(env_path(self.paths["MAIL_LIST"], self.vol_base, self.env), "csv", bbmprop_cols)

        # join with dna and calculate sampling value and segements
        join_col = calc_overlapping_cols(self.mbr_data, dna)
        dna = dna.dropDuplicates(subset=join_col)
        sampling_input = self.mbr_data.join(dna, join_col, "left")

        join_col = calc_overlapping_cols(self.mbr_data, bbmprop)
        bbmprop = bbmprop.dropDuplicates(subset=join_col)
        sampling_input = sampling_input.join(bbmprop, join_col, "left")

        sampling_value = calc_sampling_value(
            sampling_input, self.parameters["sampling_weeks_rev_columns"]
        )
        sampling_seg = calc_sampling_seg(sampling_input)

        mbr_data = self.mbr_data.join(sampling_value, "MBRSHP_SID", "left")
        mbr_data = mbr_data.join(sampling_seg, "MBRSHP_SID", "left")

        # read in other data sources and join
        for source in self.member_sources:
            if source.get('ftype') == 'parquet':
                source['ftype'] = 'table'
            print("ingesting {} to member data...".format(source["path"]))
            mbr_data = _append_source_data(mbr_data, source, self)
        mbr_data = mbr_data.dropDuplicates(subset=["MBRSHP_NBR", "MBRSHP_SID"])
        mbr_data = truncate_history(mbr_data, cache=True)

        print("Member data cached.")
        return mbr_data

    def ingest_offer_data(self):
        """Dynamic offer data ingestion.

        Different campaigns require different datasources
        in order to calculate the relevant constructs.
        This function takes a parameter/path set from configuration and uses
        it to dynamically read and ingest the desired datasources, joining
        into a member/offer indexed table for application of spot assignment
        rules.

        Parameters:

        Returns:
            assignment_pools, coupon_pools (list(dict)):
             ingested and joined offer data for assignment and coupons
        """
        mbr = self.mbr_data.select("MBRSHP_NBR", "MBRSHP_SID")

        # 2. Read in all data
        coups = read_subset_and_cast(env_path(self.paths["COUPON_BANK"], self.vol_base, self.env), "csv")
        coups_quals = read_subset_and_cast(env_path(self.paths["COUPON_QUALS"], self.vol_base, self.env), "csv")
        coups_map = read_subset_and_cast(env_path(self.paths["COUPON_MAP"], self.vol_base, self.env), "csv")
        if self.paths.get("COUPON_DISCOUNT"):
            discounts = read_subset_and_cast(env_path(self.paths["COUPON_DISCOUNT"], self.vol_base, self.env), "csv")
        else:
            discounts = None

        col_names = ["ARTICLE_NBR", "AH4_CD", "AH5_CD"]
        article_map = read_subset_and_cast(
            self.paths["ARTICLE_AH4_AH5_MAP"], "table", col_names
        )

        for names in col_names:
            article_map = article_map.withColumnRenamed(names, names.lower())
        article_map = article_map.withColumn('article_nbr', sqlf.col('article_nbr').cast('long'))

        trips = read_subset_and_cast(env_path(self.paths["COUPON_MEMTRIP"], self.vol_base, self.env), "parquet")
        trips = trips.dropna(subset=["cpn_nbr", "mbrshp_sid"])
        usage = read_subset_and_cast(env_path(self.paths["COUPON_MEMUSAGE"], self.vol_base, self.env), "parquet")
        cf_pred = read_subset_and_cast(self.paths["PRED_LIST"], "table")
        max_run_df = cf_pred.select(sqlf.max("RUN_NAME").alias("RUN_NAME"))
        # max_cf_run = cf_pred.agg(sqlf.max("RUN_NAME").alias('max_dt')).first()['max_dt']
        cf_pred = (
            cf_pred.join(max_run_df, on="RUN_NAME", how="inner")
                .drop("RUN_NAME", "CATEGORY_CD", "START_DATE", "END_DATE")
        )
        # cf_pred = cf_pred.filter(sqlf.col('RUN_NAME')==max_cf_run).drop('RUN_NAME', 'CATEORY_CD', 'START_DATE', 'END_DATE')

        # 3. Consolidate coupon data
        coups_quals = coups_quals[coups_quals.experiment_id == self.experiment]
        coups = coups.join(coups_quals, ["cpn_nbr"], "right")
        coups = coups.dropna(subset="cpn_nbr")
        coups = sqlf.broadcast(
            coups.select(
                "cpn_nbr",
                "cpn_type",
                "cpn_dollar_off",
                "cpn_dollar_threshold",
                "hero_eligible",
                "backfill_eligible",
                "offer_id",
                "cpn_class_id",
            )
        )
        article_map = article_map.dropDuplicates()
        article_map = article_map.filter(article_map.article_nbr.isNotNull())
        coups_article_map = coups_map.drop("ah4_cd", "ah5_cd")
        coups_article_map = coups_article_map.join(article_map, "article_nbr", "left")
        coups_article_map = coups_article_map.select(coups_map.columns)
        coups_article_map = coups_article_map.union(coups_map)
        coups_article_map = coups_article_map.select("cpn_nbr", "ah4_cd", "ah5_cd")
        coups_article_map = coups_article_map.dropDuplicates()
        coups = coups.join(coups_article_map, "cpn_nbr", "left")

        # 4. reduce size of cf and cache
        core_columns = [
            "CATEGORY_ID",
            "MBRSHP_SID",
            "prediction",
            "prediction_v2",
            "CATEGORY_LVL",
            "CATEGORY_NAME",
        ] + list(["hs_ind_lambda{}".format(x) for x in self.lambdas])
        print("Subsetting collaborative filter columns to {}".format(core_columns))
        cf_pred = cf_pred.select(core_columns)
        cf_pred = broadcast_filter(cf_pred, mbr, ["MBRSHP_SID"])
        cf_pred = broadcast_filter(
            cf_pred, join_category_agnostic(coups), ["CATEGORY_ID"]
        )

        # repartition then checkpoint into memory (breaks lineage properly)
        cf_pred = cf_pred.repartition("MBRSHP_SID", "CATEGORY_ID")
        cf_pred = truncate_history(cf_pred, True)
        trips = trips.repartition("MBRSHP_SID", "cpn_nbr")
        trips = truncate_history(trips, True)

        print("CF predictions and trips repartitioned and checkpointed.")

        # 5. Ingest data dynamically
        assignment_pools = dict()
        coupon_pools = dict()

        for offer_data_type in list(set(self.offer_list)):
            coupon_pools[offer_data_type] = coups.filter(
                coups.cpn_type == offer_data_type
            )

            assignment_pool = ig.ingest(
                offer_data_type,
                mbr,
                coupon_pools[offer_data_type],
                cf_pred,
                trips,
                usage,
                discounts,
                offer_data_type,
                int(self.experiment),
            )
            for source in self.offer_sources:
                if source.get('ftype') == 'parquet' or source.get('name') == 'propensity':
                    source['ftype'] = 'table'
                    if source.get('name') == 'propensity':
                        source['time_partition'] = 'fiscal_week'
                if source.get("offer_data") == offer_data_type:
                    if offer_data_type == "special":
                        assignment_pool = _append_source_data(
                            assignment_pool,
                            source,
                            self,
                            join_not_unique_key=1,
                        )
                    else:
                        assignment_pool = _append_source_data(
                            assignment_pool, source, self
                        )

            if self.parameters["downsample_coupons"]:
                print("Downsampling offer data:")
                for j in self.parameters["downsample_coupons"]:
                    print(
                        "Reducing possible assignment of coupon {} to {}"
                        " of original rows.".format(j["cpn_nbr"], j["ratio"])
                    )
                    assignment_pool = downsample_rows(
                        assignment_pool, "cpn_nbr", j["cpn_nbr"], j["ratio"]
                    )

            assignment_pool = assignment_pool.repartition("MBRSHP_SID", "cpn_nbr")

            assignment_pools[offer_data_type] = truncate_history(
                assignment_pool, cache=True
            )

        return assignment_pools, coupon_pools




    def calculate_exposure(self):
        """
        Calculates the exposure for coupons based on the offer type
         and channel.
        Returns a structure with the number of days passed since a coupon
        was last assigned.

        Returns:
            mbr_offer_recency, mbr_cpn_type_recency
             (list(pyspark.sql.DataFrame)):
             data used to in avoid back to back
        """
        exp_assign = read_subset_and_cast(
            self.paths["CDSA_ASSGN"],
            "parquet",
            ["mbrshp_sid", "experiment_id", "cpn_nbr"],
        )
        exp_channel = read_subset_and_cast(
            self.paths["CAMPAIGN"], "csv", ["experiment_id", "channel"]
        )
        exp_coupon = read_subset_and_cast(
            self.paths["CDSA_CPN_BANK"],
            "csv",
            ["cpn_nbr", "cpn_class_id", "offer_id", "cpn_type"],
        )
        inhome_date = min(self.cells["inhome_date"])
        exp_cells = read_subset_and_cast(self.paths["CELL"], "csv")
        print("generate mbr offer recency: ")
        # this mbr_offer_recency is a df with mbrship_sid, cpn_class_id, days since last cpn_class_id inhome from BBM and from MMPC
        mbr_offer_recency = apply_offer_recency(
            exp_assign,
            exp_channel,
            exp_coupon,
            exp_cells,
            inhome_date,
            "cpn_class_id",
        )

        print("generate mbr cpn type recency: ")
        # this mbr_cpn_type_recency is a df with mbrship_sid, cpn_type, days since last cpn_class_id inhome from BBM and from MMPC
        mbr_cpn_type_recency = apply_offer_recency(
            exp_assign,
            exp_channel,
            exp_coupon,
            exp_cells,
            inhome_date,
            "cpn_type",
        )

        return mbr_offer_recency, mbr_cpn_type_recency

    def calculate_offers(
        self,
        memberdata,
        assignment_pools,
        coupon_pools,
        fill_type,
        offer_exposure,
        coupon_exposure,
    ):
        """
        The method will calculate the filling for each slot group by applying
        the appropriate slot function.

        Parameters:
            memberdata (pyspark.sql.DataFrame): data related to members
            assignment_pools (dict): assignment after ingestion
            coupon_pools (dict): coupons after ingestion
            fill_type (str): one of the options from Campaign.FillType
            offer_exposure (pyspark.sql.DataFrame): exposure for coupons
                                                    having cpn_class_id
            coupon_exposure (pyspark.sql.DataFrame): exposure for coupons
                                                    having cpn_type
        Returns:
            assignment (pyspark.sql.DataFrame): dataframe with ranked coupons
        """
        is_backfill = fill_type == Campaign.FillType.BF

        current_offer = dict()
        current_coupons = copy.copy(coupon_pools)

        for offer_type, offer_data in assignment_pools.items():
            if offer_type == "basket":
                current_offer[offer_type] = offer_data.join(
                    coupon_exposure, ["MBRSHP_SID", "cpn_type"], "left"
                )
            else:
                current_offer[offer_type] = offer_data.join(
                    offer_exposure, ["MBRSHP_SID", "cpn_class_id"], "left"
                )

        if is_backfill:
            for offer_type, offer_data in current_offer.items():
                if "backfill_eligible" in offer_data.columns:
                    current_offer[offer_type] = offer_data.withColumn(
                        "backfill_eligible",
                        offer_data.backfill_eligible.cast("long"),
                    )
                    current_offer[offer_type] = offer_data.filter(
                        offer_data.backfill_eligible == 1
                    )
            for offer_type, coupons in current_coupons.items():
                if "backfill_eligible" in coupons.columns:
                    current_coupons[offer_type] = coupons.withColumn(
                        "backfill_eligible",
                        coupons.backfill_eligible.cast("long"),
                    )
                    current_coupons[offer_type] = coupons.filter(
                        coupons.backfill_eligible == 1
                    )

        assignments = []
        for slot_set in self.slots:
            offer_data_used = copy.copy(current_offer)
            coupons_used = copy.copy(current_coupons)

            # compute for each slot group more coupons than the slot group
            # size in order to be able to eliminate possible duplicates
            # between slot groups without running out of coupons for a slot
            # group:
            # 1. allocating the slot set size for each frontfill slot group
            # ensures that if all front fill slot groups have the same coupons
            # after eliminating the duplicates each slot group should get a
            # distinct part.
            # 2. allocationg 2 x slot set size for each backfill slot group
            # ensures that if all frontfill and backfill slot groups have the
            # same coupons after eliminating duplicates each slot group should
            # get a distinct part
            group_size_with_buffer = sum(
                [
                    int(slot_group[Campaign.FillType.FF]["slot_size"])
                    for slot_group in slot_set["groups"]
                ]
            )
            group_size_with_extra_buffer = 2 * group_size_with_buffer

            if is_backfill:
                total_coupons = group_size_with_extra_buffer
            else:
                total_coupons = group_size_with_buffer

            groups = [slot_group[fill_type] for slot_group in slot_set["groups"]]

            if not is_backfill:
                filled_groups = groups
            else:
                # On backfill, either all backfill in the slot_group are
                # empty or all are full. Except for joined constructs
                filled_groups = [g for g in groups if g]
                if filled_groups and len(filled_groups) != len(groups):
                    print(
                        "Mixed empty/full groups for construct {}".format(
                            slot_set.get("construct_id")
                        )
                    )

                if not filled_groups:
                    print(
                        "Skipping backfill for construct {}".format(
                            slot_set.get("construct_id")
                        )
                    )

            for group in filled_groups:
                assigned_data = _run_slot(
                    memberdata,
                    offer_data_used,
                    coupons_used,
                    group,
                    total_coupons,
                    backfill=is_backfill,
                    seed=int(self.experiment),
                )
                assignments.append(assigned_data)

        if len(assignments) > 1:
            assignment = apply_unionall(*assignments)
            assignment = truncate_history(assignment, cache=True)
        elif len(assignments) == 1:
            assignment = assignments[0]
            assignment = truncate_history(assignment, cache=True)
        else:
            # check note 1000001 to understand
            # why assignment can be None
            assignment = None

        return assignment

    def run_filters(self, memberdata):
        """Run filters for all constructs.

         Parameters:
            memberdata (pyspark.sql.DataFrame): member data for joining to

        Returns:
            memberdata (pyspark.sql.DataFrame): memberdata with filter columns added
        """
        memberdata = memberdata.withColumn("experiment_id", sqlf.lit(self.experiment))

        long_ids = {}
        for segment in self.segments:
            segment_id = segment["segment_id"]
            long_ids[segment_id] = self.cells.query("segment_id==@segment_id")[
                "longitudinal_id"
            ].unique()

        for filt in self.filters:
            print("filter for construct by {}".format(filt["filter_type"]))
            if filt["filter_type"] == "longitudinal":
                filt["filter_parameters"]["longitudinal_ids"] = long_ids[
                    filt["segment_id"]
                ]
            memberdata = filter_functions.get(filt["filter_type"])(
                memberdata, filt["filter_id"], **filt["filter_parameters"]
            )

        return memberdata

    def find_eligible_segments(self, memberdata):
        """Find eligible segments by member.

         Parameters:
            memberdata (pyspark.sql.DataFrame): member data for joining to

        Returns:
            memberdata (pyspark.sql.DataFrame): memberdata with segment eligibility columns added
        """
        memberdata = self.run_filters(memberdata)
        for segment in self.segments:
            # validate filters from simple segments by applying & between them
            grouped_filters = group_filters_by_segment(segment["filters"])

            for segment_id, filters in grouped_filters.items():
                segment_validation = "satisfy_" + segment_id
                cols_to_satisfy = [str(filt["filter_id"]) for filt in filters]
                memberdata = memberdata.withColumn(
                    segment_validation,
                    sum([memberdata[column] for column in cols_to_satisfy]),
                )

                memberdata = memberdata.withColumn(
                    segment_validation,
                    sqlf.when(
                        sqlf.col(segment_validation) == len(filters), 1
                    ).otherwise(0),
                )

            # validate the combination of multi segments by applying either
            # & or | depending on their CDSA setup
            memberdata = memberdata.withColumn("satisfy", sqlf.lit(0))
            for segment_id, _ in grouped_filters.items():
                memberdata = memberdata.withColumn(
                    "satisfy",
                    sqlf.col("satisfy") + sqlf.col("satisfy_" + segment_id),
                )

            colname = str(segment["segment_id"])

            if at_least_one_filter(str(segment["segment_id"])):
                memberdata = memberdata.withColumn(
                    colname, sqlf.when(memberdata.satisfy >= 1, 1).otherwise(0)
                )
            else:
                memberdata = memberdata.withColumn(
                    colname,
                    sqlf.when(memberdata.satisfy == len(grouped_filters), 1).otherwise(
                        0
                    ),
                )

            cols_to_drop = ["satisfy"]
            for segment_id, _ in grouped_filters.items():
                cols_to_drop.append("satisfy_" + segment_id)

            memberdata = memberdata.drop(*cols_to_drop)

        cols_to_drop = list()
        for filt in self.filters:
            cols_to_drop.append(filt["filter_id"])

        memberdata = memberdata.drop(*cols_to_drop)

        return memberdata

    def assign_cells(self, memberdata, allow_multi_long_tests):
        """Assign members to cells.

         Parameters:
            memberdata (pyspark.sql.DataFrame): member data for assigning

        Returns:
            memberdata (pyspark.sql.DataFrame): memberdata with assigned cell column (cell_id)
        """

        # reformat cell info
        cells = self.cells.sort_values("sorting_order")[
            [
                "cell_id",
                "cell_size",
                "sorting_order",
                "segment_id",
                "construct_id",
                "bf_construct_id",
                "longitudinal_id",
            ]
        ]
        cells = cells.to_dict(orient="records")

        memberdata = truncate_history(memberdata, True)

        checks.check_multi_long_tests(memberdata, cells, allow_multi_long_tests)

        # loop through cells in order and assign
        cell_samples = []
        for cell in cells:
            long_id = cell["longitudinal_id"]
            if pd.isna(long_id):
                continue

            colname = str(cell["segment_id"])
            cell_sample = memberdata.filter(
                (sqlf.col(colname) == 1) & (sqlf.col(f"l{long_id}_past_mbr") == 1)
            )

            cell_sample, memberdata = _assign_cell_sample(cell_sample, memberdata, cell)

            print(
                f"assigned {cell_sample.count()} past longitudinal members to cell"
            )
            cell_samples.append(cell_sample)

        for cell in cells:
            if pd.isnull(cell["segment_id"]):
                celldata = memberdata
            else:
                colname = str(cell["segment_id"])
                celldata = memberdata[memberdata[colname] == 1]
            size = cell["cell_size"]
            if pd.isnull(size):
                print(
                    "assign all to cell {} in segment {}...".format(
                        cell["cell_id"], cell["segment_id"]
                    )
                )
                cell_sample = celldata
            elif size < 0:
                print(
                    "assign none to cell {} in segment {}...".format(
                        cell["cell_id"], cell["segment_id"]
                    )
                )
                raise ValueError("invalid cell size given!")
            else:
                cell_sample = palindrome_sample(celldata, size, 100)
                print(
                    "assign {} to cell {} in segment {}...".format(
                        size, cell["cell_id"], cell["segment_id"]
                    )
                )

            cell_sample, memberdata = _assign_cell_sample(cell_sample, memberdata, cell)

            print(f"assigned {cell_sample.count()} members to cell")
            cell_samples.append(cell_sample)

        assigned_members = apply_unionall(*cell_samples)
        # assigned_members.groupby("CELL_ID").agg(
        #     sqlf.count("MBRSHP_SID"), sqlf.avg("sampling_value")
        # ).show()

        return assigned_members

    def map_coupons(self, assignment):
        """Map coupons based on the user input mapping.

         Parameters:
            assignment (pyspark.sql.DataFrame): coupons assigned to each member
        Returns:
            assignment (pyspark.sql.DataFrame): coupons mapped to slots based
                                                on user input
        """
        for slot_set in self.slots:
            if slot_set["is_default_mapping"]:
                continue

            slot_map = slot_set["slot_map"]
            layout_id = slot_set["groups"][0][Campaign.FillType.FF]["layout_id"]

            column_map = list()
            for key, value in slot_map.items():
                column_map.append(sqlf.lit(key))
                column_map.append(sqlf.lit(value))

            mapping = sqlf.create_map(*column_map)

            assignment = assignment.withColumn(
                "NEW_SLOT_NBR",
                sqlf.when(
                    assignment["CONSTRUCT_NAME"] == layout_id,
                    mapping.getItem(assignment["SLOT_NBR"]),
                ).otherwise(assignment["SLOT_NBR"]),
            )

            assignment = assignment.drop("SLOT_NBR").withColumnRenamed(
                "NEW_SLOT_NBR", "SLOT_NBR"
            )

        return assignment

    def sort_coupons(self, assignment, assignment_pools, coupon_pools):
        """Reorder coupons based on the user input columns.

         Parameters:
            assignment (pyspark.sql.DataFrame): coupons assigned to each member
            assignment_pools (dict): all assignments as they were ingested
        Returns:
            final_assignment (pyspark.sql.DataFrame): coupons reorder based on
                                                      extra columns +
                                                      slot number
        """
        assignments = list()
        columns = assignment.columns
        at_least_one_sort = False
        for slot_set in self.slots:
            sort_columns = slot_set["sort"]
            layout_id = slot_set["groups"][0][Campaign.FillType.FF]["layout_id"]
            current_layout = assignment.filter(
                assignment["CONSTRUCT_NAME"] == layout_id
            )

            if not sort_columns:
                assignments.append(current_layout)
                continue

            at_least_one_sort = True

            offer_data_with_extra_info = list()
            all_columns = set()
            for offer_type, offer_data in assignment_pools.items():
                # add coupon information from assignment pool
                offer_data = offer_data.drop("MBRSHP_NBR")

                extra_info = current_layout.join(
                    offer_data, ["MBRSHP_SID", "cpn_nbr"], "left"
                )

                for source in self.offer_sources:
                    if source.get('ftype') == 'parquet' or source.get('name') == 'propensity':
                        source['ftype'] = 'table'
                    if source.get("offer_data") == offer_type:
                        extra_info = self.add_missing_extra_info(
                            extra_info, self.offer_sources_df[source["name"]]
                        )

                extra_info = self.add_missing_extra_info(
                    extra_info,
                    {
                        "data": coupon_pools[offer_type],
                        "merge_col": ["cpn_nbr"],
                    },
                )

                extra_info = extra_info.filter(extra_info["cpn_type"] == offer_type)

                offer_data_with_extra_info.append(extra_info)
                all_columns.update(extra_info.columns)

            # addhoc coupons which ar enot in the coupon pool
            # such as dummy coupons
            adhoc_coupons = current_layout
            for offer_data in offer_data_with_extra_info:
                adhoc_coupons = adhoc_coupons.join(
                    offer_data.select("MBRSHP_SID", "cpn_nbr"),
                    ["MBRSHP_SID", "cpn_nbr"],
                    "leftanti",
                )
            offer_data_with_extra_info.append(adhoc_coupons)

            # making all offer data dataframe share the same columns
            # if columns do not exist they are created with a NULL value
            symmetric_offer_data = list()
            for offer_data in offer_data_with_extra_info:
                offer_data_columns = {column.lower() for column in offer_data.columns}
                for column in all_columns:
                    if column.lower() not in offer_data_columns:
                        offer_data = offer_data.withColumn(column, sqlf.lit(None))
                offer_data = offer_data.select(*all_columns)

                symmetric_offer_data.append(offer_data)

            current_layout = apply_unionall(*symmetric_offer_data)

            slot_nbr_not_present = all(
                ["slot_nbr" not in column for column in sort_columns]
            )
            if slot_nbr_not_present:
                sort_columns.append("SLOT_NBR")

            sort_columns = [
                sqlf.col(column[1:]).desc_nulls_last()
                if column.startswith("-")
                else sqlf.col(column).asc_nulls_last()
                for column in sort_columns
            ]

            # Deterministic tie-breaker for sort_coupons (same pattern as _fit_slot_group)
            current_layout = current_layout.withColumn(
                "_sort_tie_hash",
                sqlf.sha2(
                    sqlf.concat_ws(
                        "||",
                        sqlf.coalesce(sqlf.col("MBRSHP_SID").cast("string"), sqlf.lit("")),
                        sqlf.coalesce(sqlf.col("cpn_nbr").cast("string"), sqlf.lit("")),
                        sqlf.coalesce(sqlf.col("SLOT_NBR").cast("string"), sqlf.lit("")),
                        sqlf.coalesce(sqlf.col("IS_BACKFILL").cast("string"), sqlf.lit("")),
                    ),
                    256,
                ),
            )
            sort_columns.append(sqlf.col("_sort_tie_hash").asc_nulls_last())

            w = W.partitionBy("MBRSHP_SID").orderBy(*sort_columns)
            current_layout = (
                current_layout.withColumn("NEW_SLOT_NBR", sqlf.row_number().over(w))
                .drop("SLOT_NBR", "_sort_tie_hash")
                .withColumnRenamed("NEW_SLOT_NBR", "SLOT_NBR")
            )

            current_layout = current_layout.select(*columns)
            assignments.append(current_layout)

        if at_least_one_sort:
            final_assignment = truncate_history(
                apply_unionall(*assignments), cache=True
            )
        else:
            final_assignment = assignment

        return final_assignment

    def add_missing_extra_info(self, extra_info, source_df):
        """
        Add missing information from source_df in extra_info
        Parameters:
            extra_info (pyspark.sql.DataFrame): DataFrame missing all info
                due to previous incorrect filtering
            source_df (pyspark.sql.DataFrame): DataFrame from which the missing
                data will be added to extra_info
        Return:
            extra_info (pyspark.sql.DataFrame): Updated with all the missing
                information from source_df
        """
        extra_info_columns = extra_info.columns

        new_columns = [
            sqlf.col(column).alias(column + "_v2")
            for column in source_df["data"].columns
            if column not in source_df["merge_col"]
        ]

        new_data = source_df["data"].select(*(new_columns + source_df["merge_col"]))

        # Removing data with duplicate merge_col
         # Use a content-based hash instead of rand() for deterministic dedup
        hash_cols = [sqlf.coalesce(sqlf.col(c).cast("string"), sqlf.lit("__null__"))
                     for c in new_data.columns]
        new_data = new_data.withColumn("_dedup_hash", sqlf.sha2(sqlf.concat_ws("||", *hash_cols), 256))
        w = W.partitionBy(*source_df["merge_col"]).orderBy("_dedup_hash")
        new_data = (
            new_data.withColumn("rank", sqlf.row_number().over(w))
            .filter(sqlf.col("rank") == 1)
            .drop("rank")
            .drop("_dedup_hash")
        )

        extra_info = extra_info.join(new_data, source_df["merge_col"], "left")

        v1_columns = [column for column in extra_info_columns]
        v2_columns = [column for column in new_data.columns if column.endswith("_v2")]

        for v2_column in v2_columns:
            v1_column = v2_column.replace("_v2", "")
            if v1_column in v1_columns:
                extra_info = extra_info.withColumn(
                    v1_column,
                    sqlf.coalesce(sqlf.col(v1_column), sqlf.col(v2_column)),
                )
            else:
                extra_info = extra_info.withColumn(v1_column, sqlf.col(v2_column))

        extra_info = extra_info.drop(*v2_columns)

        return extra_info

    def _fit_slot_group(
        self, partitioned_assignment, per_layout_assignment, group, include
    ):
        """
        Populate the slot groups for each member with no coupon duplicates.
        Parameters:
            partitioned_assignment (pyspark.sql.DataFrame):
                all coupons assigned to all members based on construct ranking.
                The dataframe is partitioned by ["MBRSHP_SID", "cpn_nbr"] to
                help with joins.
            per_layout_assignment (pyspark.sql.DataFrame): the remaining
                available coupons for each member for a construct.
            group (dict): the group meta data for which the method was called
            include (list): a list of filling types to consider when populating
                the slot group
        Returns:
            current_group (pyspark.sql.DataFrame): a slot group filled with
                unique coupons for each member with a local slot group rank.
        """
        if group["pool_type"] == Campaign.PoolType.LAYOUT:
            current_pool = per_layout_assignment
        else:
            current_pool = partitioned_assignment.filter(
                partitioned_assignment["CONSTRUCT_NAME"] == group["layout_id"]
            )

        # deduplication at slot group level should not be needed
        # because a single slot function should not allow duplicates
        current_group = (
            current_pool.filter(current_pool["SLOT_GRP"] == sqlf.lit(group["slot_num"]))
            .filter(current_pool["IS_BACKFILL"].isin(*include))
            .withColumn("group_size", sqlf.lit(group["slot_size"]))
        )

        priority_tie_col = (
            sqlf.coalesce(sqlf.col("priority").cast("string"), sqlf.lit(""))
            if "priority" in current_group.columns
            else sqlf.lit("")
        )
        current_group = current_group.withColumn(
            "_slot_tie_hash",
            sqlf.sha2(
                sqlf.concat_ws(
                    "||",
                    sqlf.coalesce(sqlf.col("MBRSHP_SID").cast("string"), sqlf.lit("")),
                    sqlf.coalesce(sqlf.col("cpn_nbr").cast("string"), sqlf.lit("")),
                    sqlf.coalesce(sqlf.col("SLOT_NBR").cast("string"), sqlf.lit("")),
                    sqlf.coalesce(sqlf.col("IS_BACKFILL").cast("string"), sqlf.lit("")),
                    priority_tie_col,
                ),
                256,
            ),
        )

        if len(include) > 1 and group["pool_type"] == Campaign.PoolType.LAYOUT:
            # deduplicate within slot group if more than one slot function
            # was used (frontfill slot function + backfill slot function)
            # this is required when apply backfill to a slot group
            w = W.partitionBy("MBRSHP_SID", "cpn_nbr").orderBy(
                "IS_BACKFILL", "_slot_tie_hash"
            )
            current_group = current_group.withColumn("rank", sqlf.row_number().over(w))
            current_group = current_group.filter(current_group["rank"] == 1)
            current_group = current_group.drop("rank")

        w = W.partitionBy("MBRSHP_SID").orderBy(
            "IS_BACKFILL", "SLOT_NBR", "cpn_nbr", "_slot_tie_hash"
        )
        current_group = current_group.withColumn("rank", sqlf.row_number().over(w))

        current_group = current_group.filter(
            current_group.rank <= current_group.group_size
        )
        current_group = current_group.drop("_slot_tie_hash")

        return current_group

    def _frontfill_backfill_slotset(self, partitioned_assignment):
        """
        Frontfill all slot groups first and then backfill them.
        Parameters:
            partitioned_assignment (pyspark.sql.DataFrame):
                all coupons assigned to all members based on construct ranking.
                The dataframe is partitioned by ["MBRSHP_SID", "cpn_nbr"] to
                help with joins.

        Returns:
            assignments (pyspark.sql.DataFrame): unique and ordered coupons
                assigned to each member's slot groups with a local slot group
                rank.
        """

        assignments = None
        frontfill_assignment = None

        for slot_set in self.slots:
            per_layout_assignment = None
            for slot_group in slot_set["groups"]:
                group = slot_group[self.FillType.FF]

                if per_layout_assignment is None:
                    per_layout_assignment = partitioned_assignment.filter(
                        partitioned_assignment["CONSTRUCT_NAME"] == group["layout_id"]
                    )

                current_group = self._fit_slot_group(
                    partitioned_assignment,
                    per_layout_assignment,
                    group,
                    [self.FillTypeValue.FF],
                )

                if group["pool_type"] == Campaign.PoolType.LAYOUT:
                    # deduplicate within layout
                    # remove all coupons assigned
                    per_layout_assignment = per_layout_assignment.join(
                        current_group, ["MBRSHP_SID", "cpn_nbr"], "leftanti"
                    )

                if frontfill_assignment is None:
                    frontfill_assignment = current_group
                else:
                    frontfill_assignment = frontfill_assignment.union(current_group)

        backfill = partitioned_assignment.filter(
            sqlf.col("IS_BACKFILL") == self.FillTypeValue.BF
        )
        assignments = backfill.union(frontfill_assignment.select(*backfill.columns))

        assignments = self._frontfill_backfill_slotgroup(assignments)

        return assignments

    def _frontfill_backfill_slotgroup(self, partitioned_assignment):
        """
        Frontfill and backfill a slot group and then next one with the
        remainings.
        Parameters:
            partitioned_assignment (pyspark.sql.DataFrame):
                all coupons assigned to all members based on construct ranking.
                The dataframe is partitioned by ["MBRSHP_SID", "cpn_nbr"] to
                help with joins.

        Returns:
            assignments (pyspark.sql.DataFrame): unique and ordered coupons
                assigned to each member's slot groups with a local slot group
                rank.
        """

        assignments = list()

        for slot_set in self.slots:
            per_layout_assignment = None
            for slot_group in slot_set["groups"]:
                group = slot_group[self.FillType.FF]

                if per_layout_assignment is None:
                    per_layout_assignment = partitioned_assignment.filter(
                        partitioned_assignment["CONSTRUCT_NAME"] == group["layout_id"]
                    )

                current_group = self._fit_slot_group(
                    partitioned_assignment,
                    per_layout_assignment,
                    group,
                    [self.FillTypeValue.FF, self.FillTypeValue.BF],
                )

                if group["pool_type"] == Campaign.PoolType.LAYOUT:
                    # deduplicate within layout
                    # remove all coupons assigned
                    per_layout_assignment = per_layout_assignment.join(
                        current_group, ["MBRSHP_SID", "cpn_nbr"], "leftanti"
                    )

                assignments.append(current_group)

        assignments = apply_unionall(*assignments)

        return assignments

    def fit_coupons(self, member_data):
        """Arrange fillings by respecting coupon rank and deduplication.

        Parameters:
            member_data (pyspark.sql.DataFrame): represents coupons assign to each
            slot group for each member.

        Returns:
            memberdata (pyspark.sql.DataFrame): a complete assignment without
                                                segmentation
        """

        assignment = member_data.withColumn("EXPERIMENT_ID", sqlf.lit(self.experiment))

        assignment = assignment.withColumn(
            "CONSTRUCT_NAME",
            sqlf.regexp_extract(assignment["slot_structure"], CONSTRUCT_COLUMN_EXT, 1),
        )
        assignment = assignment.withColumn(
            "SLOT_NBR",
            sqlf.regexp_extract(
                assignment["slot_structure"], CONSTRUCT_COLUMN_EXT, 2
            ).cast("long"),
        )
        assignment = assignment.withColumn(
            "SLOT_GRP",
            sqlf.regexp_extract(
                assignment["slot_structure"], CONSTRUCT_COLUMN_EXT, 3
            ).cast("long"),
        )
        assignment = assignment.withColumn(
            "PARENT_CONSTRUCT",
            sqlf.regexp_extract(
                assignment["slot_structure"], CONSTRUCT_COLUMN_EXT, 4
            ).cast("long"),
        )
        assignment = assignment.withColumn(
            "IS_BACKFILL",
            sqlf.when(
                sqlf.regexp_extract(
                    assignment["slot_structure"],
                    CONSTRUCT_COLUMN_EXT_BACKFILL,
                    1,
                )
                != "",
                sqlf.lit(1),
            ).otherwise(sqlf.lit(0)),
        )

        # remove any NULL coupons out of the process from the start
        assignment = assignment.filter(assignment.cpn_nbr.isNotNull())

        # depending on the priority we can either:
        # 1. frontfill and backfill a slot group
        # before moving to the next slot group. This will give backfill for the
        # first slot group a higher priority than the front fill of the second
        # group.
        # 2. frontfill the slot set and then backfill the slot set. This will
        # give frontfill higher priority than backfill between slot groups.
        print(
            "Backfill with {priority} priority".format(priority=self.backfill_priority)
        )

        # repartition the dataframe according to the join criteria to
        # help out Spark.
        # without repartitioning joining two dataframes as big as these
        # will slow down Spark or even kill the job.
        partitioned_assignment = assignment.repartition("MBRSHP_SID", "cpn_nbr")
        partitioned_assignment = truncate_history(partitioned_assignment, cache=True)

        if self.backfill_priority == Campaign.BackfillPriority.HIGH:
            final_assignment = self._frontfill_backfill_slotgroup(
                partitioned_assignment
            )
        else:
            final_assignment = self._frontfill_backfill_slotset(partitioned_assignment)

        # convert the local slot group rank to a global slot number
        w = W.partitionBy("MBRSHP_SID", "CONSTRUCT_NAME").orderBy("SLOT_GRP", "rank")
        final_assignment = final_assignment.withColumn(
            "SLOT_NBR", sqlf.row_number().over(w)
        )

        final_assignment = final_assignment.drop("rank")

        final_assignment = truncate_history(final_assignment, cache=True)

        return final_assignment

    def pivot_assignment(self, assignment):
        """Pivot assignment to one entire construct per row.
            The new format will be used for segmentation and cell assignment.

         Parameters:
            assignment (pyspark.sql.DataFrame): a complete assignment without
                                                segmentation.

        Returns:
            pivoted_assignment (pyspark.sql.DataFrame): one member per row
                                                        containing all
                                                        assignments
        """

        print("Pivoting assigned offers.")

        normalized_slot_structure = assignment.withColumn(
            "normalized_slot_structure",
            sqlf.when(
                assignment["IS_BACKFILL"] == 0,
                sqlf.format_string(
                    Campaign.SLOT_STRUCTURE,
                    assignment["CONSTRUCT_NAME"],
                    assignment["SLOT_NBR"] - sqlf.lit(1),
                    assignment["SLOT_GRP"],
                    assignment["PARENT_CONSTRUCT"],
                ),
            ).otherwise(
                sqlf.format_string(
                    Campaign.BF_SLOT_STRUCTURE,
                    assignment["CONSTRUCT_NAME"],
                    assignment["SLOT_NBR"] - sqlf.lit(1),
                    assignment["SLOT_GRP"],
                    assignment["PARENT_CONSTRUCT"],
                )
            ),
        )

        all_slots = list()
        for slot_set in self.slots:
            layout_id = slot_set["groups"][0][Campaign.FillType.FF]["layout_id"]
            start = 0
            for slot_group in slot_set["groups"]:
                group = slot_group[Campaign.FillType.FF]
                bf_group = slot_group[Campaign.FillType.BF]
                if not bf_group:
                    # check note 1000001 to understand
                    # why bf group can be empty
                    bf_group = group
                for i in range(start, start + group["slot_size"]):
                    slot_structure = Campaign.SLOT_STRUCTURE % (
                        layout_id,
                        i,
                        group["slot_num"],
                        group["parent_construct"],
                    )
                    bf_slot_structure = Campaign.BF_SLOT_STRUCTURE % (
                        layout_id,
                        i,
                        bf_group["slot_num"],
                        bf_group["parent_construct"],
                    )
                    all_slots.append(slot_structure)
                    all_slots.append(bf_slot_structure)

                start += group["slot_size"]

        # pivot the dataframe
        # fill slot column with coupon number if there is a coupon
        # assigned or NULL if there is no coupon assigned to that slot
        #
        # it is same to group by MBRSHP_SID because each member has only one
        # slot_structure of a particular type
        pivoted_assignment = (
            normalized_slot_structure.groupby("MBRSHP_SID")
            .pivot("normalized_slot_structure", all_slots)
            .agg(sqlf.max("cpn_nbr"))
        )

        return pivoted_assignment

    def unpivot_assignment(self, pivoted_assignment):
        """Unpivot assignment to having a construct span multiple rows.
            The format will be used to store the result on disc.

         Parameters:
            pivoted_assignment (pyspark.sql.DataFrame): one member per row
                                                        containing all
                                                        assignments
        Returns:
            unpivoted_assignment (pyspark.sql.DataFrame): assignment with
                                                          assignments spanning
                                                          multiple rows
        """

        unpivoted_assignment = pivoted_assignment.withColumn(
            "EXPERIMENT_ID", sqlf.lit(self.experiment)
        )

        all_slots = [
            slot
            for slot in unpivoted_assignment.columns
            if (
                re.match(CONSTRUCT_COLUMN_EXT, slot)
                or re.match(CONSTRUCT_COLUMN_EXT_BACKFILL, slot)
            )
        ]
        meta_list = [
            "MBRSHP_SID",
            "MBRSHP_NBR",
            "EXPERIMENT_ID",
            "CELL_ID",
            "CONSTRUCT_ID",
        ]
        all_cols = all_slots + meta_list

        unpivoted_assignment = unpivoted_assignment.select(*all_cols)

        unpivoted_assignment = stack(unpivoted_assignment, meta_list)

        unpivoted_assignment = unpivoted_assignment.withColumnRenamed(
            "key", "CONSTRUCT"
        )
        unpivoted_assignment = unpivoted_assignment.withColumnRenamed("val", "CPN_NBR")

        unpivoted_assignment = unpivoted_assignment.withColumn(
            "CONSTRUCT_NAME",
            sqlf.regexp_extract(
                unpivoted_assignment["CONSTRUCT"], CONSTRUCT_COLUMN_EXT, 1
            ),
        )
        unpivoted_assignment = unpivoted_assignment.withColumn(
            "SLOT_NBR",
            sqlf.regexp_extract(
                unpivoted_assignment["CONSTRUCT"], CONSTRUCT_COLUMN_EXT, 2
            ),
        )
        unpivoted_assignment = unpivoted_assignment.withColumn(
            "SLOT_GRP",
            sqlf.regexp_extract(
                unpivoted_assignment["CONSTRUCT"], CONSTRUCT_COLUMN_EXT, 3
            ),
        )
        unpivoted_assignment = (
            unpivoted_assignment.withColumn(
                "IS_BACKFILL",
                sqlf.when(
                    sqlf.regexp_extract(
                        unpivoted_assignment["CONSTRUCT"],
                        CONSTRUCT_COLUMN_EXT_BACKFILL,
                        1,
                    )
                    != "",
                    sqlf.lit(1),
                ).otherwise(sqlf.lit(0)),
            )
            .withColumn("SLOT_NBR", sqlf.col("SLOT_NBR").cast("long"))
            .withColumn("SLOT_GRP", sqlf.col("SLOT_GRP").cast("long"))
        )

        unpivoted_assignment = unpivoted_assignment.filter(
            unpivoted_assignment["CPN_NBR"].isNotNull()
        )

        # start coupon coupon count from 1 instead of 0
        unpivoted_assignment = unpivoted_assignment.withColumn(
            "SLOT_NBR", unpivoted_assignment["SLOT_NBR"] + sqlf.lit(1)
        )

        return unpivoted_assignment

    def replace_slots_with_null(self, memberdata):
        """Replace the coupon numbers having the values specified in the config
         file with None.

         Parameters:
            memberdata (pyspark.sql.DataFrame): assigned coupons

        Returns:
            memberdata (pyspark.sql.DataFrame): assignment with specific coupon
                                                numbers replaced with None
        """
        for value in self.replace_with_null:
            memberdata = memberdata.withColumn(
                "CPN_NBR",
                sqlf.when(memberdata["CPN_NBR"] == value, None).otherwise(
                    memberdata["CPN_NBR"]
                ),
            )

        return memberdata

    def generate_output(self, memberdata):
        """Generate final output tables

         Parameters:
            memberdata (pyspark.sql.DataFrame): member data for assigning

        Returns:
            assignments (pyspark.sql.DataFrame): memberdata for output final
                                                 assignmemt.
            all_construct (pyspark.sql.DataFrame): memberdata for output all
                                                   assignmemts for every
                                                   constructs.
        """

        memberdata = memberdata.withColumn(
            "pool_type", sqlf.lit(Campaign.PoolType.LAYOUT)
        )
        for slot_set in self.slots:
            layout_id = slot_set["groups"][0][Campaign.FillType.FF]["layout_id"]

            for slot_group in slot_set["groups"]:
                ff_group = slot_group[Campaign.FillType.FF]
                memberdata = memberdata.withColumn(
                    "pool_type",
                    sqlf.when(
                        (memberdata["CONSTRUCT_NAME"] == layout_id)
                        & (memberdata["SLOT_GRP"] == ff_group["slot_num"]),
                        sqlf.lit(ff_group["pool_type"]),
                    ).otherwise(memberdata["pool_type"]),
                )

        memberdata = memberdata.withColumn(
            "BF_CONSTRUCT",
            sqlf.when(
                memberdata["IS_BACKFILL"] == 1,
                sqlf.concat(
                    sqlf.lit("c"),
                    sqlf.regexp_extract(memberdata["CONSTRUCT"], LAYOUT_ID_MATCH, 2),
                    sqlf.lit("s"),
                    memberdata["SLOT_NBR"],
                ),
            ).otherwise(sqlf.lit("-")),
        )

        memberdata = memberdata.withColumn(
            "CONSTRUCT",
            sqlf.concat(
                sqlf.lit("c"),
                sqlf.regexp_extract(memberdata["CONSTRUCT"], LAYOUT_ID_MATCH, 1),
                sqlf.lit("s"),
                memberdata["SLOT_NBR"],
            ),
        )

        # rename columns and cast as appropriate
        memberdata = memberdata.withColumn(
            "mbrshp_sid", memberdata.MBRSHP_SID.cast("long")
        )
        memberdata = memberdata.withColumn(
            "experiment_id", memberdata.EXPERIMENT_ID.cast("long")
        )
        memberdata = memberdata.withColumn(
            "cell_id", memberdata["CELL_ID"].cast("long")
        )
        memberdata = memberdata.withColumn(
            "construct", memberdata.CONSTRUCT.cast("string")
        )
        memberdata = memberdata.withColumn(
            "bf_construct", memberdata.BF_CONSTRUCT.cast("string")
        )
        memberdata = memberdata.withColumn("cpn_nbr", memberdata.CPN_NBR.cast("string"))
        memberdata = memberdata.withColumn("slot_nbr", memberdata.SLOT_NBR.cast("long"))
        memberdata = memberdata.withColumn(
            "is_backfill", memberdata.IS_BACKFILL.cast("long")
        )
        # OUTPUT 1: output all constructs for all members
        all_constructs = memberdata.select(
            "mbrshp_sid",
            "experiment_id",
            "cell_id",
            "construct",
            "bf_construct",
            "cpn_nbr",
            "is_backfill",
            "pool_type",
        )
        # filter for final construct

        memberdata = memberdata.filter(
            memberdata.CONSTRUCT_NAME == memberdata.CONSTRUCT_ID
        )

        # OUTPUT 2: output final assignment for all members
        assignments = memberdata.select(
            "mbrshp_sid",
            "experiment_id",
            "cell_id",
            "slot_nbr",
            "construct",
            "bf_construct",
            "cpn_nbr",
            "pool_type",
        )

        return (assignments, all_constructs)

    def find_past_longitudinal_mbrs(self, memberdata):
        """
        Marks members if they were in current longitudinal campaigns.

        Parameters:
            memberdata (pyspark.sql.DataFrame): assignment members to check

        Returns:
            memberdata (pyspark.sql.DataFrame): members with past longitudinal
                                                columns
        """
        long_cells = self.cells.query("~longitudinal_id.isna()")
        long_ids = long_cells["longitudinal_id"].unique()

        long_cols = []
        for long_id in long_ids:
            long_mbrs = load_past_longitudinal_mbrs(
                long_id,
                self.experiment,
                env_path(self.paths["CDSA_ASSGN"], self.vol_base, self.env),
                env_path(self.paths["CELL"], self.vol_base, self.env),
            )

            long_col = f"l{long_id}_past_mbr"
            long_mbrs = long_mbrs.withColumn(long_col, sqlf.lit(1))

            memberdata = memberdata.join(long_mbrs, on="mbrshp_sid", how="left")

            long_cols.append(long_col)

        memberdata = memberdata.fillna(0, subset=long_cols)

        return memberdata
