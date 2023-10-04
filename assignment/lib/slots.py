"""Function bank for Slot Functions.

Slot functions are responsible for filling each slot group with
corresponding unique coupons. All slot functions implement the same API, but
depending on the function it could be used for frontfill, backfill
or both.

All slot functions have two main parts:
    1. deduplication
    2. ranking

A slot function must meet the following contract:
        member_data: Dataframe that has all the member data necessary
        assignment_pool (pyspark.sql.dataframe): Usually the assignment which
                                                takes place during ingestion
                                                (a plain old cross join with
                                                possible filtering)
        coupon_pool (pyspark.sql.dataframe): The coupons with filtering done
                                             during ingestion
        total_coupons (int): number of coupons to consider for the slot group.
                            Currently set to size of of the coupon pool via
                            execution in fill_slot/etc
        kwargs (dict): contains extended parameters, such as is_backfill
                       for determining if running for backfill or frontfill.

        Return: Dataframe with at least the following columns:
                MBRSHP_SID, cpn_nbr, rank

Notes:
    Engine Input:

    Depending on the filling slot_type and filling type the assignment_pool is
    used, the coupon_pool or both.

     total_coupons could be adjusted for better performances, but it should be
     done with care. Changing total_coupons to a number smaller than the actual
     size of the slot set could end up in not having enough coupons to fill
     all slot groups, because each slot group has to have distinct coupons
     with a different ranking function.

     is_backfill specifies if the slot function is computing a frontfill or
     backfill.

     User Input:

     As visible in fill_slot() all other parameters which are not engine input
     are user input and are specified in the construct .json.

"""
import warnings
from functools import reduce

from pe_memberdna.pipelines.assignment.lib.assn_utils import (
    calc_avg_basket,
    deterministic_df,
    filter_rows,
    map_under_threshold,
    mapping,
)
from pe_memberdna.pipelines.assignment.lib.ingest import _broadcast_cross_join
from pe_memberdna.pipelines.lib.spark_util import (
    get_logger,
    union_with_mismatched_columns,
)
from pe_memberdna.pipelines.lib.utils import top_n
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as fabs
from pyspark.sql.functions import avg, col, concat, countDistinct, desc, lit
from pyspark.sql.functions import max as fmax
from pyspark.sql.functions import row_number
from pyspark.sql.functions import sum as fsum
from pyspark.sql.functions import when
from pyspark.sql.window import Window

slot_functions = {}

spark = SparkSession.builder.getOrCreate()
log = get_logger("slots")


def make_available_to_slots(f):
    """Decorator for slot function eligibility."""
    slot_functions[f.__name__] = f
    return f


@make_available_to_slots
def sql(
    member_data,
    df,
    coupon_pool,
    total_coupons=1,
    sql_string="select * from df",
    **kwargs,
):
    """
    A general function to expose lots of functionality without having to create
    a new slot function. With great power comes great responsibility,
    this function is not guaranteed to maintain the contract
    of a general slot function, that is, returning a column 'rank'

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        df (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        sql_string (str): query string to use
        kwargs (dict): additional parameters to extend functionality
    Returns:
        assignment (pyspark.sql.dataframe): filtered dataframe
    """
    df = deduplicate(df, order_by=["cpn_nbr"])

    df.createOrReplaceTempView("df")
    assignment = spark.sql(sql_string)

    return assignment


@make_available_to_slots
def compose_slots(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=1,
    slots=[],
    drop_rank=True,
    **kwargs,
):
    """
    A function that allows you to chain together slot functions.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        slots (list): the list of slot functions to compose
        drop_rank (boolean): whether or not to drop the rank column in between
                            composes
        kwargs (dict): additional parameters to extend functionality
    Returns:
        assignment (pyspark.sql.dataframe): filtered dataframe
    """
    df = assignment_pool
    for slot in slots:
        if drop_rank:
            df = df.drop("rank")
        df = fill_slot(
            member_data, df, coupon_pool, slot, total_coupons, **kwargs
        )

    assignment = df[(col("rank") <= total_coupons) | (col("rank").isNull())]
    return assignment


@make_available_to_slots
def waterfall_slots(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=-1,
    slots=[],
    dedupe=True,
    **kwargs,
):
    """
    A function that allows you to chain together slot functions.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        slots (list): the list of slot functions to compose
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
     Returns:
        assignment (pyspark.sql.dataframe): filtered dataframe
    """
    assert total_coupons > 0

    available_assignments = assignment_pool
    assigned_members = []
    for slot in slots:
        valid_offer_data_types = offer_data_to_list(slot)
        valid_offer_data = available_assignments.filter(
            col("cpn_type").isin(valid_offer_data_types)
        )

        this_slot_assignment = (
            fill_slot(
                member_data,
                valid_offer_data,
                coupon_pool,
                slot,
                total_coupons,
                dedupe=dedupe,
                **kwargs,
            )
            .withColumn("waterfall_slot_priority", lit(len(assigned_members)))
            .select("MBRSHP_SID", "cpn_nbr", "waterfall_slot_priority", "rank")
        )
        # We could code this up to be more efficient and manage everyone's
        # available slots leftover, but this is fine for now.
        assigned_members.append(this_slot_assignment)

    possible_assignments = reduce(
        lambda x, y: x.union(y), assigned_members
    ).withColumnRenamed("rank", "inner_rank")

    order_by = ["waterfall_slot_priority", "inner_rank"]

    # dedupe ordering has to match ranking criteria
    if dedupe:
        deduped_possible_assignments = deduplicate(
            possible_assignments, order_by=order_by
        ).cache()
        deduped_possible_assignments.count()
        possible_assignments = deduped_possible_assignments

    window = Window.partitionBy("MBRSHP_SID").orderBy(*order_by)
    assignment = (
        possible_assignments.withColumn("rank", row_number().over(window))
        .filter(col("rank") <= total_coupons)
        .cache()
    )
    assignment.count()
    possible_assignments.unpersist()
    return assignment


@make_available_to_slots
def rank_by_col(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    rank_col="TRIPS",
    is_ascending=True,
    secondary_sort=None,
    filter=None,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Return the 'r'th ranked item.

     This function limits a dataframe to the 'r'th in each group column,
     where items are ranked by a separate rank column.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        rank_col (str): name of column to rank by
        is_ascending (bool): type of ordering
        secondary_sort (str, opt): secondary sort column used to resolve ties.
            secondary sort currently defaults to descending order.
        filter (str): additional filtering
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal..
        kwargs (dict): additional parameters to extend functionality
    Returns:
        rth (pyspark.sql.dataFrame): filtered dataframe
    """
    df = assignment_pool

    if isinstance(filter, str) or isinstance(filter, str):
        filter = eval(filter)
    if filter is not None and isinstance(filter, dict):
        df = filter_rows(df, filter)
    elif not isinstance(filter, dict):
        warnings.warn(
            "\n WARNING: No filter applied: make sure the filter_col"
            " is a dictionary"
        )

    sort_1 = col(rank_col).asc() if is_ascending else col(rank_col).desc()
    if secondary_sort:
        sort_2 = (
            col(secondary_sort).asc()
            if is_ascending
            else col(secondary_sort).desc()
        )
        order_by = [sort_1, sort_2]
    else:
        order_by = [sort_1]

    deterministic, order_by, _ = deterministic_df(
        df, order_by, seed, random_only=False
    )

    # dedupe ordering has to match ranking criteria
    if dedupe:
        deduped = deduplicate(deterministic, order_by=order_by)
        assignment = deduped
    else:
        assignment = deterministic

    window = Window.partitionBy("MBRSHP_SID").orderBy(*order_by)
    ranked = assignment.withColumn("rank", row_number().over(window))
    rth = ranked[ranked.rank <= total_coupons]
    return rth


def rank_by_agg_col(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    group_col,
    rank_col,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Rank raw assignment by aggregated column.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        group_col (list(str)): group by column name
        rank_col (str): rank by column name
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        ranked_assignment: members with ranked coupons
    """

    df = member_data.select("MBRSHP_SID")
    agg_rank_col = "agg_" + rank_col
    coups = assignment_pool.groupby(group_col).agg(
        fsum(rank_col).alias(agg_rank_col)
    )

    df = _broadcast_cross_join(
        df, coups.select("cpn_nbr", agg_rank_col, "offer_id", "cpn_class_id")
    )

    # bring data back
    group_col.append("MBRSHP_SID")
    df = df.join(assignment_pool, group_col, "left")

    deterministic, order_by, _ = deterministic_df(
        df, [col(agg_rank_col).desc()], seed, random_only=False
    )

    # dedupe ordering has to match ranking criteria
    if dedupe:
        deduped = deduplicate(deterministic, order_by=order_by)
        assignment = deduped
    else:
        assignment = deterministic

    df = top_n(
        assignment, total_coupons, order_by, group_col="MBRSHP_SID", keep=True
    )

    ranked_assignment = df.select(
        "MBRSHP_SID", "cpn_nbr", "rank", "offer_id", "cpn_class_id"
    )

    return ranked_assignment


@make_available_to_slots
def rank_by_agg_trips(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    seed,
    dedupe=True,
    **kwargs,
):
    """Rank raw assignment by aggregated trips.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        ranked_assignment: members with ranked coupons
    """

    ranked_assignment = rank_by_agg_col(
        member_data,
        assignment_pool,
        coupon_pool,
        total_coupons,
        ["cpn_nbr", "offer_id", "cpn_class_id"],
        "trips",
        seed,
        dedupe=dedupe,
    )
    return ranked_assignment


@make_available_to_slots
def rank_by_agg_cf(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    seed,
    dedupe=True,
    **kwargs,
):
    """Rank raw assignment by aggregated cf.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        ranked_assignment: members with ranked coupons
    """

    ranked_assignment = rank_by_agg_col(
        member_data,
        assignment_pool,
        coupon_pool,
        total_coupons,
        ["cpn_nbr", "offer_id", "cpn_class_id"],
        "prediction",
        seed,
        dedupe=dedupe,
    )
    return ranked_assignment


@make_available_to_slots
def rank_by_cf(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    seed,
    dedupe=True,
    **kwargs,
):
    """Rank raw assignment by cf score.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        ranked_assignment: members with ranked coupons
    """
    # limit at most 2 offers within the same category
    combined_col = assignment_pool.withColumn(
        "combined_col",
        concat(
            assignment_pool.MBRSHP_SID, lit("_"), assignment_pool.CATEGORY_ID
        ),
    )

    deterministic, order_by, _ = deterministic_df(
        combined_col, [col("prediction").desc()], seed, random_only=False
    )

    combined_col = top_n(
        deterministic, 2, order_by, "combined_col", keep=False
    )
    combined_col = rank_by_col(
        member_data,
        combined_col,
        coupon_pool,
        total_coupons,
        rank_col="prediction",
        is_ascending=False,
        seed=seed,
        dedupe=dedupe,
    )

    combined_col = combined_col.withColumn("priority", lit(0))
    # scenario 1:
    # for article coupon, if mbr does not have cf score,
    # 2nd layer backfill upon most popular coupons defined by aggregated trips
    if "trips" in assignment_pool.columns:
        aggregation = rank_by_agg_col(
            member_data,
            assignment_pool,
            coupon_pool,
            total_coupons,
            ["cpn_nbr", "offer_id", "cpn_class_id"],
            "trips",
            seed,
            dedupe=dedupe,
        )
    # scenario 2:
    # for category coupon, if mbr does not have cf score,
    # 2nd layer backfill upon most popular coupons defined by aggregated
    # cf score
    elif "prediction" in assignment_pool.columns:
        aggregation = rank_by_agg_col(
            member_data,
            assignment_pool,
            coupon_pool,
            total_coupons,
            ["cpn_nbr", "offer_id", "cpn_class_id"],
            "prediction",
            seed,
            dedupe=dedupe,
        )
    # scenario 3: for edge cases, 2nd layer random backfill
    else:
        aggregation = random(
            member_data,
            assignment_pool,
            coupon_pool,
            total_coupons,
            seed,
            dedupe=dedupe,
        )

    aggregation = aggregation.withColumn("priority", lit(1))
    df = union_with_mismatched_columns(combined_col, aggregation)

    if dedupe:
        deduped = deduplicate(df, order_by=["priority"])
        assignment = deduped
    else:
        assignment = df

    df = assignment.withColumnRenamed("rank", "rank_within_priority")
    w = Window.partitionBy("MBRSHP_SID").orderBy(
        "priority", "rank_within_priority"
    )
    ranked_assignment = df.withColumn("rank", row_number().over(w))
    ranked_assignment = ranked_assignment[
        ranked_assignment.rank <= total_coupons
    ]

    return ranked_assignment


@make_available_to_slots
def cf(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=1,
    hs_ind_lambda=15,
    hs_ind=None,
    offer_band=None,
    rank_limit=None,
    score_limit=None,
    filter=None,
    cf_thres=None,
    limit_match=False,
    prediction_column="prediction",
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Return cf prediction.

     This function selects cf predictions from a dataframe based on selection
     criteria.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
            Requires:
                prediction: cf score
                hs_ind_lambda{}: lambda for tagging hook-stretch indicator
                hs_ind: hook-stretch indicator for prediction
                MBRSHP_SID: individual that prediction relates to
                CATEGORY_ID: category id that prediction relates to
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        hs_ind_lambda (int): lambda for hook-stretch indicator
        hs_ind (string): filter for stretch vs. hook vs. none
        rank (int, opt): return # of top coupons per member
        score_band (float, opt): score gap from top score, cat within this gap can be selected
        rank_limit (int, opt): maximum rank allowable for prediction
        score_limit (float, opt): minimum score allowable for prediction
        filter(dict, opt): filter column with conditions to select rows
        cf_thres(float, opt): top percent scores allowable for prediction
        limit_match(bool, False): whether to limit results to those that are
                                  equal in ranking to top CF prediction
        prediction_column(str, False): the column to use to predict
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        predictions (pyspark.sql.dataFrame): filtered dataframe
    """
    # 1. whether limits to stretch or not
    log.info(
        "Applying cf function with lambda {} and prediction {}".format(
            hs_ind_lambda, prediction_column
        )
    )
    df1 = assignment_pool
    if hs_ind is not None:
        df1 = df1[df1["hs_ind_lambda{}".format(hs_ind_lambda)] == hs_ind]
    df1 = df1.filter(df1[prediction_column].isNotNull())
    if cf_thres:
        df_grouped = df1.groupby("MBRSHP_SID").agg(
            fmax(prediction_column).alias("max_pred")
        )
        df_grouped = df_grouped.withColumn(
            "max_pred", col("max_pred") - cf_thres * col("max_pred")
        )
        df1 = df1.join(df_grouped, "MBRSHP_SID", "left")
        df1 = df1.where(col(prediction_column) >= col("max_pred"))
        df1 = df1.drop("max_pred")

    deterministic, order_by, unique_column = deterministic_df(
        df1, [col(prediction_column).desc()], seed, random_only=False
    )

    # dedupe before any ranking takes place
    # although this is covered in rank_by_col not deduping here
    # could leave coupons out in case of rank_limit or top 2 category_id
    if dedupe:
        deduped = deduplicate(deterministic, order_by=order_by)
        assignment = deduped
    else:
        assignment = deterministic

    window = Window.partitionBy(assignment.MBRSHP_SID).orderBy(*order_by)
    ranked = assignment.withColumn("rank", row_number().over(window))
    # 2. apply other filters if needed
    if rank_limit is not None:
        ranked = ranked[ranked.rank <= rank_limit]
    if score_limit is not None:
        ranked = ranked[ranked[prediction_column] >= score_limit]
    if isinstance(filter, str) or isinstance(filter, str):
        filter = eval(filter)
    if filter is not None and isinstance(filter, dict):
        ranked = filter_rows(ranked, filter)
    elif not isinstance(filter, dict):
        warnings.warn(
            "\n WARNING: No filter applied: make sure the filter_col is a dictionary"
        )
    # limit at most 2 offers within the same category
    ranked = ranked.withColumn(
        "combined_col", concat(ranked.MBRSHP_SID, lit("_"), ranked.CATEGORY_ID)
    )
    # convert to descending rank to fit top_n
    ranked = ranked.withColumn("reverse_rank", (-1) * ranked.rank)
    ranked = top_n(ranked, 2, "reverse_rank", "combined_col", keep=False)
    ranked = ranked.drop("combined_col", "reverse_rank")
    # 3. if a an offer band is applied run selection twice, else pick top rank
    if offer_band is not None:
        rth = rank_by_col(
            member_data,
            ranked,
            coupon_pool,
            total_coupons=offer_band,
            rank_col=prediction_column,
            is_ascending=False,
            secondary_sort=None,
            filter=None,
            seed=seed,
            dedupe=dedupe,
        )

        deterministic, _, new_unique_column = deterministic_df(
            rth,
            [],
            seed=int(seed) + 1,
            random_only=True,
            unique_column="randomness",
        )

        # randomly pick from the best
        w = Window.partitionBy("MBRSHP_SID").orderBy(new_unique_column)
        rth = deterministic.withColumn("reverse_rank", row_number().over(w))

        rth = rank_by_col(
            member_data,
            rth,
            coupon_pool,
            total_coupons=total_coupons,
            rank_col="reverse_rank",
            is_ascending=True,
            secondary_sort=None,
            filter=None,
            seed=seed,
            dedupe=dedupe,
        )
    else:
        rth = rank_by_col(
            member_data,
            ranked,
            coupon_pool,
            total_coupons=total_coupons,
            rank_col=prediction_column,
            is_ascending=False,
            secondary_sort=None,
            filter=None,
            seed=seed,
            dedupe=dedupe,
        )

    if limit_match:
        rth = limit_to_cf_match(
            member_data,
            assignment_pool,
            coupon_pool,
            rth,
            total_coupons,
            seed=seed,
            dedupe=dedupe,
        )
    return rth


@make_available_to_slots
def cf_combined(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=1,
    tenure=150,
    **kwargs,
):
    """Return cf prediction.

     This function selects cf predictions from a dataframe based on selection
     criteria. hs_ind_lambda and hs_ind will be only effective to tenured members,
     whereas new members will be assigned top hooks offers regardless

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
            Requires:
                prediction: cf score
                hs_ind_lambda{}: lambda for tagging hook-stretch indicator
                hs_ind: hook-stretch indicator for prediction
                MBRSHP_SID: individual that prediction relates to
                CATEGORY_ID: category id that prediction relates to
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        tenure (int): number of days as the threshold to define new vs.
         tenured members
        **kwargs: arguments to pass to cf() - see cf() documentation for params
    Returns:
        rth (pyspark.sql.dataFrame): filtered dataframe
    """
    df_tenure = assignment_pool.filter(assignment_pool.TENURE >= tenure)
    df_tenure_rth = cf(
        member_data, df_tenure, coupon_pool, total_coupons, **kwargs
    )
    df_new = assignment_pool.filter(assignment_pool.TENURE < tenure)
    new_kwargs = kwargs.copy()
    new_kwargs["hs_ind_lambda"] = None
    new_kwargs["hs_ind"] = None
    df_new_rth = cf(
        member_data, df_new, coupon_pool, total_coupons, **new_kwargs
    )
    rth = df_tenure_rth.union(df_new_rth.select(df_tenure_rth.columns))

    return rth


@make_available_to_slots
def cf_bau(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=12,
    category_count=1,
    hs_ind_lambda=30,
    tenure=150,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Assign slots via category offer BAU (Business As Usual).

    This uses "standard" article and CF targeting and backfill algorithms.
    As definition of "BAU" may one day change, see the code below for the
    exact algorithms used (e.g. adjusted trips for article hooks).

    Members with <total_coupons> article hooks will receive all article hooks.
    Members with fewer than <total_coupons> article hooks will receive a
    category offer instead of an article backfill, for <category_count>
    number of article backfills.

    For example, consider 12 total_coupons and a category_count of 2.
        - Members with 12 article hooks will receive 12 articles
        - Members with 11 article hooks will receieve 11 articles and 1
            category offer
        - Members with 10 or fewer article hooks will receive 10 articles and
            2 category offers

    Our new standard BAU method of targeting self-funded category offers to our
     member base is as follows, considering "X" the category_count.

    For more details check the assignment notes.txt ID 1000001.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        category_count (int): max number of category coupons to use
        tenure (int): number of days as the threshold to define new vs.
                      tenured members
        hs_ind_lambda(int): lambda for tagging hook-stretch indicator
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        bau (pyspark.sql.dataFrame): bau dataframe
    """
    articles = assignment_pool.filter(col("cpn_type") == "article")
    article_coupons = coupon_pool.filter(col("cpn_type") == "article")

    categories = assignment_pool.filter(col("cpn_type") == "category")
    category_coupons = coupon_pool.filter(col("cpn_type") == "category")

    article_hooks = rank_by_col(
        member_data,
        articles,
        None,
        total_coupons,
        rank_col="ADJUSTED_TRIPS",
        is_ascending=False,
        secondary_sort="prediction",
        filter="{'column': 'ADJUSTED_TRIPS', 'relation': '>', 'threshold': 0}",
        seed=seed,
        dedupe=dedupe,
    ).withColumn("priority", lit(1))
    article_hooks = article_hooks.withColumn("is_backfill", lit(0))

    top_category = cf_combined(
        member_data,
        categories,
        category_coupons,
        total_coupons=category_count,
        tenure=tenure,
        hs_ind="stretch",
        hs_ind_lambda=hs_ind_lambda,
        seed=seed,
        dedupe=dedupe,
    ).withColumn("priority", lit(2))
    top_category = top_category.withColumn("is_backfill", lit(0))

    if "backfill_eligible" in categories.columns:
        categories = categories.filter(categories["backfill_eligible"] == 1)
    if "backfill_eligible" in category_coupons.columns:
        category_coupons = category_coupons.filter(
            category_coupons["backfill_eligible"] == 1
        )
    top_impressions = rank_by_agg_cf(
        member_data,
        categories,
        category_coupons,
        total_coupons=category_count,
        tenure=tenure,
        hs_ind="stretch",
        hs_ind_lambda=hs_ind_lambda,
        seed=seed,
        dedupe=dedupe,
    ).withColumn("priority", lit(3))
    top_impressions = top_impressions.withColumn("is_backfill", lit(1))

    if "backfill_eligible" in articles.columns:
        articles = articles.filter(articles["backfill_eligible"] == 1)
    if "backfill_eligible" in article_coupons.columns:
        article_coupons = article_coupons.filter(
            article_coupons["backfill_eligible"] == 1
        )
    best_articles = rank_by_cf(
        member_data,
        articles,
        article_coupons,
        total_coupons,
        seed=seed,
        dedupe=dedupe,
    ).withColumn("priority", lit(4))
    best_articles = best_articles.withColumn("is_backfill", lit(1))

    window = Window.partitionBy("MBRSHP_SID").orderBy(
        col("priority").asc(), col("rank").asc()
    )
    category = union_with_mismatched_columns(top_category, top_impressions)
    category = category.withColumn("rank", row_number().over(window))
    category = category.filter(category.rank <= category_count)

    bau = union_with_mismatched_columns(article_hooks, category)
    bau = union_with_mismatched_columns(bau, best_articles)

    if dedupe:
        bau = deduplicate(
            bau, order_by=[col("priority").asc(), col("rank").asc()]
        )

    window = Window.partitionBy("MBRSHP_SID").orderBy(
        col("priority").asc(), col("rank").asc()
    )
    bau = bau.withColumn("rank", row_number().over(window))
    bau = bau.filter(bau.rank <= total_coupons)

    window = Window.partitionBy("MBRSHP_SID", "is_backfill").orderBy(
        col("priority").asc(), col("rank").asc()
    )
    bau = bau.withColumn("rank", row_number().over(window)).drop("priority")

    return bau


@make_available_to_slots
def static_bau(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=1,
    static_cpns=None,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Assign a static coupon only to eligible members. Everyone else gets and
    the rest of the slots are populated with standard hook logic. The backfill
    is the standard backfill logic for all members.

    This function is similar with cf bau. For more details check the assignment
    notes.txt ID 1000001.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        static_cpns (dict({str: str})): the coupons to be used for all members
            that are eligible for it. The key represents the coupons while the
            value the eligibility filter.
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        (pyspark.sql.dataFrame): dynamic targeting dataframe
    """
    if not static_cpns:
        raise Exception("Please provide static coupon.")

    static_offers = []

    static_assignment = assignment_pool.filter(col("cpn_type").isin("dummy"))
    for static_cpn, static_filter in static_cpns.items():
        assignment = static_assignment.filter(static_filter)
        static_offer = static(
            member_data,
            assignment,
            coupon_pool,
            total_coupons=1,
            cpn_nbr=static_cpn,
        )
        static_offer = static_offer.withColumn("priority", lit(1))
        static_offer = static_offer.withColumn("is_backfill", lit(0))
        static_offers.append(static_offer)

        static_assignment = static_assignment.join(
            assignment, "MBRSHP_SID", "leftanti"
        )

    static_offer = reduce(
        lambda x, y: union_with_mismatched_columns(x, y), static_offers
    )

    articles = assignment_pool.filter(col("cpn_type") == "article")
    article_hooks = rank_by_col(
        member_data,
        articles,
        None,
        total_coupons,
        rank_col="ADJUSTED_TRIPS",
        is_ascending=False,
        secondary_sort="prediction",
        filter="{'column': 'ADJUSTED_TRIPS', 'relation': '>', 'threshold': 0}",
        seed=seed,
        dedupe=dedupe,
    ).withColumn("priority", lit(2))
    article_hooks = article_hooks.withColumn("is_backfill", lit(0))

    if "backfill_eligible" in articles.columns:
        articles = articles.filter(articles["backfill_eligible"] == 1)

    article_coupons = coupon_pool.filter(col("cpn_type") == "article")
    if "backfill_eligible" in article_coupons.columns:
        article_coupons = article_coupons.filter(
            article_coupons["backfill_eligible"] == 1
        )

    best_articles = rank_by_cf(
        member_data,
        articles,
        article_coupons,
        total_coupons,
        seed=seed,
        dedupe=dedupe,
    ).withColumn("priority", lit(3))
    best_articles = best_articles.withColumn("is_backfill", lit(1))

    assignment = union_with_mismatched_columns(static_offer, article_hooks)
    assignment = union_with_mismatched_columns(assignment, best_articles)

    order_by = [col("priority").asc(), col("rank").asc()]

    if dedupe:
        assignment = deduplicate(assignment, order_by=order_by)

    window = Window.partitionBy("MBRSHP_SID").orderBy(*order_by)
    assignment = assignment.withColumn("rank", row_number().over(window))
    assignment = assignment.filter(assignment.rank <= total_coupons)

    return assignment


@make_available_to_slots
def basket(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=1,
    spend_col="LFIFTY-TWOW_SPEND_IN_STORE",
    trips_col="LAST_FIFTY-TWO_WEEK_TRIPS",
    basket_offer_threshold="[50:50, 400:100]",
    propensity_threshold=0,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Find the matched basket for each member

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
            Requires:
                avg_basket: average basket size per member
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        spend_col(str): name of column $ total spend for calculating avg basket
                        size
        trips_col(str): name of column # of trips for calculating avg basket
                        size
        basket_offer_threshold (dictionary): specify the matching rule on
         avg_basket->rounded_basket->basket_offer
        propensity_threshold (double): for whoever has propensity score lower
                                       than the threshold, will send the
                                        easiest offer
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        df (pyspark.sql.DataFrame): data frame with matched
                                    basket offer appended
    """
    # 1. calculate the average basket size
    # if no trips in the last # weeks, or if unlikely to visit from trip
    # propensity score then assign 0 to basket size in order to match easiest
    # offer
    df = calc_avg_basket(
        assignment_pool, spend_col, trips_col, propensity_threshold
    )
    # 2. match basket offern upon average basket size
    if isinstance(basket_offer_threshold, str) | isinstance(
        basket_offer_threshold, str
    ):
        basket_offer_threshold = eval(basket_offer_threshold)
    if basket_offer_threshold is not None and isinstance(
        basket_offer_threshold, dict
    ):
        threshold = sorted(basket_offer_threshold.keys())
    else:
        warnings.warn(
            "\n WARNING: No basket_offer_threshold applied:\
                      plese check construct json"
        )
    df = map_under_threshold(df, "avg_basket", "rounded_basket", threshold)
    df = mapping(
        df,
        "rounded_basket",
        "cpn_dollar_threshold_mapped",
        basket_offer_threshold,
    )
    df = df.filter(df.cpn_dollar_threshold == df.cpn_dollar_threshold_mapped)

    deterministic, order_by, _ = deterministic_df(
        df, [col("cpn_dollar_threshold").desc()], seed, random_only=False
    )

    if dedupe:
        df = deduplicate(deterministic, order_by=order_by)
    else:
        df = deterministic

    df = df.withColumn("rank", lit(1))

    return df


@make_available_to_slots
def static(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=1,
    cpn_nbr=None,
    **kwargs,
):
    """Assign a predefined coupon

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        cpn_nbr (string): coupon number to assign statically
        kwargs (dict): additional parameters to extend functionality
    Returns:
        df (pyspark.sql.DataFrame): static offer data
    """
    df = assignment_pool.dropDuplicates(subset=["MBRSHP_SID"])
    df = df.withColumn("cpn_nbr", lit(cpn_nbr))
    df = df.withColumn("rank", lit(1))

    return df


@make_available_to_slots
def hardest(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Fill offer by hardest threshold

    Parameters:
        member_data (pyspark.sql.DataFrame): data related to members
        assignment_pool (pyspark.sql.DataFrame): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons for assigning
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        members_with_coupons: members with coupons ordered by
                              hardest threshold
    """

    coupons = coupon_pool.select(
        "cpn_nbr", "cpn_dollar_threshold", "offer_id", "cpn_class_id"
    ).distinct()
    coupons = coupons.withColumn(
        "cpn_dollar_threshold", coupons.cpn_dollar_threshold.cast("int")
    )
    df = _broadcast_cross_join(member_data, coupons)

    deterministic, order_by, _ = deterministic_df(
        df, [col("cpn_dollar_threshold").desc()], seed, random_only=False
    )

    if dedupe:
        deduped = deduplicate(deterministic, order_by=order_by)
        assignment = deduped
    else:
        assignment = deterministic

    window = Window.partitionBy("MBRSHP_SID").orderBy(*order_by)
    ranked_assignment = assignment.withColumn(
        "rank", row_number().over(window)
    )
    ranked_assignment = ranked_assignment[
        ranked_assignment.rank <= total_coupons
    ]
    return ranked_assignment


@make_available_to_slots
def random(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=1,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Randomly pick coupons from the pool after applying filters on the
    coupon bank.

    Parameters:
        member_data (pyspark.sql.DataFrame): data related to members
        assignment_pool (pyspark.sql.DataFrame): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons for assigning
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): contains extended parameters, such as is_backfill
                       for determining if random is calculated for backfill
                       or frontfill.
    Returns:
        ranked_assignment (pyspark.sql.DataFrame): offer data after filtered
    """
    is_backfill = kwargs["is_backfill"]
    if is_backfill:
        df = _broadcast_cross_join(member_data, coupon_pool)
        deterministic, order_by, _ = deterministic_df(
            df, [], seed, random_only=True
        )
    else:
        deterministic, order_by, _ = deterministic_df(
            assignment_pool, [], seed, random_only=True
        )

    if dedupe:
        deduped = deduplicate(deterministic, order_by=order_by)
        assignment = deduped
    else:
        assignment = deterministic

    window = Window.partitionBy("MBRSHP_SID").orderBy(*order_by)
    ranked_assignment = assignment.withColumn(
        "rank", row_number().over(window)
    )

    ranked_assignment = ranked_assignment[
        ranked_assignment.rank <= total_coupons
    ]
    return ranked_assignment


@make_available_to_slots
def same(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """Pick the same offer for all members in the same order

    Parameters:
        member_data (pyspark.sql.DataFrame): data related to members
        assignment_pool (pyspark.sql.DataFrame): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons for assigning
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        ranked_assignment: members with the same offer assigned
    """
    deterministic, order_by, unique_column = deterministic_df(
        coupon_pool, [], seed, random_only=True
    )
    df = _broadcast_cross_join(member_data, deterministic)

    if dedupe:
        deduped = deduplicate(df, order_by=order_by)
        assignment = deduped
    else:
        assignment = deterministic

    window = Window.partitionBy(df["MBRSHP_SID"]).orderBy(*order_by)
    ranked_assignment = assignment.withColumn(
        "rank", row_number().over(window)
    )
    ranked_assignment = ranked_assignment[
        ranked_assignment.rank <= total_coupons
    ]
    return ranked_assignment


@make_available_to_slots
def stretch_spend(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons=1,
    cpn_list=None,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """stretch coupon assignment upon coupon redeem threshold.
    Assign coupon has closest higher threshold than mbr's average spend
    E.g. mbr who spends avg $9 on apparel gets $5 off $10 apparal,
    wle mbr who spends avg $14 on apparel gets  $5 off $15 apparal.

    Parameters:
        member_data (pyspark.sql.DataFrame): data related to members
        assignment_pool (pyspark.sql.DataFrame): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons for assigning
        cpn_list (array<string>, opt): the list of cpn number limit to
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        df (pyspark.sql.DataFrame): offer data after filtered
    """
    df = assignment_pool
    if cpn_list:
        df = df.filter(df.cpn_nbr.isin(cpn_list))
    df = df.withColumn("SPEND", when(df.SPEND.isNull(), 0).otherwise(df.SPEND))
    df = df.groupby("MBRSHP_SID", "cpn_nbr", "cpn_dollar_threshold").agg(
        fsum(df.SPEND).alias("SPEND"),
        countDistinct(df.PURCH_HDR_ID).alias("TRIPS"),
    )
    df = df.withColumn("AVG_SPEND", df.SPEND / (df.TRIPS))
    df = df.fillna(0, subset=["AVG_SPEND"])
    df = df.withColumn(
        "thresh_minus_spend", df.cpn_dollar_threshold - df.AVG_SPEND
    )

    df = df.withColumn(
        "is_hook", when(df.thresh_minus_spend <= 0, 1).otherwise(0)
    )
    deterministic, order_by, _ = deterministic_df(
        df,
        # first order priority is stretch > hook
        # second order priority is distance to 0, smaller the better
        [col("is_hook").asc(), fabs(col("thresh_minus_spend")).asc()],
        seed,
        random_only=False,
    )

    if dedupe:
        deduped = deduplicate(deterministic, order_by=order_by)
        assignment = deduped
    else:
        assignment = deterministic

    window = Window.partitionBy("MBRSHP_SID").orderBy(*order_by)
    df = assignment.withColumn("rank", row_number().over(window))
    df = df.filter(df.rank <= total_coupons)
    return df


@make_available_to_slots
def anniversary_content(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    renewable_dates,
    gas,
    non_gas,
    other,
    **kwargs,
):
    """
    The slot function is a wrapper for SQL slot function and assigns in a
    waterfall manner the following coupons:
    1. Anniversary Gas (label: gas)
    2. Anniversary Non-Gas (label: non_gas)
    3. Other (label: other)

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        df (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        renewable_dates List(str): dates provided for renewables
        gas (str): value for gas
        non_gas (str): value for non gas
        other (str): value for other. Can be NULL.
                     In this case it will be dropped from the result.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        assignment (pyspark.sql.dataframe): dataframe result
    """
    renewable_dates_str = "({0}{1}".format(
        ",".join("'{0}'".format(r) for r in renewable_dates), ")"
    )

    query = """
    select
    MBRSHP_SID,
    if
    (
        `L_FIFTY-TWOW_GAS_TRIPS` > 0
        AND `LATEST_MBRSHP_EXP_DT` IN {renewable_dates}
        AND `LAST_FIFTY-TWO_WEEK_TRIPS` > 1
        AND `L_FIFTY-TWOW_FIRST_MOST_SHOPPED_CATEGORY` is NOT NULL
        AND `TENURE` > 0
        AND (
                (
                    `LATEST_MFI_TIER` = 55
                    AND `LFIFTY-TWOW_SAVINGS_W_CLPLSS` > 75
                )
                or
                (
                    `LATEST_MFI_TIER` = 110
                    AND `LFIFTY-TWOW_SAVINGS_W_CLPLSS` > 130
                )
        ) , '{gas}',
        if
        (
            `LATEST_MBRSHP_EXP_DT` IN {renewable_dates}
            AND `LAST_FIFTY-TWO_WEEK_TRIPS` > 1
            AND `L_FIFTY-TWOW_FIRST_MOST_SHOPPED_CATEGORY` is NOT NULL
            AND `TENURE` > 0
            AND (
                    (
                        `LATEST_MFI_TIER` = 55
                        AND `LFIFTY-TWOW_SAVINGS_W_CLPLSS` > 75
                    )
                    or
                    (
                        `LATEST_MFI_TIER` = 110
                        AND `LFIFTY-TWOW_SAVINGS_W_CLPLSS` > 130
                    )
            ) ,'{non_gas}', '{other}'
        )
    ) as cpn_nbr, 1 AS rank from df
    """.format(
        renewable_dates=renewable_dates_str,
        gas=gas,
        non_gas=non_gas,
        other=other,
    )

    assignment = sql(
        member_data,
        assignment_pool,
        coupon_pool,
        total_coupons,
        sql_string=query,
        **kwargs,
    )

    assignment = assignment.filter(assignment.cpn_nbr != "None")

    return assignment


@make_available_to_slots
def rewards_content(
    member_data,
    assignment_pool,
    coupon_pool,
    total_coupons,
    rewards_to_elite,
    ic_to_plus,
    perks_plus,
    perks_elite,
    other,
    tenure=90,
    **kwargs,
):
    """
    The slot function is a wrapper for SQL slot function and assigns in a
    waterfall manner the following content versions:
    1. Rewards to Elite (label: elite_rewards)
    2. Inner Circle to Plus (label: ic_to_plus)
    3. Perks Plus (label: perks_plus)
    4. Perks Elite (label: perks_elite)
    3. Other (label: other)

    Only members with tenure <= <tenure> and at least 2 trips in the last 52
    weeks are considered for the first two versions (rewards_to_elite,
    ic_to_plus).
    Trial members are excluded from ic_to_plus since they are not eligible.

    Parameters:
        member_data (pyspark.sql.dataframe): data related to members
        assignment_pool (pyspark.sql.dataframe): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        total_coupons (int): number of coupons to consider
        rewards_to_elite (str): value for elite rewards
        ic_to_plus (str): value for IC to PLUS rewards
        perks_plus (str): value for Perks Plus rewards
        perks_elite (str): value for Perks Elite rewards
        other (str): value for other. Can be NULL.
                     In this case it will be dropped from the result.
        tenure (int): tenureship threshold for new members
        kwargs (dict): additional parameters to extend functionality
    Returns:
        assignment (pyspark.sql.dataframe): dataframe result
    """
    query = f"""
    select
    MBRSHP_SID,
    if
    (
        `TENURE` <= {tenure}
        AND `LATEST_RWDS_MBR_IND` = 'Y'
        AND `LAST_FIFTY-TWO_WEEK_TRIPS` >= 2 , '{rewards_to_elite}',
        if
        (
            `TENURE` <= {tenure}
            AND `LAST_FIFTY-TWO_WEEK_TRIPS` >= 2
            AND `LATEST_TRIAL_MBR_IND` = 0
            AND (
                `LATEST_RWDS_MBR_IND` = 'N'
                OR `LATEST_RWDS_MBR_IND` = 'D'
            ), '{ic_to_plus}',
            if
            (
                `LATEST_RWDS_MBR_IND` = 'P','{perks_plus}',
                 if
                 (
                    `LATEST_RWDS_MBR_IND` = 'E' ,'{perks_elite}',
                    '{other}'
                 )
            )
        )
    ) as cpn_nbr, 1 AS rank from df
    """

    assignment = sql(
        member_data,
        assignment_pool,
        coupon_pool,
        total_coupons,
        sql_string=query,
        **kwargs,
    )

    assignment = assignment.filter(assignment.cpn_nbr != "None")

    return assignment


def deduplicate(assignment, order_by):
    """Keep the first occurrence of cpn for each member according to a certain
    ordering.

    Parameters:
        assignment (pyspark.sql.DataFrame): assignment with possible duplicates
        order_by (list): list of criteria to order by
    Returns:
        assignment (pyspark.sql.DataFrame): assignment with no duplicates
    """
    w = Window.partitionBy("MBRSHP_SID", "cpn_nbr").orderBy(*order_by)

    assignment = assignment.withColumn("dedup_rank", row_number().over(w))
    assignment = assignment.filter(assignment.dedup_rank == 1)
    assignment = assignment.drop("dedup_rank")
    return assignment


def fill_slot(
    member_data,
    assignment_pool,
    coupon_pool,
    slot,
    total_coupons,
    is_backfill=True,
    seed=0,
    dedupe=True,
    **kwargs,
):
    """
    Function to 'wire together' the configured slot functions and run them.
    Essentially converts the map of maps which is the slot configuration from
    the construct and makes the appropriate slot calls

    Parameters:
        member_data (pyspark.sql.DataFrame): data related to members
        assignment_pool (pyspark.sql.DataFrame): assignment after ingestion
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        slot (dict): slot group structure
        total_coupons (int): total coupons to take into consideration
        is_backfill (bool): whether it is backfill or frontfill
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        slot_fill (pyspark.sql.DataFrame): offer data for slot group
    """
    log.info("Using slot {}".format(str(slot["slot_type"])))

    if slot.get("slot_parameters") is not None:
        slot_fill = slot_functions.get(slot["slot_type"])(
            member_data,
            assignment_pool,
            coupon_pool,
            total_coupons,
            is_backfill=is_backfill,
            seed=seed,
            dedupe=dedupe,
            **slot["slot_parameters"],
        )
    else:
        slot_fill = slot_functions.get(slot["slot_type"])(
            member_data,
            assignment_pool,
            coupon_pool,
            total_coupons,
            is_backfill=is_backfill,
            seed=seed,
            dedupe=dedupe,
        )
    return slot_fill


def offer_data_to_list(slot):
    """
    If the offer data is of type list, it just returns it, if it is not it will
    make a list containing the one offer data

    :param slot: slot configuration
    :return: offer data as a list
    """
    slot_offer_data_type = slot.get("offer_data")
    if not isinstance(slot_offer_data_type, list):
        slot_offer_data_type = [slot_offer_data_type]
    return slot_offer_data_type


def limit_to_cf_match(
    member_data,
    assignment_pool,
    coupon_pool,
    df_full,
    total_coupons,
    seed,
    dedupe=True,
    **kwargs,
):
    """Limit df_full to coupons that match the customer's cf coupons.

    Subset df_full to only those coupons which have the same rank as they would
    through standard CF (top prediction) logic. For example, df_full is the
    output of assigning a stretch coupon - now only pick those stretch coupons
    if they are also the customer's top CF coupon.

    Parameters:
        member_data (pyspark.sql.DataFrame): data related to members
        assignment_pool (pyspark.sql.DataFrame): Dataframe containing cf
                                                predictions sufficient to meet
                                                input requirements of the cf()
                                                slot function.
        coupon_pool (pyspark.sql.dataframe): coupons after ingestion
        df_full: Dataframe of already assigned coupons to be subsetted.
            Requires:
                MBRSHP_SID
                cpn_nbr
                rank
        total_coupons: number of CF slots to calculate per member
        seed (int): a deterministic unique value to start from when creating an
            order
        dedupe (boolean): whether the slot function should remove
            duplicates or not. This is set to True if the pool type for the
            the slot group is layout and to False if it is set to universal.
        kwargs (dict): additional parameters to extend functionality
    Returns:
        limit (pyspark.sql.dataframe): subsetted dataframe
    """
    top_cf = cf(
        member_data,
        assignment_pool,
        coupon_pool,
        total_coupons=total_coupons,
        hs_ind=None,
        seed=seed,
        dedupe=dedupe,
    ).select("MBRSHP_SID", "cpn_nbr", "rank")
    limit = df_full.join(top_cf, ["MBRSHP_SID", "cpn_nbr", "rank"], "inner")
    w = Window.partitionBy(limit.MBRSHP_SID).orderBy(limit.rank)
    limit = limit.withColumn("rank", row_number().over(w))
    return limit
