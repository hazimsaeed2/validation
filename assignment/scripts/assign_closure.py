import argparse
import itertools

from functools import reduce

import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
from pyspark.sql.window import Window

from pe_member_dna.pipelines.lib.iotools import (
    s3_copy,
    split_path_bucket_key,
    is_s3_file,
    is_s3_path,
)
from pe_member_dna.pipelines.lib.utils import apply_unionall
from pe_member_dna.pipelines.lib.spark_util import truncate_history

from pe_member_dna.pipelines.assignment.lib.schemas.coupon_schemas import (
    CAP_CLOSURE_COUPON_SCHEMA,
)
from pe_member_dna.pipelines.assignment.lib.assn_io import JobManager
from pe_member_dna.pipelines.assignment.lib.assn_utils import deterministic_df


STARTING_SEED = None
RANDOM_SEED = None


def get_seed():
    global RANDOM_SEED
    if RANDOM_SEED is None:
        RANDOM_SEED = itertools.count(STARTING_SEED)

    return next(RANDOM_SEED)


CAT_HRRCHY = "AH5_CD"
CAT_LIST = "closure_ah5_cd"


class ClosureMethod:

    COLUMN_NAME = "closure_method"

    NONE = "NONE"
    NATURAL = "NATURAL"
    SWAP = "SWAP"
    SWAP_BF = "SWAP_BF"
    HIT_MIN = "HIT_MIN"
    HIT_MAX = "HIT_MAX"
    HIT_MIN_AHCD_DEDUP = "HIT_MIN_AHCD_DEDUP"
    HIT_MAX_AHCD_DEDUP = "HIT_MAX_AHCD_DEDUP"


def get_run_args():
    """Return the call arguments for this script.

    Parameters:

    Returns:
       args (Namespace): commandline arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", nargs="?")
    parser.add_argument("--cells", nargs="*")
    parser.add_argument("--seed", nargs="?")
    parser.add_argument("--iterate", action="store_true", default=False)
    parser.add_argument("--ahcd_dedup", action="store_true", default=False)

    args, _ = parser.parse_known_args()

    return args


def dedup_on_ahcd(current_closure_eligible):
    current_closure_eligible = current_closure_eligible.withColumn(
        "eligible",
        sqlf.when(
            sqlf.size(
                sqlf.array_except(
                    sqlf.col(CAT_LIST),
                    sqlf.array_intersect(
                        sqlf.col(CAT_LIST), sqlf.col("allowed_ahcd")
                    ),
                )
            )
            == 0,
            sqlf.lit(1),
        ).otherwise(0),
    ).filter(sqlf.col("eligible") == 1)

    return current_closure_eligible


def update_allowed_ahcd(eligible_closure_assignment, new_distribution):
    """Remove ahcd from the new_distribution of closure from the
     eligible_closure_assignment allowed ahcd.

     Parameters:
        eligible_closure_assignment (pyspark.sql.DataFrame): a dataframe
            describing which closure is eligible to which member
        new_distribution (pyspark.sql.DataFrame): a set of members which got
            the current closure
    Returns:
        eligible_closure_assignment (pyspark.sql.DataFrame): a dataframe
            describing eligible closures with allowed_ahcd updated

    """
    eligible_closure_assignment = eligible_closure_assignment.join(
        new_distribution.select(
            sqlf.col("mbrshp_sid"), sqlf.col(CAT_LIST).alias("ahcd_to_remove")
        ),
        "mbrshp_sid",
        "left",
    )

    eligible_closure_assignment = eligible_closure_assignment.withColumn(
        "allowed_ahcd",
        sqlf.when(
            sqlf.col("ahcd_to_remove").isNull(), sqlf.col("allowed_ahcd")
        ).otherwise(
            sqlf.array_except(
                sqlf.col("allowed_ahcd"), sqlf.col("ahcd_to_remove")
            )
        ),
    ).drop("ahcd_to_remove")

    return eligible_closure_assignment


def expand_ahcd(ahcd_dataframe):
    """Expand dataframe with ahcd list into one ahcd per row

     Parameters:
        ahcd_dataframe (pyspark.sql.DataFrame): a dataframe with ahcd as a list
    Returns:
        expanded_ahcd (pyspark.sql.DataFrame): a dataframe with
            one ahcd per row
    """
    expanded_ahcd = (
        ahcd_dataframe.withColumn(
            CAT_HRRCHY, sqlf.explode(sqlf.split(CAT_LIST, ","))
        )
        .withColumn(CAT_HRRCHY, sqlf.trim(sqlf.col(CAT_HRRCHY)))
        .withColumn(CAT_HRRCHY, sqlf.col(CAT_HRRCHY).cast(sqlt.LongType()))
    )

    return expanded_ahcd


def match_distributions(
    job,
    closure_cells,
    cap_closure_coupons,
    eligible_closure_assignment,
    dedup_ahcd,
    update_ahcd,
    current_distributions,
    threshold,
):
    """Replace coupons from certain slots with closure coupons until
    a threshold is reached.

     Parameters:
         job (JobManager): the spark job manager
         closure_cells (pyspark.sql.DataFrame): assigned coupons which can get
                                                replaced with closure
        cap_closure_coupons (pyspark.sql.DataFrame): the closure coupons
        eligible_closure_assignment (pyspark.sql.DataFrame): a dataframe
            describing which closure is eligible to which member
        dedup_ahcd (boolean): whether to dedup using ahcd
        update_ahcd (boolean): whether to update the ahcd usage
        current_distributions (dict): a dictionary storing the count/closure
                                      coupon
        threshold (int): the threshold to hit

    Returns:
        new_distributions (list[pyspark.sql.DataFrame]): contains the assigned
                                                         closures
        closure_cells (pyspark.sql.DataFrame): the remaining assigned coupons
                                               which can get replaced with
                                               closure
        eligible_closure_assignment (pyspark.sql.DataFrame): a dataframe
            describing the remaining closures eligible to member
        current_distributions (dict): the updated counts for closures
    """

    if not dedup_ahcd:
        job.log.info(
            "    Distribute closure until %s using unique closure" % threshold
        )
    else:
        job.log.info(
            "    Distribute closure until %s using unique AHCD" % threshold
        )

    new_distributions = list()

    conditions = [sqlf.col("slot_nbr") == 12, sqlf.col("slot_nbr") == 11] + [
        (sqlf.col("slot_nbr") == slot_nbr) & (sqlf.col("bf_construct") != "-")
        for slot_nbr in range(10, 0, -1)
    ]

    for row in cap_closure_coupons.orderBy("closure_priority").collect():

        closure_coupon = row["PMR Offer ID"]
        closure_ahcd = row[CAT_LIST]
        limit = row[threshold]

        job.log.info(
            "    Conducting closure %s distribution for %s"
            % (threshold, closure_coupon)
        )

        # get members eligible for current closure
        current_closure_eligible = eligible_closure_assignment.filter(
            eligible_closure_assignment["cpn_nbr"] == closure_coupon
        )

        if dedup_ahcd:
            current_closure_eligible = dedup_on_ahcd(current_closure_eligible)

        current_closure_eligible = truncate_history(
            current_closure_eligible.select("mbrshp_sid"), cache=True
        )

        if current_closure_eligible.count() == 0:
            job.log.info(
                "    No member is eligible for the closure coupon: %s"
                % closure_coupon
            )
            continue

        to_distribute = 0

        closure_distribution = closure_cells.join(
            current_closure_eligible, "mbrshp_sid", "inner"
        )

        # keep only slots which are eligible
        closure_distribution = closure_distribution.filter(
            reduce(lambda x, y: x | y, conditions)
        )

        # first free slot for each member
        w = Window.partitionBy("mbrshp_sid").orderBy(
            sqlf.col("slot_nbr").desc()
        )
        closure_distribution = closure_distribution.withColumn(
            "rank", sqlf.row_number().over(w)
        )
        closure_distribution = closure_distribution.filter(
            closure_distribution["rank"] == 1
        )

        current_distribution = current_distributions.get(closure_coupon, 0)
        to_distribute = int(limit) - int(current_distribution)
        if to_distribute > 0:
            job.log.info("    To distribute: %s" % to_distribute)

            # distribute them starting with the least important, but
            # in a random order
            closure_distribution, order_by, _ = deterministic_df(
                closure_distribution,
                [sqlf.col("slot_nbr").desc()],
                seed=get_seed(),
                random_only=True,
                unique_column="randomness",
            )
            closure_distribution = closure_distribution.orderBy(*order_by)
            closure_distribution = closure_distribution.limit(to_distribute)

            final_distribution = closure_distribution.withColumn(
                CAT_LIST, sqlf.lit(closure_ahcd)
            )

            final_distribution = final_distribution.withColumn(
                CAT_LIST, sqlf.regexp_replace(sqlf.col(CAT_LIST), " ", "")
            ).withColumn(CAT_LIST, sqlf.split(CAT_LIST, ","))

            # assign closure in the free slot
            final_distribution = final_distribution.withColumn(
                "cpn_nbr", sqlf.lit(closure_coupon)
            )

            final_distribution = truncate_history(
                final_distribution, cache=True
            )

            # remove the coupons which were replaced
            # from the natural assignment
            closure_cells = closure_cells.join(
                final_distribution, ["mbrshp_sid", "original"], "leftanti"
            )

            if update_ahcd:
                # remove closure ahcd only from the selected members
                eligible_closure_assignment = update_allowed_ahcd(
                    eligible_closure_assignment, final_distribution
                )

            # remove the newly assigned closures from eligible pool
            eligible_closure_assignment = eligible_closure_assignment.join(
                final_distribution, ["mbrshp_sid", "cpn_nbr"], "leftanti"
            )

            new_distributions.append(final_distribution)
            total_distribution = final_distribution.count()
            to_distribute -= total_distribution
            current_distributions[closure_coupon] = (
                current_distributions.get(closure_coupon, 0)
                + total_distribution
            )

            if total_distribution > 0:
                job.log.info(
                    "    Distributed a total of: %s" % total_distribution
                )

        else:
            job.log.info(
                "    Already assigned the closure coupon %s"
                " up to the threshold %s" % (closure_coupon, threshold)
            )

        closure_cells = closure_cells.repartition("MBRSHP_SID", "cpn_nbr")
        closure_cells = truncate_history(closure_cells, cache=True)

        eligible_closure_assignment = eligible_closure_assignment.repartition(
            "MBRSHP_SID", "cpn_nbr"
        )
        eligible_closure_assignment = truncate_history(
            eligible_closure_assignment, cache=True
        )

        if to_distribute > 0:
            job.log.warn(
                "    Unable to assign closure %s up to the threshold %s"
                % (closure_coupon, threshold)
            )

    return (
        new_distributions,
        truncate_history(closure_cells, cache=True),
        truncate_history(eligible_closure_assignment, cache=True),
        current_distributions,
    )


def swap_equivalent(
    job,
    closure_cells,
    cap_closure_coupons,
    eligible_closure_assignment,
    conditions,
    dedup_ahcd,
    update_ahcd,
    current_distributions,
):
    """Swap equivalent coupons with closure coupons until the max distribution
     is reached.

     Parameters:
         job (JobManager): the spark job manager
         closure_cells (pyspark.sql.DataFrame): assigned coupons which can get
                                                swapped with closure
        cap_closure_coupons (pyspark.sql.DataFrame): the closure coupons
        eligible_closure_assignment (pyspark.sql.DataFrame): a dataframe
            describing which closure is eligible to which member
        conditions (pyspark.sql.column.Column): conditions to filter on
        dedup_ahcd (boolean): whether to dedup using ahcd
        update_ahcd (boolean): whether to update the ahcd usage
        current_distributions (dict): a dictionary storing the count/closure
                                      coupon

    Returns:
        new_distributions (list[pyspark.sql.DataFrame]): contains the assigned
                                                         closures
        closure_cells (pyspark.sql.DataFrame): the remaining assigned coupons
                                               which can get replaced with
                                               closure
        eligible_closure_assignment (pyspark.sql.DataFrame): a dataframe
            describing the remaining closures eligible to member
        current_distributions (dict): the updated counts for closures
    """
    closure_with_equivalent = (
        eligible_closure_assignment.join(
            cap_closure_coupons.select(
                sqlf.col("PMR Offer ID").alias("cpn_nbr"),
                "equivalent_offer_id",
            ).filter(cap_closure_coupons["equivalent_offer_id"].isNotNull()),
            ["cpn_nbr"],
            "inner",
        )
        .withColumnRenamed("cpn_nbr", "closure_coupon")
        .withColumnRenamed("equivalent_offer_id", "cpn_nbr")
    )

    if dedup_ahcd:
        closure_with_equivalent = dedup_on_ahcd(closure_with_equivalent)

    # get equivalent assignment that are closure eligible
    if conditions is not None:
        equivalent_assignment = closure_cells.filter(conditions)
    else:
        equivalent_assignment = closure_cells

    equivalent_assignment = equivalent_assignment.join(
        closure_with_equivalent, ["mbrshp_sid", "cpn_nbr"], "inner"
    )

    swap = list()

    for row in cap_closure_coupons.orderBy("closure_priority").collect():
        closure_coupon = row["PMR Offer ID"]
        max_distribution = row["max_distribution"]
        current_distribution = current_distributions.get(closure_coupon, 0)
        to_distribute = int(max_distribution) - int(current_distribution)

        job.log.info("    Conducting closure %s swap" % closure_coupon)

        if to_distribute <= 0:
            continue

        new_distribution = equivalent_assignment.filter(
            equivalent_assignment["closure_coupon"] == closure_coupon
        )

        new_distribution, order_by, _ = deterministic_df(
            new_distribution,
            [],
            seed=get_seed(),
            random_only=True,
            unique_column="randomness",
        )
        new_distribution = new_distribution.orderBy(*order_by).limit(
            to_distribute
        )

        # assign closure in the free slot
        new_distribution = new_distribution.withColumn(
            "cpn_nbr", new_distribution["closure_coupon"]
        ).drop("closure_coupon")

        new_distribution = truncate_history(new_distribution, cache=True)

        # remove coupons which are about to get swapped from the
        # assignment
        closure_cells = closure_cells.join(
            new_distribution, ["mbrshp_sid", "original"], "leftanti"
        )

        if update_ahcd:
            # remove closure ahcd only from the selected members
            eligible_closure_assignment = update_allowed_ahcd(
                eligible_closure_assignment, new_distribution
            )

        # removed newly assigned closures from eligible pool
        eligible_closure_assignment = eligible_closure_assignment.join(
            new_distribution, ["mbrshp_sid", "cpn_nbr"], "leftanti"
        )

        swap.append(new_distribution)

        total_distributed = new_distribution.count()

        job.log.info("      Total swapped %s" % total_distributed)

        current_distributions[closure_coupon] = (
            current_distributions.get(closure_coupon, 0) + total_distributed
        )

    return (
        swap,
        truncate_history(closure_cells, cache=True),
        truncate_history(eligible_closure_assignment, cache=True),
        current_distributions,
    )


def backup(job, source, destination):
    """
    Backup the source path to the destination path.

    Parameters:
        job (JobManager): the spark job manager
        source (str): the source path
        destination (str): the destination path

    Returns:

    """
    job.log.info("Backup natural assignment to %s" % destination)
    bucket_source, key_source = split_path_bucket_key(source)
    bucket_dest, key_dest = split_path_bucket_key(destination)

    backup_does_not_exist = not is_s3_file(
        bucket_dest, key_dest
    ) and not is_s3_path(bucket_dest, key_dest)
    if backup_does_not_exist:
        s3_copy(bucket_source, key_source, key_dest)
    else:
        raise Exception(
            "Backup already exists at %s. Please make sure your are backing up"
            " the correct content. Remove the old backup and re-run the"
            " script or use --iterate." % destination
        )


def main(job, cells, iterate, seed=None, ahcd_dedup=False):
    """
    The script entry point.

    Parameters:
        job (JobManager): the spark job manager
        cells (list[str]): list of cell ids to which to assign closures
        iterate (bool): whether to use the existing backup
        seed (str): integer seed to use when choosing members
        ahcd_dedup(bool): whether to deduplicate using AHCD

    Returns:

    """
    if not cells:
        raise Exception(
            "Please specify which cells to include in closure assignment."
        )

    global STARTING_SEED
    if seed:
        STARTING_SEED = int(seed)
    else:
        STARTING_SEED = int(job.config.params["experiment"])

    job.log.info("Running assign_closures for the following cells: %s" % cells)

    bucket, original_assignments = split_path_bucket_key(
        job.config.paths["CAP_ORIGINAL_INPUT_ASSIGNMENTS"]
    )
    bucket, original_constructs = split_path_bucket_key(
        job.config.paths["CAP_ORIGINAL_INPUT_CONSTRUCTS"]
    )
    bucket, original_mail_subset = split_path_bucket_key(
        job.config.paths["CAP_ORIGINAL_MAIL_POPULATION_ASSIGNMENT"]
    )

    backup_exists = (
        (
            is_s3_path(bucket, original_assignments)
            or is_s3_file(bucket, original_assignments)
        )
        and (
            is_s3_path(bucket, original_constructs)
            or is_s3_file(bucket, original_constructs)
        )
        and (
            is_s3_path(bucket, original_mail_subset)
            or is_s3_file(bucket, original_mail_subset)
        )
    )

    if backup_exists and iterate:
        job.log.info("Using existing assignments back up.")
    elif iterate:
        raise Exception("Cannot iterate when not all backups exist.")
    else:
        backup(
            job,
            job.config.paths["INPUT_ASSIGNMENTS"],
            job.config.paths["CAP_ORIGINAL_INPUT_ASSIGNMENTS"],
        )

        backup(
            job,
            job.config.paths["INPUT_CONSTRUCTS"],
            job.config.paths["CAP_ORIGINAL_INPUT_CONSTRUCTS"],
        )

        backup(
            job,
            job.config.paths["MAIL_POPULATION_ASSIGNMENT"],
            job.config.paths["CAP_ORIGINAL_MAIL_POPULATION_ASSIGNMENT"],
        )

    job.log.info("Ingesting data")

    job.data.read("coupon_bank", "COUPON_BANK", filetype="csv")
    job.data.read(
        "mail_subset",
        "CAP_ORIGINAL_MAIL_POPULATION_ASSIGNMENT",
        filetype="csv",
    )
    job.data.read("coupon_closure", "COUPON_CLOSURE")
    job.data.read(
        "closure_coupon_path",
        "CLOSURE_COUPON_PATH",
        filetype="csv",
        schema=CAP_CLOSURE_COUPON_SCHEMA,
    )

    mail_subset = job.data.tables["mail_subset"]
    closure_bank = job.data.tables["coupon_closure"]
    closure_bank = closure_bank.withColumnRenamed(CAT_HRRCHY, CAT_LIST)
    cap_closure_coupons = job.data.tables["closure_coupon_path"]

    articles = job.data.tables["coupon_bank"].filter(
        sqlf.col("cpn_type") == "article"
    )
    assignments = mail_subset.withColumn(
        "slot_nbr", mail_subset["slot_nbr"].cast("int")
    )
    assignments = assignments.withColumn("original", assignments["cpn_nbr"])

    columns = assignments.columns

    # select only required data (specified cells and articles)
    closure_cells = assignments.filter(assignments["cell_id"].isin(cells))

    closure_cells = (
        closure_cells.filter(sqlf.col("mail_flag") == 1)
        .join(sqlf.broadcast(articles), "cpn_nbr", "inner")
        .select(assignments.columns)
    )

    non_closure_cells = assignments.subtract(closure_cells)

    eligible_closure_assignment = closure_bank.filter(
        closure_bank["closure_flag"] == 1
    )

    # get natural assigned closures
    natural_assigned_closures = assignments.join(
        cap_closure_coupons.select("PMR Offer ID", CAT_LIST),
        assignments["cpn_nbr"] == cap_closure_coupons["PMR Offer ID"],
        "inner",
    )

    # remove natural assigned closure from the closure cells
    closure_cells = closure_cells.join(
        cap_closure_coupons.select("PMR Offer ID"),
        closure_cells["cpn_nbr"] == cap_closure_coupons["PMR Offer ID"],
        "leftanti",
    )

    # remove possible natural assigned closure from the non closure cells
    non_closure_cells = non_closure_cells.join(
        cap_closure_coupons.select("PMR Offer ID"),
        non_closure_cells["cpn_nbr"] == cap_closure_coupons["PMR Offer ID"],
        "leftanti",
    )

    closure_cells = closure_cells.repartition("MBRSHP_SID", "cpn_nbr")
    closure_cells = truncate_history(closure_cells, cache=True)

    eligible_closure_assignment = eligible_closure_assignment.repartition(
        "MBRSHP_SID", "cpn_nbr"
    )
    eligible_closure_assignment = truncate_history(
        eligible_closure_assignment, cache=True
    )

    closure_distribution = natural_assigned_closures.groupBy("cpn_nbr").agg(
        sqlf.count("cpn_nbr").alias("current_distribution")
    )

    current_distributions = dict()
    for row in closure_distribution.collect():
        current_distributions[row["cpn_nbr"]] = row["current_distribution"]

    # remove natural assigned closures from eligible closures
    eligible_closure_assignment = eligible_closure_assignment.join(
        natural_assigned_closures, ["mbrshp_sid", "cpn_nbr"], "leftanti"
    )

    # remove assigned AHCD
    if ahcd_dedup:
        allowed_ahcd = expand_ahcd(eligible_closure_assignment)
        allowed_ahcd = allowed_ahcd.select("mbrshp_sid", CAT_HRRCHY).distinct()
        allowed_ahcd = truncate_history(allowed_ahcd, cache=True)

        expanded_natural_assignment = expand_ahcd(natural_assigned_closures)
        expanded_natural_assignment = expanded_natural_assignment.select(
            "mbrshp_sid", CAT_HRRCHY
        ).distinct()
        expanded_natural_assignment = truncate_history(
            expanded_natural_assignment, cache=True
        )

        allowed_ahcd = allowed_ahcd.join(
            expanded_natural_assignment, ["mbrshp_sid", CAT_HRRCHY], "leftanti"
        )

        allowed_ahcd = allowed_ahcd.groupBy("mbrshp_sid").agg(
            sqlf.collect_set(sqlf.col(CAT_HRRCHY)).alias("allowed_ahcd")
        )

        eligible_closure_assignment = eligible_closure_assignment.join(
            allowed_ahcd, "mbrshp_sid", "left"
        )

        eligible_closure_assignment = eligible_closure_assignment.withColumn(
            "allowed_ahcd",
            sqlf.when(
                sqlf.col("allowed_ahcd").isNull(), sqlf.array()
            ).otherwise(sqlf.col("allowed_ahcd")),
        )

        eligible_closure_assignment = eligible_closure_assignment.withColumn(
            CAT_LIST, sqlf.regexp_replace(sqlf.col(CAT_LIST), " ", "")
        ).withColumn(CAT_LIST, sqlf.split(CAT_LIST, ","))

        eligible_closure_assignment = truncate_history(
            eligible_closure_assignment, cache=True
        )

    conditions = sqlf.col("bf_construct") == "-"
    job.log.info("Equivalent swap with conditions %s" % conditions)

    result = swap_equivalent(
        job,
        closure_cells,
        cap_closure_coupons,
        eligible_closure_assignment,
        conditions,
        False,
        ahcd_dedup,
        current_distributions,
    )
    swap_frontfill = result[0]
    closure_cells = result[1]
    eligible_closure_assignment = result[2]
    current_distributions = result[3]

    job.log.info("Hit min distribution")

    # match min distribution
    result = match_distributions(
        job,
        closure_cells,
        cap_closure_coupons,
        eligible_closure_assignment,
        ahcd_dedup,
        ahcd_dedup,
        current_distributions,
        "min_distribution",
    )

    if ahcd_dedup:
        min_assigned_closures = list()
        min_ahcd_assigned_closures = result[0]
    else:
        min_assigned_closures = result[0]
        min_ahcd_assigned_closures = list()

    closure_cells = result[1]
    eligible_closure_assignment = result[2]
    current_distributions = result[3]

    closure_cells = closure_cells.repartition("MBRSHP_SID", "cpn_nbr")
    closure_cells = truncate_history(closure_cells, cache=True)

    eligible_closure_assignment = eligible_closure_assignment.repartition(
        "MBRSHP_SID", "cpn_nbr"
    )
    eligible_closure_assignment = truncate_history(
        eligible_closure_assignment, cache=True
    )

    conditions = sqlf.col("bf_construct") != "-"
    job.log.info("Equivalent swap with conditions %s" % conditions)

    result = swap_equivalent(
        job,
        closure_cells,
        cap_closure_coupons,
        eligible_closure_assignment,
        conditions,
        ahcd_dedup,
        ahcd_dedup,
        current_distributions,
    )
    swap_backfill = result[0]
    closure_cells = result[1]
    eligible_closure_assignment = result[2]
    current_distributions = result[3]

    job.log.info("Hit max distribution")

    # match max distribution
    max_ahcd_assigned_closures = list()
    if ahcd_dedup:
        result = match_distributions(
            job,
            closure_cells,
            cap_closure_coupons,
            eligible_closure_assignment,
            ahcd_dedup,
            ahcd_dedup,
            current_distributions,
            "max_distribution",
        )

        max_ahcd_assigned_closures = result[0]
        closure_cells = result[1]
        eligible_closure_assignment = result[2]
        current_distributions = result[3]

    result = match_distributions(
        job,
        closure_cells,
        cap_closure_coupons,
        eligible_closure_assignment,
        False,
        False,  # False becasue it is the last step, update if this changes
        current_distributions,
        "max_distribution",
    )

    max_assigned_closures = result[0]
    closure_cells = result[1]

    job.log.info("Put result together")

    closure_assignment = (
        [
            non_closure_cells.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME, sqlf.lit(ClosureMethod.NONE)
            )
        ]
        + [
            closure_cells.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME, sqlf.lit(ClosureMethod.NONE)
            )
        ]
        + [
            natural_assigned_closures.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME, sqlf.lit(ClosureMethod.NATURAL)
            )
        ]
        + [
            df.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME, sqlf.lit(ClosureMethod.SWAP)
            )
            for df in swap_frontfill
        ]
        + [
            df.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME, sqlf.lit(ClosureMethod.SWAP_BF)
            )
            for df in swap_backfill
        ]
        + [
            df.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME,
                sqlf.lit(ClosureMethod.HIT_MIN_AHCD_DEDUP),
            )
            for df in min_ahcd_assigned_closures
        ]
        + [
            df.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME, sqlf.lit(ClosureMethod.HIT_MIN)
            )
            for df in min_assigned_closures
        ]
        + [
            df.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME,
                sqlf.lit(ClosureMethod.HIT_MAX_AHCD_DEDUP),
            )
            for df in max_ahcd_assigned_closures
        ]
        + [
            df.select(*columns).withColumn(
                ClosureMethod.COLUMN_NAME, sqlf.lit(ClosureMethod.HIT_MAX)
            )
            for df in max_assigned_closures
        ]
    )

    closure_assignment = apply_unionall(*closure_assignment)

    job.log.info("Write mail subset closure...")
    if job.config.params["run_type"].lower() == "test":
        closure_assignment = closure_assignment.repartition(1)
        closure_assignment.write.csv(
            job.config.paths["MAIL_POPULATION_ASSIGNMENT"],
            header=True,
            mode="overwrite",
        )
    else:
        closure_assignment = closure_assignment.repartition(32)
        closure_assignment.write.csv(
            job.config.paths["MAIL_POPULATION_ASSIGNMENT"],
            header=True,
            mode="overwrite",
        )

    job.log.info("Put closure assignment together")

    job.data.read(
        "input_assignments", "CAP_ORIGINAL_INPUT_ASSIGNMENTS", filetype="csv"
    )

    input_assignments = job.data.tables["input_assignments"]
    input_assignments = input_assignments.withColumnRenamed(
        "cpn_nbr", "original"
    )
    closure_input_assignments = input_assignments.join(
        closure_assignment,
        [
            "experiment_id",
            "cell_id",
            "mbrshp_sid",
            "slot_nbr",
            "construct",
            "bf_construct",
            "original",
            "pool_type",
        ],
        "left",
    )

    closure_input_assignments = closure_input_assignments.withColumn(
        ClosureMethod.COLUMN_NAME,
        sqlf.when(
            closure_input_assignments["cpn_nbr"].isNull(),
            sqlf.lit(ClosureMethod.NONE),
        ).otherwise(closure_input_assignments[ClosureMethod.COLUMN_NAME]),
    )

    closure_input_assignments = closure_input_assignments.withColumn(
        "cpn_nbr",
        sqlf.when(
            closure_input_assignments["cpn_nbr"].isNull(),
            closure_input_assignments["original"],
        ).otherwise(closure_input_assignments["cpn_nbr"]),
    )

    closure_input_assignments = closure_input_assignments.select(
        job.data.tables["input_assignments"].columns
        + ["original", ClosureMethod.COLUMN_NAME]
    )

    job.log.info("Write closure assignments...")

    if job.config.params["run_type"].lower() == "test":
        closure_input_assignments = closure_input_assignments.repartition(1)
        closure_input_assignments.write.csv(
            job.config.paths["INPUT_ASSIGNMENTS"],
            header=True,
            mode="overwrite",
        )
    else:
        closure_input_assignments = closure_input_assignments.repartition(32)
        closure_input_assignments.write.csv(
            job.config.paths["INPUT_ASSIGNMENTS"],
            header=True,
            mode="overwrite",
        )

    job.log.info("Put closure constructs together")

    job.data.read("input_constructs", "CAP_ORIGINAL_INPUT_CONSTRUCTS")

    input_constructs = job.data.tables["input_constructs"]
    input_constructs = input_constructs.withColumnRenamed(
        "cpn_nbr", "original"
    )
    closure_input_constructs = input_constructs.join(
        closure_assignment,
        [
            "experiment_id",
            "cell_id",
            "mbrshp_sid",
            "construct",
            "bf_construct",
            "original",
            "pool_type",
        ],
        "left",
    )

    closure_input_constructs = closure_input_constructs.withColumn(
        ClosureMethod.COLUMN_NAME,
        sqlf.when(
            closure_input_constructs["cpn_nbr"].isNull(),
            sqlf.lit(ClosureMethod.NONE),
        ).otherwise(closure_input_constructs[ClosureMethod.COLUMN_NAME]),
    )

    closure_input_constructs = closure_input_constructs.withColumn(
        "cpn_nbr",
        sqlf.when(
            closure_input_constructs["cpn_nbr"].isNull(),
            closure_input_constructs["original"],
        ).otherwise(closure_input_constructs["cpn_nbr"]),
    )

    closure_input_constructs = closure_input_constructs.select(
        job.data.tables["input_constructs"].columns
        + ["original", ClosureMethod.COLUMN_NAME]
    )

    job.log.info("Write closure constructs...")

    if job.config.params["run_type"].lower() == "test":
        closure_input_constructs = closure_input_constructs.repartition(1)
        closure_input_constructs.write.parquet(
            job.config.paths["INPUT_CONSTRUCTS"], mode="overwrite"
        )
    else:
        closure_input_constructs = closure_input_constructs.repartition(64)
        closure_input_constructs.write.parquet(
            job.config.paths["INPUT_CONSTRUCTS"], mode="overwrite"
        )

    job.log.info("Done.")


if __name__ == "__main__":

    jobManager = JobManager("assign_closures", "AssignClosures")
    args = get_run_args()

    main(jobManager, args.cells, args.iterate, args.seed, args.ahcd_dedup)
