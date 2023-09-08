"""Spark job to generate a subsetted population from assignment data

Hardware Requirements:
- Recommend at least 6 r4 worker nodes to run
- Runtime: Approx.

TODO:
[] Add integration test
[] Convert trial calculation to member DNA
"""

import pyspark.sql.functions as sqlf
from pe_memberdna.pipelines.assignment.lib.assn_io import JobManager
from pe_memberdna.pipelines.assignment.lib.assn_utils import (
    calc_overlapping_cols,
    calc_sampling_seg,
    calc_sampling_value,
    count_pcs_of_mail,
    palindrome_sample,
    read_subset_and_cast,
    set_mail_flag,
)
from pe_memberdna.pipelines.assignment.lib.checks import (
    check_execution_overwrite,
)
from pe_memberdna.pipelines.lib.iotools import read_s3_to_local
from pe_memberdna.pipelines.lib.spark_util import truncate_history
from pe_memberdna.pipelines.lib.utils import top_n
from pyspark import StorageLevel


def main(conf_path_in=None):

    name = "SubsamplePopulation"
    job = JobManager("subset", name, conf_path_in)

    if job.config.params["run_type"].lower() == "prod":
        check_execution_overwrite(paths_to_check=[job.config.paths["SUBSET"]])

    campaign_data = read_s3_to_local(job.config.paths["CAMPAIGN"])
    CIRC = campaign_data[
        campaign_data.experiment_id == job.config.params["experiment"]
    ]["expected_distribution"].iloc[0]
    job.log.info("Desired Circulation: {}".format(CIRC))

    # ---- Read Data ---- #

    job.log.info("1. Reading Data...")

    data = read_subset_and_cast(job.config.paths["INPUT_ASSIGNMENTS"], "csv")
    renames = [
        "mbrshp_sid",
        "experiment_id",
        "cell_id",
        "construct",
        "cpn_nbr",
    ]
    for name in renames:
        data = data.withColumnRenamed(name, name.upper())

    cols = ["MBRSHP_SID", "TENURE", "DAYS_SINCE_LAST_TRIP"] + [
        job.config.params["sampling_weeks_rev_columns"]
    ]
    dna = read_subset_and_cast(
        job.config.paths["CUBE"],
        "parquet",
        cols,
        "fiscal_week",
        job.config.params["assignment_date"],
    )

    cols = ["MBRSHP_NBR", "decile", "score"]
    props = read_subset_and_cast(job.config.paths["MAIL_LIST"], "csv", cols)

    cols = ["MBRSHP_NBR", "MBRSHP_SID"]
    mail_list = read_subset_and_cast(job.config.paths["MAIL_LIST"], "csv")
    if "MBRSHP_SID" in mail_list.columns:
        mbr_lkup = mail_list.select(*cols)
    else:
        mbr_lkup = read_subset_and_cast(
            job.config.paths["RAW_MEMBER"], "parquet", cols
        )

    cols = ["MBRSHP_NBR", "FHH_IND"]
    fhh = read_subset_and_cast(job.config.paths["FHH"], "csv", cols)
    fhh = fhh.dropDuplicates(subset=["MBRSHP_NBR"])
    fhh = fhh.withColumn(
        "FHH_IND",
        sqlf.when(fhh.FHH_IND == "Y", sqlf.lit(1)).otherwise(sqlf.lit(0)),
    )

    if job.config.params["force_in_new"]:
        job.data.read("cube", "CUBE", filetype="parquet")
        cube = job.data.tables["cube"]
        max_wk = cube.select(sqlf.max("FISCAL_WEEK_END")).collect()[0][
            0
        ]  # always want the most recent data
        cube = cube[cube.FISCAL_WEEK_END == max_wk]
        cube = cube.dropDuplicates(subset=["MBRSHP_SID"])
        cube = cube.select("MBRSHP_SID", "TENURE")
        cube = cube.withColumn(
            "IS_TENURE_NULL", sqlf.when(cube.TENURE.isNull(), 1).otherwise(0)
        )
        job.data.read(
            "mbr",
            "RAW_MEMBER",
            filetype="parquet",
            cols=["MBRSHP_SID", "MKT_CD", "MBRSHP_FEE_INC"],
        )
        mbr = job.data.tables["mbr"]
        cube = cube.join(mbr, "MBRSHP_SID", "left")
        new150 = sqlf.col("TENURE") < 150
        nofee = sqlf.col("MBRSHP_FEE_INC") == 0
        nulltenure = sqlf.col("IS_TENURE_NULL") == 1
        trialmarket = sqlf.col("MKT_CD").like("Z%")
        trial = nofee & trialmarket
        new150_forcein = new150 & ~trial & ~nulltenure

        cube = cube.withColumn(
            "NEW_150", sqlf.when(new150_forcein, 1).otherwise(0)
        )
        cube = cube.select("MBRSHP_SID", "NEW_150")

    # ---- Generate Data By Member ---- #

    job.log.info("2. Generating by-member dataset...")
    by_mbr = data.groupBy("MBRSHP_SID").agg(
        sqlf.first("CELL_ID").alias("cell")
    )

    # create base mbr data
    # join with lkup and props
    by_mbr = by_mbr.join(mbr_lkup, ["MBRSHP_SID"], "left")

    # join with dna and calculate sampling value
    join_col = calc_overlapping_cols(by_mbr, dna)
    dna = dna.dropDuplicates(subset=join_col)
    sampling_input = by_mbr.join(dna, join_col, "left")
    sampling_value = calc_sampling_value(
        sampling_input, job.config.params["sampling_weeks_rev_columns"]
    )
    by_mbr = by_mbr.join(sampling_value, "MBRSHP_SID", "left")

    # join with props and calculate sampling segment

    join_col = calc_overlapping_cols(by_mbr, props)
    props = props.dropDuplicates(subset=join_col)
    sampling_input = sampling_input.join(props, join_col, "left")
    sampling_seg = calc_sampling_seg(sampling_input)
    by_mbr = by_mbr.join(sampling_seg, "MBRSHP_SID", "left")

    # join with score and decile
    by_mbr = by_mbr.join(props, ["MBRSHP_NBR"], "left")

    if job.config.params["force_in_new"]:
        by_mbr = by_mbr.join(cube, ["MBRSHP_SID"], "left")
        by_mbr = by_mbr.fillna(1, subset=["NEW_150"])
    else:
        by_mbr = by_mbr.withColumn("NEW_150", sqlf.lit(0))

    by_mbr = by_mbr.join(fhh, ["MBRSHP_NBR"], "left")
    by_mbr = by_mbr.fillna(0, subset=["score", "FHH_IND"])
    by_mbr = by_mbr.fillna(10, subset=["decile"])

    # set baseline mail flag
    by_mbr = by_mbr.withColumn("mail_flag", sqlf.lit(None))
    by_mbr.persist(StorageLevel.DISK_ONLY)

    # ---- Set up static comparisions ---- #

    # this will be empty if force in not selected
    NEW_MBR = sqlf.col("NEW_150") == 1
    IN_FORCEIN = sqlf.col("cell").isin(job.config.params["force_in_cells"])
    IN_FORCEOUT = sqlf.col("cell").isin(job.config.params["force_out_cells"])
    IN_TOPFOUR = sqlf.col("decile").isin([1, 2, 3, 4])
    IN_FIVE = sqlf.col("decile") == 5
    IN_SIX = sqlf.col("decile") == 6
    IN_SEVEN = sqlf.col("decile") == 7
    CURRENTLY_MAILED = sqlf.col("mail_flag") == 1
    CURRENTLY_NOMAIL = sqlf.col("mail_flag") == 0
    CURRENTLY_AVAILABLE = sqlf.col("mail_flag").isNull()

    job.log.info("Exclude force-out cells")
    forced_holdout = by_mbr[IN_FORCEOUT & CURRENTLY_AVAILABLE]
    by_mbr = set_mail_flag(by_mbr, forced_holdout, 0)

    # ---- Set Aside Force-ins ---- #

    job.log.info("3. Reserving force-ins...")

    # ---- Remove New Member No-mail ---- #

    if "sample_cells" in job.config.params:
        for cell in job.config.params["sample_cells"]:
            cell_sample = (
                sqlf.col("cell") == sqlf.lit(cell)
            ) & CURRENTLY_AVAILABLE
            cell_no_mail = palindrome_sample(
                by_mbr[cell_sample], job.config.params["sample_size"]
            ).cache()
            by_mbr = set_mail_flag(by_mbr, cell_no_mail, 0)
            by_mbr = set_mail_flag(by_mbr, by_mbr[cell_sample], 1)

    if job.config.params["force_in_new"]:
        new = by_mbr[NEW_MBR & CURRENTLY_AVAILABLE]
        new_nomail = palindrome_sample(new, job.config.params["sample_size"])
        by_mbr = set_mail_flag(by_mbr, new_nomail, 0)

        forced_new = by_mbr[NEW_MBR & CURRENTLY_AVAILABLE]
        by_mbr = set_mail_flag(by_mbr, forced_new, 1)

    reserved = IN_FORCEIN | (NEW_MBR & CURRENTLY_AVAILABLE)
    forceins = by_mbr[reserved]
    by_mbr = set_mail_flag(by_mbr, forceins, 1)

    by_mbr.groupBy("decile", "mail_flag").count().orderBy(
        sqlf.col("decile").cast("long"), sqlf.col("mail_flag")
    ).show(100)

    # ---- Sample deciles 1 through 5 No-mail ---- #

    job.log.info("Sampling no-mail for BBM deciles 1-4...")

    d14_avail = CURRENTLY_AVAILABLE & IN_TOPFOUR
    decile_14 = by_mbr[d14_avail]
    d14_nomail = palindrome_sample(
        decile_14, job.config.params["sample_size"]
    ).cache()
    job.log.info("Decile 1-4 sample size: ".format(d14_nomail.count()))

    by_mbr = set_mail_flag(by_mbr, d14_nomail, 0)
    by_mbr = set_mail_flag(by_mbr, by_mbr[d14_avail], 1)

    current_pcs = count_pcs_of_mail(by_mbr[CURRENTLY_MAILED])
    current_pcs = current_pcs if current_pcs else 0

    remaining_circ = CIRC - current_pcs
    by_mbr.groupBy("decile", "mail_flag").count().orderBy(
        sqlf.col("decile").cast("long"), sqlf.col("mail_flag")
    ).show(100)

    job.log.info("Finished deciding mail and no mail for deciles 1-4.")

    if remaining_circ <= 0:
        job.log.warn("Deciles 1-4 already exceeds mailable population.")

    invalid_sid = sqlf.lit("xxxxxxxx")
    current_decile = 5

    while (remaining_circ > 0) & (current_decile <= 10):
        job.log.info(
            "Deciding mail and no mail for decile {}".format(current_decile)
        )
        job.log.info("Remaining circ needed {}".format(remaining_circ))
        sample_size = job.config.params["sample_size"]
        available_in_decile_filter = (
            sqlf.col("decile") == current_decile
        ) & CURRENTLY_AVAILABLE
        decile_members_available = by_mbr[available_in_decile_filter].cache()

        decile_members_available_count = decile_members_available.count()
        job.log.info(
            "Available members in decile {}".format(
                decile_members_available_count
            )
        )

        pieces_of_mail_available = count_pcs_of_mail(decile_members_available)
        pieces_of_mail_available = (
            pieces_of_mail_available if pieces_of_mail_available else 0
        )

        job.log.info(
            "Pieces of mail in decile {}".format(pieces_of_mail_available)
        )

        if sample_size > remaining_circ:
            job.log.info(
                "Sample size is larger than remaining circ, setting sample size equal to remaining circ"
            )
            sample_size = remaining_circ

        possible_circ_from_decile = min(
            pieces_of_mail_available, remaining_circ
        )

        if remaining_circ + sample_size >= pieces_of_mail_available:
            job.log.warn(
                "Not enough members in decile {} to finish circulation.".format(
                    current_decile
                )
            )
            if pieces_of_mail_available > 2 * sample_size:
                job.log.info(
                    "Enough in decile to get full sample size, but not enough to finish circulation"
                )
                circ_from_this_decile = possible_circ_from_decile
            else:
                job.log.info(
                    "Not enough in decile to sample, adjusting sample size to half available"
                )
                circ_from_this_decile = int(possible_circ_from_decile / 2)
                sample_size = possible_circ_from_decile - circ_from_this_decile
        else:
            circ_from_this_decile = possible_circ_from_decile

        circ_from_this_decile = (
            circ_from_this_decile + job.config.params["error"]
        )  # allow for overshoot by error factor

        # In some cases this will log more than there are pieces available, when it does its fine
        # it will readjust in the next iteration
        job.log.info(
            "Will sample {} for mail and {} for no mail from decile {}".format(
                circ_from_this_decile, sample_size, current_decile
            )
        )

        decile_members_fhh = (
            decile_members_available[decile_members_available.FHH_IND == 1]
            .withColumn("MBRSHP_SID", invalid_sid)
            .select(by_mbr.columns)
        )

        countable_decile_members = decile_members_available.union(
            decile_members_fhh
        )

        candidate_postcard_members = top_n(
            countable_decile_members,
            circ_from_this_decile + sample_size,
            "score",
        ).filter(sqlf.col("MBRSHP_SID") != invalid_sid)
        job.log.info(
            "Selected top {} from decile {} for both mail and control".format(
                candidate_postcard_members.count(), current_decile
            )
        )

        job.log.info("Pre Sample for decile {}".format(current_decile))
        by_mbr.filter(sqlf.col("decile") == current_decile).groupBy(
            "decile", "mail_flag"
        ).count().orderBy(
            sqlf.col("decile").cast("long"), sqlf.col("mail_flag")
        ).show(
            100
        )

        if sample_size > 0:
            decile_nomail = palindrome_sample(
                candidate_postcard_members, sample_size
            ).cache()
            job.log.info(
                "{} sampled for no mail".format(decile_nomail.count())
            )
            by_mbr = set_mail_flag(by_mbr, decile_nomail, 0)
        else:
            decile_nomail = candidate_postcard_members.filter(sqlf.lit(False))

        if circ_from_this_decile > 0:
            # This if statement is neccessary because left anti join
            # gives errors when run on two empty dataframes
            if (
                candidate_postcard_members.count() > 0
                and decile_nomail.count() > 0
            ):
                decile_mail = candidate_postcard_members.join(
                    decile_nomail, "MBRSHP_SID", "leftanti"
                )
            else:
                decile_mail = candidate_postcard_members

            job.log.info("{} sampled for mail".format(decile_mail.count()))
            by_mbr = set_mail_flag(by_mbr, decile_mail, 1)
        by_mbr = truncate_history(by_mbr, True)

        job.log.info("Post Sample for decile {}".format(current_decile))
        by_mbr.filter(sqlf.col("decile") == current_decile).groupBy(
            "decile", "mail_flag"
        ).count().orderBy(
            sqlf.col("decile").cast("long"), sqlf.col("mail_flag")
        ).show(
            100
        )

        if (
            by_mbr[available_in_decile_filter].count()
            < job.config.params["error"]
        ):
            current_decile = current_decile + 1

        current_pcs = count_pcs_of_mail(by_mbr[CURRENTLY_MAILED])
        current_pcs = current_pcs if current_pcs else 0

        remaining_circ = CIRC - current_pcs

    job.log.info("Final by decile")
    by_mbr.groupBy("decile", "mail_flag").count().orderBy(
        sqlf.col("decile").cast("long"), sqlf.col("mail_flag")
    ).show()
    by_mbr = by_mbr[~CURRENTLY_AVAILABLE]

    by_mbr = by_mbr.select("MBRSHP_SID", "mail_flag", "decile")
    assert by_mbr.count() == by_mbr.select("mbrshp_sid").distinct().count()

    data = data.join(by_mbr, "MBRSHP_SID", "right")

    job.log.info("Final by decile check")
    data.drop_duplicates(["mbrshp_sid", "decile", "mail_flag"]).groupBy(
        "decile", "mail_flag"
    ).count().orderBy(
        sqlf.col("decile").cast("long"), sqlf.col("mail_flag")
    ).show()

    job.log.info("Writing output...")
    job.data.add("data", data.drop("decile"))
    job.data.write("data", "SUBSET", mode="overwrite", ftype="csv")

    job.log.info("done.")


if __name__ == "__main__":
    main()
