"""
Use this module's generate_population() to ingest the skeleton data required to
build the dna features for all members which fall or not in the specified time
window.

If the members fall into the specified time window features will be calculated
based on their historic data, otherwise they will receive default values for
each feature.

This module can be ran as standalone, if one desires to check the output of the
Dataframe prior to development.
"""
import datetime
import os

import pe_memberdna.dna.member.lib.managers as managers
import pe_memberdna.dna.member.lib.utils as utils
import pe_memberdna.lib.misc as misc
import pyspark
import pyspark.sql.functions as sqlf


def apply_fw_date_range(job, df):
    """
    Apply limit on FISCAL WEEKEND and return records with FISCAL WEEKEND
    between 52 weeks prior to start date and end date

    Parameters:
        job (JobManager): a helper object for accessing data and config
        df (pyspark.sql.Dataframe): dataframe with FISCAL WEEKEND column

    Returns:
        (pyspark.sql.Dataframe): a filtered dataframe
    """
    _, end_date, lookback_start_date = utils.window_dates(job)
    df = df.filter(df.FISCAL_WEEK_END.between(lookback_start_date, end_date))
    return df


def generate_population(job, window="partition"):
    """
    Generate the population for which we want to calculate dna features

    Parameters:
        job (JobManager): a helper object for accessing data and config
        window (str): Either "feature" or "partition". "feature" will start
            the population at 52 weeks back, representing the largest
            possible window to calculate a feature. "partition" will start
            the population 52 weeks * 56 weeks back, representing the full
            window across all fiscal week partitions

    Returns:
        (pyspark.sql.Dataframe): a dataframe with all members
            and fiscal weeknds required for calculating the features
    """
    start_date, end_date, lookback_start_date = utils.window_dates(job)

    job.log.info(
        f"Using the following dates: start_date = {start_date},"
        f"end_date = {end_date}, lookback_start_date = {lookback_start_date}"
    )

    skeleton = job.data.tables["skeleton"]
    member_extended = job.data.tables["member_extended"]

    if window == "partition":
        start_date = lookback_start_date

    population = skeleton.filter(
        skeleton.FISCAL_WEEK_END.between(start_date, end_date)
    )

    fiscal_weekends = population.drop("MBRSHP_SID").distinct()

    today = misc.today_helper()
    three_months = 30 * 3
    active_threshold = today - datetime.timedelta(three_months)
    active_members = member_extended.filter(
        sqlf.col("MBRSHP_EXP_DT") >= active_threshold
    )

    missing_members = active_members.select("MBRSHP_SID").join(
        population.select("MBRSHP_SID").distinct(), "MBRSHP_SID", "leftanti"
    )
    missing_members = missing_members.crossJoin(
        sqlf.broadcast(fiscal_weekends)
    )

    columns = population.columns
    # bring in missing members for futher processing
    population = (
        population.select(*columns)
        .union(missing_members.select(*columns))
        .persist(pyspark.StorageLevel.MEMORY_ONLY)
    )

    return population


if __name__ == "__main__":
    job = managers.JobManager("dna_generate_population")

    job.data.read("skeleton", "skeleton_path")
    job.data.read("member_extended", "member_extended_path")

    population = generate_population(job)
    population.show()
