"""
The script which calculates intermediate features related to coupon and digital
and have dependencies only on the intermediate source etl data.

The output of this script is a dependency for generate_coupon_and_digital_2.py.
"""

import pyspark.sql.functions as sqlf

import memberdna.dna.lib.coupon_digital_features as features
import memberdna.dna.lib.generate_population as gp
import memberdna.dna.lib.managers as managers
import memberdna.dna.lib.utils as utils


def generate_coupon_and_digital(job):
    """
    Generate the features associated with coupon and digital

    Parameters:
        job (managers.JobManager): object which manages the Spark App
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    dna = job.data.tables["population"]
    orig_cols = dna.columns

    weeks_back = [4, 8, 12, 26, 52]

    dna = features.feature_atc(job, dna, weeks_back)

    dna = features.feature_coupon_clipped(job, dna, weeks_back)

    dna = utils.cache_df(dna)

    tender_type_cds = ["CPN", "PCUM", "PCUS", "PCUE", "PCUB", "PCUR"]

    # Calculate the total number of coupon redemptions within
    # a given fiscal week
    dna = features.feature_fiscal_coupon_general(
        job,
        dna,
        tender_type_cds=tender_type_cds,
        discount_type_cds=["ZCOU", "ZPAP"],
        aggregate=sqlf.count("*"),
        alias="FW_COUPON_REDEMPTIONS",
    )

    # Calculate the total number of coupon redemptions + clipless within
    # a given fiscal week
    dna = features.feature_fiscal_coupon_general(
        job,
        dna,
        tender_type_cds=tender_type_cds,
        discount_type_cds=["ZCOU", "ZPAP", "ZCLP"],
        aggregate=sqlf.count("*"),
        alias="FW_COUPON_REDEMPTIONS_W_CLPLSS",
    )

    # Calculate the total fiscal amount redeemed in coupons within
    # a given fiscal week
    dna = features.feature_fiscal_coupon_general(
        job,
        dna,
        tender_type_cds=tender_type_cds,
        discount_type_cds=["ZCOU", "ZPAP"],
        aggregate=sqlf.sum("savings_temp"),
        alias="FW_COUPON_SAVINGS",
    )

    # Calculate the total fiscal amount redeemed in coupons + in clipless
    # within a given fiscal week
    dna = features.feature_fiscal_coupon_general(
        job,
        dna,
        tender_type_cds=tender_type_cds,
        discount_type_cds=["ZCOU", "ZPAP", "ZCLP"],
        aggregate=sqlf.sum("savings_temp"),
        alias="FW_COUPON_SAVINGS_W_CLPLSS",
    )

    dna = features.feature_days_since_last_coupon_redeemed(job, dna)
    dna = features.feature_days_since_last_email_open(job, dna)
    dna = features.feature_days_since_last_atc_clipped(job, dna)
    dna = features.feature_email_open_rate(job, dna)
    dna = features.feature_fiscal_savings_with_clipless(job, dna)
    dna = features.feature_cpn_channel(job, dna)

    dna = utils.cache_df(dna)

    # Compute the aggregate fiscal amount redeemed in coupons + clipless
    # per customer {weeks} fiscal weeks back
    weeks = ["FOUR", "EIGHT", "TWELVE", "TWENTY-SIX", "FIFTY-TWO"]
    for num_weeks in weeks:
        dna = features.feature_aggregate_per_member_weeks(
            job, dna, num_weeks, "FW_SAVINGS_W_CLPLSS"
        )

    dna = utils.cache_df(dna)

    dna = dna.drop(
        *[
            col
            for col in orig_cols
            if col not in ["MBRSHP_SID", "FISCAL_WEEK_END"]
        ]
    )

    return dna


def main():
    job = managers.JobManager("generate_coupon_and_digital_1")

    job.data.read("skeleton", "skeleton_path")
    job.data.read("member_extended", "member_extended_path")
    job.data.read("member_history", "member_history_path")
    job.data.read("header", "header_path")
    job.data.read("detail", "detail_path")
    job.data.read("payment", "payment_path")
    job.data.read("coupon_clip", "coupon_clip_path")
    job.data.read("email", "email_path")
    job.data.read("email_fiscal", "email_fiscal_path")
    job.data.read("awards", "awards_fiscal_path")

    job.data.tables["header"] = gp.apply_fw_date_range(
        job, job.data.tables["header"]
    )

    job.data.tables["detail"] = gp.apply_fw_date_range(
        job, job.data.tables["detail"]
    )

    job.data.tables["payment"] = gp.apply_fw_date_range(
        job, job.data.tables["payment"]
    )

    job.data.tables["coupon_clip"] = gp.apply_fw_date_range(
        job, job.data.tables["coupon_clip"]
    )

    job.data.tables["awards"] = gp.apply_fw_date_range(
        job, job.data.tables["awards"]
    )

    population = gp.generate_population(job)
    job.data.tables["population"] = population

    features = generate_coupon_and_digital(job)
    job.data.tables["coupon_and_digital"] = features
    job.data.write(
        "coupon_and_digital",
        "coupon_and_digital_1_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )

    job.log.info("Done")


if __name__ == "__main__":
    main()
