"""
The script which calculates intermediate features related to coupon and digital
and have dependencies on the intermediate from generate_coupon_and_digital_1.py
"""
import pe_memberdna.dna.member.lib.coupon_digital_features as features
import pe_memberdna.dna.member.lib.managers as managers
import pe_memberdna.dna.member.lib.utils as utils


def generate_coupon_and_digital(job):
    """
    Generate the features associated with coupon and digital

    Parameters:
        job (managers.JobManager): object which manages the Spark App
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    dna = job.data.tables["population"].select(
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        "FW_COUPON_REDEMPTIONS",
        "FW_COUPON_REDEMPTIONS_W_CLPLSS",
        "FW_COUPON_SAVINGS",
        "FW_COUPON_SAVINGS_W_CLPLSS",
    )

    orig_cols = dna.columns

    # Compute the aggregate feature per customer {weeks} fiscal weeks back
    weeks = ["FOUR", "EIGHT", "TWELVE", "TWENTY-SIX", "FIFTY-TWO"]
    for num_weeks in weeks:
        dna = features.feature_aggregate_per_member_weeks(
            job, dna, num_weeks, "FW_COUPON_REDEMPTIONS"
        )

        dna = features.feature_aggregate_per_member_weeks(
            job, dna, num_weeks, "FW_COUPON_REDEMPTIONS_W_CLPLSS"
        )

        dna = features.feature_aggregate_per_member_weeks(
            job, dna, num_weeks, "FW_COUPON_SAVINGS"
        )

        dna = features.feature_aggregate_per_member_weeks(
            job, dna, num_weeks, "FW_COUPON_SAVINGS_W_CLPLSS"
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
    job = managers.JobManager("generate_coupon_and_digital_2")

    job.data.read("skeleton", "skeleton_path")
    job.data.read("member_extended", "member_extended_path")
    job.data.read("population", "coupon_and_digital_1_path")

    features = generate_coupon_and_digital(job)
    job.data.tables["coupon_and_digital_2"] = features
    job.data.write(
        "coupon_and_digital_2",
        "coupon_and_digital_2_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )
    job.log.info("Done")


if __name__ == "__main__":
    main()
