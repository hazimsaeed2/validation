"""
The script which calculates intermediate features related to member. It only
depends on source etl intermediates.
"""
import pe_memberdna.dna.member.lib.generate_population as gp
import pe_memberdna.dna.member.lib.managers as managers
import pe_memberdna.dna.member.lib.member_features as features
import pe_memberdna.dna.member.lib.utils as utils
from pe_memberdna.dna.member.lib.s3 import member_dna_input_data_validator


def generate_member(job):
    """
    Generate the features associated with member

    Parameters:
        job (managers.JobManager): object which manages the Spark App
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    dna = job.data.tables["population"]
    orig_cols = dna.columns

    dna = features.feature_member_master(job, dna)
    dna = features.feature_member_extended(job, dna)
    dna = features.feature_team_member_ind(job, dna)
    dna = features.feature_trial_member_ind(job, dna)

    dna = utils.cache_df(dna)

    dna = features.feature_base_mfi(job, dna)
    dna = features.feature_member_history(job, dna)
    dna = features.feature_calculated_mfi(job, dna)
    dna = features.feature_distance(job, dna)
    dna = features.feature_tenure(job, dna)

    dna = utils.cache_df(dna)

    dna = features.feature_days_until_exp(job, dna)
    dna = features.feature_days_since_last_rnwl(job, dna)
    dna = features.feature_num_of_rnwls(job, dna)
    dna = features.feature_quotient_id(job, dna)

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
    job = managers.JobManager("member")
    recency_lookback_duration = job.config.params["params"].get(
        "recency_lookback_duration", {}
    )
    member_dna_input_data_validator(
        job,
        recency_lookback_duration,
        [
            "skeleton_path",
            "member_extended_path",
            "member_path",
            "member_history_path",
            "census_tract_path",
            "quotient_id_path",
        ],
    )
    job.data.read("skeleton", "skeleton_path")
    job.data.read("member", "member_path")
    job.data.read("member_extended", "member_extended_path")
    job.data.read("member_history", "member_history_path")
    job.data.read("census_tract", "census_tract_path")
    job.data.read("quotient_id", "quotient_id_path")

    population = gp.generate_population(job)
    job.data.tables["population"] = population

    features = generate_member(job)
    job.data.tables["member_features"] = features
    job.data.write(
        "member_features",
        "member_features_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )

    job.log.info("Done")


if __name__ == "__main__":
    main()
