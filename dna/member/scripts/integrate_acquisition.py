import pe_memberdna.dna.member.lib.managers as managers
import pe_memberdna.dna.member.lib.acquisition_features as features
from pe_memberdna.dna.member.lib.s3 import member_dna_input_data_validator


def integrate_acquisition(job):
    """
    Integrate acquisition variables for the given population
    Parameters:
        job (object): Job Manager object based on the current config file

    Returns:
        dna (pyspark.sql.DataFrame): Acquisition variable added to the given
                                     population
    """

    dna = job.data.tables["population"]
    orig_cols = dna.columns

    dna = features.feature_age_income(job, dna)

    dna = dna.drop(
        *[
            col
            for col in orig_cols
            if col not in ["MBRSHP_SID", "FISCAL_WEEK_END"]
        ]
    )

    return dna


def main():
    job = managers.JobManager("acquisition")
    recency_lookback_duration = job.config.params["params"].get(
        "recency_lookback_duration", {}
    )
    member_dna_input_data_validator(
        job,
        recency_lookback_duration,
        [
            "member_features_path",
            "acq_dna_path",
            "mbr_basic_path",
        ],
    )
    job.data.read("population", "member_features_path")
    job.data.read("acq_dna", "acq_dna_path")
    job.data.read("mbr_basic", "mbr_basic_path", filetype="csv")

    features = integrate_acquisition(job)
    job.data.tables["acquisition"] = features
    job.data.write(
        "acquisition",
        "acquisition_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )

    job.log.info("Done")


if __name__ == "__main__":
    main()
