import memberdna.dna.lib.generate_population as gp
import memberdna.dna.lib.managers as managers
import memberdna.dna.lib.misc_features as features


def generate_misc(job):
    """
    Generate the miscellaneous variables for the given population
    Parameters:
        job (object): Job Manager object based on the current config file
    
    Returns:
        (pyspark.sql.DataFrame): Miscellaneous data of the given population
    """

    dna = job.data.tables["population"]
    member = job.data.tables["member"].select(
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        "LATEST_MBRSHP_NBR",
        "LATEST_HOME_ZIP_CD",
        "BJS_DISTANCE",
    )
    transaction1 = job.data.tables["transaction_1"].select(
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        "LAST_FIFTY-TWO_WEEK_TRIPS",
        "LAST_TWELVE_WEEK_TRIPS",
        "LAST_TWENTY-SIX_WEEK_TRIPS",
    )
    transaction2 = job.data.tables["transaction_2"].select(
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        "LAST_FIFTY-TWO_WEEK_SPEND",
        "LAST_TWELVE_WEEK_SPEND",
        "LAST_TWENTY-SIX_WEEK_SPEND",
    )

    dna = dna.join(
        member,
        ["MBRSHP_SID", "FISCAL_WEEK_END"],
        "left_outer"
    ).join(
        transaction1,
        ["MBRSHP_SID", "FISCAL_WEEK_END"],
        "left_outer"
    ).join(
        transaction2,
        ["MBRSHP_SID", "FISCAL_WEEK_END"],
        "left_outer"
    )
    orig_cols = dna.columns

    dna = features.feature_preferred_club(job, dna)
    dna = features.feature_dummy_member(job, dna)

    num_weeks = ["TWELVE", "TWENTY-SIX"]
    dna = features.feature_last_over_prior(job, dna, num_weeks, "spend")
    dna = features.feature_last_over_prior(job, dna, num_weeks, "trips")

    dna = features.feature_strategic_segment(job, dna)

    dna = features.feature_preferred_club_has_gas(job, dna, 52)

    dna = dna.drop(
        *[
            col
            for col in orig_cols
            if col not in ["MBRSHP_SID", "FISCAL_WEEK_END"]
        ]
    )

    return dna


def main():
    job = managers.JobManager("misc")

    job.data.read("header", "header_path")
    job.data.read("detail_isnr", "detail_isnr_path")
    job.data.read("skeleton", "skeleton_path")
    job.data.read("member_extended", "member_extended_path")
    job.data.read("segment", "segment_path", filetype="csv")
    job.data.read("transaction_1", "transaction_1_path")
    job.data.read("transaction_2", "transaction_2_path")
    job.data.read("member", "member_features_path")
    job.data.read("club", "club_path")

    job.data.tables["header"] = gp.apply_fw_date_range(
        job, job.data.tables["header"]
    )

    job.data.tables["detail_isnr"] = gp.apply_fw_date_range(
        job, job.data.tables["detail_isnr"]
    )

    population = gp.generate_population(job)
    job.data.tables["population"] = population

    feature_population = gp.generate_population(job, "feature")
    job.data.tables["feature_population"] = feature_population

    features = generate_misc(job)
    job.data.tables["misc"] = features
    job.data.write(
        "misc",
        "misc_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )

    job.log.info("Done")


if __name__ == "__main__":
    main()
