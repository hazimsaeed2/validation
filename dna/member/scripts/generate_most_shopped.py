import memberdna.dna.lib.generate_population as gp
import memberdna.dna.lib.managers as managers
import memberdna.dna.lib.most_shopped_features as features


def generate_most_shopped(job):
    """
    Generate first and second most shopped category for each memberber in the 
    given population
    Parameters:
        job (object): Job Manager object based on the current config file
    
    Returns:
        (pyspark.sql.DataFrame): Transaction data of the given population
    """
    dna = job.data.tables["population"]
    orig_cols = dna.columns

    num_weeks = ["FIFTY-TWO"]

    dna = features.feature_most_shopped_category(job, dna, num_weeks)

    dna = dna.drop(
        *[
            col
            for col in orig_cols
            if col not in ["MBRSHP_SID", "FISCAL_WEEK_END"]
        ]
    )

    return dna


def main():
    job = managers.JobManager("most_shopped")

    job.data.read("detail", "detail_path")
    job.data.read("member_extended", "member_extended_path")
    job.data.read("skeleton", "skeleton_path")
    job.data.read(
        "AH5_custumer_facing_desc",
        "AH5_custumer_facing_desc_path",
        filetype="csv",
    )

    job.data.tables["detail"] = gp.apply_fw_date_range(
        job, job.data.tables["detail"]
    )

    population = gp.generate_population(job)
    job.data.tables["population"] = population

    feature_population = gp.generate_population(job, "feature")
    job.data.tables["feature_population"] = feature_population

    features = generate_most_shopped(job)
    job.data.tables["most_shopped"] = features
    job.data.write(
        "most_shopped",
        "most_shopped_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )

    job.log.info("Done")


if __name__ == "__main__":
    main()
