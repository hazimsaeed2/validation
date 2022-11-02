"""
The script which merges all the intermediate files and writes the updated cube
"""
import pe_memberdna.dna.member.lib.generate_population as gp
import pe_memberdna.dna.member.lib.managers as managers


def merge(job):
    """
    Merge all the modules for the given population

    Parameters:
        job (managers.JobManager): object which manages the Spark App
    Returns:
        (pyspark.sql.DataFrame): dna with all the features
    """
    dna = job.data.tables["population"]
    dna = dna.repartition("MBRSHP_SID", "FISCAL_WEEK_END")

    intermediates = [
        "transaction_1",
        "transaction_2",
        "transaction_3",
        "coupon_and_digital_1",
        "coupon_and_digital_2",
        "member",
        "most_shopped",
        "misc",
        "acquisition",
    ]

    for i in intermediates:
        features = job.data.tables[i].repartition(
            "MBRSHP_SID", "FISCAL_WEEK_END"
        )
        dna = dna.join(
            features, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer",
        )

    return dna


def main():
    job = managers.JobManager("merge")

    job.data.read("skeleton", "skeleton_path")
    job.data.read("member_extended", "member_extended_path")
    job.data.read("transaction_1", "transaction_1_path")
    job.data.read("transaction_2", "transaction_2_path")
    job.data.read("transaction_3", "transaction_3_path")
    job.data.read("coupon_and_digital_1", "coupon_and_digital_1_path")
    job.data.read("coupon_and_digital_2", "coupon_and_digital_2_path")
    job.data.read("member", "member_features_path")
    job.data.read("most_shopped", "most_shopped_path")
    job.data.read("misc", "misc_path")
    job.data.read("acquisition", "acquisition_path")

    population = gp.generate_population(job)
    job.data.tables["population"] = population

    features = merge(job)
    job.data.tables["dna_full"] = features.filter(
        features.FISCAL_WEEK_END >= job.config.params["params"]["start_date"]
    )

    job.data.write(
        "dna_full", "dna_path", partitionby="FISCAL_WEEK_END", ftype="parquet",
    )

    # TODO: confirm if params is set up correctly
    if job.config.params["params"]["archive"]:
        job.data.write(
            "dna_full",
            "archive_base_path",
            partitionby="FISCAL_WEEK_END",
            ftype="parquet",
        )

    job.log.info("Done")


if __name__ == "__main__":
    main()
