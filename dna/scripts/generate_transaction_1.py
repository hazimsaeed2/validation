import memberdna.dna.lib.generate_population as gp
import memberdna.dna.lib.managers as managers
import memberdna.dna.lib.transaction_features as features


def generate_transaction(job):
    """
    Generate transaction based variables for the given population
    Parameters:
        job (object): Job Manager object based on the current config file

    Returns:
        dna (pyspark.sql.DataFrame): Transaction data of the given population
    """

    dna = job.data.tables["population"]
    orig_cols = dna.columns

    dna = features.feature_stdev(job, dna)

    dna = features.feature_trips(job, dna)

    dna = features.feature_spend(job, dna)

    dna = features.feature_spend_in_store(job, dna)

    dna = features.feature_units(job, dna)

    dna = features.feature_units_over_fifty(job, dna)

    dna = features.feature_gas_trips(job, dna)

    dna = features.feature_gas_distinct_days(job, dna)

    dna = features.feature_gas_spend(job, dna)

    dna = features.feature_gas_and_store_distinct_days(job, dna)

    dna = features.feature_ecommerce_metric(job, dna)

    dna = features.feature_distinct_days(job, dna)

    dna = features.feature_transactions(job, dna)

    dna = dna.drop(
        *[
            col
            for col in orig_cols
            if col not in ["MBRSHP_SID", "FISCAL_WEEK_END"]
        ]
    )

    return dna


def main():
    job = managers.JobManager("transaction_1")

    job.data.read("header", "header_path")
    job.data.read("detail", "detail_path")
    job.data.read("detail_isnr", "detail_isnr_path")
    job.data.read("skeleton", "skeleton_path")
    job.data.read("member_extended", "member_extended_path")

    job.data.tables["header"] = gp.apply_fw_date_range(
        job, job.data.tables["header"]
    )

    job.data.tables["detail"] = gp.apply_fw_date_range(
        job, job.data.tables["detail"]
    )

    job.data.tables["detail_isnr"] = gp.apply_fw_date_range(
        job, job.data.tables["detail_isnr"]
    )

    population = gp.generate_population(job)
    job.data.tables["population"] = population

    job.log.info("Calculating transaction features")

    features = generate_transaction(job)
    job.data.tables["transaction"] = features
    job.data.write(
        "transaction",
        "transaction_1_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )

    job.log.info("Done")


if __name__ == "__main__":
    main()
