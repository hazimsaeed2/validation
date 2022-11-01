import pe_memberdna.dna.member.lib.managers as managers
import pe_memberdna.dna.member.lib.transaction_features as features


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

    num_weeks = ["FOUR", "EIGHT", "TWELVE", "TWENTY-SIX", "FIFTY-TWO"]

    dna = features.feature_agg_spend(job, dna, num_weeks)

    dna = features.feature_agg_spend_in_store(job, dna, num_weeks)

    dna = features.feature_agg_units(job, dna, num_weeks)

    dna = features.feature_agg_units_over_fifty(job, dna, num_weeks)

    dna = features.feature_agg_gas_trips(job, dna, num_weeks)

    dna = features.feature_agg_gas_distinct_days(job, dna, num_weeks)

    dna = features.feature_agg_gas_spend(job, dna, num_weeks)

    dna = features.feature_agg_gas_and_store_distinct_days(job, dna, num_weeks)

    dna = features.feature_agg_ecommerce_metric(job, dna, num_weeks, "spend")

    dna = features.feature_agg_ecommerce_metric(job, dna, num_weeks, "trips")

    dna = features.feature_agg_transactions(job, dna, num_weeks)

    dna = dna.drop(
        *[
            col
            for col in orig_cols
            if col not in ["MBRSHP_SID", "FISCAL_WEEK_END"]
        ]
    )

    return dna


def main():
    job = managers.JobManager("transaction_2")

    job.data.read("population", "transaction_1_path")

    features = generate_transaction(job)
    job.data.tables["transaction"] = features
    job.data.write(
        "transaction",
        "transaction_2_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )

    job.log.info("Done")


if __name__ == "__main__":
    main()
