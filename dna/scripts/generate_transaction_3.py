"""
The script which calculates intermediate features related to transactions.
It only depends on source etl intermediates.
"""
import memberdna.dna.lib.generate_population as gp
import memberdna.dna.lib.managers as managers
import memberdna.dna.lib.transaction_features as features
import memberdna.dna.lib.utils as utils


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
    dna = features.feature_grouped_tender_spend_nw(job, dna)
    dna = features.feature_member_category(
        job,
        dna,
        ["MEN", "MENS", "MEN''S"],
        ["WOMEN", "WOMENS", "WOMEN''S"],
        ["AH4_DESC", "AH5_DESC", "AH6_DESC"],
        "MEN",
        [52],
    )
    dna = features.feature_member_category(
        job,
        dna,
        ["WOMEN", "WOMENS", "WOMEN''S"],
        ["MEN", "MENS", "MEN''S"],
        ["AH4_DESC", "AH5_DESC", "AH6_DESC"],
        "WOMEN",
        [52],
    )
    dna = features.feature_member_category(
        job,
        dna,
        ["PET", "PETS", "PET''S"],
        ["FUNERAL", "URN"],
        ["AH4_DESC", "AH5_DESC", "AH6_DESC"],
        "PET",
        [52],
    )
    dna = features.feature_member_category(
        job,
        dna,
        [
            "CHILDREN",
            "CHILDREN''S",
            "KID",
            "KIDS",
            "KID''S",
            "GIRL",
            "GIRLS",
            "GIRL''S",
            "BOY",
            "BOYS",
            "BOY''S",
        ],
        ["NEWBORN/INFANT", "CEREAL"],
        ["AH4_DESC", "AH5_DESC", "AH6_DESC"],
        "CHILDREN",
        [52],
    )
    dna = features.feature_member_category(
        job,
        dna,
        [
            "BABY",
            "BABY''S",
            "BABIES",
            "INFANT",
            "DIAPERS",
            "NEWBORN",
            "INFANT",
        ],
        ["RIBS", "COTTON SWABS", "WIPES"],
        ["AH4_DESC", "AH5_DESC", "AH6_DESC"],
        "BABY",
        [52],
    )

    dna = utils.cache_df(dna)

    dna = features.feature_member_basket_size(job, dna, 51)
    dna = features.feature_distinct_categories(job, dna)
    dna = features.feature_days_since_last_trip(job, dna)
    dna = features.feature_trip_intervals(job, dna, 52)

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
    job = managers.JobManager("transaction_3")

    job.data.read("skeleton", "skeleton_path")
    job.data.read("member_extended", "member_extended_path")
    job.data.read("payment", "payment_path")
    job.data.read("tender_map", "tender_map_path", filetype="csv")
    job.data.read("detail_isnr", "detail_isnr_path")
    job.data.read("detail", "detail_path")
    job.data.read("club", "club_path")

    job.data.tables["detail_isnr"] = gp.apply_fw_date_range(
        job, job.data.tables["detail_isnr"]
    )

    job.data.tables["detail"] = gp.apply_fw_date_range(
        job, job.data.tables["detail"]
    )

    job.data.tables["payment"] = gp.apply_fw_date_range(
        job, job.data.tables["payment"]
    )

    population = gp.generate_population(job)
    job.data.tables["population"] = population

    feature_population = gp.generate_population(job, "feature")
    job.data.tables["feature_population"] = feature_population

    features = generate_transaction(job)
    job.data.tables["transaction_3"] = features
    job.data.write(
        "transaction_3",
        "transaction_3_path",
        partitionby=["MBRSHP_SID", "FISCAL_WEEK_END"],
        ftype="parquet",
    )

    job.log.info("Done")


if __name__ == "__main__":
    main()
