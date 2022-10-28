"""
The script contains the functions required to integrate with the acquisition
data features.
"""
import pyspark.sql.functions as sqlf


def feature_age_income(job, dna):
    """
    Calculate age and household income.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    acq_dna = job.data.tables["acq_dna"]
    mbr_basic = job.data.tables["mbr_basic"]

    acq_dna_columns = [
        "mbr_sid",
        "mbr_prmry_sid",
        "member_age",
        "household_income",
    ]
    mbr_basic_columns = ["mbr_sid", "mbr_free_sup_ind", "mbr_prmry_sid"]

    ic_free_supps = dna.filter(
        sqlf.col("LATEST_MBRSHP_TYPE_ID").isin(2, 4)
    ).join(
        mbr_basic.select(*mbr_basic_columns),
        (dna["MBRSHP_SID"] == mbr_basic["mbr_sid"])
        & (sqlf.col("mbr_free_sup_ind") == "Y"),
        "inner",
    )

    ic_free_supps = ic_free_supps.join(
        acq_dna.filter(sqlf.col("mbr_household_ind") == "Y")
        .select(*acq_dna_columns)
        .drop("mbr_sid"),
        "mbr_prmry_sid",
        "left",
    )

    others = dna.join(ic_free_supps, "MBRSHP_SID", "leftanti")
    others = others.join(
        acq_dna.select(*acq_dna_columns),
        others["MBRSHP_SID"] == acq_dna["mbr_sid"],
        "left",
    )

    columns = others.columns

    return ic_free_supps.select(*columns).union(others.select(*columns))
