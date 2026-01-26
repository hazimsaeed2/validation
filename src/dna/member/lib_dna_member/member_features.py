"""
All member features are generated using the functions from this
module.

Feature functions starte with the keyword "feature_" while helper function
start with "__".
"""
import pyspark.sql.functions as sqlf
import pyspark.sql.window as W
from pyspark.sql.functions import *

import lib_dna_member.utils as utils
from lib.s3 import calculate_null_percentages


def feature_member_master(job, dna):
    """
    Take features from the member table and appends to the dna.

    Select relevant features from the member table and appends them to the
    dna. Applies the LATEST tag to indicate that they are the latest snapshot
    of these features and not "at fiscal point in time."

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    join_col = ['MBRSHP_SID']
    mod_member = __latest_rename(job.tables["member"], join_col)
    dna = dna.join(mod_member, join_col, 'left_outer')
    dna = utils.set_default_value(dna, mod_member.columns)

    return dna


def feature_member_extended(job, dna):
    """
    Take features from extended member table and appends to the dna.

    Select relevant features from the extended member table and appends them
    to the dna. Applies the LATEST tag to indicate that they are the latest
    snapshot of these features and not "at fiscal point in time."

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    select_cols = {
        'MBRSHP_SID',
        'AUTO_RNWL_IND',
        'PRI_SUPP_FHH_IND',
        'ER_SIGNUP_DT',
        'CLUB_OF_FREQUENCY',
        'SIC_CD',
        'GRP_AFFIL_ID',
        'MKT_CD',
        'HOME_ZIP_CD',
        'MBRSHP_NBR',
    }
    join_col = ['MBRSHP_SID']
    mod_member_ext = job.tables["member_extended"].select(
        *select_cols
    )
    mod_member_ext = __latest_rename(mod_member_ext, join_col)
    dna = dna.join(mod_member_ext, join_col, 'left_outer')
    dna = utils.set_default_value(dna, mod_member_ext.columns)

    return dna


def feature_team_member_ind(job, dna):
    """
    Take features from extended member table and appends to the dna.

    Select relevant features from the extended member table and appends them
    to the dna. Applies the LATEST tag to indicate that they are the latest
    snapshot of these features and not "at fiscal point in time."

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    tm_sic_cds = [9991, 9992, 1055]
    tm_grip_affil_ids = ['9993', '9994', '9997', '9995', '1055', '9992']

    sic_condition = sqlf.when(
        dna.LATEST_SIC_CD.isin(tm_sic_cds), 1
    ).otherwise(0)

    tm_ind_condition = sqlf.when(
        dna.LATEST_GRP_AFFIL_ID.isin(tm_grip_affil_ids), 1
    ).otherwise(sic_condition)

    dna = dna.withColumn('LATEST_TM_MBR_IND', tm_ind_condition)

    dna = utils.set_default_value(dna, ['LATEST_TM_MBR_IND'], value=0)

    return dna

def feature_trial_member_ind(job, dna):
    """
    Take features from extended member table and appends to the dna.

    Select relevant features from the extended member table and appends them
    to the dna. Applies the LATEST tag to indicate that they are the latest
    snapshot of these features and not "at fiscal point in time."

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    mbrshp_fee_inc_zero = sqlf.when(
        job.tables["member_extended"].MBRSHP_FEE_INC == 0, 1
    ).otherwise(0)

    trial_mem_condition = sqlf.when(
        job.tables["member_extended"].MKT_CD.like('Z%'),
        mbrshp_fee_inc_zero
    ).otherwise(0)

    trial_mem = job.tables["member_extended"].withColumn(
        'LATEST_TRIAL_MBR_IND', trial_mem_condition
    ).select('MBRSHP_SID', 'LATEST_TRIAL_MBR_IND')

    dna = dna.join(trial_mem, ['MBRSHP_SID'], 'left_outer')

    dna = utils.set_default_value(dna, ['LATEST_TRIAL_MBR_IND'], value=0)

    return dna


def feature_member_history(job, dna):
    """
    Take the member history table and applies member row corresponding to the
    fiscal week end.

    Select the member row in the history table that is aligned with the
    current fiscal week end. Row with the max EFF_DT before the
    FISCAL_WEEK_END is applied to the dna for each fiscal week.
    This applies "point in time" membership features to the dna.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    mem_hist_keys = __partial_aggregation(job, dna)
    dna = dna.join(
        mem_hist_keys,
        ['MBRSHP_SID', 'FISCAL_WEEK_END'], 'left_outer'
    )
    dna, new_features = __compute_latest_membership(dna)
    dna = dna.join(
        job.tables["member_history"].drop('FISCAL_WEEK_END'),
        ['MBRSHP_SID', 'EFF_DT'],
        'left_outer'
    )
    dna = utils.set_default_value(dna, new_features)

    return dna


def feature_base_mfi(job, dna):
    """
    Calculate the member fee income (MFI) that member would have paid
    before discount.

    Execute this after Member.execute - Requires the column LATEST_RWDS_MBR_IND

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    dna = dna.withColumn(
        "LATEST_MFI_TIER",
        sqlf.when(
            sqlf.col("LATEST_RWDS_MBR_IND").isin("P", "D", "N"), 55
        ).otherwise(
            sqlf.when(
                sqlf.col("LATEST_RWDS_MBR_IND").isin("Y", "E"), 110
            ).otherwise(
                sqlf.lit(None)
            )
        ),
    )

    dna = utils.set_default_value(dna, ["LATEST_MFI_TIER"])

    return dna


def feature_calculated_mfi(job, dna):
    """
    Calculate the first MFI (Member Fee Income) for the member and applies the
    field to the dna.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    member_history = job.tables["member_history"].select(
        'MBRSHP_SID', 'MBRSHP_FEE_INC', 'EFF_DT'
    )
    earliest_eff_dt = member_history.groupby(
        'MBRSHP_SID'
    ).agg(sqlf.min('EFF_DT').alias('EFF_DT'))
    first_member_fee = member_history.join(
        earliest_eff_dt,
        ['MBRSHP_SID', 'EFF_DT']
    ).withColumnRenamed(
        'MBRSHP_FEE_INC', 'FIRST_MBRSHP_FEE_INC'
    ).select('MBRSHP_SID', 'FIRST_MBRSHP_FEE_INC')
    dna = dna.join(first_member_fee, ['MBRSHP_SID'], 'left_outer')

    dna = utils.set_default_value(dna, ["FIRST_MBRSHP_FEE_INC"])

    return dna


def feature_distance(job, dna):
    """
    Calculate distance features and append to the dna.

    Distance features apply on distance to client and competitor locations
    based on census info. The distance comes in a couple different forms:
    distance in mileage, distance driving, time driving etc.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    drop_cols = {
        'MEMBERID',
        'CENSUS_TRACT',
        'CLUSTER',
        'LONGITUDE',
        'LATITUDE',
        'UPDATE',
        'TRACT_LATITUDE',
        'TRACT_LONGITUDE',
        'ZIP_CODE',
        'ZIP_LATITUDE',
        'MEMTYPE',
        'CENSUS_TRACT_POPULATION',
        'CENSUS_TRACT_POPULATION',
        'CENSUS_TRACT_HOUSEHOLDS',
        'ZIP_LONGITUDE',
        'ZIP_CODE_POPULATION',
        'ZIP_CODE_HOUSEHOLDS'
    }
    census_tract = job.tables["census_tract"].drop(*drop_cols)

    # print("CENSUS TRACT AFTER  DROPPING COLS :")
    # print(census_tract)

    dna = dna.join(census_tract, ['MBRSHP_SID'], 'left_outer')
    dna = utils.set_default_value(dna, census_tract.columns)

    print("DNA AFTER  JOINING WITH DNA AND CENSUS_TRACT :")
    # print(dna)

    drive_time_cols = ["BJS_DRIVE_TIME","WALMART_DRIVE_TIME","COSTCO_DRIVE_TIME","SAMS_DRIVE_TIME",]

    print("NULL % AT THE START")
    null_percentages_atstart_dna = calculate_null_percentages(dna, drive_time_cols)
    null_percentages_atstart_dna.show()

    # dna = dna.withColumn("ZIP", sqlf.lpad(dna["ZIP"].cast("string"), 5, "0"))
    dna_with_nulls = dna.filter(dna.BJS_DISTANCE.isNull())
    dna_without_nulls = dna.filter(dna.BJS_DISTANCE.isNotNull())

    median_cols_checks = ["BJS_DRIVING_DISTANCE", "BJS_DISTANCE", "BJS_DRIVE_TIME", "WALMART_DRIVE_TIME", "WALMART_DRIVING_DISTANCE",
               "WALMART_DISTANCE", "COSTCO_DRIVE_TIME", "COSTCO_DRIVING_DISTANCE", "COSTCO_DISTANCE", "SAMS_DRIVE_TIME",
               "SAMS_DRIVING_DISTANCE", "SAMS_DISTANCE"]

    # Changing from .groupBy("LATEST_HOME_ZIP_CD") to .groupBy("zip")
    median_df_for_not_null = dna_without_nulls.groupBy("ZIP").agg(
        *[sqlf.percentile_approx(col_name, 0.5, 100).alias(f"{col_name}") for col_name in median_cols_checks]
    )

    dna_with_nulls_drop_actual_col = dna_with_nulls.drop(*median_cols_checks)

    dna_with_imputed_nulls = dna_with_nulls_drop_actual_col.join(median_df_for_not_null, on="ZIP", how="left")

    dna = dna_with_imputed_nulls.unionByName(dna_without_nulls)

    print("NULL % AFTER IMPUTATION")
    null_percentages_final_dna = calculate_null_percentages(dna, drive_time_cols)
    null_percentages_final_dna.show()

    return dna


def feature_tenure(job, dna):
    """
    Calculate the tenure features and appends them to the dna.

    Tenure is defined as the number of days between the FISCAL_WEEK_END and the
    LATEST_MBRSHP_ENR_DT.
    Tenure Group classifies a member if they are expired, tenured, new (have
    never renewed membership) or None.
        LATEST_MBRSHP_EXP_DT < FISCAL_WEEK_END -> 'expired'
        LATEST_MBRSHP_EXP_DT - LATEST_MBRSHP_ENR_DT < 660 -> 'new'
        LATEST_MBRSHP_EXP_DT - LATEST_MBRSHP_ENR_DT >= 660 -> 'tenured'
        LATEST_MBRSHP_EXP_DT is Null -> None

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    dna = dna.withColumn(
        'TENURE',
        sqlf.datediff(dna.FISCAL_WEEK_END, dna.LATEST_MBRSHP_ENR_DT)
    )
    dna = dna.withColumn(
        'TENURE_GROUP',
        sqlf.when(
            sqlf.isnull(dna.LATEST_MBRSHP_EXP_DT), sqlf.lit(None)
        ).when(
            dna.LATEST_MBRSHP_EXP_DT < dna.FISCAL_WEEK_END, 'expired'
        ).when(
            sqlf.datediff(
                dna.MBRSHP_EXP_DT, dna.LATEST_MBRSHP_ENR_DT
            ) < 660,
        'new'
        ).otherwise('tenured')
    )

    dna = utils.set_default_value(dna, ["TENURE", "TENURE_GROUP"])

    return dna


def feature_days_until_exp(job, dna):
    """
    Calculate the days until expiration per member per fiscal week and appends
    to the dna.

    Days until expiration is the day difference between the point in time
    MBRSHP_EXP_DT and the FISCAL_WEEK_END.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    dna = dna.withColumn(
        'DAYS_UNTIL_EXP',
        sqlf.datediff(dna.MBRSHP_EXP_DT, dna.FISCAL_WEEK_END)
    )
    dna = utils.set_default_value(dna, ['DAYS_UNTIL_EXP'])
    return dna


def feature_days_since_last_rnwl(job, dna):
    """
    Calculate the days since the last renewal by member and appends to the dna.

    Days since last renewal is the day difference between the point in time
     MBRSHP_RNWL_DT and the FISCAL_WEEK_END.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    dna = dna.withColumn(
        'DAYS_SINCE_LAST_RNWL',
        sqlf.datediff(dna.FISCAL_WEEK_END, dna.MBRSHP_RNWL_DT)
    )
    dna = utils.set_default_value(dna, ['DAYS_SINCE_LAST_RNWL'])
    return dna


def feature_num_of_rnwls(job, dna):
    """
    Calculate the number of membership renewals per member and appends to the
    dna.

    Number of renewals is the distinct count of MBRSHP_RNWL_DT at a given point
    in time.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    window = W.Window.partitionBy('MBRSHP_SID').orderBy(
        'FISCAL_WEEK_END'
    ).rowsBetween(W.Window.unboundedPreceding, 0)
    dna = dna.withColumn(
        'NUM_OF_RNWLS',
        sqlf.size(sqlf.collect_set(dna.MBRSHP_RNWL_DT).over(window))
    )

    dna = utils.set_default_value(dna, ['NUM_OF_RNWLS'])

    return dna


def feature_quotient_id(job, dna):
    """
    Identify whether a member has ever logged in online or through mobile

    Note that this class only adds the information whether the given user
    has quotient ID. The quotient ID itself is not added.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    quotient_id = (
        job.tables["quotient_id"]
        .select("MBRSHP_NBR", "HAS_QUOTIENT_ID")
        .withColumnRenamed("MBRSHP_NBR", "LATEST_MBRSHP_NBR")
        .dropDuplicates()
    )
    quotient_id.show()

    dna = dna.join(
        quotient_id,
        "LATEST_MBRSHP_NBR",
        "left",
    )

    new_features = ["HAS_QUOTIENT_ID"]
    dna = utils.set_default_value(dna, new_features, value=0)

    return dna


def __partial_aggregation(job, dna):
    """
    Reduce all member history before this job to one entry for each member
    having the fiscal weekend the first fiscal weekend.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object
    Returns:
        (pyspark.sql.DataFrame): compressed member history
    """
    first_fiscal_week = utils.next_fiscal_week_end(job)
    mem_hist_keys = job.tables["member_history"].select(
        'MBRSHP_SID', 'EFF_DT', 'FISCAL_WEEK_END'
    )
    # Calculate the max effective membership date before the dna
    before_dna = mem_hist_keys.filter(
        mem_hist_keys.EFF_DT <= first_fiscal_week
    ).groupby('MBRSHP_SID').agg(
        sqlf.max('EFF_DT').alias('EFF_DT')
    ).withColumn(
        'FISCAL_WEEK_END',
        sqlf.to_date(sqlf.lit(first_fiscal_week), format='yyyy-MM-dd')
    )

    # Union the before dna values and during dna values
    mem_hist_keys = mem_hist_keys.filter(
        mem_hist_keys.EFF_DT > first_fiscal_week
    ).union(before_dna)

    # GroupBy To collapse double renewals within week
    mem_hist_keys = mem_hist_keys.groupby(
        'MBRSHP_SID', 'FISCAL_WEEK_END'
    ).agg(sqlf.max('EFF_DT').alias('EFF_DT'))

    return mem_hist_keys


def __compute_latest_membership(dna):
    """
    Compute the latest member effective date among all fiscal weeknds.

    Parameters:
        job (managers.JobManager): object which manages the Spark App
        dna (pyspark.sql.DataFrame): the dna object to append feature to
    Returns:
        (pyspark.sql.DataFrame): dna with new features
    """
    window = W.Window.partitionBy('MBRSHP_SID').orderBy(
        'FISCAL_WEEK_END'
    ).rowsBetween(W.Window.unboundedPreceding, 0)
    dna = dna.withColumn(
        'EFF_DT_TEMP', sqlf.max(dna.EFF_DT).over(window)
    ).drop('EFF_DT').withColumnRenamed(
        'EFF_DT_TEMP', 'EFF_DT'
    )
    new_columns = ['EFF_DT']

    return dna, new_columns


def __latest_rename(df, excluded_columns):
    """
    Adds the LATEST tag to column names with given df.

    Parameters:
        df (spark.sql.DataFrame): DataFrame to add the LATEST tag on columns.
        excluded_columns (List(str)): Columns to not modify with LATEST tag

    Returns:
        (spark.sql.DataFrame): DataFrame with the columns renamed.
    """
    cols = (name for name in df.schema.names if name not in excluded_columns)
    for col in cols:
        new_col = "{}_{}".format("LATEST", col)
        df = df.withColumnRenamed(col, new_col)
    return df