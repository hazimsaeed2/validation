"""  
The script contains all the functions required to calculate miscellaneous
features
"""

import pyspark.sql.functions as sqlf
import pyspark.sql.window as W
import word2number.w2n as w2n

import lib_dna_member.utils as utils


def feature_preferred_club(job, dna):
    """
    Calculates preferred club features and appends them to the cube.

    Description:
        First calculates the Preferred club number based on number of trips 
        over the past x weeks. Calculates the number of trips to the preferred 
        club over the past x weeks. Finally calculates the percentage of total 
        trips to the preferred club for the past x weeks.

        Note : There is a possibility for null preferred club. A member can be
               active, but also have never taken a trip. If a member has gone 
               to cafe only or only returned items etc., then their preferred 
               club will be null since they never took any trips.

    Features:
        L{X}W_PREFERRED_CLUB
        L{X}W_PREFERRED_CLUB_TRIPS
        L{X}W_PERCENT_TRIPS_PREFERRED_CLUB

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with preferred club features
    """

    detail_isnr     = job.tables["detail_isnr_fiscal"]
    header          = job.tables["header_fiscal"]
    member_extended = job.tables["member_extended"]
    skeleton        = job.tables["feature_population"]
    population      = job.tables["population"]

    new_features = []

    trips = __trips(detail_isnr, header)

    all_trips_l, features = __store_visits_last_x_weeks(population, trips)
    new_features.extend(features)

    preferences = __rank_preference(all_trips_l)

    preferred = __filter_preferred(member_extended, skeleton, preferences)

    dna = dna.join(preferred, ["MBRSHP_SID", "FISCAL_WEEK_END"], "left_outer")

    dna, features = __percent_trips_l(dna)
    new_features.extend(features)

    dna = utils.set_default_value(dna, new_features)

    return dna


def feature_dummy_member(job, dna):
    """
    Appends int flag to indicate whether record is associated with a "dummy 
    member". 
    Dummy member rules evolve but intended to find such behavior as:
      - Catch-all IDs used for e.g. non-member purchases or special 
        circumstances
      - Members with extremely abnormal behavior (e.g. very high spend)

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.Dataframe): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame): dna with the dummy member column
    """
    # General rule intended to catch otherwise undetected dummy members. Confirmed by Nick Schifferle.
    spend_over_five_hundred_thousand = (
        dna["LAST_FIFTY-TWO_WEEK_SPEND"] > 500000
    )

    # Specific site used for dummy members. Confirmed by Nick Schifferle.
    mbrshp_nbr_starts_088 = dna.LATEST_MBRSHP_NBR.startswith("088")

    dummy_member_cond = sqlf.when(
        spend_over_five_hundred_thousand | mbrshp_nbr_starts_088, 1
    ).otherwise(0)

    dna = dna.withColumn("DUMMY_MBR", dummy_member_cond)

    dna = utils.set_default_value(dna, ["DUMMY_MBR"])

    return dna


def feature_last_over_prior(job, dna, num_weeks, metric):
    """    
    Builds the last over prior feature

    Description:
        Calculates the derived feature last over prior which is the quotient 
        between the value of last {weeks} weeks of some independant variable 
        and the prior {weeks} weeks of some independant variable. For example 
        last twelve week spend over prior 12 week spend would be the spend from 
        weeks 0 -> 11 weeks back over spend 12 -> 23 weeks back. Since this is 
        a derived feature it is dependant on other cube columns.

    Features:
        L{weeks}W_OVER_P{weeks}W_{x} : Where {weeks} is some integer and x is 
        the independant variable e.g L12W_OVER_P12W_SPEND

    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        num_weeks (list(str)): List of string names of the weeks to compute 
                               back
        metric (str): trips/spend
    
    Returns:
        (pyspark.sql.DataFrame): Current customer cube with new column
    """
    metric = metric.upper()
    new_features = []
    for weeks in num_weeks:
        col = "LAST_{}_WEEK_{}".format(weeks, metric)
        feature_name = __last_over_prior_feature_name(
            w2n.word_to_num(weeks), metric
        )
        dna = __calc_last_over_prior(
            dna, w2n.word_to_num(weeks), dna[col], feature_name
        )
        new_features.append(feature_name)

    dna = utils.set_default_value(dna, new_features)

    return dna


def feature_strategic_segment(job, dna):
    """
    1. Group members into Suburban/Urban/Rural based on 'HOME_ZIP_CD'
    and create a new column 'Urbanicity'
    2. Group members into dist_range based on BJS_DISTANCE
    and create a new column 'DIST_RANGE'
    3. Decide whether a member is strategic member and create a new columns
    "IS_STRATEGIC_MBR"
    4. If a member is strategic member, caculate his or her headroom.
    Headroom = median spend of the member's group - the member's last 52 weeks 
    spend
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest

    Return:
        (pyspark.sql.DataFrame): Current customer cube with new colums
    """

    select_cols_in = {
        "MBRSHP_SID",
        "LATEST_HOME_ZIP_CD",
        "BJS_DISTANCE",
        "LAST_FIFTY-TWO_WEEK_SPEND",
        "FISCAL_WEEK_END",
    }

    select_cols_out = {
        "MBRSHP_SID",
        "AGE_RANGE",
        "DIST_RANGE",
        "IS_STRATEGIC_MBR",
        "FISCAL_WEEK_END",
        "STRATEGIC_MBR_HEADROOM",
    }

    join_col = ["MBRSHP_SID", "FISCAL_WEEK_END"]

    census = dna.select(*select_cols_in).fillna(0, subset=["BJS_DISTANCE"])

    census_segment = __get_segment(census, job.tables["segment"])

    census_segment = __get_headroom(census_segment)

    census_segment = census_segment.select(*select_cols_out)
    new_features = census_segment.columns

    dna = dna.join(census_segment, join_col, "left_outer")

    dna = utils.set_default_value(dna, new_features)

    return dna


def feature_preferred_club_has_gas(job, dna, weeks):
    """
    Calculates "preferred club has gas" feature and appends it to the dna.
    Features:
        L{X}W_PREFERRED_CLUB_HAS_GAS
    Parameters:
        job (object): Job Manager object
        dna (pyspark.sql.DataFrame): data containing the population of interest
        weeks (int): number of previous week data to consider in preferred club
                     calculation
   Returns:
        (pyspark.sql.DataFrame): Current customer cube with new colums

    
    """
    preferred_club_nbr = "L{}W_PREFERRED_CLUB_NBR".format(weeks)
    preferred_club_has_gas = "PREFERRED_CLUB_HAS_GAS"

    new_features = [preferred_club_has_gas]

    fwe_skeleton = (
        job.tables["feature_population"]
        .select("FISCAL_WEEK_END")
        .distinct()
    )

    club_has_gas = __club_has_gas(
        job.tables["club"], fwe_skeleton, preferred_club_nbr
    )

    dna = __preferred_club_has_gas(
        dna, club_has_gas, preferred_club_nbr, preferred_club_has_gas
    )

    dna = utils.set_default_value(dna, new_features)

    return dna


def __trips(detail_isnr, header):
    """
    This function filters the detail_isnr table for trips.

    Parameters:
         details_isnr (pyspark.sql.DataFrame): details_isnr dataframe
         header (pyspark.sql.Dataframe): The header intermediate dataframe

    Returns:
        (pyspark.sql.DataFrame) : The table for all trips taken
    """
    mc_cd_list = ["402030190", "402030191", "203010098"]

    trips_purch_hdr_ids = (
        detail_isnr.filter(~detail_isnr.MC_CD.isin(mc_cd_list))
        .select("PURCH_HDR_ID")
        .distinct()
    )

    select_cols = {"MBRSHP_SID", "PURCH_HDR_ID", "PURCH_DT", "SITE_NBR"}

    return header.join(trips_purch_hdr_ids, ["PURCH_HDR_ID"]).select(
        *select_cols
    )


def __store_visits_last_x_weeks(population, trips):
    """
    This function takes all trips (defined by the client) and calculates the
    number of trips to each club for the last fifty two weeks.

    Parameters:
        population (pyspark.sql.Dataframe): population dataframe
        trips (pyspark.sql.DataFrame): The table for all trips taken

    Returns:
        (pyspark.sql.DataFrame, list(str)): Table with trips taken to each club
            by customer for the past x weeks, new columns
    """
    trips = trips.withColumnRenamed("MBRSHP_SID", "ID")

    theta_join_cond = (
        (population.MBRSHP_SID == trips.ID)
        & (population.FISCAL_WEEK_END >= trips.PURCH_DT)
        & (population.FISCAL_L52W_END <= trips.PURCH_DT)
    )

    # Theta join to cube skeleton
    all_trips_l = population.join(trips, theta_join_cond).drop("ID")

    groupby_cols = {"MBRSHP_SID", "FISCAL_WEEK_END", "SITE_NBR"}

    new_columns = ["L52W_PREFERRED_CLUB_TRIPS", "L52W_PREFERRED_CLUB_NBR"]

    return (
        all_trips_l.groupby(*groupby_cols)
        .count()
        .withColumnRenamed("count", new_columns[0])
        .withColumnRenamed("SITE_NBR", new_columns[1])
    ), new_columns


def __rank_preference(all_trips_l):
    """
    This function takes a table with trips taken for the past 52 weeks and
    ranks them by amount of trips. (1) Being assigned to the member club row
    with the most trips taken in the previous 52 weeks. Ties are determined
    by selecting a club randomly.

    Parameters:
        all_trips_l (pyspark.sql.DataFrame): The dataframe with trips taken to 
                                             each club by customer for the past 
                                             52 weeks.
    
    Returns:
        (pyspark.sql.DataFrame): as explained above

    """
    w = W.Window.partitionBy("MBRSHP_SID", "FISCAL_WEEK_END").orderBy(
        sqlf.col("L52W_PREFERRED_CLUB_TRIPS").desc(),
        sqlf.col("L52W_PREFERRED_CLUB_NBR").desc(),
    )

    return all_trips_l.withColumn("RANK", sqlf.rank().over(w))


def __filter_preferred(member_extended, skeleton, preferences):
    """
    Function filters out all rows that do not include the preferred club for
    the past 52 weeks. Chooses randomly on ties.

    Parameters:
        member_extended (pyspark.sql.DataFrame): member_extended dataframe
        skeleton (pyspark.sql.DataFrame): skeleton dataframe
        preferred (spark.sql.DataFrame): The dataframe with trips taken to
                                         each club by customer for the past
                                         x weeks ranked.

    Returns:
        (pyspark.sql.DataFrame): The dataframe filtered for the top preference 
                                 for each customer for each week.
    """

    preferences = preferences.filter(preferences.RANK == 1).drop("RANK")
    preferences = __skeleton_join(skeleton, preferences)
    club_of_frequency = member_extended.select(
        "MBRSHP_SID", "CLUB_OF_FREQUENCY"
    )
    preferences = preferences.join(club_of_frequency, "MBRSHP_SID", "left")

    return preferences.withColumn(
        "L52W_PREFERRED_CLUB_NBR",
        sqlf.coalesce(
            preferences["L52W_PREFERRED_CLUB_NBR"],
            preferences["CLUB_OF_FREQUENCY"],
        ),
    ).drop("CLUB_OF_FREQUENCY")


def __percent_trips_l(dna):
    """
    Function calculates the trips to preferred club / total trips taken.

    Parameters:
        dna (pyspark.sql.Dataframe): data containing the population of interest

    Returns:
        (pyspark.sql.DataFrame, list(str)): dataframe with the new column,
            new columns
    """
    percent = sqlf.coalesce(
        sqlf.try_divide(dna.L52W_PREFERRED_CLUB_TRIPS, dna["LAST_FIFTY-TWO_WEEK_TRIPS"]),
        sqlf.lit(0)
    )

    return (
        dna.withColumn("L52W_PERCENT_TRIPS_PREFERRED_CLUB", percent),
        ["L52W_PERCENT_TRIPS_PREFERRED_CLUB"],
    )


def __skeleton_join(skeleton, right_df):
    """
    Combines the skeleton with another dataframe
    Parameters:
        skeleton (pyspark.sql.Dataframe): skeleton dataframe
        right_df (pyspark.sql.Dataframe): dataframe to be merged with skeleton

    Returns:
        (pyspark.sql.Dataframe): skeleton merged with right_df
    """
    skeleton = skeleton.drop(
        "FISCAL_L8W_END",
        "FISCAL_L4W_END",
        "FISCAL_L26W_END",
        "FISCAL_L12W_END",
        "FISCAL_L52W_END",
        "FISCAL_WEEK_START",
    )

    join_fields = ["MBRSHP_SID", "FISCAL_WEEK_END"]

    return skeleton.join(right_df, join_fields, "left")


def __last_over_prior_feature_name(weeks, ind_feature):
    """
    Builds the feature name in form 'L{x}W_OVER_P{x}W'
    Parameters:
        weeks (int): The number of weeks the feature is relevant for
        ind_feature (str) : The base feature name e.g (SPEND)

    Returns:
        (str): Feature name
    """
    fw_feature_name = utils.fiscal_week_feature_name(weeks, ind_feature)

    return "{}_OVER_P{}W_{}".format(fw_feature_name, weeks, ind_feature)


def __calc_last_over_prior(df, lag, col, feature_name):
    """
    Calculates the last over prior feature
    Parameters:
        df (pyspark.sql.DataFrame): dataframe of interest
        lag (int): number of rows to extend
        col (pyspark.sql.Column): Column for which the last over prior is 
                                  calculated
        feature_name (str): New feature name

    Returns:
        (pyspark.sql.DataFrame): dataframe with new column
    """
    w = W.Window.partitionBy("MBRSHP_SID").orderBy("FISCAL_WEEK_END")
    
    
    return df.withColumn(
        feature_name,
        sqlf.coalesce(
            sqlf.try_divide(col.cast("double"), sqlf.lag(col, offset=lag).over(w)),
            sqlf.lit(0)
        )
    )


def __get_segment(census, segment):
    """
    Get a segment based on BJ'S_DISTANCE
    Parameters:
        census (pyspark.sql.DataFrame): census based features of the 
                                        population of interest
        segment (pyspark.sql.DataFrame): dna support table

    Returns:
        (pyspark.sql.DataFrame): census that meets the segment criteria
    """

    theta1 = census["BJS_DISTANCE"] < segment["dist_upper"]
    theta2 = census["BJS_DISTANCE"] >= segment["dist_lower"]
    theta3 = segment["AGE_RANGE"] == "Missing"

    joins = theta1 & theta2 & theta3

    census_segment = census.join(segment, joins)

    return census_segment


def __get_headroom(census_segment):
    """
    Calculate whether member is strategic member and the strategic member 
    headroom
    Parameters:
        census_segment (pyspark.sql.DataFrame): output of __get_segment

    Returns:
        (pyspark.sql.DataFrame): dataframe with the new columns
    """

    strtg_mbr_cond = (sqlf.col("Strategic_segment") == 1) & (
        sqlf.col("LAST_FIFTY-TWO_WEEK_SPEND")
        < sqlf.col("25_Percentile_annual_sales")
    )

    census_segment = census_segment.withColumn(
        "IS_STRATEGIC_MBR", sqlf.when(strtg_mbr_cond, 1).otherwise(0)
    )

    census_segment = census_segment.withColumn(
        "STRATEGIC_MBR_HEADROOM",
        sqlf.when(
            sqlf.col("IS_STRATEGIC_MBR") == 1,
            (
                sqlf.col("Median_annual_sales")
                - sqlf.col("LAST_FIFTY-TWO_WEEK_SPEND")
            ),
        ).otherwise(0),
    )

    return census_segment


def __club_has_gas(club, fwe_skeleton, preferred_club_nbr):
    """
    Function determines whether or not a club has gas at a given
    FISCAL_WEEK_END. This is done by taking the FIRST_FISCAL_WEEK_HAS_GAS
    field from the club square and joining to a FISCAL_WEEK_END skeleton.

    Parameters:
        club (spark.sql.Dataframe): The club square
        fwe_skeleton (pyspark.sql.Dataframe): FISCAL_WEEK_END skeleton

    Returns:
        (pyspark.sql.Dataframe): Club and FISCAL_WEEK_END dataframe with column 
                                 indicating whether or not it has gas at that 
                                 point in time.
    """
    club = club.select("SITE_NBR", "FIRST_FW_HAS_GAS").filter(
        club.FIRST_FW_HAS_GAS.isNotNull()
    )

    theta_join_cond = club.FIRST_FW_HAS_GAS <= fwe_skeleton.FISCAL_WEEK_END

    club = club.join(fwe_skeleton, theta_join_cond)

    club = club.drop("FIRST_FW_HAS_GAS")

    club = club.withColumn(
        "PREFERRED_CLUB_HAS_GAS_TEMP", sqlf.lit(1)
    ).withColumnRenamed("SITE_NBR", preferred_club_nbr)

    return club


def __preferred_club_has_gas(
    dna, club_has_gas, preferred_club_nbr, preferred_club_has_gas
):
    """
    This function determines whether or not a members preferred club has
    gas. There is a possibility of nulls, the case in which member is active
    but has not taken any trips (only went to Cafe or only returned
    items etc.)

    Parameters:
        dna (pyspark.sql.DataFrame): data containing the population of interest
        club_has_gas (pyspark.sql.DataFrame): Output of __club_has_gas
        preferred_club_nbr (str): Column name for the preferred club nbr of 
                                  interest
        preferred_club_has_gas (str): Column name of the new feature to be 
                                      added
    Returns:
        (pyspark.sql.DataFrame): dna dataframe with the new column
    """
    join_cols = [preferred_club_nbr, "FISCAL_WEEK_END"]

    dna = dna.join(club_has_gas, join_cols, "left_outer")

    preferred_club_col = dna[preferred_club_nbr]

    has_gas_cond = sqlf.when(
        dna.PREFERRED_CLUB_HAS_GAS_TEMP.isNotNull(), 1
    ).otherwise(sqlf.when(preferred_club_col.isNotNull(), 0))

    dna = dna.withColumn(preferred_club_has_gas, has_gas_cond)

    dna = dna.drop("PREFERRED_CLUB_HAS_GAS_TEMP")

    return dna