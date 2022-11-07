"""Helpers and Functions to apply business rules to predictions.

TODO:
    [] -  Update Fallback on preferred club to club of signup
    [] - Switch club of frequency to preferred club once available


"""
from pyspark.sql.functions import sum, mean, when, lit, regexp_extract, col
from pyspark.sql.functions import max as fmax

from pe_member_dna.pipelines.cf_model.lib.cf_utils import (
    parse_data_range,
    rescale_weight_factor,
)
from pe_member_dna.pipelines.lib.utils import (
    stack,
    get_col_list,
    next_fiscal_week_end,
)


def limit_based_on_prior_trips(
    sparkcontext, paths, predictions, past_data_file_name, cutoff
):
    """Filter predictions based on a prior-trips cutoff.

    We sometimes want to predict only things that have been purchased
    in a limited fashion in the past. This function will filter
    predictions based on a defined cutoff of PERCENTAGE OF TOTAL TRIPS
    given the history period of the data in question.

    Parameters:
        predictions (pyspark.sql.DataFrame): prediction dataframe, as produced my models.predict_pf
        paths (dict): dictionary of paths (i.e. from yaml config)
        past_data_file_name (str): Name of file that describes the past data (for date range)
        cutoff (float): maximum allowable percentage of prior trips

    Returns:
        predictions (pyspark.sql.DataFrame): filtered predictions
    """
    data_range = parse_data_range(past_data_file_name)
    if paths["CUBE"][-3:] == "csv":
        members = sparkcontext.read.csv(paths["CUBE"], header=True)
    else:
        members = sparkcontext.read.parquet(paths["CUBE"])
    members = members[members.FISCAL_WEEK_END > data_range["start"]]
    members = members[members.FISCAL_WEEK_END < data_range["end"]]
    trips_per_memb = (
        members.groupBy("MBRSHP_SID")
        .agg(sum("WEEK_TRIPS").alias("TOTAL_TRIPS"))
        .fillna(0)
    )
    predictions = predictions.join(
        trips_per_memb, "MBRSHP_SID", "left"
    ).fillna(0)
    predictions = predictions.withColumn(
        "PCT_PAST_TRIPS", predictions.PAST_TRIPS / predictions.TOTAL_TRIPS
    )
    predictions = predictions[predictions.PCT_PAST_TRIPS <= cutoff]
    predictions = predictions.drop("PCT_PAST_TRIPS", "TOTAL_TRIPS")
    return predictions


def limit_based_on_score(predictions, prediction_col, score_limit):
    """Filter predictions based on a predicted score cutoff.

    We sometimes want to predict only things that have certain
    score or above lets make a simple filter for that.

    Parameters:
        predictions (pyspark.sql.DataFrame): prediction dataframe, as produced my models.predict_pf
        prediction_col (str): name of the column containing predictions
        score_limit (float): minimum allowable predicted score

    Returns:
        predictions (pyspark.sql.DataFrame): filtered predictions
    """
    filtered_preds = predictions[predictions[prediction_col] >= score_limit]
    return filtered_preds


def label_hook_stretch(sparkcontext, params, paths, predictions):
    """Label predictions as hooks or stretches for offer assignment.

    Hooks and stretches are calculated against a threshold,
    by comparing the relative frequency of making a trip in each
    category to the propensity to make a trip for each member
    and category. There are two hook/stretch calculations:
    1) comparing the relative frequency of a member's trips
    in a category to all members and 2) comparing the
    relative frequency of trips in a category to members who
    have made a trip in said category. A stretch offer is a category
    for which a member is below the threshold and has tended to take
    a smaller number of trips than the mean number over all members
    in the model training period, and/or is very unlikely to take a
    trip in general.

    Parameters:
        sparkcontext (pyspark.sparkContext): spark context with which weights are being applied
        predictions (pyspark.sql.DataFrame): prediction dataframe, as produced my models.predict_pf
        paths (dict): dictionary of paths (i.e. from yaml config)
        params (dict): dictionary of parameters (i.e. from yaml config)
        trip_prop_cap (double): putting a ceiling on trip propensity (i.e. from yaml config)

    Returns:
        predictions (pyspark.sql.DataFrame): predictions with additional hook/stretch columns, "hs_ind" and "hs_ind_v2"
    """
    # set up parameters
    cutoff_multiplier = params["lambda"]
    if not isinstance(cutoff_multiplier, list):
        cutoff_multiplier = [cutoff_multiplier]

    sigma = 0.2  # propensity-lambda shape parameter
    # propensity-lambda scale parameter (used to keep lambda scale consistent)
    beta = 5
    trip_prop_cap = params["trip_prop_cap"]

    # join propensities
    propensities = sparkcontext.read.csv(
        paths["PROPENSITY_PREDICTIONS"], header=True
    )
    propensities = propensities.select(
        "MBRSHP_SID", "probability_making_a_trip"
    )
    propensities = propensities.dropDuplicates()
    predictions = predictions.join(propensities, "MBRSHP_SID", "left")
    predictions = predictions.fillna(0, subset=["probability_making_a_trip"])
    if trip_prop_cap:
        predictions = predictions.withColumn(
            "probability_making_a_trip",
            when(
                col("probability_making_a_trip") > trip_prop_cap,
                lit(trip_prop_cap),
            ).otherwise(col("probability_making_a_trip")),
        )

    # calculate modified inverse prob
    inv_prob_expr = 1 - (col("probability_making_a_trip") ** sigma)
    predictions = predictions.withColumn("inverse_prob", inv_prob_expr)

    # calc and join overall trips per cat
    # max twice per week for average trips
    upper_trip_bound = col("PAST_TRIPS") < 100
    no_outliers = predictions[upper_trip_bound]
    pure_mean_per_cat = no_outliers.groupBy("CATEGORY_ID").agg(
        mean("PAST_TRIPS").alias("PURE_AVG_TRIPS")
    )
    no_outliers_nonzero = no_outliers.where(col("PAST_TRIPS") > 0)
    rel_mean_per_cat = no_outliers_nonzero.groupBy("CATEGORY_ID").agg(
        mean("PAST_TRIPS").alias("REL_AVG_TRIPS")
    )
    predictions = predictions.join(
        pure_mean_per_cat, "CATEGORY_ID", "left"
    ).fillna(0)
    predictions = predictions.join(
        rel_mean_per_cat, "CATEGORY_ID", "left"
    ).fillna(0)

    # calc comparison
    predictions = predictions.withColumn(
        "pure_trips_ratio", predictions.PAST_TRIPS / predictions.PURE_AVG_TRIPS
    )
    predictions = predictions.withColumn(
        "rel_trips_ratio", predictions.PAST_TRIPS / predictions.REL_AVG_TRIPS
    )
    for i in range(len(cutoff_multiplier)):
        current_lambda = cutoff_multiplier[i]
        lambda_expr = col("inverse_prob") * beta * current_lambda
        predictions = predictions.withColumn("x", lambda_expr)
        one_or_fewer_trips = predictions.PAST_TRIPS <= 1

        if params["pure_lambda"]:
            indicator_colname = "hs_ind_lambda{}".format(current_lambda)
            above_pure_avg = predictions.x >= predictions.pure_trips_ratio
            stretch_cond = above_pure_avg | one_or_fewer_trips
            predictions = predictions.withColumn(
                indicator_colname,
                when(stretch_cond, "stretch").otherwise("hook"),
            )

        if params["rel_lambda"]:
            indicator_colname_test = "hs_ind_lambdav2_{}".format(
                current_lambda
            )
            above_rel_avg = predictions.x >= predictions.rel_trips_ratio
            stretch_cond_v2 = above_rel_avg | one_or_fewer_trips
            predictions = predictions.withColumn(
                indicator_colname_test,
                when(stretch_cond, "stretch").otherwise("hook"),
            )
    predictions = predictions.drop(
        "PURE_AVG_TRIPS",
        "REL_AVG_TRIPS",
        "pure_trips_ratio",
        "x",
        "probability_making_a_trip",
        "inverse_prob",
    )
    return predictions


def apply_filters(predictions, rel_date, cube, cat_dna, club_dna, level):
    """Filter predictions to exclude or include categories.

    All categories are currently excluded at the AH4 level,
    with no breakdown to AH5 or below.

    Parameters:
        predictions (pyspark.sql.DataFrame): prediction dataframe, as produced my models.predict_pf
        rel_date (datetime.date): date at which filters should be applied
        cube (pyspark.sql.DataFrame): member cube, must contain the following columns:
            'L52W_HAS_BOUGHT_WOMEN' -- women's sensitivity flag
            'L52W_HAS_BOUGHT_PET' -- pet sensitivity flag
            'L52W_HAS_BOUGHT_CHILDREN' -- children's sensitivity flag
            'L52W_HAS_BOUGHT_BABY' -- baby sensitivity flag
            'CLUB_OF_FREQUENCY' -- member's top club
            'FISCAL_WEEK_END -- week end for subsetting
            'MBRSHP_SID' -- membership id
        level (str): heirarchy level to apply business rules at
        club_dna (pyspark.sql.DataFrame): club information dataframe, containing
            columns relevant to coverage at the given `level`.
        cat_dna (pyspark.sql.DataFrame): cat information datafram, containing
            columns relevant to category exclustions at the given `level`
    Returns:
        predictions (pyspark.sql.DataFrame): predictions with additional "filter" column
            which is boolean 1/0 on include or exclude
    """
    from datetime import datetime
    from dateutil import parser

    if level == "AH4":
        cat_code_col = "AH4_CD"
    elif level == "AH5":
        cat_code_col = "AH5_CD"
    elif level == "BRAND":
        cat_code_col = "BRAND_CD"
    else:
        raise ValueError("{0} is not an acceptable input!".format(level))

    # flat exclusions
    all_excl_types = ["PURCHASE_CYCLE", "CONTENT", "INVENTORY", "MEATBALLS"]
    all_excl = cat_dna[
        (
            (cat_dna.EXCLUSION_TYPE.isin(all_excl_types))
            & (cat_dna.INCLUDE_OR_EXCLUDE == "exclude")
        )
    ]
    all_excl = all_excl.withColumn(
        cat_code_col, all_excl[cat_code_col].cast("integer")
    )
    all_excl = get_col_list(all_excl, cat_code_col)
    # seasonal exclusions
    ssnl_excl_types = ["SEASONAL"]
    mo = rel_date.month
    ssnl_col_name = "SEASON_MONTH_" + str(mo)
    ssnl_excl = cat_dna[
        (
            (cat_dna.EXCLUSION_TYPE.isin(ssnl_excl_types))
            & (cat_dna.INCLUDE_OR_EXCLUDE == "exclude")
            & (cat_dna[ssnl_col_name] == 0)
        )
    ]
    ssnl_excl = ssnl_excl.withColumn(
        cat_code_col, ssnl_excl[cat_code_col].cast("integer")
    )
    ssnl_excl = get_col_list(ssnl_excl, cat_code_col)
    # sensitive exclusions
    snstv_excl_types = ["SENSITIVE"]
    snstv_excl = cat_dna[
        (
            (cat_dna.EXCLUSION_TYPE.isin(snstv_excl_types))
            & (cat_dna.INCLUDE_OR_EXCLUDE == "exclude")
        )
    ]
    snstv_excl = snstv_excl.withColumn(
        cat_code_col, snstv_excl[cat_code_col].cast("integer")
    )
    womens_excl = get_col_list(
        snstv_excl[snstv_excl.EXCLUSION_SUBTYPE == "WOMENS"], cat_code_col
    )
    childrens_excl = get_col_list(
        snstv_excl[snstv_excl.EXCLUSION_SUBTYPE == "CHILDREN"], cat_code_col
    )
    pet_excl = get_col_list(
        snstv_excl[snstv_excl.EXCLUSION_SUBTYPE == "PET"], cat_code_col
    )
    baby_excl = get_col_list(
        snstv_excl[snstv_excl.EXCLUSION_SUBTYPE == "BABY"], cat_code_col
    )

    # apply static exclusions
    cube = cube.select(
        "MBRSHP_SID",
        "FISCAL_WEEK_END",
        "L52W_HAS_BOUGHT_BABY",
        "L52W_HAS_BOUGHT_CHILDREN",
        "L52W_HAS_BOUGHT_PET",
        "L52W_HAS_BOUGHT_WOMEN",
        "CLUB_OF_FREQUENCY",
    )
    max_fiscal_week = cube.select(fmax("FISCAL_WEEK_END")).collect()[0][0]
    if (isinstance(max_fiscal_week, str)) | (isinstance(max_fiscal_week, str)):
        max_fiscal_week = parser.parse(max_fiscal_week).date()
    fiscal_week = next_fiscal_week_end(rel_date)
    if parser.parse(fiscal_week).date() > max_fiscal_week:
        fiscal_week = datetime.strftime(max_fiscal_week, "%Y-%m-%d")
    cube = cube[cube.FISCAL_WEEK_END == fiscal_week].drop("FISCAL_WEEK_END")
    predictions = predictions.withColumn(
        "MBRSHP_SID", predictions.MBRSHP_SID.cast("integer")
    )
    predictions = predictions.withColumn(
        "CATEGORY_ID", predictions.CATEGORY_ID
    )
    predictions = predictions.join(cube, "MBRSHP_SID", "left")

    # generate filter
    baby_cat = predictions.CATEGORY_ID.isin(baby_excl)
    child_cat = predictions.CATEGORY_ID.isin(childrens_excl)
    pet_cat = predictions.CATEGORY_ID.isin(pet_excl)
    women_cat = predictions.CATEGORY_ID.isin(womens_excl)
    not_bought_baby = predictions.L52W_HAS_BOUGHT_BABY == 0
    not_bought_child = predictions.L52W_HAS_BOUGHT_CHILDREN == 0
    not_bought_pet = predictions.L52W_HAS_BOUGHT_PET == 0
    not_bought_women = predictions.L52W_HAS_BOUGHT_WOMEN == 0
    baby_not_ok = baby_cat & not_bought_baby
    child_not_ok = child_cat & not_bought_child
    pet_not_ok = pet_cat & not_bought_pet
    women_not_ok = women_cat & not_bought_women
    seasonal_not_ok = predictions.CATEGORY_ID.isin(ssnl_excl)
    always_not_ok = predictions.CATEGORY_ID.isin(all_excl)
    not_ok = (
        baby_not_ok
        | child_not_ok
        | pet_not_ok
        | women_not_ok
        | seasonal_not_ok
        | always_not_ok
    )
    predictions = predictions.withColumn(
        "static_filter", when(not_ok, -1).otherwise(1)
    )

    n = predictions.count()
    predictions = predictions.cache()
    #  coverage with club DNA
    club_dna = club_dna.drop("SALES")
    club_dna = club_dna.withColumnRenamed("SITE_NBR", "CLUB_OF_FREQUENCY")
    club_dna = club_dna.withColumnRenamed("HAS_CATEGORY", "COVERAGE")
    club_dna = club_dna.withColumnRenamed("CATEGORY", "CATEGORY_ID")
    club_dna = club_dna[club_dna.CATEGORY_LVL == cat_code_col]
    predictions = predictions.join(
        club_dna, ["CLUB_OF_FREQUENCY", "CATEGORY_ID"], "left"
    )
    predictions = predictions.withColumn(
        "filter",
        when(
            ((predictions.static_filter == 1) & (predictions.COVERAGE == 1)), 1
        ).otherwise(-1),
    )
    predictions = predictions.drop(
        "SITE_NBR", "static_filter", "COVERAGE", "CATEGORY_LEVEL"
    )
    predictions = predictions.drop(
        "L52W_HAS_BOUGHT_BABY",
        "L52W_HAS_BOUGHT_CHILDREN",
        "L52W_HAS_BOUGHT_PET",
        "L52W_HAS_BOUGHT_WOMEN",
        "CLUB_OF_FREQUENCY",
    )
    predictions = predictions.unpersist()
    return predictions


def apply_weights(
    sparkcontext, paths, predictions, cat_dna, rel_date, level, rel_weighting
):
    """Reweight categories based on several criteria.

    All weights are currently applies at the AH4 level,
    with no breakdown to AH5 or below.

    Parameters:
        sparkcontext (pyspark.sparkContext): spark context with which weights are being applied
        paths (dict): dictionary of paths (i.e. from yaml config)
        predictions (pyspark.sql.DataFrame): prediction dataframe, as produced my models.predict_pf
        rel_date (datetime.date): date at which weights should be applied
        level (str): heirarchy level to apply business rules at
        cat_dna (pyspark.sql.DataFrame): cat information datafram, containing
            columns relevant to category exclustions at the given `level`
        rel_weighting: dectionary of weights-seasonal,cycle,margin and exposure.These values are set as parameteres in the config file.
    Returns:
        predictions (pyspark.sql.DataFrame): predictions with additional "weight" column
            which is a multiplicative overall weight factor
    """
    if level == "AH4":
        code_col = "AH4_CD"
    elif level == "AH5":
        code_col = "AH5_CD"
    elif level == "BRAND":
        code_col = "BRAND_CD"
    else:
        raise ValueError("{0} is not an acceptable input!".format(level))

    index = cat_dna
    mo = rel_date.month
    col_name = "SCALED_MONTH_" + str(mo) + "_SEASONALITY"
    # apply seasonality index
    snl_index = (
        index.select(code_col, col_name)
        .withColumnRenamed(code_col, "CATEGORY_ID")
        .withColumnRenamed(col_name, "snlty_wt")
    )

    predictions = predictions.join(snl_index, "CATEGORY_ID", "left").fillna(1)
    # apply puchase cycle index
    purch_index = index.select(code_col, "SCALED_PURCHASE_CYCLE")
    purch_index = (
        purch_index.select(code_col, "SCALED_PURCHASE_CYCLE")
        .withColumnRenamed(code_col, "CATEGORY_ID")
        .withColumnRenamed("SCALED_PURCHASE_CYCLE", "cycle_wt")
    )
    predictions = predictions.join(purch_index, "CATEGORY_ID", "left").fillna(
        1
    )
    # calculate and apply margin index --- REMOVED DUE TO DEPENENCY ON MISSING FLASH MARGIN
    # mrgn = index.select('AH4_DESC', 'PCT_GROSS_MARGIN')
    # mrgn = rescale_weight_factor(mrgn, 'PCT_GROSS_MARGIN')
    # mrgn = mrgn.select("AH4_DESC", 'PCT_GROSS_MARGIN') \
    #            .withColumnRenamed('AH4_DESC', 'CATEGORY_NAME') \
    #            .withColumnRenamed("PCT_GROSS_MARGIN", 'mrgn_wt')
    # predictions = predictions.join(mrgn, 'CATEGORY_NAME', 'left').fillna(1)
    predictions = predictions.withColumn("mrgn_wt", lit(1))
    # calculate and apply exposure index --- REMOVED DUE TO DEPENENCY ON MISSING COUPON SQUARE
    # exp = sparkcontext.read.parquet(paths['matrix']).select('MBRSHP_SID', 'CATEGORY_ID', 'EXPOSURES')
    # exp = rescale_weight_factor(exp, 'EXPOSURES').withColumnRenamed('EXPOSURES', 'exp_wt')
    # predictions = predictions.join(exp, ['MBRSHP_SID', 'CATEGORY_ID'], 'left').fillna(1)
    predictions = predictions.withColumn("exp_wt", lit(1))
    # create weight column
    if any(value != 0 for value in rel_weighting.values()):
        predictions = predictions.withColumn(
            "weight",
            (
                (predictions.snlty_wt * rel_weighting["seasonality"])
                + (predictions.cycle_wt * rel_weighting["cycle"])
                + (predictions.mrgn_wt * rel_weighting["margin"])
                + (predictions.exp_wt * rel_weighting["exposure"])
            ),
        )
    else:
        predictions = predictions.withColumn("weight", lit(1))

    predictions = predictions.drop("snlty_wt", "cycle_wt", "mrgn_wt", "exp_wt")
    return predictions


def apply_bus_rules(
    sparkcontext,
    paths,
    predictions,
    cube,
    prediction_col,
    rel_date,
    level,
    rel_weighting,
):
    """Apply business rules (filters and weights) to predictions.

    All business rules are currently at the AH4 Level.
    Current Filters list:
        Content Exclusions
        Purchase Cycle Exclusions
        Sensitivity Exclusions
        Coverage Exclusions
        Meatballs Exclusions
    Current Weights list:
        Seasonality Index
        Purchase Cycle Index
        Margin Index
        Exposure index

    Parameters:
        sparkcontext (pyspark.sparkContext): spark context with which predictions are being made
        paths (dict): dictionary of paths (i.e. from yaml config)
        predictions (pyspark.sql.DataFrame): prediction dataframe, as produced my models.predict_pf
        cube (pyspark.sql.DataFrame): cube dateaframe, as raw input
        prediction_col (str): name of the column containing predictions
        rel_date (datetime.date): relevant date at which rules should be applied
        level (str): heirarchy level to apply business rules at
        rel_weighting: dectionary of weights-seasonal,cycle,margin and exposure.These values are set as parameteres in the config file.

    Returns:
        predictions (pyspark.sql.DataFrame): predictions with adjusted prediction column
    """
    if level == "AH4":
        cat_dna_path = paths["CATEGORY_DNA_AH4"]
    elif level == "AH5":
        cat_dna_path = paths["CATEGORY_DNA_AH5"]
    elif level == "BRAND":
        cat_dna_path = paths["CATEGORY_DNA_BRAND"]
    else:
        raise ValueError("{0} is not an acceptable input!".format(level))

    club_dna = sparkcontext.read.parquet(paths["CLUB_DNA"])
    cat_dna = sparkcontext.read.parquet(cat_dna_path)
    predictions = apply_filters(
        predictions, rel_date, cube, cat_dna, club_dna, level
    )
    predictions = apply_weights(
        sparkcontext,
        paths,
        predictions,
        cat_dna,
        rel_date,
        level,
        rel_weighting,
    )
    full_pred = (
        predictions[prediction_col]
        * predictions["filter"]
        * predictions["weight"]
    )
    predictions = predictions.withColumn(prediction_col, full_pred)
    predictions = predictions.drop("filter", "weight")
    return predictions
