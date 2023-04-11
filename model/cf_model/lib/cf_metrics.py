"""Helper functions that define metrics for successful modelling."""

from pyspark.sql.functions import (
    mean,
    desc,
    isnull,
    monotonically_increasing_id,
    sum,
)

from pe_memberdna.model.cf_model.lib.cf_utils import generate_hist
from pe_memberdna.lib.utils import top_n


def hits_at_score(predictions, score):
    """Determine number of "hits" at a given predicted score.

    A "hit" is determined to be a case where a prediction of a certain score
    is connected to a future purchase (the prediction came true). We can use
    hits at a given score to assess model performance. Generally a better model
    will have more hits at a lower score. Please note that scores are binned to
    deviations of 0.01, so further subdivision will not yield different results.

    Parameters
        predictions (pyspark.sql.DataFrae): pyspark dataframe of FULL predictions
            easiest way to get this is with the models.predict_pf() function.
            requires the following columns:
                PAST_TRIPS: count of trips in the training example
                FUTURE_TRIPS: count of trips in the test period
                prediction: predicted score by collaborative filter model.

    Returns
        hits_at_score (float): The hitrate at the provided score
    """
    true_preds = predictions[predictions.PAST_TRIPS == 0]
    wins = predictions[
        ((predictions.PAST_TRIPS == 0) & (predictions.FUTURE_TRIPS > 0))
    ]
    # handle case where no wins are found :(
    if wins.count() == 0:
        return 0
    else:
        hits = generate_hist(
            [true_preds, wins], "prediction", ["total", "wins"], 500
        )
        hits["hitrate"] = hits["wins"] / hits["total"]
        hits["diff"] = (hits["prediction"] - score).abs()
        hits_at_score = hits.iloc[hits["diff"].idxmin()].hitrate
    return hits_at_score


def median_top_overall(predictions, n=10):
    """Determine median number of top overall categories per prediction.

    Compare the top global categories by trips to the top predictions
    for each member, and count the overlap. Return the median overlap for
    all predicted members.

    Parameters
        predictions (pyspark.sql.DataFrae): pyspark dataframe of FULL predictions
            easiest way to get this is with the models.predict_pf() function.
            requires the following columns:
                PAST_TRIPS: count of trips in the training example
                FUTURE_TRIPS: count of trips in the test period
                CATEGORY_NAME: category names for categories used.
                MBRSHP_SID: member ids for predicted members
                prediction: predicted score by collaborative filter model.
        n (int, optional): number of categories to test with.

    Returns
        n_top_overall (float): median number of top overall categories in each member's predictions
    """
    # set up top categories
    true_preds = predictions[predictions.PAST_TRIPS == 0]
    top_cats = true_preds.groupBy("CATEGORY_NAME").agg(
        mean("prediction").alias("mean_score")
    )
    top_cats = top_cats.orderBy(desc("mean_score"))
    top_cats = top_cats.limit(n).toPandas()
    top_catname = top_cats["CATEGORY_NAME"].tolist()

    # set up top per member and aggregate
    top_per_memb = top_n(true_preds, n, "prediction", "MBRSHP_SID")
    top_per_memb = top_per_memb.withColumn(
        "intop", top_per_memb.CATEGORY_NAME.isin(top_catname)
    )
    top_per_memb = top_per_memb.withColumn(
        "intop", top_per_memb.intop.cast("integer")
    )
    top_per_memb = top_per_memb.groupBy("MBRSHP_SID").agg(
        sum("intop").alias("n_intop")
    )
    n_top_overall = top_per_memb.orderBy("n_intop").approxQuantile(
        "n_intop", [0.5], 0.001
    )[0]
    return n_top_overall


def median_top_personal(predictions, n=10):
    """Determine median number of top personal categories per prediction.

    Compare the top ten categories by trips for an individual to to their
    top predictions, and count the overlap. Return the median overlap for
    all predicted members.

    Parameters
        predictions (pyspark.sql.DataFrae): pyspark dataframe of FULL predictions
            easiest way to get this is with the models.predict_pf() function.
            requires the following columns:
                PAST_TRIPS: count of trips in the training example
                CATEGORY_NAME: category names for categories used.
                MBRSHP_SID: member ids for predicted members
                prediction: predicted score by collaborative filter model.
        n (int, optional): number of categories to test with.

    Returns
        n_top_personal (float): median number of top overall categories in each member's predictions
    """
    # set up top personal categories
    top_cats = top_n(
        predictions, n, "prediction", "MBRSHP_SID", keep=True
    ).withColumnRenamed("rank", "top_rank")
    top_cats = top_cats.select("CATEGORY_ID", "MBRSHP_SID", "top_rank")
    # set up top per member and aggreagate
    true_preds = predictions[predictions.PAST_TRIPS == 0]
    top_per_memb = top_n(
        true_preds, n, "prediction", "MBRSHP_SID", keep=True
    ).withColumnRenamed("rank", "pred_rank")
    top_per_memb = top_per_memb.join(
        top_cats, ["CATEGORY_ID", "MBRSHP_SID"], "left"
    )
    top_per_memb = top_per_memb.withColumn(
        "intop", ~isnull(top_per_memb.top_rank)
    )
    top_per_memb = top_per_memb.withColumn(
        "intop", top_per_memb.intop.cast("integer")
    )
    top_per_memb = top_per_memb.groupBy("MBRSHP_SID").agg(
        sum("intop").alias("n_intop")
    )
    n_top_personal = top_per_memb.orderBy("n_intop").approxQuantile(
        "n_intop", [0.5], 0.001
    )[0]
    return n_top_personal


def lookup_tbl_check(predictions, n=10):
    """Determine difference between model and a lookup table.

    Compare the top ten category predictions for each member to the set that
    one would get by ranking overall categories by trips and removing the ones
    that each member has shopped in before.

    Parameters
        predictions (pyspark.sql.DataFrae): pyspark dataframe of FULL predictions
            easiest way to get this is with the models.predict_pf() function.
            requires the following columns:
                PAST_TRIPS: count of trips in the training example
                CATEGORY_NAME: category names for categories used.
                MBRSHP_SID: member ids for predicted members
                prediction: predicted score by collaborative filter model.
        n (int, optional): number of categories to test with.

    Returns
        n_lut (float): number of the top n predictions that would be found in lookup table
    """
    # rank all categories and join on
    by_cat = predictions.groupBy("CATEGORY_ID", "CATEGORY_NAME").agg(
        sum("PAST_TRIPS").alias("trips_count")
    )
    by_cat = by_cat.orderBy(desc("trips_count")).withColumn(
        "overall_rank", monotonically_increasing_id()
    )
    by_cat = by_cat.select("CATEGORY_ID", "overall_rank")
    full_preds = predictions.join(by_cat, "CATEGORY_ID", "inner")
    full_preds.cache()
    # set up top per member and and top lookup
    true_preds = predictions[predictions.PAST_TRIPS == 0]
    true_preds = full_preds[full_preds.PAST_TRIPS == 0]
    top_pred = top_n(
        true_preds, n, "prediction", "MBRSHP_SID", keep=True
    ).withColumnRenamed("rank", "pred_rank")
    top_lut = top_n(
        true_preds, n, "overall_rank", "MBRSHP_SID", keep=True
    ).withColumnRenamed("rank", "lut_rank")
    top_lut = top_lut.select("MBRSHP_SID", "CATEGORY_ID", "lut_rank")
    # join sets and compare
    top_per_memb = top_pred.join(
        top_lut, ["CATEGORY_ID", "MBRSHP_SID"], "left"
    )
    top_per_memb = top_per_memb.withColumn(
        "intop", ~isnull(top_per_memb.lut_rank)
    )
    top_per_memb = top_per_memb.withColumn(
        "intop", top_per_memb.intop.cast("integer")
    )
    top_per_memb = top_per_memb.groupBy("MBRSHP_SID").agg(
        sum("intop").alias("n_intop")
    )
    n_top_lut = top_per_memb.orderBy("n_intop").approxQuantile(
        "n_intop", [0.5], 0.001
    )[0]
    full_preds.unpersist()
    return n_top_lut
