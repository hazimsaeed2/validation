"""Modeling helper functions."""

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import (
    col,
    min,
    max,
    stddev,
    mean,
    lit,
    udf,
    countDistinct,
    when,
)
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

from pe_memberdna.model.cf_model.lib.cf_utils import generate_hist
from pe_memberdna.model.cf_model.lib.cf_metrics import (
    hits_at_score,
    median_top_overall,
    median_top_personal,
    lookup_tbl_check,
)
from pe_memberdna.model.cf_model.lib.backtest import (
    prep_backtest,
    run_backtest,
)
from pe_memberdna.lib.utils import top_n


def predict_pf(
    params,
    model,
    slate,
    pdata,
    fdata=None,
    cat_lookup=None,
    col_of_interest="TRIPS",
):
    """Make predictions and combine with past and future data for analysis and evaluation.

    Many times model preditcions need to be comined with past and future data for evaluation
    purposes. This function can create 'full' predictions using a model and template.

    Parameters:
        params (dict): parameters dictionary. Requires:
            future_run (boolean): if the models being run with future data
        model (SparkML model): model to transform data with
        slate (pyspark.sql.DataFrame): 'blank' dataframe to be transformed by model. Must contain:
            MBRSHP_SID (pyspark.types.LongType): Members to predict for
            CATEGORY_ID (pyspark.types.LongType): Categories to predict for
        pdata (pyspark.sql.DataFrame):  past information about members/categories
        fdata (pyspark.sql.DataFrame): future information about members/categories
        cat_lookup (pyspark.sql.DataFrame, optional): lookup table to add column names to returned data
        col_of_interest (str): column name describing the past and future data to be combined with predictions

    Returns:
        alldata (pyspark.sql.DataFrame): joined dataframe containing predictions, past data, and future data
    """
    predictions = model.transform(slate)
    past_colname = "PAST_" + col_of_interest
    pdata = pdata.select("MBRSHP_SID", "CATEGORY_ID", col_of_interest)
    pdata = pdata.withColumnRenamed(col_of_interest, past_colname)
    past = predictions.join(pdata, ["MBRSHP_SID", "CATEGORY_ID"], "left")

    if params["future_run"] or params["rmse"]:
        future_colname = "FUTURE_" + col_of_interest
        fdata = fdata.select("MBRSHP_SID", "CATEGORY_ID", col_of_interest)
        fdata = fdata.withColumnRenamed(col_of_interest, future_colname)
        alldata = past.join(fdata, ["MBRSHP_SID", "CATEGORY_ID"], "left")
    else:
        alldata = past
    alldata = alldata.fillna(0)
    if cat_lookup is not None:
        alldata = alldata.join(cat_lookup, "CATEGORY_ID")
    return alldata


def meanscale_window(c, w):
    """Apply mean-scaling over window..

    Often we want to normalize our data over a window rather than
    as a whole (i.e. normalize by member or by category) We can do
    this using a window function. This is written as a pyspark "apply"
    function, and should be used as combined with a 'withColumn'.

    Parameters:
        c (str): String name of dataframe column to normalize
        w (pyspark.sql.window.Window): window object to use for windowing

    Returns:
        scaled (pyspark.sql.DataFrame.column): normalized column over window
    """
    scaled = col(c) / mean(c).over(w)
    return scaled


def stdscale_window(c, w):
    """Apply standard scaling over a column with a window.

    Often we want to normalize our data over a window rather than
    as a whole (i.e. normalize by member or by category) We can do
    this using a window function. This is written as a pyspark "apply"
    function, and should be used as combined with a 'withColumn'.

    Parameters:
        c (str): String name of dataframe column to normalize
        w (pyspark.sql.window.Window): window object to use for windowing

    Returns:
        scaled (pyspark.sql.DataFrame.column): normalized column over window
    """
    scaled = (col(c) - mean(c).over(w)) / stddev(c).over(w)
    return scaled


def minmaxscale_window(c, w, maximum, minimum):
    """Apply min-max scaling over a column with a window.

    Often we want to normalize our data over a window rather than
    as a whole (i.e. normalize by member or by category) We can do
    this using a window function. This is written as a pyspark "apply"
    function, and should be used as combined with a 'withColumn'.

    Parameters:
        c (str): String name of dataframe column to normalize
        w (pyspark.sql.window.Window): window object to use for windowing
        maximum (int): intended maximum value of the normalized range
        minimum (int): intended minimum value of the normalized range

    Returns:
        scaled (pyspark.sql.DataFrame.column): normalized column over window
    """
    dscale = (col(c) - min(c).over(w)) / (max(c).over(w) - min(c).over(w))
    abscale = maximum - minimum
    scaled = dscale * abscale + minimum
    return scaled


def scale_by_group(df, col, group_col, method="minmax", minimum=0, maximum=1):
    """Apply scaling to a column while grouped by another column.

    Often we want to normalize our data over a window rather than
    as a whole (i.e. normalize by member or by category). Use this convenience
    function to scale data in one column within groups of another column.
    Currently supports:
        arbitrary minmax ('minmax')
        standard ('standard') scaling
        mean ('mean') scaling

    Parameters:
        df (pyspark.sql.DataFrame): pyspark dataframe containing data to normalize
        col (str): name of dataframe column to normalize
        group_col (str): name of column to group by when scaling
        method (str, optional): name of method to use for scaling
        minimum (int, optional): intended maximum value of the minmax range (minmax only)
        maximum (int, optional): intended minimum value of the minmax range (minmax only)

    Returns:
        df (pyspark.sql.DataFrame): Dataframe with normalized column
    """
    window = Window.partitionBy(group_col)
    if method == "minmax":
        scaled = df.withColumn(
            col, minmaxscale_window(col, window, maximum, minimum)
        )
    elif method == "standard":
        scaled = df.withColumn(col, stdscale_window(col, window))
    elif method == "mean":
        scaled = df.withColumn(col, meanscale_window(col, window))
    else:
        raise ValueError("normalization method not supported!")
    scaled = scaled.fillna(0, subset=[col])
    return scaled


def purchase_cycle_normalize(sparkcontext, params, paths, data, timeperiod):
    """Normalize data by purchase cycle.

    Read in category dna to gather purchase cycle
    for categories and calculate "expected" interactions over
    time period. These are based on trips, but should be on average
    proportional. Normalize data against expected interval.

    Parameters:
        sparkcontext (pyspark.sparkContext): spark context with additional data can be read
        params (dict): parameters dictionary. Requires:
            inputtype (str): type of relevant input data
            category (str): category level for normalization
        paths (dict): paths dictionary: Requires:
            CATEGORY_DNA_AH4: path for AH4 category DNA
            CATEGORY_DNA_AH5: path for AH5 category DNA
        data (pyspark.sql.DataFrame): data to normalize. Requires:
            MBRSHP_SID (int): members
            CATEGORY_ID (int): categories
            inputtype (int): metric of preference (e.g. TRIPS)
        timeperiod (int): length of time-period of the data in days

    Returns:
        normalized (pyspark.sql.DataFrame): normalized data. Includes:
            MBRSHP_SID (int): members
            CATEGORY_ID (int): categories
            inputtype (int): metric of preference normalized by purchase cycle
    """
    # 1. handle parameters
    cat_code_col = params["category"] + "_CD"
    cat_dna_key = "CATEGORY_DNA_" + params["category"]
    cat_dna_path = paths[cat_dna_key]
    # 2. read and prep category_dna
    cat_dna = sparkcontext.read.parquet(cat_dna_path)
    cat_dna = cat_dna.withColumn(
        "CATEGORY_ID", cat_dna[cat_code_col].cast("long")
    )
    # 3. calculate expected interactions
    cat_dna = cat_dna.withColumn("days", lit(timeperiod))
    cat_dna = cat_dna.withColumn(
        "exp_interactions", cat_dna.days / cat_dna.PURCHASE_CYCLE_DAYS
    )
    cat_dna = cat_dna.select("CATEGORY_ID", "exp_interactions")
    # 4. join data and normalize
    combined = data.join(cat_dna, "CATEGORY_ID", "left").fillna(0)
    inputtype = params["inputtype"]
    combined = combined.withColumn(
        inputtype, combined[inputtype] / combined.exp_interactions
    )
    normalized = combined.drop("exp_interactions")
    normalized = normalized[normalized[inputtype].isNotNull()]
    normalized = normalized[normalized[inputtype] > 0]
    return normalized


def cat_size_normalize(sparkcontext, params, paths, data):
    """Normalize data by category size.

    Read in item data to gather size for categories and
    calculate relative size vs averagetime period.
    Normalize data by relative category size.

    Parameters:
        sparkcontext (pyspark.sparkContext): spark context with additional data can be read
        params (dict): parameters dictionary. Requires:
            inputtype (str): type of relevant input data
            category (str): category level for normalization
        paths (dict): paths dictionary: Requires:
            ITEM_INT: path for item intermediates
        data (pyspark.sql.DataFrame): data to normalize. Requires:
            MBRSHP_SID (int): members
            CATEGORY_ID (int): categories
            inputtype (int): metric of preference (e.g. TRIPS)

    Returns:
        normalized (pyspark.sql.DataFrame): normalized data. Includes:
            MBRSHP_SID (int): members
            CATEGORY_ID (int): categories
            inputtype (int): metric of preference normalized by purchase cycle
    """
    # 1. handle parameters
    cat_code_col = params["category"] + "_CD"
    item_int_path = paths["ITEM_INT"]
    # 2. read and prep item intermediates
    item_dtl = sparkcontext.read.parquet(item_int_path)
    item_counts = item_dtl.groupBy(cat_code_col).agg(
        countDistinct("ARTICLE_NBR").alias("item_count")
    )
    mean_count = item_counts.groupBy("item_count").mean().collect()[0][0]
    item_counts = item_counts.withColumn(
        "CATEGORY_ID", item_counts[cat_code_col].cast("long")
    )
    item_counts = item_counts.withColumn("avg", lit(mean_count))
    # 3. calculate relative size
    rel_size = item_counts.withColumn(
        "cat_size", item_counts.item_count / item_counts.avg
    )
    rel_size = rel_size.select("CATEGORY_ID", "cat_size")
    # 4. Join and normalize data
    combined = data.join(rel_size, "CATEGORY_ID", "left").fillna(0)
    inputtype = params["inputtype"]
    combined = combined.withColumn(
        inputtype, combined[inputtype] / combined.cat_size
    )
    normalized = combined.drop("cat_size")
    normalized = normalized[normalized[inputtype].isNotNull()]
    normalized = normalized[normalized[inputtype] > 0]
    return normalized


def add_cf_reg(sparkcontext, params, predictions):
    """Add CF_REG (prediction_v2) to predictions

    Read in predictions and add the CF_REG column to
    the dataset. The difference between CF_REG and
    classic CF is that CF_REG incorporates a member's
    empirical data to their CF score. This is meant
    to serve as a solution to the model's bias towards
    popular items while still using insights the model
    derives from the population.

    Parameters:
        sparkcontext (pyspark.sparkContext): spark context with additional data can be read
        params (dict): parameters dictionary. Requires:
            cf_reg (double): weighting factor of rel_trips_ratio
        data (pyspark.sql.DataFrame): predictions file. Requires:
            MBRSHP_SID (int): members
            CATEGORY_ID (int): categories
            rel_trips_ratio (double): members trips in a category over
                                      the mean of trips for members who
                                      have made a trip in said category
            predictions (double): CF score

    Returns:
        predictions (pyspark.sql.DataFrame): predictions data with CF_REG
    """
    weight = params["cf_reg"]
    if weight is not None:
        emp_factor = 1.0 + col("rel_trips_ratio") * weight
        predictions = predictions.withColumn(
            "prediction_v2", col("prediction") * emp_factor
        )
    predictions = predictions.drop("rel_trips_ratio")
    return predictions


def cap_outliers(data, colname):
    """Remove extreme values from data.

    Generic method of capping extreme values on data
    to prevent them from skewing results/fitting/etc.
    Method assumes std dev is approximately equal to mean,
    and caps values at 2x that deviation in either direction

    Parameters:
        data (spark.sql.DataFrame): dataframe to transform
        colname (str): name of column to remove outliers from

    Returns:
        capped (spark.sql.DataFrame): dataframe with capped values
    """
    mean_value = data.select(mean(data[colname])).collect()[0][0]
    allowable_delta = 2 * mean_value
    max_value = mean_value + allowable_delta
    min_value = mean_value - allowable_delta
    max_comp = data[colname] > max_value
    data = data.withColumn(
        colname, when(max_comp, max_value).otherwise(data[colname])
    )
    min_comp = data[colname] < min_value
    capped = data.withColumn(
        colname, when(min_comp, min_value).otherwise(data[colname])
    )
    return capped


def sigmoid(x, x0, k, m):
    """Basic sigmoid function.

    Returns sigmoid values for input value x
    based on parameters x0, k, m, representing
    curve location, slope, and max value

    Parameters:
        x (float): The value to transform
        x0 (float): The x-location of the sigmoid curve
        k (float): The slope of the sigmoid curve
        m (float): The max value of the sigmoid curve

    Returns:
        y (float): The sigmoid function value at x
    """
    y = m / (1 + 2.718268237 ** (-k * (x - x0)))
    return y


sigmoid_udf = udf(sigmoid, FloatType())


def fit_output_scaler(sparkcontext, predictions):
    """Fit output scaler to prediction set.

    Fits a scaler that will estimate the probability
    of future purchase from a raw predicted score for
    a given model. Returns the scaler to save or use
    later to adjust output scores.

    Parameters:
        sparkcontext (pyspark.SparkContext): The spark context to return the scaler to
        predictions (pyspark.sql.DataFrame): The dataframe of predictions to fit contains:
            PAST_TRIPS (int): number of prior trips to count past purchases
            FUTURE TRIPS (int): number of future trips to count future purchases
            prediction (float): raw predicted score for example

    Returns:
        params (pyspark.broadcast): broadcast variable containing parameters
    """
    from scipy.optimize import curve_fit
    import pandas as pd

    true_preds = predictions[predictions.PAST_TRIPS == 0]
    wins = predictions[
        ((predictions.PAST_TRIPS == 0) & (predictions.FUTURE_TRIPS > 0))
    ]
    hits = generate_hist(
        [true_preds, wins], "prediction", ["total", "wins"], -5, 5, 500
    )
    hits["hitrate"] = hits["wins"] / hits["total"]
    # add 'safety point' for sigmoids that max beyond score range
    # ensures a good fit
    x = pd.concat([hits["prediction"], pd.Series([3]), pd.Series([4])], axis=0)
    y = pd.concat(
        [
            hits["hitrate"],
            pd.Series([hits["hitrate"].max()]),
            pd.Series([hits["hitrate"].max()]),
        ],
        axis=0,
    )
    popt, pcov = curve_fit(sigmoid, x, y)
    x_o = popt[0]
    k = popt[1]
    m = popt[2]
    obj = {"x_o": x_o, "k": k, "m": m}
    params = sparkcontext.broadcast(obj)
    return params


def apply_output_scaling(predictions, prediction_col, scaler):
    """Apply scaling to predictions.

    Uses an output scaler to apply scaling to a set of predictions
    scaler must be fit using the fit_output_scaler method.

    Parameters:
        predictions (pyspark.sql.DataFrame): The dataframe of predictions to transform
        prediction_col (str): name of column containing predictions
        scaler (pyspark.broadcast): broadcast object containing scaling parameters

    Returns:
        predictions (pyspark.sql.DataFrame): dataframe with scaled column
    """
    predictions = predictions.withColumn("x_o", lit(scaler.value["x_o"]))
    predictions = predictions.withColumn("k", lit(scaler.value["k"]))
    predictions = predictions.withColumn("m", lit(scaler.value["m"]))
    predictions = predictions.withColumn(
        prediction_col,
        sigmoid_udf(
            predictions[prediction_col],
            predictions.x_o,
            predictions.k,
            predictions.m,
        ),
    )
    predictions = predictions.drop("x_o").drop("k").drop("m")
    return predictions


def evaluate_cf_train(
    sparksession,
    model,
    paths,
    eval_col="trips",
    backtest=True,
    level="AH4",
    params=None,
):
    """Run all evaluation metrics on model.

    Convenience wrapper for running full series of evaluation
    functions on a model fit.

    Parameters
        sparksession (pyspark.SparkSession): The spark session to use for reading data
        model (pyspark.ml.recommendation.ALSModel): the model to evaluate
        paths (dict): paths configuration dictionary (from yaml config file)
        eval_col (str, optional): the name of the column to use for evalating metrics
        backtest (str, optional): t/f string for whether to include backtest or not
        level (str, optional): the heirarchy level at which to run evaluations
        params(dict): parameters dictionary. Requers values for rmse . These parameters are arguments in the spark submit
                        to indicate if we want to calculate rmse(implicit in here is that we also need future data)
    Returns
        evals (dict): evaluation metrics results labeled by key
    """
    if eval_col.lower() == "trips":
        curr_eval = "TRIPS"
        future_eval = "FUTURE_TRIPS"
    else:
        raise ValueError("evaluation column not currently supported")

    # read in evaluation data
    print("reading eval data...")
    matrix = paths["matrix"]
    cat = paths["cat"]
    slate = paths["slate"]
    future = paths["future"]

    slate = sparksession.read.parquet(slate)
    pdata = sparksession.read.parquet(matrix).select(
        "MBRSHP_SID", "CATEGORY_ID", curr_eval
    )
    fdata = sparksession.read.parquet(future).select(
        "MBRSHP_SID", "CATEGORY_ID", curr_eval
    )
    cat_lookup = sparksession.read.parquet(cat)

    # make predictions
    print("making predictions...")
    predictions = predict_pf(
        params,
        model,
        slate,
        pdata,
        fdata,
        cat_lookup=cat_lookup,
        col_of_interest=curr_eval,
    )
    predictions = predictions.cache()

    # run evaluations
    print("evaluating ...")
    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol=future_eval, predictionCol="prediction"
    )

    rmse = evaluator.evaluate(predictions)
    hits = hits_at_score(predictions, 1.0)
    hits_half = hits_at_score(predictions, 0.5)
    overall = median_top_overall(predictions, n=10)
    personal = median_top_personal(predictions, n=10)
    lut = lookup_tbl_check(predictions, n=10)
    avg_score = predictions.select(mean(predictions.prediction)).collect()[0][
        0
    ]
    under05 = predictions[predictions.prediction < 0.05].count()

    # top prediction per member
    top_preds = top_n(predictions, 1, "prediction", "MBRSHP_SID")
    top_by_cat = top_preds.groupBy("CATEGORY_NAME").agg(
        countDistinct("MBRSHP_SID").alias("member_count")
    )
    top_by_cat = (
        top_by_cat.sort("member_count", ascending=False).limit(1).collect()[0]
    )
    top_name = top_by_cat.CATEGORY_NAME
    top_count = top_by_cat.member_count

    if backtest:
        print("backtesting...")
        members = sparksession.read.load(paths["CUBE"])
        cat_lookup = "s3://memberanalytics-data-out/MODELDATA/DATASETS/cat_lookup_20160101-20180526_AH4_215_v3"
        cat_lookup = sparksession.read.parquet(cat_lookup)
        pdata_path = paths["DATA"] + "matrix_20170910-20171010_AH4_215_v4/"
        fdata_path = paths["DATA"] + "matrix_20171011-20171111_AH4_215_v4/"
        bt_pdata = sparksession.read.parquet(pdata_path)
        bt_fdata = sparksession.read.parquet(fdata_path)
        backtest_data = prep_backtest(
            sparksession,
            predictions,
            cat_lookup,
            members,
            bt_pdata,
            bt_fdata,
            cutoff=0.05,
            cats_to_test=1,
            campaign="bbm14",
            level=level,
        )
        results = run_backtest(
            backtest_data,
            cat_lookup,
            metric="future_weeksales",
            campaign="bbm14",
        )
        lift_diff = results["frac_lift"][0] - results["frac_lift"][1]
    else:
        lift_diff = None

    # organze and return results
    evals = {
        "rmse": rmse,
        "mean_score": avg_score,
        "count_under05": under05,
        "top_cat": top_name,
        "top_cat_ct": top_count,
        "hits_1": hits,
        "hits_05": hits_half,
        "overall": overall,
        "personal": personal,
        "lut": lut,
        "backtest": lift_diff,
    }
    predictions.unpersist()
    return evals
