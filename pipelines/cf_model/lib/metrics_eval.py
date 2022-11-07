import math

from pyspark.ml.evaluation import Evaluator
from pyspark.sql.functions import (
    udf,
    struct,
    lit,
    collect_list,
    mean,
    desc,
    isnull,
)
from pyspark.sql.types import DoubleType
from scipy.stats import percentileofscore


class MeanAveragePrecisionK(Evaluator):
    """Pyspark Evaluator class for Mean Average Precision.

    Evaluates results based on mean average precision, which expects four input columns:
    member, category, true value, predicted value. Also requires a value for K to be set, which
    will be the desired rank up to which we evaluate (i.e. up to 5 predictions.)
    The true and predicted values column can be of type double (binary 0/1 prediction, or probability of label
    1) or of type int.

    Parameters:
        member_col (str): Name of member column to use in evaluation
        cat_col (str): Name of category column to use in evaluation
        true_col (str): Name of true column to use in evaluation
        prediction_col (str): Name of prediction column to use in evaluation
        n_recc (str): value of K to use in evaluation

    """

    def __init__(
        self,
        membr_col="MBRSHP_SID",
        cat_col="CATEGORY_ID",
        true_col="TRIPS",
        prediction_col="prediction",
        n_recc=5,
    ):
        self.membr = membr_col
        self.cat = cat_col
        self.true = true_col
        self.pred = prediction_col
        self.recc = n_recc

    def isLargerBetter(self):
        """Identify if larger scores are better or worse."""
        return True

    def apk(self, actual, predicted, k=10):
        """Computes the average precision at k.

        This function computes the average prescision at k between two lists of
        items.

        Parameters
            actual (list): A list of elements that are to be predicted
            predicted (list): A list of predicted elements
            k (int, optional): The maximum number of predicted elements

        Returns
            score (double): The average precision at k over the input lists
        """
        if len(predicted) > k:
            predicted = predicted[:k]
        score = 0.0
        num_hits = 0.0
        for i, p in enumerate(predicted):
            if p in actual[:i]:
                num_hits += 1.0
                score += num_hits / (i + 1.0)
        if not actual:
            return 0.0
        if num_hits > 0:
            return score / num_hits
        else:
            return 0.0

    def apply_apk(self, t, p, k):
        """UDF to apply APK to columns of pyspark dataframe.

        This function computes the average prescision at k on a per-row
        basis across all rows of a pyspark dataframe.

        Parameters
            t (list(tuple)): A list of tuples containing (score, category)
            p (list(tuple)): A list of tuples containing (score, category)
            k (int): The maximum number of predicted elements

        Returns
            score (double): The average precision at k over the input lists
        """
        t_s = [i[1] for i in sorted(t, reverse=True)]
        p_s = [i[1] for i in sorted(p, reverse=True)]
        print(t_s)
        print(p_s)
        precision = self.apk(t_s, p_s, k)
        return float(precision)

    def _evaluate(self, dataset):
        """Evaluate mapk across result set.

        This function returns a mean value for average precision across
        all members and categories.

        Parameters
            dataset (dataframe): pyspark dataframe containing the requried four columns for evaluation


        Returns
            mapk (double): The mean average precision at k over the dataset
        """
        dataset = dataset.withColumn(
            "true", struct(dataset[self.true], dataset[self.cat])
        )
        dataset = dataset.withColumn(
            "pred", struct(dataset[self.pred], dataset[self.cat])
        )
        dataset = dataset.groupBy(self.membr).agg(
            collect_list("true").alias("TRUE"),
            collect_list("pred").alias("PRED"),
        )
        dataset = dataset.withColumn("n", lit(self.recc))
        apk_udf = udf(self.apply_apk, returnType=DoubleType())
        dataset = dataset.withColumn(
            "apk", apk_udf(dataset.TRUE, dataset.PRED, dataset.n)
        )
        mapk = dataset.select([mean("apk")]).limit(1).collect()[0][0]
        return mapk


class PercentileRank(Evaluator):
    """Pyspark Evaluator class for Percentile Rank.

    Evaluates results based on percentile rank, which expects four input columns:
    member, category, true value, predicted value. The true and predicted values
    column can be of type double (binary 0/1 prediction, or probability of label
    1) or of type int.

    Parameters:
        member_col (str): Name of member column to use in evaluation
        cat_col (str): Name of category column to use in evaluation
        true_col (str): Name of true column to use in evaluation
        prediction_col (str): Name of prediction column to use in evaluation

    """

    def __init__(
        self,
        membr_col="MBRSHP_SID",
        cat_col="CATEGORY_ID",
        true_col="TRIPS",
        prediction_col="prediction",
    ):
        self.membr = membr_col
        self.cat = cat_col
        self.true = true_col
        self.pred = prediction_col

    def isLargerBetter(self):
        """Identify if larger scores are better or worse."""
        return False

    def apply_pctile(self, t, p):
        """UDF to apply pctile rank to columns of pyspark dataframe.

        This function computes the suggestion percentile on a per-row
        basis across all rows of a pyspark dataframe.

        Parameters
            t (list(tuple)): A list of tuples containing (score, category)
            p (list(tuple)): A list of tuples containing (score, category)

        Returns
            score (double): The perentile of p in t over the input lists
        """
        t_s = [i[1] for i in sorted(t, reverse=True)]
        p_s = [i[1] for i in sorted(p, reverse=True)]
        rec = p_s[0]
        scored = percentileofscore(t_s, rec, kind="rank")
        return float(scored)

    def _evaluate(self, dataset):
        """Evaluate percentile across result set.

        This function returns a mean value for prediction percentile across
        all members and categories.

        Parameters
            dataset (dataframe): pyspark dataframe containing the requried four columns for evaluation


        Returns
            mean_pctile (double): The mean percentile rank across all items in the dataset
        """
        dataset = dataset.withColumn(
            "true", struct(dataset[self.true], dataset[self.cat])
        )
        dataset = dataset.withColumn(
            "pred", struct(dataset[self.pred], dataset[self.cat])
        )
        dataset = dataset.groupBy(self.membr).agg(
            collect_list("true").alias("TRUE"),
            collect_list("pred").alias("PRED"),
        )
        pctile_udf = udf(self.apply_pctile, returnType=DoubleType())
        dataset = dataset.withColumn(
            "pctile", pctile_udf(dataset.TRUE, dataset.PRED)
        )
        mean_pctile = (
            dataset.select([mean("pctile")]).limit(1).collect()[0][0] / 100
        )
        return mean_pctile
