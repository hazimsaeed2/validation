"""QC tests library for generating and running QC reports."""

import functools
import operator

import pandas as pd
import pyspark.sql.functions as sqlf
from pyspark.sql.functions import avg, coalesce, col, countDistinct
from pyspark.sql.functions import lower as flower
from pyspark.sql.functions import max as fmax
from pyspark.sql.functions import min as fmin
from pyspark.sql.functions import when

import pe_memberdna.assignment.lib.qc as qc
import pe_memberdna.lib.iotools as iotools
from pe_memberdna.assignment.lib.campaign import Campaign
from pe_memberdna.lib.spark_util import get_logger


# ---- Helpers ---- #


def calc_avg_col_value(df, colname):
    """Calculate the average value of dataframe column.

    Parameters:
        df (pyspark.sql.DataFrame): dataframe containg column
        colname (str): column to calculate average of

    Returns:
        avg_val (float): average value of columns
    """
    avg_val = df.select(avg(colname)).limit(1).collect()[0][0]
    return avg_val


def percent_difference(val1, val2):
    """Calculate percent difference between two values.

    Parameters:
        val1 (float): value 1
        val2 (float): value 2

    Returns
        pct_diff (float): percent difference between values
    """
    diff = abs(val1 - val2)
    pct_diff = diff / val1
    return pct_diff


def calc_col_avg_differnece(df1, df2, colname):
    """Calculate the percent difference in avg col values between DF.

    Parameters:
        df1 (pyspark.sql.DataFrame): First comparision dataframe
        df2 (pyspark.sql.DataFrmae): Second comparision dataframe
        colname (str): name of column to compare on.

    Returns:
        pct_diff (float): percent difference between avg col values.
    """
    df1_avg = calc_avg_col_value(df1, colname)
    df2_avg = calc_avg_col_value(df2, colname)
    pct_diff = percent_difference(df1_avg, df2_avg)
    return pct_diff


def celltype_by_name(df, celltype, comparison):
    """Helper test for comparing celltypes by name.

    Parameters:
        df (pyspark.sql.DataFrame): dataframe of data to compare. Requires --
            CELL_DESC (str): cell description for finding cell type
            AVG_SPEND (float): average spend by cell
        celltype (str): string identifying cell "type" to compare
        comparision (operator): operator object for comparions
    Returns:
        pf (str): pass/fail indicator
    """
    df = df.withColumn("lowercase", flower(df.CELL_DESC))
    df = df.withColumn(
        "identifier", when(df.lowercase.contains(celltype), 1).otherwise(0)
    )
    id_expr = col("identifier") == 1
    cell_in = df[id_expr]
    cell_out = df[~id_expr]
    # yes, this is avg of avg. Should be OK here to measure tendency
    cols = ["AVG_SPEND_4", "AVG_SPEND_12", "AVG_SPEND_52"]
    comps = []
    for column in cols:
        in_spend = calc_avg_col_value(cell_in, column)
        out_spend = calc_avg_col_value(cell_out, column)
        if in_spend is None:
            in_spend = 0.0
        if out_spend is None:
            out_spend = 0.0
    comp = comparison(in_spend, out_spend)
    comps.append(comp)
    if all(comps):
        return "Pass"
    else:
        return "Fail"


def count_oob_cell_sizes(cell):
    """Helper for check cell size.
    Compares "cell size" and "estimated_size" columns -
    a 25% difference or 100000 records is an acceptable tolerance
    Parameters:
        cell (pyspark.sql.DataFrame): cell dataset

    Returns:
        total_oob (int): count of oob cells - those that are outside of
        the tolerance"""
    temp = cell
    temp = temp.withColumn(
        "cell_size_out_of_bounds",
        sqlf.when(
            (
                sqlf.abs(sqlf.col("cell_size") - sqlf.col("estimated_size"))
                > 0.25 * sqlf.col("estimated_size")
            )
            | (
                sqlf.abs(sqlf.col("cell_size") - sqlf.col("estimated_size"))
                > 100000
            ),
            sqlf.lit(1),
        ).otherwise(sqlf.lit(0)),
    )

    total_oob = temp.where(sqlf.col("cell_size_out_of_bounds") == 1).count()

    return total_oob


def count_null_estimated_sizes(cell):
    """Helper for check cell size
    Parameters:
        cell (pyspark.sql.DataFrame): cell dataset

    Returns:
        total_nulls (int): count of null cells in estimated_size column"""
    temp = cell

    temp = temp.withColumn(
        "Null_estimate",
        sqlf.when(sqlf.col("estimated_size").isNull(), sqlf.lit(1)).otherwise(
            sqlf.lit(0)
        ),
    )

    total_nulls = temp.where(sqlf.col("Null_estimate") == 1).count()

    return total_nulls


# ---- Test Functions ---- #


def basket_cells(cells):
    """Test if basket cells have lower sales than other cells."""
    result = celltype_by_name(cells, "basket", operator.lt)
    return result


def trial_cells(cells):
    """Test if trial cells have lower sales than other cells."""
    result = celltype_by_name(cells, "trial", operator.lt)
    return result


def control_test(cells):
    """Test if control/test pairs are comparable."""
    allowable_difference = 0.05

    cells_control = cells[cells.ctrl_flag == 1]
    cells_test = cells[cells.test_flag == 1]

    cols = ["AVG_SPEND_4", "AVG_SPEND_12", "AVG_SPEND_52", "AVG_TRIPS"]
    comps = []
    for column in cols:

        diff = calc_col_avg_differnece(cells_control, cells_test, column)
        comp = diff < allowable_difference
        comps.append(comp)

    if all(comps):
        return "Pass"
    else:
        return "Fail"


def duplicate_membs(base):
    """Test if member is in multiple cell."""
    grouped = base.groupBy("MBRSHP_SID").agg(
        countDistinct("CELL_ID").alias("cell_ct")
    )
    multi_cell = grouped[grouped.cell_ct > 1]
    num_multi_cell_mbr = multi_cell.count()
    if num_multi_cell_mbr == 0:
        return "Pass"
    else:
        return "Fail"


def duplicate_coups(base):
    """Test if member has duplicate coupons ."""
    grouped = base.groupBy("MBRSHP_SID", "CPN_NBR").count()
    by_mbr = grouped.groupBy("MBRSHP_SID").agg(
        fmax("count").alias("max_count")
    )
    multi_assign = by_mbr[by_mbr.max_count > 1]
    num_multi_assign_mbr = multi_assign.count()
    if num_multi_assign_mbr == 0:
        return "Pass"
    else:
        return "Fail"


def coups_qualified(mbr):
    """Test that a member's coupons were all qualified in their slots."""
    mbr = mbr.withColumn(
        "all_qual", when(mbr.QUAL_CPN == mbr.DSTNCT_CPN, 1).otherwise(0)
    )
    non_allqual = mbr[mbr.all_qual == 0].count()
    if non_allqual == 0:
        return "Pass"
    else:
        return "Fail"


def always_fail(dummyvar):
    return "Fail"


def same_num_coups(assignment):
    """Test that everyone has same number of coupons assigned.
    Check that the minimum and maximum number of coupons assigned to a member
     are equal.]

    Parameters:
        assignment (pyspark.sql.DataFrame): assignment dataframe

    Returns:
        'Pass'/'Fail' (str): pass/fail indicator
    """
    grouped = assignment.groupBy("MBRSHP_SID").count()
    max_by_mbr = grouped.agg(fmax("count").alias("max_count"))
    min_by_mbr = grouped.agg(fmin("count").alias("min_count"))
    diff = max_by_mbr.collect()[0][0] - min_by_mbr.collect()[0][0]

    if diff == 0:
        return "Pass"
    else:
        return "Fail"


def check_cell_size(cell):
    """
    Check that actual cell_size is within +/- 25% of estimate or 100000
    or if estimated calls are empty
    """
    total_oob = count_oob_cell_sizes(cell)

    total_nulls = count_null_estimated_sizes(cell)

    if total_oob == 0 and total_nulls == 0:
        return "Pass"
    else:
        return "Fail"


def check_sensitive_content(job):
    """
    Check that members who have not purchased from sensitive categories in the
    last 52 weeks have not been backfilled with sensitive coupons.

    This function works by repeatedly reducing the assignment dataframe,
    removing correct assignments and ultimately ending up with just those which
    are incorrectly assigned sensitive content.

    Parameters:
        job (Job Manager): Leverages already read tables:
            assignment, dna, cf_original, exclusion_rules, coup_map,
            article_map
     Returns:
        (str): Pass / Fail / NA indicator
    """

    def cleanup(assignment=None):
        """
        Cleanup before exiting, writing any inccorrect sensitive assignments
        to s3.

        Parameters:
            assignment (pyspark.sql.DataFrame): incorrect sensitive assignments
        """
        path = job.config.paths["SENSITIVE_QC"]
        bucket, key = iotools.split_path_bucket_key(path)
        if assignment:
            assignment.repartition(1).write.csv(
                path, header=True, mode="overwrite"
            )
        else:
            # There is no incorrect assignment to write, but we don't want old
            # incorrect assignment from a previous run to remain
            if iotools.is_s3_path(bucket, key):
                iotools.s3_delete(
                    bucket, key, allowed_paths=path
                )  # OK to pass variable because it's not actually in the
                # config, but set in assn_io

    rules = job.data.tables.get("exclusion_rules")

    if not rules:
        cleanup()
        return "NA"

    sensitive_exclusions = rules.filter(
        rules.EXCLUSION_TYPE == "SENSITIVE"
    ).filter(rules.INCLUDE_OR_EXCLUDE == "exclude")
    if len(sensitive_exclusions.head(1)) == 0:
        cleanup()
        return "NA"

    assignment = job.data.tables["assignment"]
    assignment = assignment.filter(
        assignment.bf_construct != "-"
    ).withColumnRenamed("mbrshp_sid", "MBRSHP_SID")

    coup_map = job.data.tables["coup_map"].select("cpn_nbr", "article_nbr")
    article_map = job.data.tables["article_map"].select(
        "article_nbr", "AH4_CD", "AH5_CD"
    )
    assignment = (
        assignment.join(coup_map, "cpn_nbr", "inner")
        .join(article_map, "article_nbr", "inner")
        .select("MBRSHP_SID", "cpn_nbr", "article_nbr", "AH4_CD", "AH5_CD")
        .dropDuplicates()
    )

    assn_copy = assignment
    for i, category in enumerate(("AH4_CD", "AH5_CD")):
        tmp = assn_copy.filter(sqlf.col(category).isNotNull())
        tmp = tmp.withColumn("CATEGORY_TYPE", sqlf.lit(category)).withColumn(
            "CATEGORY_CD", sqlf.col(category)
        )

        if i == 0:
            assignment = tmp
        else:
            assignment = assignment.union(tmp)

    assignment = assignment.join(
        sensitive_exclusions, ["CATEGORY_TYPE", "CATEGORY_CD"], "left"
    )

    sens_assn = assignment.filter(assignment.EXCLUSION_TYPE == "SENSITIVE")
    if len(sens_assn.head(1)) == 0:
        cleanup()
        return "NA"

    cf = job.data.tables["cf_original"]
    cf = (
        cf.withColumnRenamed("CATEGORY_LVL", "CATEGORY_TYPE")
        .withColumnRenamed("CATEGORY_ID", "CATEGORY_CD")
        .select("MBRSHP_SID", "CATEGORY_TYPE", "CATEGORY_CD", "prediction")
    )
    assignment = assignment.join(
        cf, ["MBRSHP_SID", "CATEGORY_TYPE", "CATEGORY_CD"], "left"
    )

    ok_assignment = (
        assignment.filter(assignment.prediction >= 0)
        .filter((assignment.EXCLUSION_TYPE.isNull()))
        .select("MBRSHP_SID", "cpn_nbr")
        .dropDuplicates()
    )

    assignment = assignment.join(
        ok_assignment, ["MBRSHP_SID", "cpn_nbr"], "left_anti"
    ).filter(assignment.EXCLUSION_TYPE == "SENSITIVE")

    if len(assignment.head(1)) == 0:
        cleanup()
        return "Pass"

    sensitive_map = {
        "BABY": "L52W_HAS_BOUGHT_BABY",
        "CHILDREN": "L52W_HAS_BOUGHT_CHILDREN",
        "PET": "L52W_HAS_BOUGHT_PET",
        "WOMENS": "L52W_HAS_BOUGHT_WOMEN",
    }
    dna_cols = ["MBRSHP_SID"] + list(sensitive_map.values())
    dna = job.data.tables["dna"].select(*dna_cols)
    assignment = assignment.join(dna.select(*dna_cols), "MBRSHP_SID", "left")

    sensitive_filter = functools.reduce(
        operator.or_,
        [
            (sqlf.col("EXCLUSION_SUBTYPE") == cat) & (sqlf.col(scol) == 0)
            for cat, scol in sensitive_map.items()
        ],
    )

    assignment = (
        assignment.filter(sensitive_filter)
        .select("MBRSHP_SID", "cpn_nbr", "article_nbr", "AH4_CD", "AH5_CD")
        .distinct()
    )

    if len(assignment.head(1)) == 0:
        cleanup()
        return "Pass"
    else:
        cleanup(assignment)
        return "Fail"


def count_cf_ineligible(job):
    """
    Return the number of members in the population without a valid CF score for
    the campaign's category coupons.

    Parameters:
        job (jobManager): job manager object

    Returns:
        (int) number of cf ineligible members
              0 if no category coupons
    """
    params = dict(
        list(job.config.params.items())
        + list(job.config.cnf["assignment"].items())
    )
    paths = job.config.paths
    campaign = Campaign(params, paths)

    assignment_pools, _ = campaign.ingest_offer_data()
    if "category" not in assignment_pools:
        return 0
    category_pool = assignment_pools["category"]

    category_pool = category_pool.filter(category_pool.prediction.isNotNull())
    cf_eligible = category_pool.select("MBRSHP_SID").distinct().count()

    num_members = campaign.ingest_member_data().count()
    cf_ineligible = num_members - cf_eligible
    return cf_ineligible


# --- Tests Object ---- #


class QCTestRunner:
    """Runner for QC test functions

    Stores and runs tests provided
    by name at initialization.
    """

    def __init__(self, job, teststorun):
        """Initialize SessionManager.

        Parameters:
            job (JobManager): job to run tests on
            teststorun (list[str]): list of tests to run

        Returns:
            None
        """
        self.tests = {
            "count_cf_ineligible": {
                "text": "CF Ineligible Members (out of ~5.4m full BBM pop): ",
                "fn": count_cf_ineligible,
                "input_data": "job",
            },
            "basket_cell": {
                "text": "Basket Cells Have Lower Sales: ",
                "fn": basket_cells,
                "input_data": "compare_cell_id",
            },
            "trial_cell": {
                "text": "Trial Cells Have Lower Sales: ",
                "fn": trial_cells,
                "input_data": "compare_cell_id",
            },
            "control_test": {
                "text": "Control/Test Pairs are Comparable: ",
                "fn": control_test,
                "input_data": "compare_cell_id",
            },
            "duplicate_members": {
                "text": "Members are in One Cell Only: ",
                "fn": duplicate_membs,
                "input_data": "base",
            },
            "duplicate_coupons": {
                "text": "Members are only Assigned Coupons Once: ",
                "fn": duplicate_coups,
                "input_data": "assignment",
            },
            "qualified_coupons": {
                "text": "Members Qualify for All Coupons: ",
                "fn": coups_qualified,
                "input_data": "member",
            },
            "always_fail": {
                "text": "This Test Always Fails: ",
                "fn": always_fail,
                "input_data": "compare_cell_id",
            },
            "downsampled_coupons": {
                "text": "Downsampled coupons: ",
                "fn": self.downsampled_coupons,
                "input_data": "base",
            },
            "same_num_coups": {
                "text": "Members assigned same number of coupons: ",
                "fn": same_num_coups,
                "input_data": "assignment",
            },
            "avg_spend_decr": {
                "text": "Average spend decreases by decile: ",
                "fn": self.avg_spend_decr,
                "input_data": "base",
            },
            "check_cell_size": {
                "text": "actual cell size within tolerance of estimated: ",
                "fn": check_cell_size,
                "input_data": "cell",
            },
            "check_sensitive_content": {
                "text": "Sensitive content assignment: ",
                "fn": check_sensitive_content,
                "input_data": "job",
            },
        }
        self.torun = teststorun
        self.job = job
        self.log = get_logger("qc_tests")

    def run_test(self, testname):
        """Run test from global test list.

        Parameters:
            testname (str): name of test to run

        Returns:
            testdata (pd.DataFrame): DataFrame of test outcome with --
                test (str): descriptive text of test
                outcome (str): identifier of pass/fail for test
        """
        test_params = self.tests[testname]
        input_param = test_params["input_data"]

        if isinstance(input_param, str):
            if input_param == "job":
                input_data = self.job
            else:
                input_data = self.job.data.tables[input_param]
            outcome = test_params.get("fn")(input_data)
        elif isinstance(input_param, list):
            input_list = []
            for param in input_param:
                if param == "job":
                    input_list.append(self.job)
                else:
                    input_list.append(self.job.data.tables[param])
            outcome = test_params.get("fn")(*input_list)

        text = test_params["text"]
        testdata = pd.DataFrame([{"test": text, "outcome": outcome}])
        return testdata

    def run_tests(self):
        """Run a set of tests from global test list.

        Parameters:
            None!

        Returns
            testdata (pd.DataFrame): DataFrame of test outcome with --
                test (str): descriptive text of test
                outcome (str): identifier of pass/fail for test
        """
        test_table = pd.DataFrame(columns=["test", "outcome"])
        for i, test in enumerate(self.torun):
            test_identifier = "  " + str(i + 1) + ": " + test
            self.log.info(test_identifier)
            testdata = self.run_test(test)
            test_table = pd.concat([testdata, test_table], axis=0)
        test_table = test_table[["test", "outcome"]]
        return test_table

    def read_config(self):
        """
        Helper for downsampled_coupons test. Loads and parses config,
        similar to ConfigManager in assn_io.py
        Parameters:
            None
        Returns:
            config (dictionary) : The parameters of a config file
        """

        config = dict(
            list(self.job.config.params.items())
            + list(self.job.config.cnf["assignment"].items())
        )

        return config

    def downsampled_coupons(self, base):
        """Check if there are any downsampled coupons and if so indicate the
         coupon number(s) in the report
        as a single line.
        Parameters:
            None
        Returns:
            output (string) : Either the list of downsampled coupon numbers
            or "No downsampled coupons"
        """
        output = ""
        config = self.read_config()
        downsampled_coupon_numbers = config["downsample_coupons"]

        if (
            downsampled_coupon_numbers is not None
            and len(downsampled_coupon_numbers) > 0
        ):
            ds_coupons = []
            for cpn in downsampled_coupon_numbers:
                ds_coupons.append(str(cpn["cpn_nbr"]))
            output = ", ".join(ds_coupons)
        else:
            output = "No Downsampled Coupons"

        return output

    def avg_spend_decr(self, base):
        """Check that avg_spend_4, avg_spend_12, and avg_spend_52 are
        decreasing by decile.

        Parameters: base (None): placeholder, not used

        Returns: 'Pass'/'Fail' (str): pass/fail indicator
        """
        subset_mail_list = qc.get_basedata_with_new_150(self.job)

        subset_mail_list_by_decile = (
            subset_mail_list.where(sqlf.col("decile") != "new_member")
            .groupBy("decile")
            .agg(
                sqlf.mean("LFOURW_SPEND_IN_STORE").alias("AVG_SPEND_4"),
                sqlf.mean("LTWELVEW_SPEND_IN_STORE").alias("AVG_SPEND_12"),
                sqlf.mean("LFIFTY-TWOW_SPEND_IN_STORE").alias("AVG_SPEND_52"),
            )
        )

        subset_by_decile = subset_mail_list_by_decile.toPandas()
        subset_by_decile["decile"] = subset_by_decile["decile"].astype("int64")
        subset_by_decile = subset_by_decile.sort_values(by=["decile"])
        ordered_avgs = subset_by_decile.drop(columns=["decile"])
        spend_diff = ordered_avgs.diff()
        if spend_diff[spend_diff > 0].count().sum() != 0:
            return "Fail"

        return "Pass"
