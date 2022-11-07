"""Spark job to generate measurement assignments and afeymants

TODO:
    - [] integration test
"""
import operator
import os

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql.functions import count, countDistinct, lit
from pyspark.sql.functions import max as fmax
from pyspark.sql.functions import mean
from pyspark.sql.functions import min as fmin
from pyspark.sql.functions import regexp_extract, when
from pyspark.sql.window import Window

from pe_member_dna.pipelines.assignment.lib.assn_io import JobManager
from pe_member_dna.pipelines.assignment.lib.assn_utils import (
    CONSTRUCT_COLUMN,
    calc_overlapping_cols,
)
from pe_member_dna.pipelines.lib.iotools import (
    is_s3_path,
    split_path_bucket_key,
)
from pe_member_dna.pipelines.lib.spark_util import truncate_history
from pe_member_dna.pipelines.lib.utils import lowercase_col_names


# ---- Helpers ---- #
def cast_cell(df):
    """cast the column data type for cells tables

    Parameters:
        df(pyspark.sql.DataFrame): The spark dataframe need to be cast
            requires: cell_id, experiment_id, is_primary

    Returns:
        df(pyspark.sql.DataFrame): The spark dataframe after cast
    """
    df = df.withColumn("cell_id", df.cell_id.cast("integer"))
    df = df.withColumn("experiment_id", df.experiment_id.cast("integer"))
    df = df.withColumn("is_primary", df.is_primary.cast("integer"))
    return df


def transform_data(job):
    collect = job.data.tables
    collect = {
        tbl_name: lowercase_col_names(collect[tbl_name])
        for tbl_name in collect
    }

    # dna
    dna = collect["dna"]
    max_wk = dna.select(fmax("fiscal_week_end")).collect()[0][0]
    dna = dna[dna.fiscal_week_end == max_wk]
    dna = dna.dropDuplicates(subset=["mbrshp_sid"])
    job.data.add("dna", dna)

    # reset cell and msmt cell
    msmt_cell = collect["msmt_cell"]
    msmt_cell = cast_cell(msmt_cell)
    experiment = job.config.params["experiment"]
    if job.config.params["run_type"].lower() == "adhoc":
        msmt_cell = msmt_cell.filter(msmt_cell.experiment_id == experiment)
    else:
        msmt_cell = msmt_cell.filter(msmt_cell.experiment_id != experiment)
    # extract primary cells from the experiment
    cell = collect["cell"]
    cell = cast_cell(cell)
    cell = cell.withColumn("cell_id", cell.cell_id.cast("integer"))
    cell = cell.filter(
        (cell.experiment_id == experiment) & (cell.is_primary == 1)
    )
    # append priamry cells into msmt cells
    if job.config.params["run_type"].lower() != "adhoc":
        msmt_cell = msmt_cell.union(cell.select(msmt_cell.columns))
    job.data.add("msmt_cell", msmt_cell)
    # sava cell and contruct map for later use in afeynment table generation
    cell = cell.select("cell_id", "construct_id")
    job.data.add("cell", cell)

    # mail_circ
    mail_circ = collect["mail_circ"]
    if "bbm" in job.config.params["campaign"].lower():
        mail_circ = mail_circ.withColumn("mail_flag", lit(1))
        if job.config.paths.get("VERSION_MAP") is not None:
            version_map = collect["version_map"]
            version_map = version_map.withColumnRenamed("version", "cpn_nbr")
            mail_circ = mail_circ.join(version_map, "cpn_nbr", "left")
            mail_circ = mail_circ.withColumn(
                "cpn_nbr",
                when(mail_circ.cpn1.isNotNull(), mail_circ.cpn1).otherwise(
                    mail_circ.cpn_nbr
                ),
            )
    cols_name = [
        "mbrshp_sid",
        "experiment_id",
        "cell_id",
        "slot_nbr",
        "cpn_nbr",
        "mail_flag",
    ]
    mail_circ = mail_circ.select(cols_name)
    job.data.add("mail_circ", mail_circ)

    # constructs
    constructs = collect["constructs"]
    if (
        "bbm" in job.config.params["campaign"].lower()
        and job.config.paths.get("VERSION_MAP") is not None
    ):
        constructs = constructs.join(version_map, "cpn_nbr", "left")
        constructs = constructs.withColumn(
            "cpn_nbr",
            when(constructs.cpn1.isNotNull(), constructs.cpn1).otherwise(
                constructs.cpn_nbr
            ),
        )

    constructs = constructs.withColumn(
        "construct_id",
        regexp_extract(constructs.construct, CONSTRUCT_COLUMN, 1),
    )
    constructs = constructs.withColumn(
        "slot_nbr", regexp_extract(constructs.construct, CONSTRUCT_COLUMN, 2)
    )
    constructs = constructs.withColumnRenamed("cpn_nbr", "feynman_cpn_nbr")
    job.data.add("constructs", constructs)

    # mbr_lkup
    mbr_lkup = collect["mbr_lkup"]
    mbr_lkup = mbr_lkup.dropDuplicates(subset=["mbrshp_sid"])
    job.data.add("mbr_lkup", mbr_lkup)

    # score
    score = collect["score"]
    score = score.dropDuplicates(subset=["mbrshp_nbr"])
    job.data.add("score", score)


def generate_base_assgn(job):
    mail_circ = job.data.tables["mail_circ"]
    mbr_lkup = job.data.tables["mbr_lkup"]
    dna = job.data.tables["dna"]
    score = job.data.tables["score"]

    if job.config.params["run_type"].lower() in ("prod", "test"):
        cells = (
            mail_circ.select("cell_id")
            .distinct()
            .toPandas()["cell_id"]
            .tolist()
        )
        check_used_cell(job, cells, "assgn")

    force_in_cells = job.config.params["force_in_cells"]
    force_out_cells = job.config.params["force_out_cells"]

    if force_in_cells is None:
        force_in_cells = []
    if force_out_cells is None:
        force_out_cells = []

    join_col = calc_overlapping_cols(mail_circ, mbr_lkup)
    base_assgn = mail_circ.join(mbr_lkup, join_col, "left")
    base_assgn = base_assgn.join(dna, "mbrshp_sid", "left")
    base_assgn = base_assgn.join(score, "mbrshp_nbr", "left")

    base_nomail = base_assgn.filter(
        (base_assgn.mail_flag == 0)
        & (~base_assgn.cell_id.isin(force_out_cells))
        & (~base_assgn.cell_id.isin(force_in_cells))
    )
    base_mail = base_assgn.filter(
        (base_assgn.mail_flag == 1)
        & (~base_assgn.cell_id.isin(force_out_cells))
        & (~base_assgn.cell_id.isin(force_in_cells))
    )
    force_nomail = base_assgn.filter(base_assgn.cell_id.isin(force_out_cells))
    force_mail = base_assgn.filter(base_assgn.cell_id.isin(force_in_cells))

    job.data.add("base_nomail", base_nomail)
    job.data.add("base_mail", base_mail)
    job.data.add("force_nomail", force_nomail)
    job.data.add("force_mail", force_mail)


def generate_base_afeyn(job):
    constructs = job.data.tables["constructs"]
    cell = job.data.tables["cell"]
    base_mail = job.data.tables["base_mail"]
    force_mail = job.data.tables["force_mail"]
    force_nomail = job.data.tables["force_nomail"]

    cell = cell.withColumnRenamed("cell_id", "feynman_cell_id")
    if job.config.params["run_type"].lower() in ("prod", "test"):
        cells = (
            constructs.select("cell_id")
            .distinct()
            .toPandas()["cell_id"]
            .tolist()
        )
        check_used_cell(job, cells, "afeyn")

    assign = base_mail.union(force_mail).union(force_nomail)
    assign_mbrs = assign.select("mbrshp_sid", "decile", "TENURE")
    assign_mbrs = assign_mbrs.dropDuplicates(subset=["mbrshp_sid"])

    base_afeyn = constructs.join(cell, "construct_id", "inner")
    base_afeyn = base_afeyn.join(assign_mbrs, "mbrshp_sid", "inner")

    job.data.add("base_afeyn", base_afeyn)


def check_used_cell(job, cells, group):
    """
    Check if the cells already have assignment or afeynman outputs. Raises
    an error if the outputs already exist.

    Parameters:
        job (JobManager): Job used during assignment with relevant paths
        cells (list): List of cells to check
        group (str): assgn or afeyn, to determine what location to check

    Returns:
        None
    """
    path_head = job.config.paths[f"MSMT_{group.upper()}"]
    path_tail = "cell_id={}.0"
    if group == "feynman":
        path_tail = "feynman_" + path_tail
    for cell in cells:
        path_to_check = os.path.join(path_head, path_tail.format(int(cell)))
        bucket, key = split_path_bucket_key(path_to_check)
        if is_s3_path(bucket, key):
            raise ValueError("cell id is being used in assigments already!")


def create_cell_file(job, vals):
    """append cells records for new segmented cells to msmt_cells.csv
       This will minimize manual edits on msmt_cells.csv

    Parameters:
        job(JobManager): Job to operate on required tables and parameters:
        vals(list): A list of [new cell id, description of cell id]

    Returns:
        msmt_cell: the complete tables that include new segmented cells
    """
    msmt_cell = job.data.tables["msmt_cell"]
    cell = msmt_cell.filter(
        msmt_cell.experiment_id == job.config.params["experiment"]
    )
    inhome_date = cell.groupby().agg(fmin("inhome_date")).collect()[0][0]
    cell_start = cell.groupby().agg(fmin("cell_start")).collect()[0][0]
    cell_end = cell.groupby().agg(fmax("cell_end")).collect()[0][0]

    rdd = job.sc.parallelize(vals)
    df = rdd.toDF(["cell_id", "cell_name"])
    df = (
        df.withColumn("experiment_id", lit(job.config.params["experiment"]))
        .withColumn("cell_desc", df.cell_name)
        .withColumn("is_primary", lit(0))
        .withColumn("inhome_date", lit(inhome_date))
        .withColumn("cell_start", lit(cell_start))
        .withColumn("cell_end", lit(cell_end))
    )
    for col_name in msmt_cell.columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    msmt_cell = msmt_cell.union(df.select(msmt_cell.columns))
    return msmt_cell


def generate_cell_id(
    df, ini_cell_id, force_in_new, sample_cells, circ_or_holdout
):
    """create segmented cells for measurement with assigning a globally unique cell id
    In assignment subsampling step, we subsampled by tenure and decile with a collated holdout group
    To measure the circ. vs. holdout by these segments, we need to create a new cell id for each and
    then be picked by measurment.

    Parameters:
        df(pyspark.sql.DataFrame): The spark dataframe that has assignment mbr data to be segmented
        ini_cell_id(int): the minimum cell id in msmt_cells.csv
        force_in_new(boolean): input parameters from subset
        sample_cells(array<int>): input parameters from subset
        circ_or_holdout(str): flag of 'circ' vs. 'holdout'

    Returns:
        df (pyspark.sql.DataFrame): assignment mbr data with segmented cell id
        vals (list): a list of ([new cell id, description of new cell id])
    """
    i = 0
    df = df.withColumn("seg_cell_id", lit(None))
    vals = []
    if sample_cells:
        for cell in sample_cells:
            i += 1
            df = df.withColumn(
                "seg_cell_id",
                when(
                    (df.cell_id == cell) & (df.seg_cell_id.isNull()),
                    ini_cell_id - i,
                ).otherwise(df.seg_cell_id),
            )
            vals.append(
                [
                    ini_cell_id - i,
                    circ_or_holdout + "_sample_cell:" + str(cell),
                ]
            )
            df = truncate_history(df, cache=True)

    if force_in_new:
        i += 1
        df = df.withColumn(
            "seg_cell_id",
            when(
                (df.tenure < 150) & (df.seg_cell_id.isNull()), ini_cell_id - i
            ).otherwise(df.seg_cell_id),
        )
        vals.append([ini_cell_id - i, circ_or_holdout + "_new"])
        df = truncate_history(df, cache=True)

    for d in range(1, 11):
        i += 1
        df = df.withColumn(
            "seg_cell_id",
            when(
                (df.decile == d) & (df.seg_cell_id.isNull()), ini_cell_id - i
            ).otherwise(df.seg_cell_id),
        )
        vals.append([ini_cell_id - i, circ_or_holdout + "_decile:" + str(d)])
        df = truncate_history(df, cache=True)

    df = df.withColumn("cell_id", df.seg_cell_id)
    df = df.filter(df.cell_id.isNotNull())
    return df, vals


def generate_segmented_cell(job):
    msmt_cell = job.data.tables["msmt_cell"]
    if job.config.params["run_type"].lower() == "adhoc":
        min_cell_id = (
            msmt_cell.filter(msmt_cell.cell_id < 0)
            .agg(fmax(msmt_cell.cell_id))
            .collect()[0][0]
            + 1
        )
    else:
        min_cell_id = msmt_cell.agg(fmin(msmt_cell.cell_id)).collect()[0][0]

    force_in_new = job.config.params["force_in_new"]
    sample_cells = (
        job.config.params["sample_cells"]
        if "sample_cells" in job.config.params
        else None
    )

    # calculate cell id for mail by seg
    base_mail = job.data.tables["base_mail"]
    base_mail_seg, mail_cells_seg = generate_cell_id(
        base_mail, min_cell_id, force_in_new, sample_cells, "circ"
    )
    min_cell_id = base_mail_seg.agg(fmin(base_mail_seg.cell_id)).collect()[0][
        0
    ]

    # calculate cell id for no mail hold out by seg
    base_nomail = job.data.tables["base_nomail"]
    print(
        base_nomail.groupby("cell_id").agg(countDistinct("mbrshp_sid")).show()
    )
    base_nomail_seg, nomail_cells_seg = generate_cell_id(
        base_nomail, min_cell_id, force_in_new, sample_cells, "holdout"
    )
    print(
        base_nomail_seg.groupby("cell_id")
        .agg(countDistinct("mbrshp_sid"))
        .show()
    )

    base_segmented = base_mail_seg.union(
        base_nomail_seg.select(base_mail_seg.columns)
    )
    if job.config.params["run_type"].lower() in ("prod", "test"):
        decile_cells = [i[0] for i in mail_cells_seg + nomail_cells_seg]
        check_used_cell(job, decile_cells, "assgn")
    job.data.add("base_segmented", base_segmented)

    msmt_cell = create_cell_file(job, mail_cells_seg + nomail_cells_seg)
    job.data.add("msmt_cell", msmt_cell)


def cast_assignments(df):
    df = df.withColumn("mbrshp_sid", df.mbrshp_sid.cast("integer"))
    df = df.withColumn("experiment_id", df.experiment_id.cast("integer"))
    df = df.withColumn("slot_nbr", df.slot_nbr.cast("string"))
    df = df.withColumn("cpn_nbr", df.cpn_nbr.cast("string"))
    df = df.withColumn("cell_id", df.cell_id.cast("double"))
    df = df.select(
        "mbrshp_sid", "experiment_id", "slot_nbr", "cpn_nbr", "cell_id"
    )
    df = df.repartition("cell_id")
    return df


def cast_afeynmants(df):
    df = df.withColumn("mbrshp_sid", df.mbrshp_sid.cast("integer"))
    df = df.withColumn("experiment_id", df.experiment_id.cast("integer"))
    df = df.withColumn("slot_nbr", df.slot_nbr.cast("string"))
    df = df.withColumn("feynman_cpn_nbr", df.feynman_cpn_nbr.cast("string"))
    df = df.withColumn("cell_id", df.cell_id.cast("double"))
    df = df.withColumn("feynman_cell_id", df.feynman_cell_id.cast("double"))
    df = df.select(
        "mbrshp_sid",
        "experiment_id",
        "cell_id",
        "slot_nbr",
        "feynman_cpn_nbr",
        "feynman_cell_id",
    )
    df = df.repartition("feynman_cell_id")
    return df


# ---- MAIN ---- #


def main(conf_path_in=None):

    job = JobManager("subset", "generate msmt input", conf_path_in)

    # 1. Read in raw tables
    print("reading data...")
    job.data.read("cell", "CELL", filetype="csv", schema=None)
    job.data.read("msmt_cell", "MSMT_CELL", filetype="csv", schema=None)
    job.data.read(
        "mail_circ", "MAIL_POPULATION_ASSIGNMENT", filetype="csv", schema=None
    )
    job.data.read(
        "constructs",
        "INPUT_CONSTRUCTS",
        filetype=job.config.params["ftype"],
        schema=None,
        cols=[
            "MBRSHP_SID",
            "EXPERIMENT_ID",
            "CELL_ID",
            "CPN_NBR",
            "CONSTRUCT",
        ],
    )
    job.data.read(
        "mbr_lkup",
        "RAW_MEMBER",
        filetype="parquet",
        schema=None,
        cols=["MBRSHP_SID", "MBRSHP_NBR"],
    )
    job.data.read(
        "score",
        "MAIL_LIST",
        filetype="csv",
        schema=None,
        cols=["MBRSHP_NBR", "decile"],
    )
    job.data.read(
        "dna",
        "CUBE",
        filetype="parquet",
        schema=None,
        cols=["MBRSHP_SID", "FISCAL_WEEK_END", "TENURE"],
    )
    if (
        "bbm" in job.config.params["campaign"].lower()
        and job.config.paths.get("VERSION_MAP") is not None
    ):
        job.data.read(
            "version_map",
            "VERSION_MAP",
            filetype="csv",
            schema=None,
            cols=["CPN1", "VERSION"],
        )

    # 2. subset and clean data
    print("transforming data...")
    transform_data(job)  # requires base, coups, quals, preds, and memtrips

    # 3. create base table: for mail assignment, no mail assignment, force mail
    # force no mail, afeynmants and segmented assignments
    print("creating base tables...")
    generate_base_assgn(job)
    generate_base_afeyn(job)
    if "mmpc" in job.config.params["campaign"].lower():
        generate_segmented_cell(job)

    # 4. cast dataframe to targeted schema
    print("casting data...")
    base_mail = job.data.tables["base_mail"]
    base_mail = cast_assignments(base_mail)

    base_afeyn = job.data.tables["base_afeyn"]
    base_afeyn = cast_afeynmants(base_afeyn)

    force_mail = job.data.tables["force_mail"]
    force_mail = cast_assignments(force_mail)

    force_nomail = job.data.tables["force_nomail"]
    force_nomail = cast_assignments(force_nomail)

    if "mmpc" in job.config.params["campaign"].lower():
        base_segmented = job.data.tables["base_segmented"]
        base_segmented = cast_assignments(base_segmented)

    # 5. write output
    print("writing output...")
    if job.config.params["run_type"].lower() in ("prod", "test"):
        write_mode = "append"
    else:
        write_mode = "overwrite"

    base_afeyn.write.partitionBy("feynman_cell_id").mode(write_mode).parquet(
        job.config.paths["MSMT_AFEYN"]
    )

    assgn_output = base_mail.union(force_mail).union(force_nomail)
    if "mmpc" in job.config.params["campaign"].lower():
        assgn_output.union(base_segmented)
    assgn_output.write.partitionBy("cell_id").mode(write_mode).parquet(
        job.config.paths["MSMT_ASSGN"]
    )

    if job.config.params["run_type"].lower() in ["prod", "dev", "test"]:
        msmt_cell = job.data.tables["msmt_cell"].toPandas()
        job.data.add("msmt_cell", msmt_cell)
        job.data.write(
            "msmt_cell",
            "MSMT_CELL",
            mode="overwrite",
            singlefile=False,
            writetype="s3",
        )
        job.data.write(
            "msmt_cell",
            "MSMT_CELL_ARCHIVE",
            mode="overwrite",
            singlefile=False,
            writetype="s3",
        )
    print("done.")


if __name__ == "__main__":

    main()
