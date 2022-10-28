import argparse
import logging
import os

import pe_memberdna.lib.misc as misc
import pe_memberdna.etl.utils.validations_ETL as validations
from pe_memberdna.lib.job_manager import JobManager


def load_and_register(job, path, register_name, filetype="parquet"):
    """
    Load a file, display a log message and register
    the resulting table under the supplied name.

    Args:
        job.spark: SPARK object
        path: path to the file to be loaded
        register_name: name to register the table under
        filetype: extension ('csv' or 'parquet') of the file(s) under path
    Returns:
        (pyspark df) table read from the file
    """
    logging.info("Reading the input file " + path)

    if filetype == "parquet":
        df_temp = job.spark.read.parquet(path)
    elif filetype == "csv":
        df_temp = job.spark.read.csv(path, header=True)
    else:
        raise Exception(
            "Unknown filetype '"
            + str(filetype)
            + "'. Expecting 'csv' or 'parquet'. "
        )

    df_temp.registerTempTable(register_name)

    return df_temp


def log_cache_save(df, path):
    """
    Repartition, cache, and save the df into a csv at path, and write to log.

    Args:
        df: pyspark df
        path: path where the file is meant to be saved
    Generates:
        File at path
        line in log
    """
    logging.info("Saving dataframe to " + path)
    df = df.repartition(1)
    df.write.option("emptyValue", None).option("nullValue", None).csv(
        path, mode="overwrite", header=True
    )
    df.cache()
    return df


def control_tab_01(job, data_paths, const_setup):
    """
    Calculate aggregates by date from detail table and create a
    comparison table that shows detail and redshift aggregates
    side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_01_redshift: See the unittests for sample of this table
    Returns:
        df_tab_01_comparison: A table that shows the aggregates based on
        pipelined_intermediates and those based on redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_01")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_01_redshift"],
        "df_tab_01_redshift",
        filetype="csv",
    )

    df_tab_01_engine = job.spark.sql(
        """
        SELECT
            a.purch_dt AS purch_dt,
            SUM(extended_prc_amt) AS tot_sales,
            COUNT(DISTINCT a.purch_hdr_id) AS tot_trips,
            COUNT(PURCH_DTL_ID) AS tot_dtls,
            SUM(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    extended_prc_amt
                ELSE
                    0
                END
                ) AS tot_non_gas_sales,
            COUNT(DISTINCT
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    a.purch_hdr_id
                ELSE
                    NULL
                END
                ) AS tot_non_gas_trips,
            COUNT(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    PURCH_DTL_ID
                ELSE
                    NULL
                END
                ) AS tot_non_gas_dtls,
            SUM(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    extended_prc_amt
                ELSE
                    0
                END
                ) AS tot_gas_sales,
            COUNT(DISTINCT
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    a.purch_hdr_id
                ELSE
                    NULL
                END
                ) AS tot_gas_trips,
            COUNT(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    PURCH_DTL_ID
                ELSE
                    NULL
                END
                ) AS tot_gas_dtls
        FROM
            df_temp_detail a
        LEFT JOIN
            df_temp_header b
        ON
            (a.purch_hdr_id = b.purch_hdr_id)
        WHERE
            a.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
            AND b.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
        group by
            1
        """
        % const_setup
    )
    df_tab_01_engine.registerTempTable("df_tab_01_engine")

    df_tab_01_comparison = job.spark.sql(
        """
        SELECT
            e.purch_dt AS engine_purch_dt,

            e.tot_sales AS engine_tot_sales,
            e.tot_trips AS engine_tot_trips,
            e.tot_dtls AS engine_tot_dtls,
            e.tot_non_gas_sales AS engine_tot_non_gas_sales,
            e.tot_non_gas_trips AS engine_tot_non_gas_trips,
            e.tot_non_gas_dtls AS engine_tot_non_gas_dtls,
            e.tot_gas_sales AS engine_tot_gas_sales,
            e.tot_gas_trips AS engine_tot_gas_trips,
            e.tot_gas_dtls AS engine_tot_gas_dtls,

            r.purch_dt AS redshift_purch_dt,

            r.tot_sales AS redshift_tot_sales,
            r.tot_trips AS redshift_tot_trips,
            r.tot_dtls AS redshift_tot_dtls,
            r.tot_non_gas_sales AS redshift_tot_non_gas_sales,
            r.tot_non_gas_trips AS redshift_tot_non_gas_trips,
            r.tot_non_gas_dtls AS redshift_tot_non_gas_dtls,
            r.tot_gas_sales AS redshift_tot_gas_sales,
            r.tot_gas_trips AS redshift_tot_gas_trips,
            r.tot_gas_dtls AS redshift_tot_gas_dtls
        FROM
            df_tab_01_engine AS e
        FULL OUTER JOIN
            df_tab_01_redshift AS r
        ON
            e.purch_dt = r.purch_dt
        WHERE
            dayofmonth(e.purch_dt)>1
            and dayofmonth(r.purch_dt)>1
        ORDER BY
            e.purch_dt
        """
    )

    df_tab_01_comparison = log_cache_save(
        df_tab_01_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_01_comparison.csv",
    )

    return df_tab_01_comparison


def control_tab_02(job, data_paths, const_setup):
    """
    Calculate aggregates by date and site/club number from detail
    table and create a comparison table that shows detail
    and redshift aggregates side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_02_redshift: See the unittests for sample of this table
    Returns:
        df_tab_02_comparison: A table that shows the aggregates based on
        pipelined_intermediates and those based on redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_02")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_02_redshift"],
        "df_tab_02_redshift",
        filetype="csv",
    )

    df_tab_02_engine = job.spark.sql(
        """
        SELECT
            a.purch_dt,
            b.site_nbr,
            SUM(extended_prc_amt) AS tot_sales,
            COUNT(DISTINCT a.purch_hdr_id) AS tot_trips,
            COUNT(PURCH_DTL_ID) AS tot_dtls,
            SUM(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    extended_prc_amt
                ELSE
                    0
                END
                ) AS tot_non_gas_sales,
            COUNT(DISTINCT
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    a.purch_hdr_id
                ELSE
                    NULL
                END
                ) AS tot_non_gas_trips,
            COUNT(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    PURCH_DTL_ID
                ELSE
                    NULL
                END
                ) AS tot_non_gas_dtls,
            SUM(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    extended_prc_amt
                ELSE
                    0
                END
                ) AS tot_gas_sales,
            COUNT(DISTINCT
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    a.purch_hdr_id
                ELSE
                    NULL
                END
                ) AS tot_gas_trips,
            COUNT(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    PURCH_DTL_ID
                ELSE
                    NULL
                END
                ) AS tot_gas_dtls
        FROM
            df_temp_detail AS a
        LEFT JOIN
            df_temp_header AS b
        ON
            (a.purch_hdr_id = b.purch_hdr_id)
        WHERE
            a.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
            AND b.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
        group by
            1,2
        """
        % const_setup
    )

    df_tab_02_engine.registerTempTable("df_tab_02_engine")

    df_tab_02_comparison = job.spark.sql(
        """
        SELECT
            e.purch_dt AS engine_purch_dt,
            e.site_nbr AS engine_site_nbr,

            e.tot_trips AS engine_tot_trips,
            e.tot_dtls AS engine_tot_dtls,
            e.tot_non_gas_sales AS engine_tot_non_gas_sales,
            e.tot_non_gas_trips AS engine_tot_non_gas_trips,
            e.tot_non_gas_dtls AS engine_tot_non_gas_dtls,
            e.tot_gas_sales AS engine_tot_gas_sales,
            e.tot_gas_trips AS engine_tot_gas_trips,
            e.tot_gas_dtls AS engine_tot_gas_dtls,

            r.purch_dt AS redshift_purch_dt,
            r.site_nbr AS redshift_site_nbr,

            r.tot_trips AS redshift_tot_trips,
            r.tot_dtls AS redshift_tot_dtls,
            r.tot_non_gas_sales AS redshift_tot_non_gas_sales,
            r.tot_non_gas_trips AS redshift_tot_non_gas_trips,
            r.tot_non_gas_dtls AS redshift_tot_non_gas_dtls,
            r.tot_gas_sales AS redshift_tot_gas_sales,
            r.tot_gas_trips AS redshift_tot_gas_trips,
            r.tot_gas_dtls AS redshift_tot_gas_dtls
        FROM
            df_tab_02_engine AS e
        FULL OUTER JOIN
            df_tab_02_redshift AS r
        ON
            e.site_nbr = r.site_nbr
            AND e.purch_dt = r.purch_dt
        WHERE
            dayofmonth(e.purch_dt)>1
            and dayofmonth(r.purch_dt)>1
        ORDER BY
            e.purch_dt,
            e.site_nbr
        """
    )

    df_tab_02_comparison = log_cache_save(
        df_tab_02_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_02_comparison.csv",
    )

    return df_tab_02_comparison


def control_tab_03(job, data_paths, const_setup):
    """
    Calculate aggregates grouped by member from detail table
    and create a comparison table that shows detail and redshift
    aggregates side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_03_redshift: See the unittests for sample of this table
    Returns:
        df_tab_03_comparison: A table that shows the aggregates based on
        pipelined_intermediates and those based on redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_03")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_03_redshift"],
        "df_tab_03_redshift",
        filetype="csv",
    )

    df_tab_03_01 = job.spark.sql(
        """
        SELECT
            a.mbrshp_sid,
            SUM(b.extended_prc_amt) AS tot_sales,
            COUNT(DISTINCT b.purch_hdr_id) AS tot_trips,
            COUNT(b.purch_hdr_id) AS tot_dtls
        FROM
            df_temp_member AS a
        LEFT JOIN
            df_temp_detail b
        ON
            a.mbrshp_sid = b.mbrshp_sid
        LEFT JOIN
            df_temp_header h
        ON
            b.purch_hdr_id = h.purch_hdr_id
        WHERE
            b.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
            and mbrshp_exp_dt >= '%(end_date)s'
            and h.sales_channel_id IN (%(sales_channel_id_nongas)s)
            and b.SALES_CTGRY_CD IN (%(allowed_sales_ctgry_cd)s)
        GROUP BY
            a.mbrshp_sid
        """
        % const_setup
    )
    df_tab_03_01.registerTempTable("df_tab_03_01")

    df_tab_03_engine = job.spark.sql(
        """
        SELECT
            d.mbrshp_sid,
            d.tot_sales,
            d.tot_trips,
            d.tot_dtls
        FROM
            df_tab_03_01 AS d
        inner join
            df_tab_03_redshift AS r
        on
            r.mbrshp_sid = d.mbrshp_sid
        """
    )

    df_tab_03_engine.registerTempTable("df_tab_03_engine")

    df_tab_03_comparison = job.spark.sql(
        """
        SELECT
            e.mbrshp_sid AS engine_mbrshp_sid,

            e.tot_sales AS engine_tot_sales,
            e.tot_trips AS engine_tot_trips,
            e.tot_dtls AS engine_tot_dtls,

            r.mbrshp_sid AS redshift_mbrshp_sid,

            r.tot_sales AS redshift_tot_sales,
            r.tot_trips AS redshift_tot_trips,
            r.tot_dtls AS redshift_tot_dtls
        FROM
            df_tab_03_engine AS e
        FULL OUTER JOIN
            df_tab_03_redshift AS r
        ON
            e.mbrshp_sid = r.mbrshp_sid
        ORDER BY
            e.mbrshp_sid
        """
    )

    df_tab_03_comparison = log_cache_save(
        df_tab_03_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_03_comparison.csv",
    )

    return df_tab_03_comparison


def control_tab_04(job, data_paths, const_setup):
    """
    Join a sample of rows from detail table in redshift to the
    detail table in pipelined_intermediates and create a comparison
    table that shows detail_fiscal and redshift rows side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_04_redshift: See the unittests for sample of this table
    Returns:
        df_tab_04_comparison: A table that shows the rows from
        pipelined_intermediates and rows from redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_04")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_04_redshift"],
        "df_tab_04_redshift",
        filetype="csv",
    )

    df_tab_04_comparison = job.spark.sql(
        """
        SELECT
            e.purch_hdr_id AS engine_purch_hdr_id,
            e.purch_dtl_id AS engine_purch_dtl_id,

            e.purch_dt AS engine_purch_dt,
            e.gtin_cd AS engine_gtin_cd,
            e.article_nbr AS engine_article_nbr,
            e.mc_cd AS engine_mc_cd,
            e.extended_prc_amt AS engine_extended_prc_amt,
            e.sales_qty AS engine_sales_qty,
            e.extended_unit_prc_amt AS engine_extended_unit_prc_amt,
            e.normal_prc_amt AS engine_normal_prc_amt,
            e.normal_unit_prc_amt AS engine_normal_unit_prc_amt,
            e.reduction_amt AS engine_reduction_amt,
            e.site_nbr AS engine_site_nbr,
            e.sales_ctgry_cd AS engine_sales_ctgry_cd,

            r.purch_hdr_id AS redshift_purch_hdr_id,
            r.purch_dtl_id AS redshift_purch_dtl_id,

            r.purch_dt AS redshift_purch_dt,
            r.gtin_cd AS redshift_gtin_cd,
            r.article_nbr AS redshift_article_nbr,
            r.mc_cd AS redshift_mc_cd,
            r.extended_prc_amt AS redshift_extended_prc_amt,
            r.sales_qty AS redshift_sales_qty,
            r.extended_unit_prc_amt AS redshift_extended_unit_prc_amt,
            r.normal_prc_amt AS redshift_normal_prc_amt,
            r.normal_unit_prc_amt AS redshift_normal_unit_prc_amt,
            r.reduction_amt AS redshift_reduction_amt,
            r.site_nbr AS redshift_site_nbr,
            r.sales_ctgry_cd AS redshift_sales_ctgry_cd
        FROM
            df_tab_04_redshift AS r
        LEFT JOIN
            df_temp_detail AS e
        ON
            e.purch_hdr_id = r.purch_hdr_id
            AND e.purch_dtl_id = r.purch_dtl_id
        WHERE
            dayofmonth(e.purch_dt)>1
            and dayofmonth(r.purch_dt)>1
        ORDER BY
            e.purch_hdr_id,
            e.purch_dtl_id
        """
    )

    df_tab_04_comparison = log_cache_save(
        df_tab_04_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_04_comparison.csv",
    )

    return df_tab_04_comparison


def control_tab_05(job, data_paths, const_setup):
    """
    Calculate aggregates by date from payment_fiscal table and create a
    comparison table that shows payment_fiscal and redshift aggregates
    side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_05_redshift: See the unittests for sample of this table
    Returns:
        df_tab_05_comparison: A table that shows the aggregates based on
        pipelined_intermediates and those based on redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_05")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_05_redshift"],
        "df_tab_05_redshift",
        filetype="csv",
    )

    df_tab_05_engine = job.spark.sql(
        """
        SELECT
            a.purch_dt,
            SUM(SALES_PYMT_AMT) AS tot_sales,
            COUNT(DISTINCT a.purch_hdr_id) AS tot_trips,
            COUNT(a.purch_hdr_id) AS tot_pymts,
            SUM(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    SALES_PYMT_AMT
                END
                ) AS tot_non_gas_sales,
            COUNT(DISTINCT
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    a.purch_hdr_id
                END
                ) AS tot_non_gas_trips,
            COUNT(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    a.purch_hdr_id
                END
                ) AS tot_non_gas_pymts,
            SUM(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    SALES_PYMT_AMT
                END
                ) AS tot_gas_sales,
            COUNT(DISTINCT
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    a.purch_hdr_id
                END
                ) AS tot_gas_trips,
            COUNT(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    a.purch_hdr_id
                END
                ) AS tot_gas_pymts
        FROM
            df_temp_payment AS a
        LEFT JOIN
            df_temp_header AS b
        ON
            a.PURCH_HDR_ID = b.PURCH_HDR_ID
        WHERE
            a.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
            AND b.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
        group by
            1
        """
        % const_setup
    )
    df_tab_05_engine.registerTempTable("df_tab_05_engine")

    df_tab_05_comparison = job.spark.sql(
        """
        SELECT
            e.purch_dt AS engine_purch_dt,

            e.tot_sales AS engine_tot_sales,
            e.tot_trips AS engine_tot_trips,
            e.tot_pymts AS engine_tot_pymts,
            e.tot_non_gas_sales AS engine_tot_non_gas_sales,
            e.tot_non_gas_trips AS engine_tot_non_gas_trips,
            e.tot_non_gas_pymts AS engine_tot_non_gas_pymts,
            e.tot_gas_sales AS engine_tot_gas_sales,
            e.tot_gas_trips AS engine_tot_gas_trips,
            e.tot_gas_pymts AS engine_tot_gas_pymts,

            r.purch_dt AS redshift_purch_dt,

            r.tot_sales AS redshift_tot_sales,
            r.tot_trips AS redshift_tot_trips,
            r.tot_pymts AS redshift_tot_pymts,
            r.tot_non_gas_sales AS redshift_tot_non_gas_sales,
            r.tot_non_gas_trips AS redshift_tot_non_gas_trips,
            r.tot_non_gas_pymts AS redshift_tot_non_gas_pymts,
            r.tot_gas_sales AS redshift_tot_gas_sales,
            r.tot_gas_trips AS redshift_tot_gas_trips,
            r.tot_gas_pymts AS redshift_tot_gas_pymts
        FROM
            df_tab_05_engine AS e
        FULL OUTER JOIN
            df_tab_05_redshift AS r
        ON
            e.purch_dt = r.purch_dt
        WHERE
            dayofmonth(e.purch_dt)>1
            and dayofmonth(r.purch_dt)>1
        ORDER BY
            e.purch_dt
        """
    )

    df_tab_05_comparison = log_cache_save(
        df_tab_05_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_05_comparison.csv",
    )

    return df_tab_05_comparison


def control_tab_06(job, data_paths, const_setup):
    """
    Calculate aggregates by date and site/club number from payment_fiscal
    table and create a comparison table that shows payment_fiscal
    and redshift aggregates side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_06_redshift: See the unittests for sample of this table
    Returns:
        df_tab_06_comparison: A table that shows the aggregates based on
        pipelined_intermediates and those based on redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_06")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_06_redshift"],
        "df_tab_06_redshift",
        filetype="csv",
    )

    df_tab_06_engine = job.spark.sql(
        """
        SELECT
            b.site_nbr,
            b.purch_dt,
            SUM(a.SALES_PYMT_AMT) AS tot_sales,
            COUNT(DISTINCT b.purch_hdr_id) AS tot_trips,
            COUNT(b.purch_hdr_id) AS tot_pymts,
            SUM(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    a.SALES_PYMT_AMT
                END
                ) AS tot_non_gas_sales,
            COUNT(DISTINCT
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    b.purch_hdr_id
                END
                ) AS tot_non_gas_trips,
            COUNT(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_nongas)s) THEN
                    b.purch_hdr_id
                END
                ) AS tot_non_gas_pymts,
            SUM(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                     a.SALES_PYMT_AMT
                END
                ) AS tot_gas_sales,
            COUNT(DISTINCT
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    b.purch_hdr_id
                END
                ) AS tot_gas_trips,
            COUNT(
                CASE WHEN b.sales_channel_id IN (%(sales_channel_id_gas)s) THEN
                    b.purch_hdr_id
                END
                ) AS tot_gas_pymts
        FROM
            df_temp_payment AS a
        LEFT JOIN
            df_temp_header AS b
        ON
            a.PURCH_HDR_ID = b.PURCH_HDR_ID
        WHERE
            a.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
            AND b.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
        group by
            1,2
        """
        % const_setup
    )

    df_tab_06_engine.registerTempTable("df_tab_06_engine")

    df_tab_06_comparison = job.spark.sql(
        """
        SELECT
            e.site_nbr AS engine_site_nbr,
            e.purch_dt AS engine_purch_dt,

            e.tot_trips AS engine_tot_trips,
            e.tot_pymts AS engine_tot_pymts,
            e.tot_non_gas_sales AS engine_tot_non_gas_sales,
            e.tot_non_gas_trips AS engine_tot_non_gas_trips,
            e.tot_non_gas_pymts AS engine_tot_non_gas_pymts,
            e.tot_gas_sales AS engine_tot_gas_sales,
            e.tot_gas_trips AS engine_tot_gas_trips,
            e.tot_gas_pymts AS engine_tot_gas_pymts,

            r.site_nbr AS redshift_site_nbr,
            r.purch_dt AS redshift_purch_dt,

            r.tot_trips AS redshift_tot_trips,
            r.tot_pymts AS redshift_tot_pymts,
            r.tot_non_gas_sales AS redshift_tot_non_gas_sales,
            r.tot_non_gas_trips AS redshift_tot_non_gas_trips,
            r.tot_non_gas_pymts AS redshift_tot_non_gas_pymts,
            r.tot_gas_sales AS redshift_tot_gas_sales,
            r.tot_gas_trips AS redshift_tot_gas_trips,
            r.tot_gas_pymts AS redshift_tot_gas_pymts
        FROM
            df_tab_06_engine AS e
        FULL OUTER JOIN
            df_tab_06_redshift AS r
        ON
            e.site_nbr = r.site_nbr
            AND e.purch_dt = r.purch_dt
        WHERE
            dayofmonth(e.purch_dt)>1
            and dayofmonth(r.purch_dt)>1
        ORDER BY
            e.site_nbr,
            e.purch_dt
        """
    )

    df_tab_06_comparison = log_cache_save(
        df_tab_06_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_06_comparison.csv",
    )

    return df_tab_06_comparison


def control_tab_07(job, data_paths, const_setup):
    """
    Calculate aggregates grouped by purch_dt and tender_type_cd
    from payment_fiscal table and create a comparison table that
    shows payment_fiscal and redshift aggregates side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_07_redshift: See the unittests for sample of this table
    Returns:
        df_tab_07_comparison: A table that shows the aggregates based on
        pipelined_intermediates and those based on redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_07")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_07_redshift"],
        "df_tab_07_redshift",
        filetype="csv",
    )

    df_tab_07_engine = job.spark.sql(
        """
        SELECT
            a.purch_dt,
            a.tender_type_cd,
            SUM(a.sales_pymt_amt) AS tot_sales,
            COUNT(DISTINCT a.purch_hdr_id) AS tot_trips,
            COUNT(a.purch_hdr_id) AS tot_pymts
        FROM
            df_temp_payment AS a
        LEFT JOIN
            df_temp_header AS b
        ON
            a.PURCH_HDR_ID = b.PURCH_HDR_ID
        WHERE
            a.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
            AND b.sales_channel_id IN (%(sales_channel_id_gas)s, %(sales_channel_id_nongas)s)
        GROUP BY
            a.purch_dt,
            a.tender_type_cd
        """
        % const_setup
    )
    df_tab_07_engine.registerTempTable("df_tab_07_engine")

    df_tab_07_comparison = job.spark.sql(
        """
        SELECT
            e.purch_dt AS engine_purch_dt,
            e.tender_type_cd AS engine_tender_type_cd,

            e.tot_sales AS engine_tot_sales,
            e.tot_trips AS engine_tot_trips,
            e.tot_pymts AS engine_tot_pymts,

            r.purch_dt AS redshift_purch_dt,
            r.tender_type_cd AS redshift_tender_type_cd,

            r.tot_sales AS redshift_tot_sales,
            r.tot_trips AS redshift_tot_trips,
            r.tot_pymts AS redshift_tot_pymts
        FROM
            df_tab_07_engine AS e
        FULL OUTER JOIN
            df_tab_07_redshift AS r
        ON
            e.purch_dt = r.purch_dt
            AND e.tender_type_cd = r.tender_type_cd
        WHERE
            dayofmonth(e.purch_dt)>1
            AND dayofmonth(r.purch_dt)>1
        """
        % const_setup
    )

    df_tab_07_comparison = log_cache_save(
        df_tab_07_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_07_comparison.csv",
    )

    return df_tab_07_comparison


def control_tab_08(job, data_paths, const_setup):
    """
    Calculate aggregates grouped by mbrshp_sid
    from payment_fiscal table and create a comparison table that
    shows payment_fiscal and redshift aggregates side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_08_redshift: See the unittests for sample of this table
    Returns:
        df_tab_08_comparison: A table that shows the aggregates based on
        pipelined_intermediates and those based on redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_08")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_08_redshift"],
        "df_tab_08_redshift",
        filetype="csv",
    )

    df_tab_08_01 = job.spark.sql(
        """
        SELECT
            a.mbrshp_sid,
            SUM(b.sales_pymt_amt) AS tot_sales,
            COUNT(DISTINCT b.purch_hdr_id) AS tot_trips,
            COUNT(b.purch_hdr_id) AS tot_pymts
        FROM
            df_temp_member_extended AS a
        LEFT JOIN
            df_temp_payment AS b
        ON
            a.mbrshp_sid = b.mbrshp_sid
            AND b.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
        LEFT JOIN
            df_temp_header h
        ON
            b.purch_hdr_id = h.purch_hdr_id
        WHERE
            h.sales_channel_id IN (%(sales_channel_id_nongas)s)
            and a.mbrshp_exp_dt >= '%(end_date)s'
        GROUP BY
            a.mbrshp_sid
        """
        % const_setup
    )
    df_tab_08_01.registerTempTable("df_tab_08_01")

    df_tab_08_comparison = job.spark.sql(
        """
        SELECT
            e.mbrshp_sid AS engine_mbrshp_sid,

            e.tot_sales AS engine_tot_sales,
            e.tot_trips AS engine_tot_trips,
            e.tot_pymts AS engine_tot_pymts,

            r.mbrshp_sid AS redshift_mbrshp_sid,

            r.tot_sales AS redshift_tot_sales,
            r.tot_trips AS redshift_tot_trips,
            r.tot_pymts AS redshift_tot_pymts
        FROM
            df_tab_08_redshift AS r
        LEFT JOIN
            df_tab_08_01 as e
        ON
            e.mbrshp_sid = r.mbrshp_sid
        ORDER BY
            e.mbrshp_sid
        """
    )

    df_tab_08_comparison = log_cache_save(
        df_tab_08_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_08_comparison.csv",
    )

    return df_tab_08_comparison


def control_tab_09(job, data_paths, const_setup):
    """
    Join a sample of rows from payment table in redshift to the
    payment table in pipelined_intermediates and create a comparison
    table that shows payment_fiscal and redshift rows side-by-side.

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        start_date: Defines the lower bound of the date interval the
            calculated aggregates are based on
        end_date:  Defines the upper bound of the date interval the
            calculated aggregates are based on
    Registered tables required:
        df_tab_09_redshift: See the unittests for sample of this table
    Returns:
        df_tab_09_comparison: A table that shows the rows from
        pipelined_intermediates and those from redshift
        outer joined side-by-side
    """

    logging.info("Starting processing table control_table_09")

    load_and_register(
        job,
        data_paths["source"]["control_files_tab_09_redshift"],
        "df_tab_09_redshift",
        filetype="csv",
    )

    df_tab_09_comparison = job.spark.sql(
        """
        SELECT
            e.mbrshp_sid AS engine_mbrshp_sid,
            e.purch_pymt_seq_id AS engine_purch_pymt_seq_id,
            e.purch_hdr_id AS engine_purch_hdr_id,

            e.cpn_nbr AS engine_cpn_nbr,
            e.sales_pymt_amt AS engine_sales_pymt_amt,

            r.mbrshp_sid AS redshift_mbrshp_sid,
            r.purch_pymt_seq_id AS redshift_purch_pymt_seq_id,
            r.purch_hdr_id AS redshift_purch_hdr_id,

            r.cpn_nbr AS redshift_cpn_nbr,
            r.sales_pymt_amt AS redshift_sales_pymt_amt
        FROM
            df_tab_09_redshift AS r
        LEFT JOIN
            df_temp_payment AS e
        ON
            e.mbrshp_sid = r.mbrshp_sid
            and e.purch_hdr_id = r.purch_hdr_id
            and e.purch_pymt_seq_id = r.purch_pymt_seq_id
        WHERE
            e.purch_dt BETWEEN '%(start_date)s' AND '%(end_date)s'
        ORDER BY
            e.mbrshp_sid,
            e.purch_pymt_seq_id,
            e.purch_hdr_id
        """
        % const_setup
    )

    df_tab_09_comparison = log_cache_save(
        df_tab_09_comparison,
        data_paths["intermediate"]["control_files"] + "/tab_09_comparison.csv",
    )

    return df_tab_09_comparison


def main(job, data_paths, club_square_config, config_validation):
    """
    Create control tables based on pipelined_intermediates and compare them
    to control tables based on redshift

    Args:
        job.spark: SPARK object
        data_paths: dictionary structure carrying the source and intermediate
            paths
        club_square_config: dictionary structure used to get the source_etl
            max_date
        config_validation: dictionary structure that stores configuration
            associated with data quality checks
    Returns:
        Nothing
    """

    if not data_paths["intermediate"].get("control_files"):
        return

    end_date = club_square_config.get("max_date")

    start_date, end_date = misc.get_last_fiscal_weekend(
        start_date_delta=365, end_date_str_in=end_date
    )

    const_setup = {
        "start_date": start_date,
        "end_date": end_date,
        "sales_channel_id_nongas": "'10','30','40'",
        "sales_channel_id_gas": "'60'",
        # These allowed_sales_ctgry_cds represent actual transactions
        # happening in stores other values represent various fees, etc.
        "allowed_sales_ctgry_cd": "'03', '01'",
    }

    load_and_register(
        job,
        data_paths["intermediate"]["member_extended"],
        "df_temp_member_extended",
    )

    load_and_register(
        job,
        data_paths["intermediate"]["detail_fiscal"],
        "df_temp_detail",
    )

    load_and_register(
        job,
        data_paths["intermediate"]["payment_fiscal"],
        "df_temp_payment",
    )

    load_and_register(
        job,
        data_paths["intermediate"]["header_fiscal"],
        "df_temp_header",
    )

    load_and_register(
        job, data_paths["intermediate"]["member"], "df_temp_member"
    )

    control_table_to_function = {
        "control_file_table_01": control_tab_01,
        "control_file_table_02": control_tab_02,
        "control_file_table_03": control_tab_03,
        "control_file_table_04": control_tab_04,
        "control_file_table_05": control_tab_05,
        "control_file_table_06": control_tab_06,
        "control_file_table_07": control_tab_07,
        "control_file_table_08": control_tab_08,
        "control_file_table_09": control_tab_09,
    }

    df_comparison_tables = {}
    for name, control_tab in control_table_to_function.items():
        df_comparison_tables[name] = control_tab(job, data_paths, const_setup)

    for name, df_comparison in df_comparison_tables.items():
        validations.validate_table(
            job.spark,
            "intermediate",
            name,
            config_validation,
            df_comparison,
            check_list=[validations.TestControlTable],
        )


job = JobManager("Control_files")
parser = argparse.ArgumentParser()

parser.add_argument(
    "--config_path",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../configs/config.yaml",
    ),
    help=(
        """
        path to the config file
        """
    ),
)
args = parser.parse_args()
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths, club_square_config, config_validation)
