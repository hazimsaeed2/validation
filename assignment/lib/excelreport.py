"""Write Formatted Excel Report"""

import importlib
import io
import sys

import boto3
import pandas as pd
import pe_memberdna.pipelines.assignment.lib.qc as qc
from pemember_dna.pipelines.lib.iotools import split_path_bucket_key

import pe_memberdna.lib.xlsx_helper as xlh

# This is required because xmlrunner relys on sys.stdout having the attribule
# delegate
stdin, stdout, stderr = sys.stdin, sys.stdout, sys.stderr
importlib.reload(sys)  # Hack to get setdefaultencoding
sys.stdin, sys.stdout, sys.stderr = stdin, stdout, stderr


def write_test_excel_table(job, tablename, writer):
    """Write a formatted 'test' style table to excel.

    Parameters:
        test_table (pandas.DataFrame): table to write
        sheet_name (str): name of sheet to write to
        writer (ExcelWriter): excel writer to handle write/

    Returns:
        Nothing!
    """
    df = job.data.tables[tablename]
    df.to_excel(
        writer, sheet_name=tablename, index=False, header=False, startrow=3
    )

    workbook = writer.book
    worksheet = writer.sheets[tablename]
    fmts = xlh.formats(workbook)

    worksheet.set_column("A:Z", None, fmts["test_table"])

    # conditional color formatting for pass/fail indicators
    worksheet.conditional_format(
        "B1:B99",
        {
            "type": "cell",
            "criteria": "equal to",
            "value": '"Fail"',
            "format": fmts["red_cell"],
        },
    )
    worksheet.conditional_format(
        "B1:B99",
        {
            "type": "cell",
            "criteria": "equal to",
            "value": '"Pass"',
            "format": fmts["green_cell"],
        },
    )

    # format column widths
    xlh.set_column_widths(df, worksheet)


def write_format_excel_table(
    df, sheet_name, writer, start_row=0, start_col=0, header=True
):
    """Write and format an excel table from pandas.

    Parameters:
        df (pandas.DataFrame): table to write
        sheet_name (str): name of sheet to write to
        writer (ExcelWriter): excel writer to handle write/
        start_row (int, opt): optional starting row for write
        start_col (int, opt): optional starting column for write
        header (bool, opt): toggle inclusion of header

    Returns:
        Nothing!
    """
    df.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        startrow=start_row,
        startcol=start_col,
        header=header,
    )

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    fmts = xlh.formats(workbook)

    # add number format
    xlh.format_column_by_type(df, workbook, worksheet)

    # Add a header format.
    hdr_fmt = fmts["base_table"]
    for col_num, value in enumerate(df.columns.values, start=start_col):
        worksheet.write(start_row, col_num, value, hdr_fmt)

    # fit column width
    xlh.set_column_widths(df, worksheet, start_col=start_col)


def write_excel_table(job, tablename, writer):
    """Write formatted excel table as part of report.

    Parameters:
        job (JobManager): job manager containing tables to write
        tablename (str): name of table to write
        writer (ExcelWriter): excel writer to handle write/format
    Returns:
        Nothing!
    """
    table = job.data.tables[tablename]
    if isinstance(table, pd.DataFrame):
        df = table
    else:
        df = table.toPandas()
    write_format_excel_table(df, tablename, writer)


def write_example_tables(job, tables, writer):
    """Write formatted excel tables sheet as part of report.

    Parameters:
        job (JobManager): job manager containing tables to write
        tables (list[dict]): list of tables to write
        writer (ExcelWriter): excel writer to handle write/format
    Returns:
        Nothing!
    """
    sheet_name = "examples"
    rowindex = 0
    titles = []
    for table in tables:
        df = table["data"]
        spacer = 4
        titletext = table["name"] + ":"
        title = {"title": titletext, "row": rowindex}
        titles.append(title)
        length = len(df) + spacer
        write_format_excel_table(
            df, sheet_name, writer, start_row=rowindex + 1
        )
        rowindex += length

    # add titles after (worksheet already created)
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    fmts = xlh.formats(workbook)

    ttl_fmt = fmts["table_title"]
    for title in titles:
        worksheet.write(title["row"], 0, title["title"], ttl_fmt)


def write_report(job, tables, encryption="aws:kms"):
    """Write QC report containing selected tables.

    Parameters:
        job (JobManager): JobManager containing tables to write
        tables (list[str]): list of table names to include
    Returns:
        Nothing!
    """
    path = job.config.paths["QC_REPORT"]
    bucket, key = split_path_bucket_key(path)
    buffer = csv_buffer = io.BytesIO()
    s3_resource = boto3.resource("s3")
    writer = pd.ExcelWriter(buffer, engine="xlsxwriter")

    with writer as writer:
        for table in tables:
            if isinstance(table, list):
                write_example_tables(job, table, writer)
            elif table == "tests":
                write_test_excel_table(job, table, writer)
            elif table.startswith("renewal"):
                xlh.write_double_header_table(
                    job.data.tables[table].toPandas(),
                    table,
                    writer,
                    qc.RENEWAL_STATS,
                    freeze=True,
                )
            elif table.startswith("descriptive"):
                xlh.write_double_header_table(
                    job.data.tables[table].toPandas(),
                    table,
                    writer,
                    ["pct"] + qc.DESCRIPTIVE_CDS,
                    freeze=True,
                )
            elif table.startswith("personalized"):
                xlh.write_double_header_table(
                    job.data.tables[table].toPandas(),
                    table,
                    writer,
                    ["Percentage of Members Qualifying for Targeting", "prct"]
                    + qc.PERSONALIZED_CONTENT_CDS
                    + qc.PERSONALIZED_CONTENT_STATS,
                    freeze=True,
                )
                definitions = {
                    "Content": [
                        "BJ's App",
                        "EZ Renewal",
                        "Rewards Upgrade",
                        "Credit Card",
                        "Same Day Delivery",
                    ],
                    "Targeting Strategy": [
                        "Members who have not signed up for BJ's App",
                        "Members not enrolled in EZ and not opted out;\
                        3+ months to DTR",
                        "IC / Plus who spend $1.5K - $3.5K in-club last 52W,\
                        or last 12W equivalent",
                        "IC/Rewards Members who spend $1.5K over last 52W in\
                        club or 10+ gas trips, or last 12W equivalent",
                        "Members who qualify for same day delivery",
                    ],
                    "Relevant DNA Fields": [
                        "[HAS_QUOTIENT_ID] = 0\
                        \r\n[L26W_ATC_CPN_RED] = 0",
                        "[LATEST_AUTO_RNWL_IND] = N, O, or Q\
                        \r\n[LATEST_MBRSHP_EXP_DT] > 90 + In-home Date",
                        "[RWDS_MBR_IND] = N or P \
                        \r\n[LFIFTY-TWOW_SPEND_IN_STORE]\
                        between 1.5K and 3.5K\
                        \r\n        or [LTWELVEW_SPEND_IN_STORE] \
                        between 400 and 900",
                        "[RWDS_MBR_IND] = Y or N\
                        \r\n[LFIFTY_TWOW_SPEND_IN_STORE] >= 1.5K\
                        \r\n    or [LTWELVEW_SPEND_IN_STORE] >= 400 \
                        \r\n    or [L_FIFTY-TWOW_GAS TRIPS] >= 10\
                        \r\n    or [L_TWELVEW_GAS TRIPS] > 2",
                        "zip code in list\
                        \r\n[HAS_QUOTIENT_ID] = 1",
                    ],
                }
                terms = pd.DataFrame(
                    definitions,
                    columns=[
                        "Content",
                        "Targeting Strategy",
                        "Relevant DNA Fields",
                    ],
                )
                write_format_excel_table(
                    terms,
                    table,
                    writer,
                    1,
                    len(job.data.tables[table].columns) + 4,
                )
            elif table == "nomail_check":
                xlh.write_double_header_table(
                    job.data.tables[table].toPandas(),
                    table,
                    writer,
                    qc.NOMAIL_STATS + qc.NOMAIL_CDS,
                    freeze=True,
                )
            elif table == "savings_summary":
                start, end = qc.generate_dates(job)
                wb = writer.book
                ws = wb.add_worksheet(table)
                writer.sheets[table] = ws
                fmts = xlh.formats(wb)

                for col, val in enumerate(["Start:", start]):
                    ws.write(0, col, val, fmts["header1"])
                for col, val in enumerate(["End:", end]):
                    ws.write(1, col, val, fmts["header2_bold"])

                ws.merge_range(4, 1, 5, 1, "By MFI Tier", fmts["header1"])
                ws.merge_range(7, 1, 8, 1, "By Tenure", fmts["header1"])

                write_format_excel_table(
                    job.data.tables["top_10"].toPandas(),
                    table,
                    writer,
                    job.data.tables[table].count() + 4,
                    2,
                )

                xlh.write_double_header_table(
                    job.data.tables[table].toPandas(),
                    table,
                    writer,
                    qc.SAVINGS_OVERVIEW,
                    start_col=2,
                    freeze=False,
                    suffixes=["nulls", "pct_1", "mean", "pct_99"],
                )
            elif table.startswith("savings_content"):
                xlh.write_double_header_table(
                    job.data.tables[table].toPandas(),
                    table,
                    writer,
                    qc.SAVINGS_KEYED,
                    freeze=True,
                    suffixes=["nulls", "pct_1", "mean", "pct_99"],
                )
            else:
                write_excel_table(job, table, writer)

    if encryption is None:
        s3_resource.Object(bucket, key).put(Body=buffer.getvalue())
    else:
        s3_resource.Object(bucket, key).put(
            Body=buffer.getvalue(), ServerSideEncryption=encryption
        )
