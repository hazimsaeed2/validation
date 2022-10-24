"""
Helper functions and objects to work with xlsx module.
"""


import copy
import importlib
import sys

# This is required because xmlrunner relys on sys.stdout having the attribule
# delegate
stdin, stdout, stderr = sys.stdin, sys.stdout, sys.stderr
importlib.reload(sys)  # Hack to get setdefaultencoding
sys.stdin, sys.stdout, sys.stderr = stdin, stdout, stderr

# Custom defined formats
__formats = {
    "float": {"num_format": "#,##0.00"},
    "int": {"num_format": "#,##0"},
    "base_table": {
        "bold": True,
        "text_wrap": True,
        "font_name": "Trebuchet MS",
        "font_size": 10,
        "valign": "top",
        "fg_color": "#f2f2f2",
        "bottom": 2,
        "bottom_color": "#c22e46",
    },
    "test_table": {"bg_color": "#ffffff", "font_name": "Trebuchet MS", "font_size": 12},
    "table_title": {"bold": True, "font_name": "Trebuchet MS", "font_size": 10},
    "header1": {
        "bold": True,
        "text_wrap": True,
        "font_name": "Trebuchet MS",
        "font_size": 10,
        "align": "center",
        "valign": "vcenter",
        "fg_color": "#f2f2f2",
    },
    "header2": {
        "text_wrap": True,
        "font_name": "Trebuchet MS",
        "font_size": 10,
        "align": "center",
        "valign": "vcenter",
        "fg_color": "#f2f2f2",
        "bottom": 2,
        "bottom_color": "#c22e46",
    },
    "header2_bold": {
        "bold": True,
        "text_wrap": True,
        "font_name": "Trebuchet MS",
        "font_size": 10,
        "align": "center",
        "valign": "vcenter",
        "fg_color": "#f2f2f2",
        "bottom": 2,
        "bottom_color": "#c22e46",
    },
    "red_cell": {"bg_color": "#FFC7CE", "font_color": "#9C0006"},
    "green_cell": {"bg_color": "#C6EFCE", "font_color": "#006100"},
}


def formats(wb, borders=None):
    """
    Add custom formats to workbook and return dictionary of applied formats.

    Parameters:
        wb (xlsx workbook): workbook to add formats to
        borders ([str], opt): list of borders to set to True for all formats

    Returns:
        (dict): dictionary where key is string identifier of desired format,
                value is wb object of that format.
    """
    fmts = {}
    for format, s in __formats.items():
        specs = copy.deepcopy(s)
        if borders:
            for bdr in borders:
                specs[bdr] = True
        f = wb.add_format(specs)
        fmts[format] = f
    return fmts


def calc_max_length(df, padding):
    """Find max string length per column for a dataframe.

    Parameters:
        df (pandas.DataFrame): dataframe to find lengths for
        padding (float): multiplication factor for length to add padding

    Returns:
        overall_max (list[float]): list of max lengths for each column
    """
    header_length_list = [len(x) * padding for x in df.columns]
    max_content_list = [
        df[col].map(lambda x: len(str(x))).max() * padding for col in df.columns
    ]
    overall_max = list(map(max, list(zip(header_length_list, max_content_list))))
    return overall_max


def set_column_widths(df, worksheet, padding=1.25, start_col=0):
    """Set column widths for a table to make space for all text.

    Parameters:
        df (pd.DataFrame): dataframe representing table written to excel
        worksheet (Xlsxwriter.worksheet): worksheet to set widths on
        start_col (int, opt): optional starting column for write

    Returns:
        Nothing!
    """
    widths = calc_max_length(df, padding)
    for i, width in enumerate(widths):
        worksheet.set_column(i + start_col, i + start_col, width)


def format_column_by_type(df, workbook, worksheet, fmts=None, column=None):
    """Set column number formats for table by the datatype.

    Parameters:
        df (pd.DataFrame): dataframe representing table written to excel
        workbook (Xlsxwriter.workbook): wookbook to set format on
        worksheet (Xlsxwriter.worksheet): worksheet to set format on
        fmts ({str:Xlsxwriter.format}): optional formats
        column (str): individual column to format ; all if set to None

    Returns:
        Nothing!
    """
    if not fmts:
        fmts = formats(workbook)

    def set_type(col):
        """Set format for column given its data type."""
        dtype = df.dtypes.iloc[col]
        if dtype in ("int32", "int64"):
            f = fmts["int"]
        elif dtype in ("float32", "float64"):
            f = fmts["float"]
        else:
            return
        worksheet.set_column(col, col, None, f)

    if column:
        set_type(column)
    else:
        for col, _ in enumerate(df.columns.values):
            set_type(col)


def write_format_excel_table(df, sheet_name, writer, start_row=0, start_col=0):
    """Write and format an excel table from pandas.

    Parameters:
        df (pandas.DataFrame): table to write
        sheet_name (str): name of sheet to write to
        writer (ExcelWriter): excel writer to handle write/
        start_row (int, opt): optional starting row for write
        start_col (int, opt): optional starting column for write

    Returns:
        Nothing!
    """
    df.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        startrow=start_row,
        startcol=start_col,
    )

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    fmts = formats(workbook)

    format_column_by_type(df, workbook, worksheet)

    fmt_hdr = fmts["base_table"]
    for col_num, value in enumerate(df.columns.values, start=start_col):
        worksheet.write(start_row, col_num, value, fmt_hdr)

    set_column_widths(df, worksheet, start_col=start_col)


def write_double_header_table(
    df,
    sheet_name,
    writer,
    levels,
    start_row=0,
    start_col=0,
    freeze=False,
    suffixes=None,
):
    """
    Write table with a double header.

    For example, if df contains columns "L1_MEAN", "L1_VAR", "L2_MEAN", "L2_VAR"
    and function is called with levels = ["L1", "L2"] then the first header
    will have "L1" spanning two columns and "L2" spanning two columns, with
    "MEAN" and "VAR" as the second header below the first.

    Parameters:
        df (pandas.DataFrame): table to write
        sheet_name (str): name of sheet to write to
        writer (ExcelWriter): excel writer to handle write
        levels ([str]): list of levels to group the first header on
        start_row (int, opt): starting row for write
        start_col (int, opt): starting column for write
        freeze (bool, opt): whether to freeze the first 2 rows and column
            (will freeze second column if contains "desc")
        suffixes (list[str], opt): list of suffixes used to get the levels
            (the remaining part) from the columns

    Generates:
        worksheet in the workbook containing data from df
    """

    def get_level(col, suffixes):
        """
        Given a column name, identify its corresponding level.
        Parameters:
            col (str) : column name
            suffixes (list[str]): parts of the name that are
                not applicable to the level name.
        Returns:
            level if present
        """
        for level in levels:
            if suffixes:
                # handle multiple columns with partial overlap in names
                # i.e. TRIPS and GAS_TRIPS
                col_suffix = col[len(level) + 1 :]
                if col_suffix in suffixes:
                    return level
            elif level.lower() in col.lower():
                return level
        return None

    df.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        startrow=start_row + 1,
        startcol=start_col,
    )

    wb = writer.book
    ws = writer.sheets[sheet_name]
    fmts = formats(wb)
    fmts_bdr = formats(wb, borders=["left"])
    format_column_by_type(df, wb, ws, fmts)

    start_lvl, end_lvl = start_col, start_col
    grp = None
    for c, col in enumerate(df.columns, start=start_col):
        lvl = get_level(col, suffixes)
        if lvl is None:  # no merge required
            # pandas does not allow overwriting header format with set_row
            # so must rewrite cell with desired format
            ws.write(start_row, c, "", fmts["header1"])
            ws.write(start_row + 1, c, col, fmts["header2"])
            start_lvl += 1
            end_lvl += 1
            continue

        nm = col.replace(lvl, "").strip("_")
        if grp is None:  # first level seen
            grp = lvl
            ws.write(start_row + 1, c, nm, fmts_bdr["header2"])
            format_column_by_type(df, wb, ws, fmts_bdr, start_lvl)
        elif lvl == grp:  # same level as previous column
            ws.write(start_row + 1, c, nm, fmts["header2"])
            end_lvl += 1
        else:  # new level seen
            ws.write(start_row + 1, c, nm, fmts_bdr["header2"])
            format_column_by_type(df, wb, ws, fmts_bdr, start_lvl)
            if start_lvl != end_lvl:
                ws.merge_range(
                    start_row, start_lvl, start_row, end_lvl, grp, fmts_bdr["header1"]
                )
            else:
                ws.write(start_row, start_lvl, grp, fmts_bdr["header1"])
            grp = lvl
            start_lvl = end_lvl + 1
            end_lvl = start_lvl
    # handle last group
    ws.write(start_row + 1, end_lvl, nm, fmts["header2"])
    format_column_by_type(df, wb, ws, fmts_bdr, start_lvl)
    ws.merge_range(start_row, start_lvl, start_row, end_lvl, grp, fmts_bdr["header1"])

    if freeze:
        if "desc" in df.columns[1].lower():
            ws.freeze_panes(start_row + 2, 2)
        else:
            ws.freeze_panes(start_row + 2, 1)
