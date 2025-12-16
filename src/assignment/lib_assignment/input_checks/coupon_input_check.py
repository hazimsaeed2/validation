"""
Spark job to perform coupon input checks
"""

from lib_assignment.assn_io import JobManager
from lib_assignment.assn_utils import read_subset_and_cast
from lib_assignment.input_checks import checker
from lib_assignment.validators import (
    check_column_duplicates,
    check_column_empty,
    check_column_name_value,
    check_data_type_convertable,
    check_date_format,
    check_date_range,
    check_header,
    check_price_in_article,
)


def input_coupon_check_wrapper(
    df,
    schema,
    column_check_date_format=None,
    earlier_date=None,
    later_date=None,
    column_check_empty=None,
    file=None,
    column_check_dups=None,
    ignore_empty_rows=False,
):
    """
    This is a wrapper function that calls several coupon check functions
    """
    checks = []

    validators = [
        ("header",          check_header(df, schema)),
        ("data_type_convertable", check_data_type_convertable(df, schema)),
        ("column_empty",    check_column_empty(df, column_check_empty, ignore_empty_rows),   ),
        ("duplicate",       check_column_duplicates(df, column_check_dups)),
    ]

    if column_check_date_format:
        validators.append( ( "coupon_date_format", check_date_format(df, column_check_date_format), ))

    if earlier_date and later_date:
        validators.append( ( "coupon_date_range", check_date_range(df, earlier_date, later_date),   ))

    for check in validators:
        name = check[0]
        status, details = check[1]
        obj = checker.Check(name, file, status, details)
        checks.append(obj)
    return checks


# ---- Main functions ---- #


def check_coupons(job):



    # 1. Read in raw tables
    print("Reading data...")

    job.data.read(        "article",        "CPG_COUPON_LIST_PATH", filetype="csv", required=False)
    # CPG_COUPON_LIST_PATH: 's3://memberanalytics-data-out-prod/ASSIGNMENTS/campaigns/FY26/MMPC18FY26/Coupon Input/Article_GF18.csv' #for now added pathbut have to upload this 

    job.data.read(        "category",       "CATEGORY_COUPON_PATH", filetype="csv", required=False)
    # commented CATEGORY_COUPON_PATH: 's3://memberanalytics-data-out-prod/ASSIGNMENTS/campaigns/FY22/MMPC11FY22/Coupon_Input/category_pc11.csv'

    job.data.read(        "basket",         "BASKET_COUPON_PATH",   filetype="csv", required=False)
    # commented BASKET_COUPON_PATH: 's3://memberanalytics-data-out-prod/ASSIGNMENTS/campaigns/FY25/MMPC04FY25/Coupon_Input/Basket_GF4.csv'

    job.data.read(        "prices",         "COUPON_PRICE_PATH",    filetype="csv", required=False) # missing
    
    job.data.read(        "closure_coupon", "CLOSURE_COUPON_PATH",  filetype="csv", required=False)
    # commented CLOSURE_COUPON_PATH: 's3://memberanalytics-data-out-prod/ASSIGNMENTS/campaigns/campaign/input_coupon_list/cap_closure_coupons.csv'



    # TODO: Can this be converted to job.data.read?
    if job.config.paths.get("EXTRA_INFO_CLOSURE_COUPON_LOOKUP_PATH"):
        closure_lkup = read_subset_and_cast(  job.config.paths["EXTRA_INFO_CLOSURE_COUPON_LOOKUP_PATH"], "csv"
        )
        job.data.add("closure_lkup", closure_lkup)
        # commented  EXTRA_INFO_CLOSURE_COUPON_LOOKUP_PATH: "s3://memberanalytics-data-out-prod/ASSIGNMENTS/campaigns/campaign/input_coupon_list/cap_closure_coupons_test.csv"







    # 2. Check coupon inputs
    checks = []

    if job.data.tables.get("article"):
        print("-----Checking article.csv: ")

        checks.extend(
            input_coupon_check_wrapper(
                job.data.tables["article"],
                job.data.schemas["article_coupon"],
                ["Valid From", "Valid To"],
                "Valid From",
                "Valid To",
                file="article.csv",
                column_check_dups=["PMR Offer ID"],
            )
        )


    if job.data.tables.get("category"):
        print("-----Checking category.csv: ")

        fully_filled_columns = list(
            set(job.data.tables["category"].columns)
            - set(["cpn_ah4_cd", "cpn_ah5_cd"])
        )

        checks.extend(
            input_coupon_check_wrapper(
                job.data.tables["category"],
                job.data.schemas["category_coupon"],
                ["cpn_start", "cpn_end"],
                "cpn_start",
                "cpn_end",
                fully_filled_columns,
                file="category.csv",
                column_check_dups=["cpn_nbr"],
                ignore_empty_rows=True,
            )
        )

    if job.data.tables.get("basket"):
        print("-----Checking basket.csv: ")

        checks.extend(
            input_coupon_check_wrapper(
                job.data.tables["basket"],
                job.data.schemas["basket_coupon"],
                ["cpn_start", "cpn_end"],
                "cpn_start",
                "cpn_end",
                file="basket.csv",
                column_check_dups=["cpn_nbr"],
            )
        )


    if job.data.tables.get("prices"):
        file = "COUPON_PRICE_PATH"

        print("-----Checking {}: ".format(file))
        status, details = check_header(
            job.data.tables["prices"], job.data.schemas["prices"]
        )
        check = checker.Check("price_in_article", file, status, details)
        checks.append(check)

        if job.data.tables.get("article"):
            status, details = check_price_in_article(
                job.data.tables["prices"], job.data.tables["article"]
            )
            check = checker.Check("price_in_article", file, status, details)
            checks.append(check)



    if job.data.tables.get("closure_coupon"):
        file = "cap_closure_coupons.csv"
        print("-----Checking {}: ".format(file))
        fully_filled_columns = job.data.tables["closure_coupon"].columns
        fully_filled_columns.remove("equivalent_offer_id")

        checks.extend(
            input_coupon_check_wrapper(
                job.data.tables["closure_coupon"],
                job.data.schemas["cap_closure_coupon"],
                column_check_empty=fully_filled_columns,
                file="cap_closure_coupons.csv",
                column_check_dups=["PMR Offer ID"],
            )
        )

        if job.data.tables.get("article"):
            status, details = check_cap_in_article(
                job.data.tables["closure_coupon"], job.data.tables["article"]
            )
            check = checker.Check("cap_in_article", file, status, details)
            checks.append(check)

        if job.data.tables.get("closure_lkup"):
            status1, details1 = check_cap_in_lkup(
                job.data.tables["closure_coupon"],
                job.data.tables["closure_lkup"],
            )
            status2, details2 = check_column_name_value(
                job.data.tables["closure_lkup"], "is_closure", 1
            )
            check1 = checker.Check("cap_in_lkup", file, status1, details1)
            check2 = checker.Check(
                "is_closure_flagged", file, status2, details2
            )
            checks.extend([check1, check2])


    print("coupon_input_check.py: All coupon checks passed")
    return checks







if __name__ == "__main__":
    job = JobManager("coupon_input_check", "coupon_input_check")
    checks = check_coupons(job)
    checker.print_summary(checks)
