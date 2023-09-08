"""Spark job to QC an assignment run and generate a QC report for inspection."""


from pe_member_dna.assignment.lib.assn_io import (
    JobManager,
    move_to_outbound,
)
from pe_member_dna.assignment.lib.assn_utils import (
    has_coupons,
    subset_by_time,
)
from pe_member_dna.assignment.lib.checks import (
    check_execution_overwrite,
)
from pe_member_dna.assignment.lib.excelreport import write_report
from pe_member_dna.assignment.lib.qc import *
from pe_member_dna.assignment.lib.qctests import QCTestRunner


def main(conf_path_in=None):

    job = JobManager("qc", "assignment QC", conf_path_in)
    job.config.params = dict(
        list(job.config.params.items())
        + list(job.config.cnf["sizing"].items())
        + list(job.config.cnf["subset"].items())
    )

    if job.config.params["run_type"].lower() == "prod":
        check_execution_overwrite(
            paths_to_check=[
                job.config.paths["QC_REPORT"],
                job.config.paths["SAVINGS"],
            ]
        )

    # basic job params
    TYPE = int(
        "BBM" in job.config.params["campaign"].upper()
    )  # 1 for BBM, 0 for other
    EXAMPLES = [
        {"name": "Rong", "id": 58564596},
        {"name": "Doug", "id": 41510081},
        {"name": "Susan", "id": 76882031},
        {"name": "Keith", "id": 37837336},
        {"name": "Kristy", "id": 5285455},
        {"name": "Tom", "id": 9665302},
        {"name": "MEGHAN JOLIE", "id": 68770944},
        {"name": "SAMANTHA MANZELLO", "id": 61620238},
        {"name": "SONYA MCCORMACK", "id": 61842674},
        {"name": "Kelsey Gainor", "id": 70558415},
        {"name": "Michelle Crockford", "id": 58432200}
    ]

    TESTS_TO_RUN = [
        "avg_spend_decr",
        "duplicate_coupons",
        "duplicate_members",
        "basket_cell",
        "trial_cell",
        "same_num_coups",
        "downsampled_coupons",
        "check_cell_size",
        "check_sensitive_content",
        "count_cf_ineligible", # keep last in list
    ]

    # getting the downsampling information
    job.config.params = dict(
        list(job.config.params.items())
        + list(job.config.cnf["assignment"].items())
    )
    # 1. Read in raw tables
    job.log.info("Reading data...")
    job.data.read("assignment", "MAIL_POPULATION_ASSIGNMENT", filetype="csv")
    job.data.read("input_assignment", "INPUT_ASSIGNMENTS", filetype="csv")
    job.data.read("mail_list", "MAIL_LIST", filetype="csv")
    job.data.read("final_mailhouse", "INPUT_MAILHOUSE", filetype="csv")
    job.data.read("raw_member", "RAW_MEMBER", filetype="parquet")
    job.data.read("constructs", "INPUT_CONSTRUCTS", filetype="parquet")
    job.data.read(
        "cell", "CELL", filetype="csv", schema=job.data.schemas["cells"]
    )
    job.data.read(
        "campaign",
        "CAMPAIGN",
        filetype="csv",
        schema=job.data.schemas["campaigns"],
    )
    job.data.read("memtrips", "COUPON_MEMTRIP", filetype="parquet")
    job.data.read(
        "quals",
        "COUPON_QUALS",
        filetype="csv",
        schema=job.data.schemas["coupon_quals"],
    )
    job.data.read(
        "coups",
        "COUPON_BANK",
        filetype="csv",
        schema=job.data.schemas["coupon_bank"],
    )
    job.data.read(
        "cpg_coupon", "CPG_COUPON_LIST_PATH", filetype="csv", required=False
    )
    job.data.read(
        "category_coupon",
        "CATEGORY_COUPON_PATH",
        filetype="csv",
        required=False,
    )
    job.data.read(
        "basket_coupon", "BASKET_COUPON_PATH", filetype="csv", required=False
    )
    job.data.read(
        "coup_map",
        "COUPON_MAP",
        filetype="csv",
        schema=job.data.schemas["coupon_map"],
    )
    job.data.read("dna", "CUBE", filetype="parquet")
    job.data.tables["dna"] = subset_by_time(
        job.data.tables["dna"],
        job.config.params["assignment_date"],
        "fiscal_week",
    )
    job.data.read("article_dna", "ARTICLE_DNA_PATH", filetype="parquet")
    job.data.read("cf", "PRED_LIST", filetype="parquet")
    job.data.read("item_master", "ITEM_MASTER", filetype="parquet")
    if job.config.paths["EXCLUSIONS"]:
        job.data.read("exclusion_rules", "EXCLUSIONS", filetype="csv")
    job.data.read("article_map", "ARTICLE_AH4_AH5_MAP", filetype="parquet")
    job.data.tables["article_map"] = job.data.tables[
        "article_map"
    ].dropDuplicates(["article_nbr"])

    job.data.read("coupon_bank", "COUPON_BANK", filetype="csv")
    job.data.read("cdsa_assgn", "CDSA_ASSGN", filetype="parquet")

    if TYPE == 1 and job.config.paths.get("VERSION_MAP") is not None:
        job.data.read("version_map", "VERSION_MAP", filetype="csv")

    # 2. Make full base dataset
    job.log.info("Create core datasets")
    generate_full_basedata(
        job, TYPE
    )  # requires base, coups, quals, preds, and memtrips

    # 3. Aggregate Data and calculate stats
    mailfile_to_savings(job)
    job.log.info("aggregating...")
    configured_tabs = generate_configured_qc(job)
    generate_coupon_articles(job)
    generate_coupon_dataset(job)  # requires full base and articles
    generate_member_dataset(job)  # requires full base
    generate_nomail_dataset(job)
    generate_decile_by_mail_dataset(job)  # requires full base and cell
    generate_group_count_dataset(
        job,
        "slot-cpn",
        ["SLOT_NBR", "CPN_TYPE"],
        [sqlf.col("CPN_TYPE"), sqlf.col("SLOT_NBR")],
    )  # requires full base
    generate_group_count_dataset(
        job,
        "cell-cpn",
        ["CELL_ID", "CPN_TYPE"],
        [sqlf.col("CELL_ID"), sqlf.col("CPN_TYPE")],
    )  # requires full base
    is_long = generate_longitudinal_dataset(job)

    EXAMPLES = generate_example_data(job, EXAMPLES)  # requires full base

    # 4. Run tests
    job.log.info("running tests...")
    runner = QCTestRunner(job, TESTS_TO_RUN)
    testdata = runner.run_tests()
    job.data.add("tests", testdata)

    # 5. write output
    job.log.info("writing QC report...")
    if is_long:
        output_tables = [
            "tests",
            "nomail_check",
            "decile_mail",
            "longitudinal",
            EXAMPLES,
            "coupon",
            "articles",
            "cell-cpn",
            "slot-cpn",
        ]
    else:
        output_tables = [
            "tests",
            "nomail_check",
            "decile_mail",
            EXAMPLES,
            "coupon",
            "articles",
            "cell-cpn",
            "slot-cpn",
        ]
    compare_tabs = [ct for ct in configured_tabs if "compare" in ct]
    configured_tabs = [c for c in configured_tabs if c not in compare_tabs]
    temp = [output_tables[0]]
    temp.extend(compare_tabs)
    temp.extend(output_tables[1:])
    output_tables = temp
    output_tables.extend(configured_tabs)
    write_report(job, output_tables)
    if job.config.params["run_type"].lower() == "prod":
        move_to_outbound(
            job.config.paths["QC_REPORT"],
            "qc",
            "xlsx",
            job.config.params,
            job.config.paths,
        )

    job.log.info("done.")


if __name__ == "__main__":
    main()
