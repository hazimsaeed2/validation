"""Spark job to create an ingestable coupon list

TODO: Add integration test
"""

from datetime import datetime, timedelta

import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
from pemember_dna.pipelines.assignment.lib.assn_io import JobManager
from pemember_dna.pipelines.assignment.lib.checks import (
    check_execution_overwrite,
    check_prod_status,
)
from pemember_dna.pipelines.assignment.lib.coupon_utils import (
    add_coupons,
    append_new_campaign,
    append_non_duplicates,
    calculate_discount,
    calculate_member_trips,
    calculate_member_usage,
    clean_cpg_coupon_file,
    format_coupons,
    remove_exclusions,
)
from pemember_dna.pipelines.assignment.lib.schemas.coupon_schemas import (
    BASKET_COUPON_SCHEMA,
    CAT_COUPON_SCHEMA,
    CPN_BNK_SCHEMA,
    CPN_DISCOUNT_SCHEMA,
    CPN_MAP_SCHEMA,
    CPN_QUALS_SCHEMA,
    MEM_TRIP_SCHEMA,
    MEM_USAGE_SCHEMA,
)
from pemember_dna.pipelines.lib.iotools import write_local_to_s3
from pyspark import StorageLevel


def has_coupons(job):
    """Identify whether the campaign has coupons"""
    if (
        job.config.paths.get("CPG_COUPON_LIST_PATH")
        or job.config.paths.get("CATEGORY_COUPON_PATH")
        or job.config.paths.get("BASKET_COUPON_PATH")
    ):
        return True
    return False


def initialize_dfs(job):
    """Initialize the dfs required to store the final outputted data"""
    for name, schema in (
        ("quals", CPN_QUALS_SCHEMA),
        ("memtrips", MEM_TRIP_SCHEMA),
        ("bank", CPN_BNK_SCHEMA),
        ("coupmap", CPN_MAP_SCHEMA),
        ("memusage", MEM_USAGE_SCHEMA),
        ("discounts", CPN_DISCOUNT_SCHEMA),
    ):
        tbl = job.spark.createDataFrame([], schema)
        job.data.tables[name] = tbl


def prepare_data(job):
    """Prepare input data"""
    job.data.read("article_dna", "ARTICLE_DNA_PATH", filetype="parquet")
    job.data.tables["article_dna"] = job.data.tables[
        "article_dna"
    ].dropDuplicates(["article_nbr"])
    job.data.read("article_map", "ARTICLE_AH4_AH5_MAP", filetype="parquet")
    job.data.tables["article_map"] = job.data.tables[
        "article_map"
    ].dropDuplicates(["article_nbr"])
    job.data.read("ah5_dna", "AH5_DNA_PATH", filetype="parquet")
    job.data.tables["ah5_dna"] = job.data.tables["ah5_dna"].dropDuplicates(
        ["AH5_DESC"]
    )
    job.data.read("ah4_dna", "AH4_DNA_PATH", filetype="parquet")
    job.data.tables["ah4_dna"] = job.data.tables["ah4_dna"].dropDuplicates(
        ["AH4_DESC"]
    )
    job.data.read("transactions", "TRANSACTIONS_PATH", filetype="parquet")
    job.data.read("item", "ITEM_MASTER", filetype="parquet")

    # prep transactions
    trans_end = datetime.strptime(
        job.config.params["assignment_date"], "%Y-%m-%d"
    )
    trans_start = trans_end - timedelta(days=(365))
    member_trips = calculate_member_trips(
        job.data.tables["transactions"],
        job.data.tables["item"],
        trans_start,
        trans_end,
    )
    member_trips.persist(StorageLevel.DISK_ONLY)

    # prep category_dna
    article_map = job.data.tables["article_map"]
    article_ah5_ah4 = article_map.select(
        "article_nbr", "AH5_CD", "AH5_DESC", "AH4_CD", "AH4_DESC"
    )
    article_map = job.data.tables["article_map"].select(
        "article_nbr", "MCH3_DESC", "AH5_CD", "AH5_DESC"
    )
    article_map = article_map.filter(
        article_map.MCH3_DESC.isin(
            job.config.params["backfill"]["substitutable_MCH3_exclusion"]
        )
    )
    article_map = article_map.select(
        "article_nbr", "AH5_CD", "AH5_DESC"
    ).distinct()
    article_dna = job.data.tables["article_dna"].drop(
        "AH5_CD", "AH4_CD", "AH5_DESC", "AH4_DESC"
    )
    article_map = article_map.join(article_dna, ["article_nbr"], "left")
    article_map = article_map.filter(article_map.AH5_CD.isNotNull())
    article_dna = job.data.tables["article_dna"]
    article_dna = article_dna.withColumnRenamed("ARTICLE_NBR", "category")
    article_dna = article_dna.withColumnRenamed(
        "ARTICLE_DESC", "category_desc"
    )
    ah5_dna = job.data.tables["ah5_dna"]
    ah5_dna = ah5_dna.withColumnRenamed("AH5_CD", "category")
    ah5_dna = ah5_dna.withColumnRenamed("AH5_DESC", "category_desc")
    ah4_dna = job.data.tables["ah4_dna"]
    ah4_dna = ah4_dna.withColumnRenamed("AH4_CD", "category")
    ah5_dna = ah5_dna.withColumnRenamed("AH4_DESC", "category_desc")
    article_dna = article_dna.drop("AH5_CD", "AH5_DESC", "AH4_CD", "AH4_DESC")
    cat_dna = article_dna.union(ah5_dna)
    cat_dna = cat_dna.union(ah4_dna)
    cat_dna = cat_dna.withColumn("category", cat_dna.category.cast("integer"))
    cat_dna.persist(StorageLevel.DISK_ONLY)

    for name, table in (
        ("member_trips", member_trips),
        ("article_map", article_map),
        ("ah5_dna", ah5_dna),
        ("ah4_dna", ah4_dna),
        ("cat_dna", cat_dna),
        ("article_ah5_ah4", article_ah5_ah4),
    ):
        job.data.tables[name] = table


def create_coupons(job):

    quals = job.data.tables["quals"]
    bank = job.data.tables["bank"]
    coupmap = job.data.tables["coupmap"]
    memtrips = job.data.tables["memtrips"]
    cat_dna = job.data.tables["cat_dna"]
    member_trips = job.data.tables["member_trips"]
    discounts = job.data.tables["discounts"]

    category = basket = article = prices = False
    # handle basket type offers
    if job.config.paths.get("BASKET_COUPON_PATH"):
        basket = True
        job.log.info("    Creating basket coupons...")

        # 1. Read in data
        basket_coups = job.spark.read.csv(
            job.config.paths["BASKET_COUPON_PATH"],
            header=True,
            schema=BASKET_COUPON_SCHEMA,
        )
        basket_cnt = basket_coups.count()
        job.log.info("    num basket coupons: {}".format(basket_cnt))
        # 2. Format data
        basket, basket_map, _, _ = format_coupons(
            basket_coups, member_trips, cat_dna, job.config.params, "basket"
        )
        # 3. attach to dynamic tables
        quals = add_coupons(quals, basket, subset=quals.columns)
        bank = add_coupons(bank, basket, subset=bank.columns)
        coupmap = add_coupons(coupmap, basket_map, subset=coupmap.columns)

    # handle category type offers
    if job.config.paths.get("CATEGORY_COUPON_PATH"):
        category = True
        job.log.info("    Creating category coupons...")

        # 1. Read in data
        cat_coups = job.spark.read.csv(
            job.config.paths["CATEGORY_COUPON_PATH"],
            header=True,
            inferSchema=True,
        )
        cat_coups = cat_coups.withColumn(
            "cpn_ah4_cd",
            sqlf.explode(
                sqlf.when(
                    sqlf.split(cat_coups.cpn_ah4_cd, ",").isNotNull(),
                    sqlf.split(cat_coups.cpn_ah4_cd, ","),
                ).otherwise(sqlf.array(sqlf.lit(None).cast(sqlt.LongType())))
            ),
        )
        cat_coups = cat_coups.withColumn(
            "cpn_ah4_cd", sqlf.trim(cat_coups.cpn_ah4_cd)
        )
        cat_coups = cat_coups.withColumn(
            "cpn_ah5_cd",
            sqlf.explode(
                sqlf.when(
                    sqlf.split(cat_coups.cpn_ah5_cd, ",").isNotNull(),
                    sqlf.split(cat_coups.cpn_ah5_cd, ","),
                ).otherwise(sqlf.array(sqlf.lit(None).cast(sqlt.LongType())))
            ),
        )
        cat_coups = cat_coups.withColumn(
            "cpn_ah5_cd", sqlf.trim(cat_coups.cpn_ah5_cd)
        )
        for column in CAT_COUPON_SCHEMA:
            cat_coups = cat_coups.withColumn(
                column.name, sqlf.col(column.name).cast(column.dataType)
            )

        cat_cnt = cat_coups.select("cpn_nbr").distinct().count()
        job.log.info("    num category coupons: {}".format(cat_cnt))

        # 2. Format data
        cat, cat_map, cat_memtrips, _ = format_coupons(
            cat_coups, member_trips, cat_dna, job.config.params, "category"
        )

        # 3. attach to dynamic tables
        memtrips = add_coupons(memtrips, cat_memtrips)
        quals = add_coupons(quals, cat, subset=quals.columns)
        bank = add_coupons(bank, cat, subset=bank.columns)
        coupmap = add_coupons(coupmap, cat_map, subset=coupmap.columns)

    # handle article type offers
    if job.config.paths.get("CPG_COUPON_LIST_PATH"):
        article = True
        job.log.info("    Creating article coupons...")

        # 1. Read in data and clean
        cpg_coups = job.spark.read.csv(
            job.config.paths["CPG_COUPON_LIST_PATH"], header=True
        )
        cpg_coups = clean_cpg_coupon_file(
            cpg_coups, job.config.params["coupon_types"]
        )
        # removing exclusions, need to rename column to preserve general excl. fn.
        cpg_coups = cpg_coups.withColumnRenamed("article_nbr", "category")

        cpg_coups, exclusion_log = remove_exclusions(
            cpg_coups,
            cat_dna,
            job.data.tables["article_ah5_ah4"],
            job.config.params["exclusion_types"],
            job.config.params["excluded_coupons_force_back_in"],
        )

        cpg_coups = cpg_coups.withColumnRenamed("category", "article_nbr")
        cpg_cnt = cpg_coups.select("cpn_nbr").distinct().count()
        job.log.info("    num article coupons: {}".format(cpg_cnt))

        # 2. Format data
        cpg, cpg_map, cpg_memtrips, cpn_excl_log = format_coupons(
            cpg_coups,
            member_trips,
            cat_dna,
            job.config.params,
            "article",
            job.data.tables["article_map"],
        )

        # 3. attach to dynamic tables
        memtrips = add_coupons(memtrips, cpg_memtrips).dropna(how="all")
        quals = add_coupons(quals, cpg, subset=quals.columns).dropna(how="all")
        bank = add_coupons(bank, cpg, subset=bank.columns).dropna(how="all")
        coupmap = add_coupons(coupmap, cpg_map, subset=coupmap.columns).dropna(
            how="all"
        )

    if job.config.paths.get("COUPON_PRICE_PATH"):
        prices = True
        job.log.info("    Creating coupon discounts...")
        article_prices = job.spark.read.csv(
            job.config.paths["COUPON_PRICE_PATH"], header=True
        )
        discounts = calculate_discount(article_prices)

    for name, table in (
        ("quals", quals),
        ("bank", bank),
        ("coupmap", coupmap),
    ):
        job.data.tables[name] = table

    # Member usage table, dependent on some above tables
    trans_end = datetime.strptime(
        job.config.params["assignment_date"], "%Y-%m-%d"
    )
    trans_start = trans_end - timedelta(days=(365))
    memusage = calculate_member_usage(
        job.data.tables["transactions"],
        job.data.tables["item"],
        quals,
        coupmap,
        trans_start,
        trans_end,
        job.config.params["experiment"],
    )
    job.data.tables["memusage"] = memusage

    if prices:
        job.data.tables["discounts"] = discounts
    if category or article:
        job.data.tables["memtrips"] = memtrips
        job.data.tables["memusage"] = memusage
    if article:
        return exclusion_log, cpn_excl_log

    return None, None


def write(job):
    """Write coupons out to final coupon bank"""

    quals = job.data.tables["quals"]
    bank = job.data.tables["bank"]
    coupmap = job.data.tables["coupmap"]
    memtrips = job.data.tables["memtrips"]
    memusage = job.data.tables["memusage"]
    discounts = job.data.tables["discounts"]

    PATHS = job.config.paths

    job.log.info("    total number of coupons: {}".format(bank.count()))

    discounts.write.csv(
        PATHS["COUPON_DISCOUNT"], mode="overwrite", header=True
    )

    if bank.count() > 2:
        memtrips = memtrips.repartition(int(bank.count() / 2))
        memusage = memusage.repartition(int(bank.count() / 2))
    memtrips.write.parquet(PATHS["COUPON_MEMTRIP"], mode="overwrite")
    job.log.info("finished printing COUPON_MEMTRIP")
    memusage.write.parquet(PATHS["COUPON_MEMUSAGE"], mode="overwrite")
    job.log.info("finished printing COUPON_MEMUSAGE")

    if job.config.params["run_type"].lower() == "prod":
        if has_coupons(job):
            job.data.read("existing_quals", "COUPON_QUALS", filetype="csv")
            append_non_duplicates(
                job.spark,
                job.data.tables["existing_quals"],
                quals,
                PATHS["COUPON_QUALS"],
                "csv",
                "experiment_id",
            )

            job.data.read("updated_quals", "COUPON_QUALS", filetype="csv")
            updated_quals = job.data.tables["updated_quals"]

            append_new_campaign(
                job,
                coupmap,
                "COUPON_MAP",
                "csv",
                updated_quals,
                ["cpn_nbr", "article_nbr"],
            )
            append_new_campaign(
                job,
                bank,
                "COUPON_BANK",
                "csv",
                updated_quals,
                ["cpn_nbr", "cpn_desc"],
            )

        else:
            job.log.info(
                "No new coupons, not adding to production coupon bank"
            )
    else:
        for tbl, path in (
            (quals, "COUPON_QUALS"),
            (bank, "COUPON_BANK"),
            (coupmap, "COUPON_MAP"),
        ):
            if tbl.count() == 0:
                tbl = tbl.columns
                tbl = job.sc.parallelize([tbl]).toDF(["header"])
                # tbl = job.spark.createDataFrame(tbl, StringType())
                tbl.coalesce(1).write.csv(
                    PATHS[path], mode="overwrite", header=False
                )
            else:
                tbl.coalesce(1).write.csv(
                    PATHS[path], mode="overwrite", header=True
                )
            job.log.info("finished printing {}".format(path))


def main(conf_path_in=None):

    name = "CreateCoupons"
    job = JobManager("coupon_creation", name, conf_path_in)

    if job.config.params["run_type"].lower() == "prod":
        check_execution_overwrite(
            paths_to_check=[job.config.paths["COUPON_MEMTRIP"]]
        )
        check_prod_status(job)

    job.log.info("1. Initialize coupon dataframes")
    initialize_dfs(job)

    if has_coupons(job):
        job.log.info("2. Read in reference data and prepare static data")
        prepare_data(job)

        job.log.info("3. Creating Coupons...")
        exclusion_log, cpn_excl_log = create_coupons(job)
    else:
        job.log.info("Skipping steps 2 & 3 - no coupons provided")

    job.log.info("4. Write output to file...")
    write(job)

    job.log.info("5. Write logging file and shut down...")

    if job.config.paths.get("CPG_COUPON_LIST_PATH"):
        exclusion_log.coalesce(1).write.csv(
            job.config.paths["COUPON_EXCLUSION_LOG"],
            mode="overwrite",
            header=True,
        )
        cpn_excl_log.coalesce(1).write.csv(
            job.config.paths["COUPON_BACKFILL_ELIGIBILITY_LOG"],
            mode="overwrite",
            header=True,
        )

    if job.config.params["run_type"].lower() == "prod":
        log_line = {
            "campaign": job.config.params["campaign"],
            "run_name": job.config.params["run_name"],
            "run_type": job.config.params["run_type"],
            "log_event": "END",
            "time": str(datetime.now().strftime("%Y-%m-%d-%H-%M-%S")),
        }

        write_local_to_s3(log_line, job.config.paths["COUPON_LOG"])

    job.log.info("done")


if __name__ == "__main__":

    main()
