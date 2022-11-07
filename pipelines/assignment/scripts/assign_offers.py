"""Spark Job to carry out assignment of offers to members."""


from datetime import datetime

from pe_member_dna.pipelines.assignment.lib.assn_io import JobManager
from pe_member_dna.pipelines.assignment.lib.campaign import Campaign
from pe_member_dna.pipelines.assignment.lib.checks import (
    check_execution_overwrite,
)
from pe_member_dna.pipelines.lib.iotools import (
    copy_file_to_s3,
    write_local_to_s3,
)
from pe_member_dna.pipelines.lib.utils import apply_unionall


def main(conf_path_in=None):

    name = "CampaignAssignment"
    job = JobManager("assignment", name, conf_path_in)

    save_path = (
        "{output_dir}config_assign_{campaign}{run_name}{run_type}.yml".format(
            output_dir=job.config.paths["OUTPUT_DIR"],
            campaign=job.config.params["campaign"],
            run_name=job.config.params["run_name"],
            run_type=job.config.params["run_type"],
        )
    )

    if job.config.params["run_type"].lower() == "prod":
        paths_to_check = [
            save_path,
            job.config.paths["ASSIGNMENT_PATH"],
            job.config.paths["CONSTRUCTS_PATH"],
        ]

        check_execution_overwrite(paths_to_check)

    # --- Save config ---- #
    copy_file_to_s3(job.config.cfg_path, save_path)
    job.log.info("Config saved at {path}".format(path=save_path))

    # ---- 1. Initialize campaign ---- #
    job.log.info("1. Initializing...")
    campaign = Campaign(job.config.params, job.config.paths)

    # ---- 2. ingest offer and member data ---- #
    job.log.info("2. ingesting data sources...")
    job.log.info("2.1 ingesting members")
    member_data = campaign.ingest_member_data()
    job.log.info("Member count: {}".format(member_data.count()))

    job.log.info("2.2 ingesting offers")
    assignment_pools, coupon_pools = campaign.ingest_offer_data()

    job.log.info("2.3 calculate exposure")
    offer_exposure, coupon_exposure = campaign.calculate_exposure()

    # ---- 3. Frontfill ---- #
    job.log.info("3. Calculate frontfill")
    frontfill_offer = campaign.calculate_offers(
        member_data.select("MBRSHP_SID"),
        assignment_pools,
        coupon_pools,
        Campaign.FillType.FF,
        offer_exposure,
        coupon_exposure,
    )

    # ---- 4. Backfill ---- #
    job.log.info("4. Calculate backfill")
    backfill_offer = campaign.calculate_offers(
        member_data.select("MBRSHP_SID"),
        assignment_pools,
        coupon_pools,
        Campaign.FillType.BF,
        offer_exposure,
        coupon_exposure,
    )

    # ---- 5. Append fills ---- #
    job.log.info("5. Append fills")
    if frontfill_offer is not None and backfill_offer is not None:
        assignment = apply_unionall(frontfill_offer, backfill_offer)
    elif frontfill_offer is not None:
        assignment = frontfill_offer
    elif backfill_offer is not None:
        assignment = backfill_offer
    else:
        raise Exception("Both frontfill and backfill are empty!")

    # ---- 6. Fit coupons to the correct slot ---- #
    job.log.info("6. Place coupons in the right position")
    all_assignments = campaign.fit_coupons(assignment)

    # ---- 7. Change layout for segment validation ---- #
    job.log.info("7. Change layout for segment validation")
    pivoted_assignment = campaign.pivot_assignment(all_assignments)
    pivoted_assignment = member_data.join(pivoted_assignment, ["MBRSHP_SID"])

    # ---- 8 Load past longitudinal mbrs ---- #
    job.log.info("8. Add past longitudinal member eligibility")
    pivoted_assignment = campaign.find_past_longitudinal_mbrs(
        pivoted_assignment
    )

    # ---- 9. Find eligible segments ---- #
    job.log.info("9. Determining segment eligibility...")
    eligible_assignments = campaign.find_eligible_segments(pivoted_assignment)

    # ---- 10. Assign Members to cells ---- #
    job.log.info("10. Assigning members to cells...")
    member_data = campaign.assign_cells(eligible_assignments)
    member_data = campaign.unpivot_assignment(member_data)

    # ---- 11. Reorder coupons based on user input ---- #
    job.log.info("11. Map coupons based on layout mapping")
    member_data = campaign.map_coupons(member_data)

    # ---- 12. Reorder coupons based on user input ---- #
    job.log.info("12. Reorder coupons based on user input")
    member_data = campaign.sort_coupons(
        member_data, assignment_pools, coupon_pools
    )

    # ---- 13. Replace with null ---- #
    job.log.info("13. Replace with null")
    member_data = campaign.replace_slots_with_null(member_data)

    # ---- 14. Build final assignment---- #
    job.log.info("14. Generating assignment output...")
    assigns, constructs = campaign.generate_output(member_data)

    # ---- 15. Write Output ---- #
    job.log.info("15. Writing output...")
    job.data.add("assigns", assigns)
    job.data.add("constructs", constructs)

    if job.config.params["run_type"].lower() == "test":
        write_mode = "overwrite"
        job.data.write(
            "assigns",
            "ASSIGNMENT_PATH",
            mode=write_mode,
            singlefile=True,
            ftype="csv",
        )
        job.data.write(
            "constructs",
            "CONSTRUCTS_PATH",
            mode=write_mode,
            singlefile=True,
            ftype="parquet",
        )
    else:
        write_mode = "overwrite"

        job.log.info("15.1 Writing true assignments.")
        job.data.write(
            "assigns",
            "ASSIGNMENT_PATH",
            mode=write_mode,
            ftype="csv",
            partitionby=32,
        )
        job.log.info("15.2 Writing all constructs.")
        job.data.write(
            "constructs",
            "CONSTRUCTS_PATH",
            mode=write_mode,
            ftype="parquet",
            partitionby=64,
        )

    # ---- 16. Write Log Output and Shut Down---- #
    job.log.info("16. Write Log Output and Shut Down...")

    log_line = {
        "campaign": job.config.params["campaign"],
        "date": datetime.now(),
        "run_name": job.config.params["run_name"],
        "run_type": job.config.params["run_type"],
        "experiment": job.config.params["experiment"],
        "assignment_date": job.config.params["assignment_date"],
        "mail_list": job.config.paths["MAIL_LIST"],
        "pred_list": job.config.paths["PRED_LIST"],
    }

    write_local_to_s3(log_line, job.config.paths["ASSIGN_LOG"])

    job.log.info("done")


if __name__ == "__main__":
    main()
