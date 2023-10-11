"""
Wrapper to execute all input checks for the assignment engine.
"""

import pe_memberdna.assignment.lib.assn_io as assn_io
import pe_memberdna.assignment.lib.input_checks.campaign_check as cc
import pe_memberdna.assignment.lib.input_checks.cells_check as cells_check
import pe_memberdna.assignment.lib.input_checks.checker as checker
import pe_memberdna.assignment.lib.input_checks.coupon_input_check as cpn
import pe_memberdna.assignment.lib.input_checks.json_check as jsn
import pe_memberdna.assignment.lib.input_checks.longitudinal_design_check as ldc
import pe_memberdna.assignment.lib.input_checks.member_check as mc
import pe_memberdna.assignment.lib.input_checks.path_check as path_check


def main():
    checks = cpn.check_coupons(job)
    checks.extend(path_check.check_paths(job))
    checks.extend(cc.check_campaign(job))
    checks.extend(cells_check.check_cells_csv(job))
    checks.extend(jsn.check_jsons(job))
    checks.extend(mc.check_dups_membership(job))
    checks.extend(ldc.check_longitudinal_design(job))
    print("\n")
    checker.print_summary(checks)


if __name__ == "__main__":
    job = assn_io.JobManager("checker", "Assignment Input Checker")
    main()
