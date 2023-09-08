import warnings

from pe_memberdna.assignment.lib.assn_io import JobManager
from pe_memberdna.assignment.lib.input_checks.checker import (
    Check,
    print_summary,
)
from pe_memberdna.assignment.lib.input_checks.exceptions import (
    AssignmentInputError,
    AssignmentInputWarning,
)
from pe_memberdna.lib.iotools import read_s3_to_local


def check_campaign(job):
    """
    Checks campaign.csv file for structural and functional validity

    Parameters:
        job (JobManager):
    Returns:
        [Check(), Check()]: list of checks
    """

    checks = []
    campaigns = read_s3_to_local(job.config.paths["CAMPAIGN"])
    campaigns = campaigns.astype("str")

    current_experiment = campaigns[
        campaigns["experiment_id"] == str(job.config.params["experiment"])
    ]
    if len(current_experiment) == 0:
        raise AssignmentInputError(
            "The current experiment_id is not present" " in the campaign.csv"
        )
    elif len(current_experiment) > 1:
        raise AssignmentInputError(
            "The current experiment_id is duplicated" " in the campaign.csv"
        )
    else:
        checks.append(Check("check_experiment_id", "campaign.csv", True))

    current_channel = current_experiment.iloc[0]["channel"]
    current_expected_distribution = current_experiment.iloc[0][
        "expected_distribution"
    ]

    rest_channel_experiments = campaigns[
        (campaigns["channel"] == current_channel)
        & (campaigns["experiment_id"] != str(job.config.params["experiment"]))
    ]

    if len(rest_channel_experiments):
        minimum_value = rest_channel_experiments["expected_distribution"].min()
        maximum_value = rest_channel_experiments["expected_distribution"].max()

        details = (
            "Channel {channel} Min expected dist. {min_dist}. Max"
            " expected dist. {max_dist}. Current expected dist.:"
            " {current_dist}"
        )

        if minimum_value <= current_expected_distribution <= maximum_value:
            distribution_between_min_max = True
        else:
            distribution_between_min_max = False
            warnings.warn(
                details.format(
                    channel=current_channel,
                    min_dist=minimum_value,
                    max_dist=maximum_value,
                    current_dist=current_expected_distribution,
                ),
                AssignmentInputWarning,
            )
        checks.append(
            Check(
                "check_distribution_between_min_max",
                "campaign.csv",
                distribution_between_min_max,
                details.format(
                    channel=current_channel,
                    min_dist=minimum_value,
                    max_dist=maximum_value,
                    current_dist=current_expected_distribution,
                ),
            )
        )
    else:
        checks.append(
            Check(
                "check_distribution_between_min_max",
                "campaign.csv",
                True,
                "No previous value. Current expected dist. "
                "{current_dist}:".format(
                    current_dist=current_expected_distribution
                ),
            )
        )

    job.log.info("check_campaign: Completed")
    return checks


if __name__ == "__main__":
    job = JobManager("campaign_check", "campaign_check")
    campaign_checks = check_campaign(job)
    print_summary(campaign_checks)
