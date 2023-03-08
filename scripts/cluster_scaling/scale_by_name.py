"""
Provides easier way to scale our EMR clusters.

Specifically, you give it cluster AWS console name, node target
and optionally the desired wait time.
The script will update the instance count of the instance group
or instance fleet on that cluster.

Intended to be used with Jenkins project, only use this directly from cmd
in extreme circumstances.

If the --wait option is specified the application will continue checking whether
the cluster has been re-scaled or not. If the cluster does not reach the
required size within the given number of minutes the application will
raise an error.
"""

import argparse
import time
from platform import node

from pe_memberdna.scripts.cluster_scaling.utility import (
    get_ec2_client,
    get_emr_client,
)


def get_params():
    """
    Collects the parameters required for scaling

    Args:

    Returns:
        target_cluster (string) - cluster to be upscaled
        spot_node_cnt (int) - count of spot instance(s) to spin up
        ondemand_node_cnt (int) - count of on-demand instance(s) to spin up
        wait_number_mins (int) - waiting period in minutes
        api_call_retry_interval (int) - defines the lengths of interval
            between API call retries. The default is 30 mins
    """

    parser = argparse.ArgumentParser(
        description="This is an application intended for scaling clusters"
    )

    parser.add_argument(
        "--target_cluster",
        required=True,
        type=str,
        default=None,
        help="Name of the cluster to be scaled i.g. 'Dev-03' ",
    )

    parser.add_argument(
        "--wait",
        type=int,
        default=None,
        help="""
                If this argument is supplied, scale_by_name will
                wait for the specified number of minutes. If the
                cluster doesn't reach the required number
                of nodes an error will be raised.

                If this argument is not supplied scale_by_name will
                just send the request for rescaling of the cluster
                and then gracefully quit.
            """,
    )

    parser.add_argument(
        "--force_terminate",
        type=bool,
        default=False,
        help="""
                Defines if the script should try calling EC2 terminate instance
                on the TASK nodes after unsuccessfull downscaling
            """,
    )

    parser.add_argument(
        "--api_call_retry_interval",
        type=int,
        default=30,
        help="""
                Defines the interval between rescaling API calls
                if the cluster rescaling is unsuccessful in minutes.
            """,
    )

    parser.add_argument(
        "--spot_node_cnt",
        type=int,
        default=0,
        help="""
                Requested number of spot nodes. Must be 0-50.
            """,
    )

    parser.add_argument(
        "--ondemand_node_cnt",
        type=int,
        default=0,
        help="""
                Requested number of ondemand nodes. Must be 0-50.
            """,
    )

    args = parser.parse_args()

    target_cluster = args.target_cluster
    spot_node_cnt = args.spot_node_cnt
    ondemand_node_cnt = args.ondemand_node_cnt
    wait_number_mins = args.wait
    api_call_retry_interval = args.api_call_retry_interval
    force_terminate = args.force_terminate

    return (
        target_cluster,
        spot_node_cnt,
        ondemand_node_cnt,
        wait_number_mins,
        api_call_retry_interval,
        force_terminate,
    )


def rescale_cluster_call_api(
    client_emr,
    target_cluster_ig_type,
    target_cluster_id,
    target_cluster_node_ig_id,
    spot_node_cnt,
    ondemand_node_cnt,
):
    """
    Calls a boto3 endpoint either modifying the
    number of nodes in an instance group or an instance fleet

    Args:
        client_emr (boto3 emr client_emr class) - boto3 EMR client
        target_cluster_ig_type (string) - cluster instance group type
        target_cluster_id (string)
        target_cluster_node_ig_id (string) - id of the target instance aggregation
            (INSTANCE_FLEET or INSTANCE_GROUP)
        spot_node_cnt (int) - count of spot instance(s) to spin up
        ondemand_node_cnt (int) - count of on-demand instance(s) to spin up

    Returns:

    """

    if target_cluster_ig_type == "INSTANCE_GROUP":

        client_emr.modify_instance_groups(
            ClusterId=target_cluster_id,
            InstanceGroups=[
                {
                    "InstanceGroupId": target_cluster_node_ig_id,
                    "InstanceCount": (spot_node_cnt + ondemand_node_cnt),
                }
            ],
        )

    elif target_cluster_ig_type == "INSTANCE_FLEET":
        try:
            client_emr.modify_instance_fleet(
                ClusterId=target_cluster_id,
                InstanceFleet={
                    "InstanceFleetId": target_cluster_node_ig_id,
                    "TargetOnDemandCapacity": ondemand_node_cnt,
                    "TargetSpotCapacity": spot_node_cnt,
                },
            )
        except Exception as e:
            raise Exception(
                """
                An exception occurred while trying to
                invoke modify_instance_fleet API.
                Exception: {}
                """.format(
                    e
                )
            )

    else:
        raise Exception(
            """
            target_cluster_ig_type '{}' is not from
            ['INSTANCE_GROUP', 'INSTANCE_FLEET']
            """.format(
                target_cluster_ig_type
            )
        )


def check_rescaling_status(
    client_emr,
    spot_node_cnt,
    ondemand_node_cnt,
    target_cluster_id,
    target_cluster_ig_type,
    target_cluster_node_ig_id,
):
    """
    Calls a boto3 endpoint to confirm if the number of
    nodes in the given instance group or instance fleet matches the
    required number of nodes

    Args:
        client_emr (boto3 emr client_emr class) - boto3 EMR client
        spot_node_cnt (int) - count of spot instance(s) to spin up
        ondemand_node_cnt (int) - count of on-demand instance(s) to spin up
        target_cluster_id (string)
        target_cluster_ig_type (string) - type of the instance aggregation
            (INSTANCE_FLEET or INSTANCE_GROUP)
        target_cluster_node_ig_id (string) - id of the target instance group or fleet
    Returns:
        finished_rescaling (boolean) - specifies whether the rescaling operation
        is finished or not

    """
    if target_cluster_ig_type == "INSTANCE_GROUP":

        instance_group_list = client_emr.list_instance_groups(
            ClusterId=target_cluster_id
        )["InstanceGroups"]

        for ig_desc_curr in instance_group_list:
            if ig_desc_curr["Id"] == target_cluster_node_ig_id:
                ig_desc = ig_desc_curr
                break
        else:
            raise Exception(
                "target_cluster_node_ig_id '{}' not found ".format(
                    target_cluster_node_ig_id
                )
            )

        node_difference = ig_desc["RunningInstanceCount"] - (
            spot_node_cnt + ondemand_node_cnt
        )

        finished_rescaling = (node_difference == 0) and (
            ig_desc["Status"]["State"] == "RUNNING"
        )

        print(
            (
                "RunningInstanceCount = {}".format(
                    ig_desc["RunningInstanceCount"]
                )
            )
        )

    elif target_cluster_ig_type == "INSTANCE_FLEET":

        instance_fleet_list = client_emr.list_instance_fleets(
            ClusterId=target_cluster_id
        )["InstanceFleets"]

        for ig_desc_curr in instance_fleet_list:
            if ig_desc_curr["Id"] == target_cluster_node_ig_id:
                ig_desc = ig_desc_curr
                break
        else:
            raise Exception(
                "target_cluster_node_ig_id '{}' not found ".format(
                    target_cluster_node_ig_id
                )
            )
        try:
            node_difference = (
                ig_desc["ProvisionedSpotCapacity"] - spot_node_cnt
            ) + (ig_desc["ProvisionedOnDemandCapacity"] - ondemand_node_cnt)
        except Exception as e:
            raise Exception(
                """
                An exception occurred while trying to
                compute node_difference.
                Kindly chceck the value of spot_node_cnt and ondemand_node_cnt.
                Exception: {}
                """.format(
                    e
                )
            )

        finished_rescaling = (
            (
                ig_desc["ProvisionedOnDemandCapacity"]
                == ig_desc["TargetOnDemandCapacity"]
            )
            and (node_difference == 0)
            and (ig_desc["Status"]["State"] == "RUNNING")
        )

        print(
            (
                "ProvisionedOnDemandCapacity = {}".format(
                    ig_desc["ProvisionedOnDemandCapacity"]
                )
            )
        )

        print(
            (
                "ProvisionedSpotCapacity = {}".format(
                    ig_desc["ProvisionedSpotCapacity"]
                )
            )
        )

    return finished_rescaling, node_difference


def terminate_extra_ec2_instances(
    client_emr,
    client_ec2,
    node_difference,
    target_cluster_id,
    target_cluster_ig_type,
    target_cluster_node_ig_id,
):
    """
    Calls a boto3 endpoint to terminate the ec2 instances that are
    above the requested number of nodes in the TASK group

    Args:
        client_emr (boto3 emr client_emr class) - boto3 EMR client
        client_ec2 (boto3 emr client_ec2 class) - boto3 EC2 client
        node_difference (int) - the number of nodes that are extra
        target_cluster_id (string)
        target_cluster_ig_type (string) - type of the instance aggregation
            (INSTANCE_FLEET or INSTANCE_GROUP)
        target_cluster_node_ig_id (string) - id of the target instance group or fleet
    Returns:
        finished_rescaling (boolean) - specifies whether the rescaling operation
        is finished or not

    """
    if target_cluster_ig_type == "INSTANCE_GROUP":
        instance_list = client_emr.list_instances(
            ClusterId=target_cluster_id,
            InstanceGroupId=target_cluster_node_ig_id,
            InstanceStates=["RUNNING"],
        )
    elif target_cluster_ig_type == "INSTANCE_FLEET":
        instance_list = client_emr.list_instances(
            ClusterId=target_cluster_id,
            InstanceFleetId=target_cluster_node_ig_id,
            InstanceStates=["RUNNING"],
        )
    print("Running instances discovered:")
    print(instance_list)

    instance_ids = [
        instance["Ec2InstanceId"] for instance in instance_list["Instances"]
    ]

    if node_difference > 0:
        res = client_ec2.terminate_instances(
            InstanceIds=instance_ids[0:node_difference]
        )
        print(res)


def rescale_cluster(
    client_emr,
    client_ec2,
    target_cluster,
    spot_node_cnt,
    ondemand_node_cnt,
    api_call_retry_interval=60 * 30,
    wait_number_mins=None,
    force_terminate=False,
):
    """
    Collects the parameters required for scaling checking the command line
    arguments

    Args:
        client_emr (boto3 emr client_emr class) - EMR client created by Boto3
        client_ec2 (boto3 emr client_emr class) - EC2 client created by Boto3
        target_cluster (string) - cluster to be upscaled
        spot_node_cnt (int) - count of spot instance(s) to spin up
        ondemand_node_cnt (int) - count of on-demand instance(s) to spin up
        api_call_retry_interval (int) - defines the lengths of interval
            between API call retries. The default is 30 mins
        wait_number_mins (int) - waiting period in minutes, the defaults
            is None or "don't wait for rescaling to be finished"

    Returns:
        (boolean) result of upscaling operation
    """

    if not 0 <= (spot_node_cnt + ondemand_node_cnt) <= 50:
        error_msg = "Unacceptable number of nodes."
        " Contact Hazim/Prezmek/Shubham if scaling 50+ nodes."
        raise ValueError(error_msg)

    resp_active_clusters = client_emr.list_clusters(
        ClusterStates=["WAITING", "RUNNING"]
    )

    names = [cluster["Name"] for cluster in resp_active_clusters["Clusters"]]
    ids = [cluster["Id"] for cluster in resp_active_clusters["Clusters"]]

    cluster_list = {}
    for curr_id, curr_name in zip(ids, names):

        try:
            for i in range(10):
                cluster_info = client_emr.describe_cluster(ClusterId=curr_id)
                break
        except Exception as e:
            print(
                f"Failed to make DecribeCluster API call - {i} iteration: {e}"
            )

        cluster_project_tags = [
            tag["Value"]
            for tag in cluster_info["Cluster"]["Tags"]
            if tag["Key"] == "Project"
        ]

        if "MemberDataAggregation" not in cluster_project_tags:

            cluster_list[curr_name] = {
                "Id": "bad_project",
                "ig": None,
                "dns": None,
                "ig_type": None,
            }

        else:

            master_public_dns_name = cluster_info["Cluster"][
                "MasterPublicDnsName"
            ]

            ig_type = cluster_info["Cluster"]["InstanceCollectionType"]

            if ig_type == "INSTANCE_GROUP":
                instance_groups = client_emr.list_instance_groups(
                    ClusterId=curr_id
                )
                ig_names = [
                    ig["Id"] for ig in instance_groups["InstanceGroups"]
                ]
                ig_ids = [
                    ig["InstanceGroupType"]
                    for ig in instance_groups["InstanceGroups"]
                ]
                ig_ifl_dict = dict(list(zip(ig_ids, ig_names)))
            elif ig_type == "INSTANCE_FLEET":
                instance_fleets = client_emr.list_instance_fleets(
                    ClusterId=curr_id
                )
                ifl_names = [
                    ifl["Id"] for ifl in instance_fleets["InstanceFleets"]
                ]
                ifl_ids = [
                    ifl["InstanceFleetType"]
                    for ifl in instance_fleets["InstanceFleets"]
                ]
                ig_ifl_dict = dict(list(zip(ifl_ids, ifl_names)))
            else:
                raise Exception("non-existant instance type")

            cluster_list[curr_name] = {
                "Id": curr_id,
                "ig": ig_ifl_dict.get("TASK"),
                "dns": master_public_dns_name,
                "ig_type": ig_type,
            }

    print("Cluster INFO:")
    print(
        (
            "".join(
                [
                    curr_name
                    + " -- "
                    + cluster["dns"]
                    + " -- "
                    + cluster["Id"]
                    + "\n"
                    for curr_name, cluster in cluster_list.items()
                    if cluster["dns"]
                ]
            )
        )
    )

    if target_cluster not in cluster_list:
        raise ValueError(
            "Cluster "
            + target_cluster
            + " does not exist"
            + "\n available clusters:"
            + str(list(cluster_list.keys()))
        )

    if cluster_list[target_cluster]["Id"] == "bad_project":
        raise ValueError(
            "Cluster "
            + target_cluster
            + " belongs to a different project"
            + "\n available clusters:"
            + str(list(cluster_list.keys()))
        )

    target_cluster_id = cluster_list[target_cluster]["Id"]
    target_cluster_node_ig_id = cluster_list[target_cluster]["ig"]
    target_cluster_ig_type = cluster_list[target_cluster]["ig_type"]

    rescale_cluster_call_api(
        client_emr,
        target_cluster_ig_type,
        target_cluster_id,
        target_cluster_node_ig_id,
        spot_node_cnt,
        ondemand_node_cnt,
    )

    if not wait_number_mins:
        print("Cluster rescaling successfully requested.")
        return "success_without_wait"
    else:
        # poll the boto3 API every 60 seconds
        status_check_interval = 60

        max_retries = int(60 * wait_number_mins / status_check_interval)

        for retries in range(0, max_retries):

            time.sleep(status_check_interval)

            finished_rescaling, node_difference = check_rescaling_status(
                client_emr,
                spot_node_cnt,
                ondemand_node_cnt,
                target_cluster_id,
                target_cluster_ig_type,
                target_cluster_node_ig_id,
            )

            if finished_rescaling:
                break

            count_from_last_api_retry = (retries + 1) % int(
                60 * api_call_retry_interval / status_check_interval
            )

            if count_from_last_api_retry == 0:
                print("Retrying upscale API call...")
                rescale_cluster_call_api(
                    client_emr,
                    target_cluster_ig_type,
                    target_cluster_id,
                    target_cluster_node_ig_id,
                    spot_node_cnt,
                    ondemand_node_cnt,
                )

            print(
                (
                    "Cluster is re-scaling, waiting for {}s...".format(
                        status_check_interval
                    )
                )
            )

        if finished_rescaling:
            print("Cluster rescaled successfully.")
            return "success_after_wait"

        if force_terminate and (node_difference > 0):

            terminate_extra_ec2_instances(
                client_emr,
                client_ec2,
                node_difference,
                target_cluster_id,
                target_cluster_ig_type,
                target_cluster_node_ig_id,
            )
            print("Terminating EC2 instances.")

            return "success_with_ec2_termination"

        raise Exception(
            "Rescaling failed! Maximum number of retries exceeded."
        )


def main():
    """
    Initiates the emr client_emr and rescales the cluster

    Args:

    Returns:

    """
    client_emr = get_emr_client()
    client_ec2 = get_ec2_client()

    (
        target_cluster,
        spot_node_cnt,
        ondemand_node_cnt,
        wait_number_mins,
        api_call_retry_interval,
        force_terminate,
    ) = get_params()

    rescale_cluster(
        client_emr,
        client_ec2,
        target_cluster,
        spot_node_cnt,
        ondemand_node_cnt,
        api_call_retry_interval,
        wait_number_mins,
        force_terminate,
    )


if __name__ == "__main__":
    main()
