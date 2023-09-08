#!/bin/sh

params=("$@")
num_params=${#params[@]}

for cluster_name in "$@"; do
    clust_id=${cluster_name:(-2)}
    status=$(aws emr list-clusters --active --query "Clusters[?Name=='$cluster_name'].Status.State" --output text)

    if  [ "$status" == "RUNNING" ] || [ "$status" == "WAITING" ]; then
        echo "Cluster $cluster_name is online and terminating it"
        curl -X POST -k "https://mda-jenkins.bjs.com:8443/job/PE-Cluster-Management/job/PE-Weekly-Clusters/job/cluster_term_${clust_id}/build/" --user "mdabuild:11e650303e6e73d2d2ecdedb6cbb6a2a8b"
        sleep 30

        spin_status=$(aws emr list-clusters --active --query "Clusters[?Name=='$cluster_name'].Status.State" --output text)
        if  [ "$spin_status" == "TERMINATED" ] || [ "$spin_status" == "TERMINATED_WITH_ERRORS" ]; then
            pass
        else
            echo "Cluster $cluster_name is not Terminated"
    else
        echo "Cluster $cluster_name is in Terminated state"

    fi
done