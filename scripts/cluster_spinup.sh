# Accept parameters
params=("$@")
num_params=${#params[@]}

for cluster_name in "$@"; do
    clust_id=${cluster_name:(-2)}
    status=$(aws emr list-clusters --active --query "Clusters[?Name=='$cluster_name'].Status.State" --output text)

    if  [ "$status" == "RUNNING" ] || [ "$status" == "WAITING" ]; then
        echo "Cluster $cluster_name is already online"
    else
        echo "Cluster $cluster_name is Terminated state and bringing it online"
        curl -X POST -k "https://mda-jenkins.bjs.com:8443/job/PE-Cluster-Management/job/PE-Weekly-Clusters/job/EMR-Build${clust_id}-v6.5.0-Static/build/" --user "mdabuild:11e650303e6e73d2d2ecdedb6cbb6a2a8b"
        sleep 480

        spin_status=$(aws emr list-clusters --active --query "Clusters[?Name=='$cluster_name'].Status.State" --output text)
        if  [ "$spin_status" == "RUNNING" ] || [ "$spin_status" == "WAITING" ]; then
            pass
        else
            echo "Cluster $cluster_name is not spinned up"
        fi
    fi
done