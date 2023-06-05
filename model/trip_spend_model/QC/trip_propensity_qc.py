import argparse
import pandas as pd
import re
import yaml

from pe_memberdna.lib.iotools import (
    list_s3_dir,
    split_path_bucket_key,
    write_local_to_s3,
)

############# READ IN COMMAND LINE ARGUMENTS #########################
parser = argparse.ArgumentParser(description="trip and spend propensity QC")
parser.add_argument("config", action="store", help="configuration file")
parser.add_argument("save", action="store", help="save option")
args = parser.parse_args()

############## SET VARIABLES FROM CONFIG ###########################
with open(args.config, "r") as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)

BUCKET = config["shared"]["bucket"]
run_name = config["shared"]["run_name"]

path = "s3://{}/{}/{}/{}/".format(
    BUCKET,
    config["shared"]["base_path"],
    config["shared"]["model_path"],
    run_name,
)

bucket, key = split_path_bucket_key(path)
trip_output_file_list = list_s3_dir(bucket, key)

regexFeature = re.compile(r".+second.+")
feature_path = list(filter(regexFeature.match, trip_output_file_list))[0]

current_features = (
    pd.read_csv(feature_path)
    .sort_values("Importance", ascending=False)
    .head(10)
)
past_features = pd.read_csv(
    "s3://{}/{}".format(
        BUCKET,
        config["shared"]["past_features"],
    )
)
current_features.reset_index(drop=True, inplace=True)
current_features.rename(columns=lambda x: f"{x}_{run_name}", inplace=True)
current_features[f"Importance_{run_name}"] = current_features[
    f"Importance_{run_name}"
].map(lambda x: f"{round(100*x, 2)}%")

features_vertical = pd.melt(
    past_features.filter(like="Feature")
    .reset_index()
    .rename(columns={"index": "rank"}),
    id_vars=["rank"],
    value_name="feature",
)

feature_appearances = (
    features_vertical.groupby("feature")["rank"]
    .agg(["mean", "count"])
    .reset_index()
)
feature_appearances["rate"] = (
    feature_appearances["count"]
    / past_features.filter(like="Feature").shape[1]
)
feature_appearances.sort_values(
    ["rate", "mean"], ascending=[False, True], inplace=True
)
feature_appearances.rename(
    columns={"mean": "avg_rank", "rate": "use_rate"}, inplace=True
)
feature_appearances.reset_index(drop=True, inplace=True)
feature_appearances.drop(columns=["count"], inplace=True)
feature_appearances = feature_appearances.round(2)

rename_cols = {
    col: "feature"
    for col in current_features.filter(like="Feature_Name").columns
}
rename_cols["index"] = "current_rank"
current_feature_ranks = (
    current_features.filter(like="Feature")
    .reset_index()
    .rename(columns=rename_cols)
)
current_feature_ranks = current_feature_ranks[["feature", "current_rank"]]

feature_ranks_df = pd.merge(
    current_feature_ranks, feature_appearances, how="left", on="feature"
)
write_local_to_s3(feature_ranks_df, f"{path}qc_feature_ranks.csv")

feature_melt = pd.melt(
    past_features.filter(like="Feature")
    .rename(columns=lambda x: x.split("Feature_Name_")[1])
    .reset_index(),
    id_vars=["index"],
    value_name="feature",
).rename(columns={"index": "rank", "variable": "run_name"})

importance_melt = pd.melt(
    past_features.filter(like="Importance")
    .rename(columns=lambda x: x.split("Importance_")[1])
    .reset_index(),
    id_vars=["index"],
    value_name="importance",
).rename(columns={"index": "rank", "variable": "run_name"})

feature_importance_vertical = pd.merge(
    importance_melt, feature_melt, how="left", on=["rank", "run_name"]
)
feature_importance_vertical["importance"] = feature_importance_vertical[
    "importance"
].map(lambda x: float(x.rstrip("%")))

feature_importance = (
    feature_importance_vertical.groupby("feature")["importance"]
    .sum()
    .reset_index()
)
feature_importance["avg_importance"] = (
    feature_importance["importance"]
    / past_features.filter(like="Importance").shape[1]
)
feature_importance = feature_importance.round(2)
feature_importance.drop(columns=["importance"], inplace=True)
feature_importance.sort_values("avg_importance", ascending=False, inplace=True)
feature_importance.reset_index(drop=True, inplace=True)

current_importance = current_features.rename(
    columns=lambda x: x.split("_")[0].lower()
)
current_importance.rename(
    columns={"importance": "current_importance"}, inplace=True
)
current_importance["current_importance"] = current_importance[
    "current_importance"
].map(lambda x: float(x.rstrip("%")))
current_importance = current_importance[["feature", "current_importance"]]

feature_imp_df = pd.merge(
    current_importance, feature_importance, how="left", on="feature"
).fillna(0)
write_local_to_s3(feature_imp_df, f"{path}qc_feature_importance.csv")

past_features = pd.concat([past_features, current_features], axis=1)
regexFeature = re.compile(r".+metric_5_7+")
metric_path = list(filter(regexFeature.match, trip_output_file_list))[0]

current_metric = pd.read_csv(metric_path)
current_metric["iter"] = [1, 2]
current_metric = current_metric[current_metric.iter == 2]
run_date = re.findall(r"[0-9]{8}", metric_path)[0]
current_metric["date"] = run_date
current_metric = current_metric[
    [
        "date",
        "accuracy_score",
        "precision_score",
        "recall_score",
        "roc_auc_score",
    ]
]
past_metric = pd.read_csv(
    "s3://{}/{}".format(
        BUCKET,
        config["shared"]["past_metric"],
    )
)
all_metric = pd.concat([current_metric, past_metric]).drop_duplicates()
all_metric.reset_index(drop=True, inplace=True)

if args.save == "yes":
    print("saving")
    write_local_to_s3(
        past_features,
        "s3://{}/{}/{}".format(
            BUCKET,
            config["shared"]["base_path"],
            config["shared"]["current_features"],
        ),
    )
    write_local_to_s3(
        all_metric,
        "s3://{}/{}/{}".format(
            BUCKET,
            config["shared"]["base_path"],
            config["shared"]["current_metric"],
        ),
    )
else:
    print("not saving")
