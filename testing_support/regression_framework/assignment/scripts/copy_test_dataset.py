import argparse
import os
import yaml

from memberdna.pipelines.lib.iotools import (
    s3_copy,
    split_path_bucket_key,
    s3_delete,
    is_s3_file,
    is_s3_path,
)


if __name__ == "__main__":

    print("############################################")
    print("# Copy regression test dataset to new test #")
    print("############################################")
    print("\n")

    dir_path = os.path.dirname(os.path.abspath(__file__))

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--name", nargs="?"
    )
    parser.add_argument(
        "--test-suite", nargs="?"
    )
    args, _ = parser.parse_known_args()
    test_name = args.name
    test_suite = args.test_suite

    if not test_name:
        raise Exception("Please pass a test name.")

    cdsa_name = test_suite if test_suite else test_name

    with open(
        os.path.join(
            dir_path,
            "../../../../pipelines/assignment/regression_tests/conf/"
            "CustomizableBackfill_config.yml",
        ),
        "r",
    ) as ymlfile:
        customizable_backfill = yaml.load(ymlfile, Loader=yaml.FullLoader)

    with open(
        os.path.join(
            dir_path,
            "../../../../pipelines/assignment/regression_tests/conf/"
            "{name}_config.yml".format(name=test_name),
        ),
        "r",
    ) as ymlfile:
        new_regression_test = yaml.load(ymlfile, Loader=yaml.FullLoader)

    allowed_paths = [
        "REGRESSION_TESTS/assignment/{name}".format(name=test_name)
    ]
    if test_suite:
        allowed_paths.append(
            "REGRESSION_TESTS/assignment/{name}".format(name=cdsa_name)
        )

    exclude_paths = [
        "CDSA_LOC",
        "ASSN_LOC",
        "LOG_LOC",
        "INPUT_ASSIGNMENTS",
        "INPUT_CONSTRUCTS",
        "INPUT_MAILHOUSE",
        "MAIL_POPULATION_ASSIGNMENT",
        "EXCLUSIONS",
    ]

    for key, path in customizable_backfill["paths"].items():
        if key not in exclude_paths and (
            key in new_regression_test["paths"]
            or key == "EXTRA_INFO_COUPON_PATH"
        ):
            bucket, source = split_path_bucket_key(path)

            if key in new_regression_test["paths"]:
                _, destination = split_path_bucket_key(
                    new_regression_test["paths"][key]
                )

                if not destination:
                    print(f"Empty path for {key}. Skiping.\n")
                    continue

                print("Clean %s" % destination)
                s3_delete(bucket, destination, allowed_paths)

            source = source.rstrip("/")
            destination = source.replace("CustomizableBackfill", cdsa_name)

            print("Copy %s to %s" % (source, destination))
            s3_copy(bucket, source, destination)

            new_regression_test["paths"][key] = path.replace(
                "CustomizableBackfill", cdsa_name
            )

            print("\n")
    new_regression_test["paths"]["INPUT_ASSIGNMENTS"] = ""
    new_regression_test["paths"]["INPUT_CONSTRUCTS"] = ""
    new_regression_test["paths"]["INPUT_MAILHOUSE"] = ""
    new_regression_test["paths"]["MAIL_POPULATION_ASSIGNMENT"] = ""

    print("############################################")
    print("# Copy regression tests test CDSA to new test #")
    print("############################################")
    print("\n")

    constructs_dir = (
        "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/"
        "assignment/CustomizableBackfill/ASSIGNMENTS/cdsa/CONSTRUCTS"
    )
    segments_dir = (
        "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/"
        "assignment/CustomizableBackfill/ASSIGNMENTS/cdsa/SEGMENTS"
    )

    constructs = (
        "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/"
        "assignment/CustomizableBackfill/ASSIGNMENTS/cdsa/constructs.csv"
    )
    segments = (
        "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/"
        "assignment/CustomizableBackfill/ASSIGNMENTS/cdsa/segments.csv"
    )
    cells = (
        "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/"
        "assignment/CustomizableBackfill/ASSIGNMENTS/cdsa/cells.csv"
    )
    campaign = (
        "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/"
        "assignment/CustomizableBackfill/ASSIGNMENTS/cdsa/campaign.csv"
    )
    hadnshakes = (
        "s3://memberanalytics-data-out-prod/REGRESSION_TESTS/"
        "assignment/CustomizableBackfill/ASSIGNMENTS/cdsa/handshakes.csv"
    )

    cdsa = [
        constructs_dir,
        segments_dir,
        constructs,
        segments,
        cells,
        campaign,
        hadnshakes,
    ]

    for path in cdsa:
        bucket, source = split_path_bucket_key(path)
        destination = source.replace("CustomizableBackfill", cdsa_name)

        print("Clean %s" % destination)
        s3_delete(bucket, destination, allowed_paths)

        print("Copy %s to %s" % (source, destination))
        s3_copy(bucket, source, destination)

    keys = [
        "assignment",
        "coupon_creation",
        "qc",
        "regression_test",
        "shared",
        "sizing",
        "subset",
    ]

    for key in keys:
        new_regression_test[key] = customizable_backfill[key]

    print("Updated config file")
    yaml_content = yaml.dump(
        new_regression_test,
        indent=6,
        default_flow_style=False,
        sort_keys=False,
    )
    with open(
        os.path.join(
            dir_path,
            "../../../../pipelines/assignment/regression_tests/conf/"
            "{name}_config.yml".format(name=test_name),
        ),
        "w",
    ) as ymlfile:
        ymlfile.write(yaml_content)

    print("######################################")
    print("# Please copy the new config to git. #")
    print("######################################")
    print("\n")
    print("#####################################")
    print(" Next step is to create a new CDSA. #")
    print("#####################################")
