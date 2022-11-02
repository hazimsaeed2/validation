"""
"""
import argparse
import pprint
import time

import yaml
from memberdna.category_square.category_square.build import Build


def open_configs(args):
    """ """
    with open(args.config_path) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    with open(args.reqs_path) as reqs_file:
        reqs = yaml.load(reqs_file, Loader=yaml.FullLoader)

    return (reqs, config)


def parse(parser):
    parser.add_argument(
        "--config_path",
        type=str,
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../configs/prod/square_AH4.yaml",
        ),
        help=(
            """
            path to the config file
            """
        ),
    )

    parser.add_argument(
        "--reqs_path",
        type=str,
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../configs/prod/reqs.yaml",
        ),
        help=(
            """
            path to the path definitions file
            """
        ),
    )

    parser.add_argument("--full-mode", dest="full_mode", action="store_true")

    parser.set_defaults(full_mode=False)
    return parser.parse_args()


def main():
    parser = argparse.ArgumentParser()
    args = parse(parser)
    reqs, config = open_configs(args)
    config["full_mode"] = args.full_mode

    Build.execute(reqs, config)


if __name__ == "__main__":
    start_time = time.time()
    print(f"\n\nStarting main method at time {str(start_time)} ")
    main()
    end_time = time.time() - start_time
    print(
        f"\n\nTotal time taken for execution is {str(end_time)} seconds for no persisting method in build.py"
    )
