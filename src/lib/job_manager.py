from lib.misc import (
    get_last_fiscal_weekend,
    get_latest_path,
    today_helper,
)
import yaml

def load_config(config_path, spark=None):
    """
    Read the config file, replace date placeholders in paths
    and check if this is a single task run

    Args:
        args - object containing the command line arguments as attrubutes

    Returns:
        config - dictionary structure containign the config file
    """
    with open(config_path) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    end_date_str = config["club_square_config"].get("max_date")

    if not end_date_str:
        _, end_date_str = get_last_fiscal_weekend()

    end_date_str_no_dashes = end_date_str.replace("-", "")

    # Quotient ID uses the latest data instead of last fiscal weekend
    try:
        config["data_paths"]["source"]["quotient_id"] = get_latest_path(
            spark,
            config["data_paths"]["source"]["quotient_id"]
        )
    except:
        # For tests, the get_latest_path should raise an exception because the
        # file will not have the variable date portion, which is ignored here
        pass

    for data_name, data_path in config["data_paths"]["source"].items():
        config["data_paths"]["source"][data_name] = data_path % {
            "curr_date": end_date_str_no_dashes
        }

    return config

def split_config(config):
    """
    Read the config file and split into each indiviual sections
    Args:
        args - object containing the command line arguments as attrubutes

    Returns:
        config - returns the indivial sections of the config.
    """

    data_paths = config["data_paths"]
    club_square_config = config["club_square_config"]
    config_validation = config["validation"]

    data_paths["archived"] = (
        data_paths["archived"] + "/" + "{:%Y-%m-%d}".format(today_helper())
    )
    return data_paths, club_square_config, config_validation