import yaml

class JobManager(object):
    """
    Primary Manager Class for pipeline Jobs

    Handles spark, configruation, and data management
    for pipeline jobs.
    """

    def __init__(self, spark, table_paths, conf_path_in):
        """
        Initialize pipeline job.

        Parameter
            jobname (str): name of job
            appname (str, opt): optional name of app (defaults to jobname)
            conf_path_in (str): path to the local config file if not
                supplied as a command line argument (optional)
        Returns:
            None
        """
        self.spark = spark
        self.config_path = conf_path_in
        self.config = self.load_config()
        self.table_paths = table_paths
        self.tables = dict()


    def load_config(self):
        """
        Read in configuration file.

        Reads in confuguration file given path
        and retuns dictionary of full config.

        Parameters:
            conf_path_in (str): local path to config file

        Returns:
            (dict): dictionary representation of config
        """

        with open(self.config_path, "r") as ymlfile:
            cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

        return cfg
    
    def read_table(self, table_name):
        self.tables[table_name] = self.spark.table(self.table_paths[table_name])
