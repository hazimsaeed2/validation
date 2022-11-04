"""
Main class for the category square
"""

import datetime as dt

from pe_memberdna.dna.category.lib.loader import Loader
from pyspark.storagelevel import StorageLevel

import pe_memberdna.lib.misc as misc


class Square(object):
    def __init__(self, reqs, config):
        if len({"start", "end"} - config.keys()) == 2:
            config["start"], config["end"] = misc.get_last_fiscal_weekend()
        if len({"start", "end"} - config.keys()) == 1:
            raise Exception(
                "In the config file you have to either specify both start_date and end_date or neighter of them"
            )

        self.config = config
        self.reqs = reqs
        self.category = config["category"]
        self.description = config["description"]
        self.frames = Loader.load(reqs, config)
        self.square = self.__init_square()
        self.spark = Loader.spark

    def __init_square(self):
        """
        Initializes the category square by selecting the category.

        Returns:
            square: the base of the category square
        """
        return (
            self.frames["detail"]["def"].select(self.category).dropDuplicates()
        )

    def __time_string(self):
        """
        Returns string format of current time. Used for unique cube writes.

        Returns:
            String of the form yyyy-MM-dd_HH:mm:00
        """
        return str(dt.datetime.now().replace(second=0, microsecond=0)).replace(
            " ", "_"
        )

    def __square_name(self, path, full_mode=False):
        """
        Formats the output square name with the category and unique time it was built.
        Format is defined here:
            {category}_{start}_{end}_{time}
        Where the start and end are the filters applied to the detail during the load,
        and time is the time in which the category dna/square was produced.

        Returns:
            Unique string name of the category square
        """
        if full_mode:
            return "{}/{}/CATEGORY_DNA_{}".format(path, self.category, "full")
        else:
            return "{}/{}/CATEGORY_DNA_{}_{}_{}".format(
                path,
                self.category,
                self.config["start"],
                self.config["end"],
                self.__time_string(),
            )

    def __write(self, path):
        """
        Writes the square to dev directory in csv and parquet format.
        """
        print("************* Entered  __write method")
        square_path = self.__square_name(path)
        parquet_path = square_path + "/PARQUET/"
        csv_path = square_path + "/CSV/"
        #self.square.persist(storageLevel=StorageLevel.DISK_ONLY)
        self.square.repartition(1).write.parquet(parquet_path)
        self.square.repartition(1).write.option("emptyValue", None).option(
            "nullValue", None
        ).csv(csv_path, header=True, escape='"')
        if self.config["full_mode"]:
            print("************* inside full_mode if condition")
            full_square = self.__square_name(
                path, full_mode=self.config["full_mode"]
            )
            full_parquet = full_square + "/PARQUET/"
            full_csv = full_square + "/CSV/"
            self.square.repartition(1).write.parquet(
                full_parquet, mode="overwrite"
            )
            self.square.repartition(1).write.option("emptyValue", None).option(
                "nullValue", None
            ).csv(full_csv, header=True, escape='"', mode="overwrite")
        print("************* Exiting from __write method")

    def feature_join(self, df):
        """
        NOTE: MUTATES THE SQUARE IN PLACE

        Joins any feature dataframe to the cateogry square with left outer
        join on category.

        Args:
            df: The dataframe with the features
        """
        self.square = self.square.join(df, self.category, "left_outer")
        self.square = self.square.dropDuplicates([self.category])

    def feature_column(self, new_col, func):
        """
        NOTE: MUTATES SQUARE IN PLACE

        Applies features with withColumn using the provided func.

        Args:
            new_col: Name of the new feature column
            func: Function to build the new feature column.
        """
        self.square = self.square.withColumn(new_col, func)

    def write(self):
        """
        Writes the category square to S3 depending on what mode it is in.
        """
        self.__write(self.reqs["out"]["path"])
