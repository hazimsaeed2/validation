import argparse
import logging
import os

import pe_memberdna.etl.utils.validations_ETL as validations
from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table census_tract")

    logging.info(
        "Reading the input file " + data_paths["source"]["member_tract"]
    )
    member_tract = job.spark.read.csv(
        data_paths["source"]["member_tract"], header=True
    )

    validations.validate_table(
        job.spark, "source", "member_tract", config_validation, member_tract
    )

    member_tract = member_tract.withColumnRenamed("TRACT", "CENSUS_TRACT")
    # In some cases 1 member has 2 rows in member_tract, and we need to drop them
    # to prevent duplicate rows.
    member_tract = member_tract.dropDuplicates(["MBRSHP_SID"])

    distance = job.spark.read.csv(
        data_paths["source"]["distance"], header=True
    )
    distance = distance.withColumnRenamed("Census_Tract", "CENSUS_TRACT")

    census_tract = member_tract.join(distance, "CENSUS_TRACT", "left_outer")
    census_tract.registerTempTable("census_tract")

    cast_sql = """
    select
         CENSUS_TRACT
        ,cast(MBRSHP_SID as int) as MBRSHP_SID
        ,MEMBERID
        ,MEMTYPE
        ,cast(CLUSTER as int) as CLUSTER
        ,ZIP
        ,cast(LONGITUDE as double) as LONGITUDE
        ,cast(LATITUDE as double) as LATITUDE
        ,UPDATE
        ,cast(Census_Tract_Population as int) as CENSUS_TRACT_POPULATION
        ,cast(Census_Tract_Households as int) as CENSUS_TRACT_HOUSEHOLDS
        ,cast(Lat as double) as TRACT_LATITUDE
        ,cast(Lon as double) as TRACT_LONGITUDE
        ,ZIP_CODE
        ,cast(Zip_Code_Population as int) as ZIP_CODE_POPULATION
        ,cast(Zip_Code_Households as int) as ZIP_CODE_HOUSEHOLDS
        ,cast(Zip_Lat as double) as ZIP_LATITUDE
        ,cast(Zip_Lon as double) as ZIP_LONGITUDE
        ,cast(Zip_Dist as double) as ZIP_DISTANCE
        ,cast(BJS_Distance as double) as BJS_DISTANCE
        ,cast(BJS_Driving_Distance as double) as BJS_DRIVING_DISTANCE
        ,cast(BJS_Drive_Time as double) as BJS_DRIVE_TIME
        ,cast(Costco_Distance as double) as COSTCO_DISTANCE
        ,cast(Costco_Driving_Distance as double) as COSTCO_DRIVING_DISTANCE
        ,cast(Costco_Drive_Time as double) as COSTCO_DRIVE_TIME
        ,cast(Sams_Distance as double) as SAMS_DISTANCE
        ,cast(Sams_Driving_Distance as double) as SAMS_DRIVING_DISTANCE
        ,cast(Sams_Drive_Time as double) as SAMS_DRIVE_TIME
        ,cast(Walmart_Distance as double) as WALMART_DISTANCE
        ,cast(Walmart_Driving_Distance as double) as WALMART_DRIVING_DISTANCE
        ,cast(Walmart_Drive_Time as double) as WALMART_DRIVE_TIME
    from census_tract
    """

    census_tract_schema = job.spark.sql(cast_sql)
    census_tract_schema.dropDuplicates()
    dest_path = data_paths["intermediate"]["census_tract"]

    validations.validate_table(
        job.spark,
        "intermediate",
        "census_tract",
        config_validation,
        census_tract_schema,
    )

    logging.info("Saving the intermediate file " + dest_path)
    census_tract_schema.repartition(30).write.parquet(
        dest_path, mode="overwrite"
    )


job = JobManager("census_tract")
parser = argparse.ArgumentParser()

parser.add_argument(
    "--config_path",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../configs/config.yaml",
    ),
    help=(
        """
        path to the config file
        """
    ),
)
args = parser.parse_args()
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths, config_validation)
