
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
import logging

from memberdna.source_etl.utils.utility import *
import memberdna.source_etl.utils.validations_ETL as validations

def calculate_gas_spend_by_club(detail):
    '''
    Calculates the total gas spend at each club.

    Args:
        detail (spark.sql.Dataframe): the detail table

    Returns:
        gas_spend (spark.sql.Dataframe): table with last four week gas spend per club
    '''
    detail = detail.filter((detail.SALES_CTGRY_CD == '01') & (detail.EXTENDED_PRC_AMT > 0.0)) \
                   .groupby('SITE_NBR', 'FISCAL_WEEK_END') \
                   .agg(F.sum(detail.EXTENDED_PRC_AMT).alias('GAS_SPEND'))

    window = create_window('SITE_NBR', 4)

    gas_spend = detail.withColumn('EPOCH', to_epoch(detail.FISCAL_WEEK_END)) \
                      .withColumn('L4W_GAS_SPEND', F.sum('GAS_SPEND').over(window))

    return gas_spend


def club_gas_start_week(gas_spend):
    '''
    Returns a two column with SITE_NBR and the first FISCAL_WEEK_END
    that it had gas.

    Args:
        gas_spend (spark.sql.Dataframe): table with last four week gas spend per club

    Returns:
        first_week_has_gas (spark.sql.Dataframe): Dataframe with club and gas start week
    '''
    last_four_week_spend_threshold = 10000
    gas_spend = gas_spend.filter(gas_spend.L4W_GAS_SPEND >= last_four_week_spend_threshold)
    first_week_has_gas = gas_spend.groupby('SITE_NBR').agg(F.min('FISCAL_WEEK_END').alias('FIRST_FW_HAS_GAS'))
    return first_week_has_gas


def add_gas_start_date(club, detail):
    '''
    Adds gas start date to club intermediate

    Args:
        club (spark.sql.Dataframe): the club intermediate
        detail (spark.sql.Dataframe): the detail intermediate
    '''
    gas_spend = calculate_gas_spend_by_club(detail)

    first_week_has_gas = club_gas_start_week(gas_spend)

    club = club.join(first_week_has_gas, ['SITE_NBR'],'left_outer')
    return club


def main(spark, data_paths,config_validation):

    logging.info('Starting processing table club')

    # Paths
    club_source_path = data_paths['source']['club']
    club_dest_path = data_paths['intermediate']['club']
    detail_path = data_paths['intermediate']['detail_fiscal']

    logging.info('Reading the input file ' + club_source_path)
    # Read Dataframes
    club = spark.read.csv(club_source_path, header=False, sep='|')
    validations.validate_table(
        spark,
        'source',
        'club',
        config_validation,
        club
        )


    detail = spark.read.parquet(detail_path)


    club.registerTempTable('club')
    cast_sql = '''
        select
            cast(_c0 as integer) as SITE_NBR
            ,cast(_c1 as string) as SITE_NAME_2
            ,cast(_c2 as string) as ADDR_LINE_2
            ,cast(_c3 as string) as CITY_NAME
            ,cast(_c4 as string) as STATE_CD
            ,cast(_c5 as string) as ZIP_CD
            ,cast(_c6 as integer) as ZN_NBR
            ,cast(_c7 as integer) as RGN_NBR
            ,cast(_c8 as string) as SITE_TYPE
            ,cast(_c9 as string) as COMP_STTS
        from club
    '''

    club = spark.sql(cast_sql)
    club = add_gas_start_date(club, detail)
    club.dropDuplicates()

    validations.validate_table(
        spark,
        'intermediate',
        'club',
        config_validation,
        club
        )

    logging.info('Saving the intermediate file ' + club_dest_path)
    # Write club
    club.repartition(1).write.parquet(club_dest_path, mode='overwrite')
