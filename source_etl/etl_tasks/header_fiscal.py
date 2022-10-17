
import logging

from memberdna.source_etl.utils.utility import *
import memberdna.source_etl.utils.validations_ETL as validations

def main(spark, data_paths, config_validation):
    logging.info('Starting processing table header_fiscal')

    header = spark.read.parquet(data_paths['intermediate']['header'])
    header.registerTempTable('header')

    fiscal_days = spark.read.parquet(data_paths['intermediate']['fiscal_days'])
    fiscal_days.registerTempTable('fiscal_days')

    sql = '''
    select h.*, f.FISCAL_WEEK_START, f.FISCAL_WEEK_END
    from header h
    join fiscal_days f
        on  h.PURCH_DT = f.FISCAL_DAY
    '''
    header_fiscal = spark.sql(sql)

    validations.validate_table(
        spark,
        'intermediate',
        'header_fiscal',
        config_validation,
        header_fiscal
        )

    logging.info('Saving the intermediate file ' + data_paths['intermediate']['header_fiscal'])

    header_fiscal.repartition('FISCAL_WEEK_END').write.parquet( \
        data_paths['intermediate']['header_fiscal'],
        partitionBy='FISCAL_WEEK_END', mode='overwrite')
