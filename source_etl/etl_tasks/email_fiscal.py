import logging

from memberdna.source_etl.utils.utility import *
import memberdna.source_etl.utils.validations_ETL as validations


def main(spark, data_paths, config_validation):

    logging.info('Starting processing table email_fiscal')

    email = spark.read.parquet(data_paths['intermediate']['email'])
    email.registerTempTable('email')

    fiscal_days = spark.read.parquet(data_paths['intermediate']['fiscal_days'])
    fiscal_days.registerTempTable('fiscal_days')

    sql = '''
    SELECT e.*, f.FISCAL_WEEK_START, f.FISCAL_WEEK_END
    FROM email e
    JOIN fiscal_days f
    ON e.FIRST_OPEN_DATE = f.FISCAL_DAY
    '''

    email_fiscal = spark.sql(sql)

    validations.validate_table(
        spark,
        'intermediate',
        'email_fiscal',
        config_validation,
        email_fiscal
    )

    logging.info('Saving the intermediate file ' +
                 data_paths['intermediate']['email_fiscal'])
    email_fiscal.repartition('FISCAL_WEEK_END').write.parquet(
        data_paths['intermediate']['email_fiscal'],
        partitionBy='FISCAL_WEEK_END', mode='overwrite')
