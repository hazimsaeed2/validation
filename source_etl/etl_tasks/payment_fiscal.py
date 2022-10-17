
import logging

from memberdna.source_etl.utils.utility import *
import memberdna.source_etl.utils.validations_ETL as validations

def main(spark, data_paths, config_validation):

    logging.info('Starting processing table payment fiscal')

    payment = spark.read.parquet(data_paths['intermediate']['payment'])
    payment.registerTempTable('payment')

    header_fiscal = spark.read.parquet(data_paths['intermediate']['header_fiscal'])
    header_fiscal.registerTempTable('header_fiscal')

    header_fields = ['PURCH_HDR_ID','PURCH_DT','FISCAL_WEEK_START','FISCAL_WEEK_END']
    payment_fiscal = payment.join(header_fiscal[header_fields], 'PURCH_HDR_ID')

    validations.validate_table(
        spark,
        'intermediate',
        'payment_fiscal',
        config_validation,
        payment_fiscal,
        [
            validations.CompareColAggregatesPrior,
            validations.TestColNames,
            validations.TestDuplicates,
            validations.TestOutlierDays
        ]
        )

    logging.info('Saving the intermediate file ' + data_paths['intermediate']['payment_fiscal'])

    payment_fiscal.repartition('FISCAL_WEEK_END').write.parquet( \
        data_paths['intermediate']['payment_fiscal'],
        partitionBy='FISCAL_WEEK_END', mode='overwrite')
