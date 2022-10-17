import logging

from memberdna.source_etl.utils.utility import *
import memberdna.source_etl.utils.validations_ETL as validations


def main(spark, data_paths, config_validation):

    logging.info('Starting processing table detail_isnr_fiscal')

    detail_fiscal = spark.read.parquet(
        data_paths['intermediate']['detail_fiscal'])

    filter_in_store = detail_fiscal['SALES_CTGRY_CD'] == '03'
    filter_no_returns = detail_fiscal['SALES_QTY'] > 0
    # still saw returns after prior filter
    filter_no_returns_deli = detail_fiscal['QTY_IN_UNITS'] > 0

    filters = filter_in_store & filter_no_returns & filter_no_returns_deli

    detail_isnr_fiscal = detail_fiscal.filter(filters)

    validations.validate_table(
        spark,
        'intermediate',
        'detail_isnr_fiscal',
        config_validation,
        detail_isnr_fiscal
    )

    logging.info('Saving the intermediate file ' +
                 data_paths['intermediate']['detail_isnr_fiscal'])
    detail_isnr_fiscal.repartition('FISCAL_WEEK_END').write.parquet(
        data_paths['intermediate']['detail_isnr_fiscal'],
        partitionBy='FISCAL_WEEK_END', mode='overwrite')
