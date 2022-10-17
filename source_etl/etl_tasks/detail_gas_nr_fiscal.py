import logging

import memberdna.source_etl.utils.validations_ETL as validations


def filter_gas_nr(detail_fiscal):
    '''
    Filters the detail table for only positive sales (no returns).
    And filters out transactions at BJs cafe and other minor exclusions
    (established by the client).
    '''
    mc_cds = ['402030190', '402030191', '203010098']

    return detail_fiscal.filter((detail_fiscal.EXTENDED_PRC_AMT > 0) &
                                (detail_fiscal.QTY_IN_UNITS > 0) &
                                (~detail_fiscal.MC_CD.isin(mc_cds)))


def main(spark, data_paths, config_validation):

    logging.info('Starting processing table detail_gas_nr_fiscal')

    detail_fiscal = spark.read.parquet(
        data_paths['intermediate']['detail_fiscal'])

    detail_gas_nr_fiscal = filter_gas_nr(detail_fiscal)

    validations.validate_table(
        spark,
        'intermediate',
        'detail_gas_nr_fiscal',
        config_validation,
        detail_gas_nr_fiscal
    )

    logging.info('Saving the intermediate file ' +
                 data_paths['intermediate']['detail_gas_nr_fiscal'])

    detail_gas_nr_fiscal.repartition('FISCAL_WEEK_END') \
                        .write \
                        .partitionBy('FISCAL_WEEK_END') \
                        .parquet(data_paths['intermediate']['detail_gas_nr_fiscal'], mode='overwrite')
