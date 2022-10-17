
import logging

from memberdna.source_etl.utils.utility import *
import memberdna.source_etl.utils.validations_ETL as validations

def main(spark, data_paths, config_validation):

    logging.info('Starting processing table skeleton')

    header_fiscal = spark.read.parquet(data_paths['intermediate']['header_fiscal'])
    fwe_min = header_fiscal.agg({'FISCAL_WEEK_END': 'min'}).collect()[0][0]
    fwe_max = header_fiscal.agg({'FISCAL_WEEK_END': 'max'}).collect()[0][0]

    members = header_fiscal.select('MBRSHP_SID').distinct()

    fiscal_days = spark.read.parquet(data_paths['intermediate']['fiscal_days'])
    fiscal_weeks = fiscal_days.drop('FISCAL_DAY').distinct()
    fiscal_weeks_inscope = fiscal_weeks.filter(
        'FISCAL_WEEK_END between "{}" and "{}"'.format(fwe_min, fwe_max))

    skeleton = members.crossJoin(fiscal_weeks_inscope)

    validations.validate_table(
        spark,
        'intermediate',
        'skeleton',
        config_validation,
        skeleton,
        [validations.TestColNames, validations.TestDuplicates]
        )

    logging.info('Saving the intermediate file ' + data_paths['intermediate']['skeleton'])

    skeleton.repartition('FISCAL_WEEK_END').write.parquet( \
        data_paths['intermediate']['skeleton'],
        partitionBy='FISCAL_WEEK_END', mode='overwrite')
