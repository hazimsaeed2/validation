import logging

from memberdna.source_etl.utils.utility import *
import memberdna.source_etl.utils.validations_ETL as validations


def main(spark, data_paths, config_validation):

    logging.info('Starting processing table coupon_clip_fiscal')

    coupon_clip = spark.read.parquet(data_paths['intermediate']['coupon_clip'])
    coupon_clip.registerTempTable('coupon_clip')

    fiscal_days = spark.read.parquet(data_paths['intermediate']['fiscal_days'])
    fiscal_days.registerTempTable('fiscal_days')

    sql = '''
    SELECT c.*, f.FISCAL_WEEK_START, f.FISCAL_WEEK_END
    FROM coupon_clip c
    JOIN fiscal_days f
    ON c.EVENTDATETIME = f.FISCAL_DAY
    '''

    coupon_clip_fiscal = spark.sql(sql)

    validations.validate_table(
        spark,
        'intermediate',
        'coupon_clip_fiscal',
        config_validation,
        coupon_clip_fiscal,
        # coupon_clip_fiscal.py is last job called in run.py
        # We want to archive the final stats, so set to True for this last job
        archive=True
    )

    logging.info('Saving the intermediate file ' +
                 data_paths['intermediate']['coupon_clip_fiscal'])

    coupon_clip_fiscal.repartition('FISCAL_WEEK_END').write.parquet(
        data_paths['intermediate']['coupon_clip_fiscal'],
        partitionBy='FISCAL_WEEK_END', mode='overwrite')
