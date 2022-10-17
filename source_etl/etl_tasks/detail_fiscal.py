import logging

from memberdna.source_etl.utils.utility import *
import memberdna.source_etl.utils.validations_ETL as validations


def main(spark, data_paths, config_validation):

    logging.info('Starting processing table detail_fiscal')

    detail = spark.read.parquet(data_paths['intermediate']['detail'])

    detail.registerTempTable('detail')

    header_fiscal = spark.read.parquet(
        data_paths['intermediate']['header_fiscal'])
    header_fiscal.registerTempTable('header_fiscal')

    item = spark.read.parquet(data_paths['intermediate']['item'])
    item.registerTempTable('item')

    sql = '''
    select
         d.*
        ,h.MBRSHP_SID
        ,h.SITE_NBR
        ,h.FISCAL_WEEK_START
        ,h.FISCAL_WEEK_END
        ,i.ARTICLE_DESC
        ,i.MCH4_CD
        ,i.MCH4_DESC
        ,i.MCH3_CD
        ,i.MCH3_DESC
        ,i.MCH2_CD
        ,i.MCH2_DESC
        ,i.MCH1_CD
        ,i.MCH1_DESC
        ,i.MC_DESC
        ,i.AH1_CD
        ,i.AH1_DESC
        ,i.AH2_CD
        ,i.AH2_DESC
        ,i.AH3_CD
        ,i.AH3_DESC
        ,i.AH4_CD
        ,i.AH4_DESC
        ,i.AH5_CD
        ,i.AH5_DESC
        ,i.AH6_CD
        ,i.AH6_DESC
        ,i.BRAND_TYPE
        ,i.EFF_DT
        ,i.EXP_DT
    from detail d
    join header_fiscal h
        on  d.PURCH_DT = h.PURCH_DT and
            d.PURCH_HDR_ID = h.PURCH_HDR_ID
    left join item i
        on  d.GTIN_CD = i.GTIN_CD and
            d.ARTICLE_NBR = i.ARTICLE_NBR
    '''
    detail_fiscal = spark.sql(sql)

    validations.validate_table(
        spark,
        'intermediate',
        'detail_fiscal',
        config_validation,
        detail_fiscal
    )

    logging.info('Saving the intermediate file ' +
                 data_paths['intermediate']['detail_fiscal'])

    detail_fiscal.repartition('FISCAL_WEEK_END').write.parquet(
        data_paths['intermediate']['detail_fiscal'],
        partitionBy='FISCAL_WEEK_END', mode='overwrite')
