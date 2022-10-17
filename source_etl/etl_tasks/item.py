import logging

from memberdna.source_etl.utils.utility import *

def main(spark, data_paths, config_validation):

    logging.info('Starting processing table item')

    source_path = data_paths['source']['item']

    cast_sql = '''
    select
        GTIN_CD
        ,ARTICLE_DESC
        ,ARTICLE_NBR
        ,MCH4_CD
        ,MCH4_DESC
        ,MCH3_CD
        ,MCH3_DESC
        ,MCH2_CD
        ,MCH2_DESC
        ,MCH1_CD
        ,MCH1_DESC
        ,MC_CD
        ,MC_DESC
        ,AH1_CD
        ,AH1_DESC
        ,AH2_CD
        ,AH2_DESC
        ,AH3_CD
        ,AH3_DESC
        ,AH4_CD
        ,AH4_DESC
        ,AH5_CD
        ,AH5_DESC
        ,AH6_CD
        ,AH6_DESC
        ,BRAND_TYPE
        ,cast(EFF_DT as date) as EFF_DT
        ,cast(EXP_DT as date) as EXP_DT
        ,ORDR_AS as REPLACEMENT_ARTICLE
    from df
    '''
    dest_path = data_paths['intermediate']['item']
    repartition_val = 1

    createSchemaParquet(spark, source_path, config_validation, 'item', cast_sql, dest_path, repartition_val,
        header=True)
