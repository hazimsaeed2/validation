
import logging

from memberdna.source_etl.utils.utility import *

def main(spark, data_paths, config_validation):
    logging.info('Starting processing table header')

    source_path = data_paths['source']['header']

    cast_sql = '''
    select
         cast(PURCH_HDR_ID as int) as PURCH_HDR_ID
        ,cast(MBRSHP_SID as int) as MBRSHP_SID
        ,cast(SITE_NBR as int) as SITE_NBR
        ,cast(PURCH_DT as date) as PURCH_DT
        ,SALES_CHANNEL_ID
        ,cast(TOT_SALES_AMT as double) as TOT_SALES_AMT
        ,cast(TAX_AMT as double) as TAX_AMT
        ,PURCHASE_TM
    from df
    '''
    dest_path = data_paths['intermediate']['header']
    repartition_val = 'PURCH_DT'
    filter_cond = 'PURCH_DT >= "2012-09-01"'

    createSchemaParquet(spark, source_path, config_validation, 'header', cast_sql, dest_path, repartition_val, filter_cond)
