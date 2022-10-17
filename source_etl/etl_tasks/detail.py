import logging

import pyspark.sql.functions as sqlf

from memberdna.source_etl.utils.utility import *


def main(spark, data_paths, config_validation):

    logging.info('Starting processing table detail')

    source_path = data_paths['source']['detail']

    # EXTENDED_PRC_AMT needs special selection because of a data problem
    # upstream. For more details on the bug please consult notes.txt
    # ID:1000001.
    cast_sql = '''
    select
         cast(PURCH_HDR_ID as int) as PURCH_HDR_ID
        ,cast(PURCH_DTL_ID as int) as PURCH_DTL_ID
        ,cast(PURCH_DT as date) as PURCH_DT
        ,GTIN_CD
        ,ARTICLE_NBR
        ,MC_CD
        ,cast(
            CASE WHEN DISCOUNT_TYPE_CD = "ZECM" AND SALES_CTGRY_CD = "03"
            THEN 0
            ELSE EXTENDED_PRC_AMT END
            as double
        ) as EXTENDED_PRC_AMT
        ,cast(EXTENDED_UNIT_PRC_AMT as double) as EXTENDED_UNIT_PRC_AMT
        ,cast(SALES_QTY as double) as SALES_QTY
        ,SALES_UOM
        ,cast(QTY_IN_UNITS as int) as QTY_IN_UNITS
        ,cast(NORMAL_PRC_AMT as double) as NORMAL_PRC_AMT
        ,cast(NORMAL_UNIT_PRC_AMT as double) as NORMAL_UNIT_PRC_AMT
        ,cast(REDUCTION_AMT as double) as REDUCTION_AMT
        ,SCANNED_VS_KEYED_IND
        ,DISCOUNT_TYPE_CD
        ,cast(DISCOUNT_PURCH_DTL_ID as int) as DISCOUNT_PURCH_DTL_ID
        ,VOIDED_FLAG
        ,VOIDED_PURCH_DTL_ID
        ,RSN_CD
        ,SALES_CTGRY_CD
        ,RETURN_IND
        ,REBATE_IND
        ,OFFER_ID as VECTOR_OFFER_ID
    from df
    '''

    dest_path = data_paths['intermediate']['detail']
    repartition_val = 'PURCH_DT'
    filter_cond = 'PURCH_DT >= "2016-01-01"'

    createSchemaParquet(spark, source_path, config_validation, 'detail',
                        cast_sql, dest_path, repartition_val, filter_cond,
                        del_dup=True)
