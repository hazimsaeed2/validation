
import logging

from memberdna.source_etl.utils.utility import *

def main(spark, data_paths, config_validation):

    logging.info('Starting processing table member')

    source_path = data_paths['source']['member']

    cast_sql = '''
    select
         cast(_c0 as int) as MBRSHP_SID
        ,_c1 as MBRSHP_TYPE_ID
        -- remove leading zeros
        ,cast(regexp_replace(trim(_c2), "^0+", "") as decimal(5,2)) as MBRSHP_FEE_INC
        ,_c3 as MBRSHP_SUB_TYPE
        ,cast(_c4 as date) as MBRSHP_ENR_DT
        ,cast(_c5 as date) as MBRSHP_EXP_DT
        ,cast(_c6 as date) as MBRSHP_RNWL_DT
        ,_c7 as RWDS_MBR_IND
    from df
    '''
    dest_path = data_paths['intermediate']['member']
    repartition_val = 1

    createSchemaParquet(spark, source_path, config_validation, 'member', cast_sql, dest_path,
        repartition_val, header=False)
