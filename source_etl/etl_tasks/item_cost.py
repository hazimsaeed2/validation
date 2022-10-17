import logging

from memberdna.source_etl.utils.utility import *

def main(spark, data_paths, config_validation):

    logging.info('Starting processing table item_cost')

    source_path = data_paths['source']['item_cost']

    cast_sql = '''
    select
         ARTICLE_NBR
        ,cast(SITE_NBR as int) as SITE_NBR
        ,cast(DIST_CHANNEL as int) as DIST_CHANNEL
        ,cast(SITE_LANDED_COST as double) as SITE_LANDED_COST
        ,cast(EFF_DT as date) as EFF_DT
        ,cast(EXP_DT as date) as EXP_DT
    from df
    '''
    dest_path = data_paths['intermediate']['item_cost']
    repartition_val = 1

    createSchemaParquet(spark, source_path, config_validation, table_nbame, cast_sql, dest_path, repartition_val,
        sep=',', quote="'")
