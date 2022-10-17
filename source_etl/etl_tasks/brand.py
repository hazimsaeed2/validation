import logging

from memberdna.source_etl.utils.utility import *


def main(spark, data_paths, config_validation):

    logging.info('Starting processing table brand')

    source_path = data_paths['source']['brand']

    cast_sql = '''
    select
         _c0 as ARTICLE_NBR
        ,_c1 as CASE_EXPRESSION
        ,_c2 as BRAND
    from df
    '''

    dest_path = data_paths['intermediate']['brand']
    repartition_val = 1

    createSchemaParquet(spark, source_path, config_validation, 'brand',  cast_sql, dest_path, repartition_val,
                        sep=',', header=False)
