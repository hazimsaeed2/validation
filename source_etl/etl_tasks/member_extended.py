import logging

from memberdna.source_etl.utils.utility import *

def main(spark, data_paths, config_validation):

    logging.info('Starting processing table member_extended')

    source_path = data_paths['source']['member_extended']

    cast_sql = '''
    select
         cast(MBRSHP_NBR as string) as MBRSHP_NBR
        ,cast(MBRSHP_SID as int) as MBRSHP_SID
        ,cast(MBRSHP_TYPE_ID as int) as MBRSHP_TYPE_ID
        ,cast(MBRSHP_FEE_INC as decimal(10,3)) as MBRSHP_FEE_INC
        ,cast(MBRSHP_SUB_TYPE as string) as MBRSHP_SUB_TYPE
        ,to_date(MBRSHP_ENR_DT, "yyyy-MM-dd") as MBRSHP_ENR_DT
        ,to_date(MBRSHP_EXP_DT, "yyyy-MM-dd") as MBRSHP_EXP_DT
        ,to_date(MBRSHP_RNWL_DT, "yyyy-MM-dd") as MBRSHP_RNWL_DT
        ,cast(RWDS_MBR_IND as string) as RWDS_MBR_IND
        ,to_date(RWDS_MBR_ENR_DT, "yyyy-MM-dd") as RWDS_MBR_ENR_DT
        ,cast(CLUB_OF_FREQUENCY as int) as CLUB_OF_FREQUENCY
        ,cast(MKT_CD as string) as MKT_CD
        ,cast(AUTO_RNWL_IND as string) as AUTO_RNWL_IND
        ,to_date(ER_SIGNUP_DT, "yyyy-MM-dd") as ER_SIGNUP_DT
        ,cast(HOME_ZIP_CD as string) as HOME_ZIP_CD
        ,cast(SIC_CD as int) as SIC_CD
        ,cast(GRP_AFFIL_ID as string) as GRP_AFFIL_ID
        ,cast(HH_SID as int) as HH_SID
        ,cast(trim(PRI_SUPP_FHH_IND) as int) as PRI_SUPP_FHH_IND
    from df
    '''
    dest_path = data_paths['intermediate']['member_extended']
    repartition_val = 20

    createSchemaParquet(spark, source_path, config_validation, 'member_extended', cast_sql, dest_path,
        repartition_val, sep=',')
