import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel


def base(s):
    '''
    Function calculates basic features per category

    Args:
        s: Square object
    '''
    base_feats = s.frames['detail']['def'] \
                  .groupby(s.category, s.description) \
                  .agg(F.countDistinct('MBRSHP_SID').alias('QTY_MBRS'),
                       F.countDistinct('PURCH_DT', 'MBRSHP_SID').alias('TRIPS'),
                       F.sum('QTY_IN_UNITS').alias('UNITS'),
                       F.sum('EXTENDED_PRC_AMT').alias('SALES'))

    deduped = base_feats.groupby(s.category).agg(F.max(s.description).alias(s.description))
    base_feats = base_feats.join(deduped, [s.category, s.description], 'inner')

    s.feature_join(base_feats)

def unit_retail_price(s):
    '''
    Calculates unit retail price per item and joins to category square.

    Args:
        s: Square object
    '''
    urp = s.frames['detail']['def'] \
           .groupby(s.category) \
           .agg(F.sum('NORMAL_PRC_AMT').alias('NORMAL_PRC_AMT'),
                F.sum('SALES_QTY').alias('SALES_QTY'))

    urp = urp.withColumn('UNIT_RETAIL_PRICE', urp.NORMAL_PRC_AMT / urp.SALES_QTY)

    s.feature_join(urp.select(s.category, 'UNIT_RETAIL_PRICE'))

def monthly_mbrs(s):
    '''
    Calculates the number of distinct members per month in a given category
    '''
    grouped = s.frames['detail']['def'].groupBy(s.category,
                                                F.month('PURCH_DT').alias('MONTH')) \
                                       .agg(F.countDistinct('MBRSHP_SID').alias('COUNT'))

    # grouped.persist(StorageLevel.DISK_ONLY)
    final = grouped.groupBy(s.category).pivot('MONTH').sum('COUNT')

    names = (name for name in final.schema.names if name != s.category)

    for name in names:
        final = final.withColumnRenamed(name, 'QTY_MBRS_' + name)

    s.feature_join(final)

def all_members(s):
    '''
    Get a count of all members over time period
    '''

    agged = s.frames['detail']['def'].agg(F.countDistinct('MBRSHP_SID').alias('ALL_MEMBERS'))

    # agged.persist(storageLevel=StorageLevel.DISK_ONLY)

    return F.lit(agged.collect()[0][0])

def per(s):
    '''
    Calculates "per" based features and applies to category square using feature_column.
    These are calculated off of the base features.

    Casted as doubles to ensure non-integer division

    Args:
        s: Square object
    '''
    s.feature_column('UNITS_PER_TRIP', s.square.UNITS.cast('double') / s.square.TRIPS)
    s.feature_column('UNITS_PER_MBR', s.square.UNITS.cast('double') / s.square.QTY_MBRS)
    s.feature_column('TRIPS_PER_MBR', s.square.TRIPS.cast('double') / s.square.QTY_MBRS)
    s.feature_column('PENETRATION_RATE', s.square.QTY_MBRS.cast('double') / all_members(s))

def add_ah_to_article(s):
    '''
    Adds AH4_CD, AH4_DESC, AH5_CD, and AH5_DESC to article DNA

    Args:
        s: Square object
    '''
    item_with_brand = s.frames['item']
    desc = item_with_brand.select('ARTICLE_NBR', 'AH4_CD', 'AH4_DESC', 'AH5_CD', 'AH5_DESC')

    s.feature_join(desc)
