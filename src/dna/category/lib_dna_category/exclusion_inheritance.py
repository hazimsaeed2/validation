import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

'''
Business Context for algorithm

Our modeling algorithms work very hard to pick coupons and offers
from the category most relevant to each member. however, there are preexisting
rules which do not allow for coupons and offers in various categories.

This logic was designed to ensure that if a category is excluded at a certain level
(ah4, or ah5, etc) that the effect of that exclusion trickles down to lower levels
of granularity, (e.g. if i exclude an ah4 category and run category dna at the ah5
level, then every ah5 category that the excluded ah4 category maps to should also
be excluded.

We want, moreover, to be able to provide lower level granularity exceptions to
exclusions.  For example, if i specify an ah4 category exclusion, and that
ah4 category exclusion maps to 8 ah5 cateogires, it might be the case that i want
only 7 of the 8 excluded.  In this case I would use the same template to
specify that the 8th ah5 category be included as an exception to inheriting from
that ah4 exclusion.

I used ah4 and ah5 as examples, but these sorts of exceptions can be supplied
at any level of the hierarchy, down to the article level if you so choose.
'''

'''
More technical overview of algorithm

The goal is to ingest a template which provides exclusions at
any level and pass those exclusions down to the level of desired
category_square output (s.category).

We include by default and exclude if the template INCLUDE_OR_EXCLUDE
field reads EXCLUDE.  INCLUDE should only be passed to this field
if there is a category that you DO NOT WANT EXCLUDED  that will be
excluded from an exclusion specified at a higher level of granularity.
For example if you exclude the ah4 category tires which maps to 8 tire
brands at ah5, but you don't want to exclude michelin at ah5, you would
specify michelin as an INCLUDE at the ah5 level.

We handle this by abstracting the process for two category levels,
a predecessor_cd and a successor_cd and mutating those values along
the hierarchy until we have running_output for the level of s.category.

Rough qualitative description of algorithm:

1.  For a given pair, filter running_output for category_type==predecessor_cd
2.  From the template, filter for category_type==successor_cd
3.  From item, we get a map from predecessor_cd to successor_cd.  We filter
    out all successor_cds that we have left from step 2.
4.  We join this filtered_map to the output from step 1 to get the more
    granular exclusions
5.  We split the output from step 2 by include_or_exclude column into
    include and exclude.
6.  We append the exclusion output from step 5 to the output from step 4
    (the inclusion output we've handled by not including them in the filtered map)
7.  We append this to the running_output and set predecessor_cd=successor_cd and
    continue steps 1-6 until predecessor_cd=s.category
8.  We filter running_output for s.category and join it to the existing square
'''

column_order = ['CATEGORY_TYPE',
                'CATEGORY_CD',
                'CATEGORY_DESCRIPTION',
                'INCLUDE_OR_EXCLUDE',
                'EXCLUSION_TYPE',
                'EXCLUSION_SUBTYPE',
                'SEASON_MONTH_1',
                'SEASON_MONTH_2',
                'SEASON_MONTH_3',
                'SEASON_MONTH_4',
                'SEASON_MONTH_5',
                'SEASON_MONTH_6',
                'SEASON_MONTH_7',
                'SEASON_MONTH_8',
                'SEASON_MONTH_9',
                'SEASON_MONTH_10',
                'SEASON_MONTH_11',
                'SEASON_MONTH_12']


# if there are changes to the hierarchy, or you want to apply this algorithm to the mc categories,
# make those updates here by changing starting category and levels and descriptions

starting_category = 'AH4_CD'

levels = {'AH4_CD': 'AH5_CD',
          'AH5_CD': 'ARTICLE_NBR',
          'ARTICLE_NBR': 'BRAND_CD'}

descriptions = {'AH4_CD': 'AH4_DESC',
                'AH5_CD': 'AH5_DESC',
                'AH6_CD': 'AH6_DESC',
                'ARTICLE_NBR': 'ARTICLE_DESC',
                'BRAND_CD': 'BRAND_DESC'}


def filter_rename(df, cd):
    '''
    Filters CATEGORY_TYPE based on cd and renames CATEGORY_CD
    to cd
    '''

    filt = df['CATEGORY_TYPE'] == cd

    filtered = df.filter(filt) \
                 .withColumnRenamed('CATEGORY_CD', cd)

    return filtered


def preprocess(running_output, template, predecessor_cd, successor_cd):
    '''
    Filters running output by category_type==predecessor_cd and the
    hiercarchy template cateogory_type==successor_cd. Renames each of those filtered
    dfs CATEGORY_CD columns predecessor_cd and successor_cd respectively.

    Puts those filtered outputs in a dictionary of the following form
    {predecessor_cd: filtered_running_output,
     successor_cd: filtered_template}
    '''

    preprocessed = {predecessor_cd: filter_rename(running_output,
                                                  predecessor_cd),
                    successor_cd: filter_rename(template,
                                                successor_cd)}
    return preprocessed


def include_exclude(successor_df):
    '''
    For the next level in the hierarchy, we have both inclusion and exclusion exceptions,
    we need to divide those here.
    '''

    inc = successor_df['INCLUDE_OR_EXCLUDE'] == 'include'
    exc = successor_df['INCLUDE_OR_EXCLUDE'] == 'exclude'

    return {'include': successor_df.filter(inc),
            'exclude': successor_df.filter(exc)}


def form_map(item, predecessor_cd, successor_cd):
    '''
    We select two successive categories in the item file granularity  hierarchy:

    For example: ah4->ah5->->article (this is the hierarchy used at present, but can
    be changed to the mc's or any other hierarchy you so choose)

    And drop duplicates.  To be used as a map to get from the first level to the more
    granular one.

    NOTE: we should always be using _CD, since _DESC is at risk for non-uniqueness
    '''

    mapping = item.select(predecessor_cd,
                          successor_cd,
                          descriptions[successor_cd]).dropDuplicates()

    return mapping


def filter_mapping(mapping, preprocessed, successor_cd):
    '''
    We only want to map to successors that are going to inherit, so
    we left anti join the mapping to the successor_df to ensure we only map to
    those successor_cds.

    mapping - df - map from one category_type to the next in the hierarchy
    preprocessed - dictionary - dict of dataframes keyed by category_type
    successor_cd - string - the level in the hierarchy ('AH4_CD', 'AH5_CD', etc)
    '''

    successor_df = preprocessed[successor_cd].select(successor_cd) \
        .dropDuplicates()

    broadcasted = F.broadcast(successor_df)

    filtered_map = mapping.join(broadcasted,
                                successor_cd,
                                'left_anti')

    return filtered_map


def all_mapping(preprocessed, item, predecessor_cd, successor_cd):
    '''
    Both forms the map from the item file and filters based on
    exceptions at the successor_cd level
    '''

    mapping = form_map(item,
                       predecessor_cd,
                       successor_cd)

    filtered_map = filter_mapping(mapping,
                                  preprocessed,
                                  successor_cd)

    return filtered_map


def map_to_successor(preprocessed, predecessor_cd, filtered_mapping):
    '''
    Take the less granular predecessor_cd in the pair and join it to the more
    granular one (e.g. ah4 to ah5)
    '''

    less_granular = preprocessed[predecessor_cd].drop('CATEGORY_DESCRIPTION')

    more_granular = less_granular.join(filtered_mapping,
                                       predecessor_cd,
                                       'inner')

    # need to fill in the new tables' 'CATEGORY_TYPE' column
    more_granular_name = filtered_mapping.drop(predecessor_cd).schema.names[0]

    return more_granular.drop(predecessor_cd).withColumn('CATEGORY_TYPE',
                                                         F.lit(more_granular_name))


def append_exclusions(excluded, more_granular, successor_cd):
    '''
    Once we have the more granular dataframe and the filtered map has been applied,
    we can bring the exclusions from the input template back in
    '''
    more_granular = more_granular.withColumnRenamed(
        successor_cd, 'CATEGORY_CD')
    more_granular = more_granular.withColumnRenamed(
        descriptions[successor_cd], 'CATEGORY_DESCRIPTION')
    excluded = excluded.withColumnRenamed(successor_cd, 'CATEGORY_CD')

    # reorder so unionAll will work
    excluded = excluded.select(*column_order)
    more_granular = more_granular.select(*column_order)

    final_successor = excluded.unionAll(more_granular)

    return final_successor


def append_successor(running_output, predecessor_cd, final_successor):
    '''
    Now that the more_granular exclusions have been processed, we can
    append them to the running output either for completion or to be
    used as input for the next level of granularity exclusions
    '''

    original = running_output.withColumnRenamed(predecessor_cd, 'CATEGORY_CD')
    original = running_output.select(*column_order)

    return original.unionAll(final_successor)


def produce_final_successor(preprocessed, filtered_map, predecessor_cd, successor_cd):
    '''
    First use the filtered map to map to the new level of granularity and then
    append the new exclusions at that level from the template
    '''

    more_granular = map_to_successor(preprocessed,
                                     predecessor_cd,
                                     filtered_map)
    inc_exc = include_exclude(preprocessed[successor_cd])

    final_successor = append_exclusions(inc_exc['exclude'],
                                        more_granular,
                                        successor_cd)

    return final_successor


def process_pair(running_output, template, item, predecessor_cd, successor_cd):
    '''
    Takes the running output and a hierarchy pair: predecessor_cd and successor_cd
    and updates running_output with exclusions at the successor_cd granularity
    '''

    # preprocess
    preprocessed = preprocess(running_output,
                              template,
                              predecessor_cd,
                              successor_cd)
    # map
    filtered_map = all_mapping(preprocessed,
                               item,
                               predecessor_cd,
                               successor_cd)

    # get final successor
    final_successor = produce_final_successor(preprocessed,
                                              filtered_map,
                                              predecessor_cd,
                                              successor_cd)

    # mutate running output
    running_output = append_successor(running_output,
                                      predecessor_cd,
                                      final_successor)
    return running_output


def initialize(s):
    '''
    The plan is to loop through the hierarchy and land at the desired level
    of granularity of exclusions (s.category).  This function initalizes the parameters
    needed to start that looping
    '''

    incoming_category = s.category
    template = s.frames['exclusion']
    item = s.frames['item']

    predecessor_cd = starting_category

    temp_filt = template['CATEGORY_TYPE'] == predecessor_cd

    running_output = template.filter(temp_filt)

    return incoming_category, template, item, predecessor_cd, running_output


def postprocess(running_output, incoming_category):
    '''
    Remove category_description column and rename category_cd column from
    running_output so that it can be joined seamlessly to the category_square
    '''

    return running_output.drop('CATEGORY_DESCRIPTION').withColumnRenamed('CATEGORY_CD', incoming_category)


def start_to_finish(s, test=False):
    '''
    Initialize the exclusion iterator and do the iteration
    '''

    incoming_category, template, item, predecessor_cd, running_output = initialize(
        s)

    if predecessor_cd == incoming_category:
        return postprocess(running_output, incoming_category)

    while predecessor_cd != incoming_category:
        successor_cd = levels[predecessor_cd]
        running_output = process_pair(running_output,
                                      template,
                                      item,
                                      predecessor_cd,
                                      successor_cd)

        predecessor_cd = successor_cd

    if test:
        running_output.repartition(1).write.csv(
            's3://memberanalytics-data-out-s3-2590092904/TEMP/cat_dna/exclusions_test', header=True, mode='overwrite', escape='"')

    return postprocess(running_output, incoming_category)


def exclusions(s):

    output = start_to_finish(s)

    # only provide output at the needed resolution
    filtered = output.filter(
        output['CATEGORY_TYPE'] == s.category).drop('CATEGORY_TYPE')

    # filtered.persist(StorageLevel.DISK_ONLY)

    s.feature_join(filtered)
