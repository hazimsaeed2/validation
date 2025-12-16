import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

days_per_month = {'1': 31,
                  '2': 28,
                  '3': 31,
                  '4': 30,
                  '5': 31,
                  '6': 30,
                  '7': 31,
                  '8': 31,
                  '9': 30,
                  '10': 31,
                  '11': 30,
                  '12': 31}

days_per_year = 365


def full_years(detail):
    '''
    For annual units per time, it's important that we include each
    season the same number of times, so we don't want to include
    partial years
    '''

    dates = detail.select('PURCH_DT').sort('PURCH_DT').dropDuplicates()

    year = F.year(dates['PURCH_DT'])
    month = F.month(dates['PURCH_DT'])
    year_month = dates.withColumn('YEAR', year) \
                      .withColumn('MONTH', month)

    first_month = F.min('MONTH').alias('FIRST_MONTH')
    last_month = F.max('MONTH').alias('LAST_MONTH')
    grouped = year_month.groupBy('YEAR').agg(first_month, last_month)

    fm_filt = grouped['FIRST_MONTH'] == 1
    lm_filt = grouped['LAST_MONTH'] == 12
    completes = grouped.filter(fm_filt & lm_filt).drop('FIRST_MONTH','LAST_MONTH')

    return completes


def remove_incomplete_years(detail):
    '''
    Identify only the full years viea full_years(), then broadcast join
    to ensure we only include those
    '''
    completes = full_years(detail)

    add_year = detail.withColumn('YEAR', F.year(detail['PURCH_DT']))

    bc_completes = completes

    joined = add_year.repartition('YEAR').join(bc_completes,
                                               'YEAR',
                                               'inner') \
                                         .drop('YEAR')

    return joined

def units_per_time(s, detail, time_unit=None):
    '''
    computes the qty_in_units/total_days.  if time_unit =='month'
    it does it for a month, otherwise a year
    '''
    if time_unit == 'month': # monthly filtering
        filtered = detail.withColumn('MONTH', F.month('PURCH_DT')).withColumn('DAYS', F.dayofmonth(F.last_day('PURCH_DT')))
        filtered = remove_incomplete_years(filtered)
        qty = F.sum('QTY_IN_UNITS')
        total = F.first('DAYS')
        ratio = qty/total
        final = filtered.groupBy(s.category, 'MONTH').agg(ratio.alias('MONTH_QTY'))
    else: # yearly filtering
        filtered = remove_incomplete_years(detail)
        total = days_per_year
        name = create_name()
        qty = F.sum('QTY_IN_UNITS')
        ratio = qty / total
        final = filtered.groupBy(s.category).agg(ratio.alias(name))

    return final

def create_name(month=None, brand='upd'):
    '''
    Creates the output column name dynamically
    based on month and brand.  Brand can be 'upd'
    or 'season' and month can be any key in the
    days_per_month dictionary
    '''
    types = {'upd': '_UNITS_PER_DAY',
             'season': '_SEASONALITY'}

    if month:
        col_name = 'MONTH_'+ month + types[brand]
    else:
        col_name = 'TOTAL' + types[brand]

    return col_name


def pivot_seasonalities(s, detail):
    '''
    Compute the units per year as a df, year.
    Compute the units per month as a df, months

    Join these two frames together by category and compute units_per_month/units_per_year
    to produce df, joined, at resolution: category, month, seasonality.

    Pivot so that each month gets its own column (new resolution: category)
    '''
    year = units_per_time(s, detail)
    months = units_per_time(s, detail, time_unit='month')

    joined = year.repartition(s.category).join(months.repartition(s.category), s.category, 'inner')

    unpivoted = joined.repartition(s.category).withColumn('SEASONALITY', joined['MONTH_QTY'] / joined['TOTAL_UNITS_PER_DAY']).drop('MONTH_QTY', 'TOTAL')

    # unpivoted.persist(StorageLevel.DISK_ONLY)

    pivoted = unpivoted.groupBy(s.category).pivot('MONTH').sum('SEASONALITY')

    return rename_seasons(pivoted, days_per_month)


def rename_seasons(df, days_per_month):
    '''
    Need to rename each feature more descriptively than just the month number
    '''
    renames = (month_number for month_number in days_per_month)

    for month_number in renames:
        df = df.withColumnRenamed(month_number, 'MONTH_' + month_number + '_SEASONALITY')

    return df


def compute_seasonality(s):
    '''
    Filters detail and computes seasonality features
    '''
    detail = s.frames['detail']['season'].select('MBRSHP_SID',
                                                 s.category,
                                                 'PURCH_DT',
                                                 'QTY_IN_UNITS')
    seasons = pivot_seasonalities(s, detail)

    s.feature_join(seasons)

def epoch_to_days(epoch, column=True):
    '''
    Converts epoch time to days.  We use this both as a scalar and
    as a column, so we return either F.lit or the scalar itself
    '''
    seconds_per_minute = 60
    minutes_per_hour = 60
    hours_per_day = 24

    recip = seconds_per_minute * minutes_per_hour * hours_per_day

    final_scalar = recip
    if column:
        scaled = epoch / F.lit(final_scalar)
    else:
        scaled = epoch/ final_scalar

    return scaled


def diffs(purch_detail, partition):
    '''
    En route to computing the average purchase cycle in days, we compute
    the time between trips (in epoch time)
    '''
    window = Window.partitionBy(*partition) \
                   .orderBy('EPOCH') \
                   .rowsBetween(-1, 0)

    trip_dt = F.last(purch_detail['EPOCH']).over(window)
    prior_trip_dt  = F.first(purch_detail['EPOCH']).over(window)

    time_between_trips = trip_dt - prior_trip_dt

    no_zeroes = F.when(time_between_trips==0, None).otherwise(time_between_trips) # if there's only one day of spend, this will be 0 and ruin the average

    diffs = purch_detail.withColumn('DIFF', no_zeroes)

    return diffs

def categorical_means(s, categorical_diffs):
    '''
    By category, we compute the average time between trips, converting
    epoch time to days
    '''
    mean = F.mean('DIFF').alias('_CYCLE')
    cycle = categorical_diffs.groupBy(s.category).agg(mean)

    mean_in_days = epoch_to_days(cycle['_CYCLE'])
    cycle = cycle.withColumn('PURCHASE_CYCLE_DAYS', mean_in_days) \
                 .drop('_CYCLE')

    return cycle


def purchase_cycle(s, detail):
    '''
    Compute the ratio of the average time between trips across all categories to the
    average time between trips in a particular category
    '''
    # get unique purchase dates by member AND category
    purch_detail = detail.select('MBRSHP_SID',
                                 s.category,
                                 'PURCH_DT') \
                         .dropDuplicates()

    # convert to epoch time otherwise windowing won't work
    epoch = purch_detail['PURCH_DT'].cast('timestamp') \
                                    .cast('long')

    purch_detail = purch_detail.withColumn('EPOCH', epoch)

    # get unique purchase dates by member ONLY
    norm = purch_detail.select('MBRSHP_SID', 'PURCH_DT', 'EPOCH').dropDuplicates()

    # form partitions for windowing
    normalizing_partition = ['MBRSHP_SID']
    categorical_partition = ['MBRSHP_SID',
                             s.category]

    # compute time between trips by member (normalizing_diffs)
    normalizing_diffs = diffs(norm, normalizing_partition)
    # compute time between trips by member AND category
    categorical_diffs = diffs(purch_detail, categorical_partition)

    # average across a category the time between trips (now at category resolution)
    cycle = categorical_means(s, categorical_diffs)

    # average time between trips across all members
    mean_df = normalizing_diffs.groupBy().mean('DIFF')
    # mean_df.persist(StorageLevel.DISK_ONLY)
    # get the time in days
    mean = epoch_to_days(mean_df.collect()[0][0])

    pc = mean / cycle['PURCHASE_CYCLE_DAYS']
    purch_cycle = cycle.withColumn('PURCHASE_CYCLE', pc)

    return purch_cycle


def compute_purchase_cycle(s):
    detail = s.frames['detail']['season']

    purch_cycle = purchase_cycle(s, detail)

    s.feature_join(purch_cycle)
