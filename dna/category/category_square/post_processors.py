import pyspark.sql.functions as F
from pyspark.sql.window import Window

def change_average(df, column_name, desired_mean):
    '''
    Supply a df, column name and desired mean, and
    this will uniformly multiply every entry by
    desired_mean/current_mean so that the column has
    the desired mean
    '''
    current_mean = df.groupBy().mean(column_name).collect()[0][0]
    ratio = desired_mean/current_mean

    new_name = 'SCALED_' + column_name

    return df.withColumn(new_name, ratio*df[column_name])


def df_change_averages(means, df):
    '''
    Pass a dictionary {column_name: desired_mean} and change the
    average to the desired one for each of the columns in that dictionary
    '''
    for mean in means:
        df = change_average(df, mean, means[mean])

    return df

def apply_bounds(df, column_name, upper_bound, lower_bound):
    '''
    Don't allow these unitless metrics to drop below lower_bound or drift
    above upper_bound.  Since df gets mutated in place, the order of
    declarations here seems peculiar, but is necessary
    '''
    lower = F.when(df[column_name] < lower_bound, lower_bound).otherwise(df[column_name])
    df = df.withColumn(column_name, lower)

    upper = F.when(df[column_name] > upper_bound, upper_bound).otherwise(df[column_name])
    df = df.withColumn(column_name, upper)

    return df

def df_apply_bounds(bounds, df):
    '''
    uses apply_bounds on a dictionary {column_name: {'lower': lower_bound, 'upper': upper_bound}}
    '''
    for column in bounds:
        lower = bounds[column]['lower']
        upper = bounds[column]['upper']

        df = apply_bounds(df, column, upper, lower)

    return df
