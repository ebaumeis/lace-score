

def get_sample(spark):
    """ Return the sample data as a dataframe. """
    df = spark.read.load('data/Sample Data 2016.csv', format='csv', sep=',', inferSchema=True, header=True)
    return df