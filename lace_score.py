import argparse
from pyspark.sql import SparkSession


def main(spark, measure):

    return measure


if __name__ == '__main__':
    # Get arguments from spark submit call
    parser = argparse.ArgumentParser()
    parser.add_argument("measure_name")
    args = parser.parse_args()
    measure_name = args.measure_name

    # Start spark session and call main
    spark = SparkSession.builder.appName("LACE Score").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print main(spark, measure_name)
