import argparse
from pyspark.sql import SparkSession


def main(spark, measure):
    from extract import get_sample
    from mapping import get_dx_codes, get_comorbidity_cols

    # Make sure measure name is uppercase to match mappings
    measure_name = measure.upper()

    # Get mappings for diagnosis and comorbidity columns
    codes = get_dx_codes(spark)
    comorb_map = get_comorbidity_cols(spark)

    # Get data sample, join to dx codes, and filter to measure
    data = get_sample(spark)
    data = data.join(codes, 'diagnosis_code')
    data = data.filter(data.measure_name == measure_name)

    return data.count()


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
