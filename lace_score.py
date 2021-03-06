import argparse
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import Bucketizer


def main(spark, measure):
    from extract import get_sample
    from mapping import get_dx_codes, get_comorbidity_cols
    from point_ranges import range_map

    # Make sure measure name is uppercase to match mappings
    measure_name = measure.upper()

    # Get mappings for diagnosis and comorbidity columns
    codes = get_dx_codes(spark)
    comorb_map = get_comorbidity_cols(spark)

    # Get data sample, join to dx codes, and filter to measure
    data = get_sample(spark)
    data = data.join(codes, 'diagnosis_code')
    data = data.filter(data.measure_name == measure_name)

    if data.count() > 0:
        # Convert comorbidity columns to ints and sum them
        com_cols = comorb_map.get(measure_name, [])
        for col in com_cols:
            data = data.withColumn(col, (functions.upper(data[col]) == functions.lit('YES')).cast(IntegerType()))
        data = data.withColumn('ComorbidityScore', sum(data[x] for x in com_cols))

        # Reduce dataframe to relevant columns
        score_cols = ['LengthofStay', 'ED_visits', 'ComorbidityScore', 'Inpatient_visits']
        data = data.select(['encounter_id', 'patient_nbr'] + score_cols)

        # Assign point values for each of the score columns
        for col in score_cols:
            pts_col = col + '_pts'

            # Get ranges and point vals from range config
            attr_range = range_map.get(col)
            splits = [x[1] for x in attr_range] + [float("inf")]
            pts = functions.array([functions.lit(x[0]) for x in attr_range])

            # Transform data with bucketizer
            buckets = Bucketizer(splits=splits, inputCol=col, outputCol=pts_col)
            data = buckets.transform(data)

            # Turn bucket numbers into point values
            data = data.withColumn(pts_col, pts.getItem(data[pts_col].cast(IntegerType())))

        # Get LACE score for each row
        data = data.withColumn('LACEScore', sum(data[x + '_pts'] for x in score_cols))

        # Calculate ratio score
        num = data.filter(data.LACEScore > 9).count()
        denom = data.count()
        return num / float(denom)
    else:
        return None


if __name__ == '__main__':
    # Get arguments from spark submit call
    parser = argparse.ArgumentParser()
    parser.add_argument("measure_name")
    args = parser.parse_args()
    measure_name = args.measure_name

    # Start spark session and call main
    spark = SparkSession.builder.appName("LACE Score").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    score = main(spark, measure_name)

    # Print score output
    print '\n'
    if score:
        print 'LACE Score for {0}: {1}'.format(measure_name, score)
    else:
        print 'Measure name {0} not found in dataset'.format(measure_name)
    print '\n'
