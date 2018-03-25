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

    # Convert comorbidity columns to ints and sum them
    com_cols = comorb_map.get(measure_name, [])
    for col in com_cols:
        data = data.withColumn(col, (functions.upper(data[col]) == functions.lit('YES')).cast(IntegerType()))
    data = data.withColumn('ComorbidityScore', sum(data[x] for x in com_cols))

    # Reduce dataframe to relevant columns
    score_cols = ['LengthofStay', 'ED_visits', 'ComorbidityScore'] #['LengthofStay', 'EmergencyAdmission', 'ED_visits', 'ComorbidityScore']
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

    # # Add score for acute admissions
    # data = data.withColumn('EmergencyAdmission_pts', 3*(functions.upper(data['EmergencyAdmission']) == functions.lit('YES')).cast(IntegerType()))

    # Get LACE score for each row
    data = data.withColumn('LACEScore', sum(data[x + '_pts'] for x in score_cols))

    data.show()
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
