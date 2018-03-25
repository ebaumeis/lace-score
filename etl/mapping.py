from pyspark.sql import functions


def get_dx_codes(spark, code_col='diagnosis_codes'):
    """ Return the diagnosis codes as a dataframe with rows for individual codes. """
    df = spark.read.load('data/diagnosis_codes.txt', format='csv', sep='\t', inferSchema=True, header=True)

    # Remove spaces and split into arrays
    df = df.withColumn(code_col, functions.regexp_replace(df[code_col], ' ', ''))
    df = df.withColumn(code_col, functions.split(df[code_col], ','))

    # Explode arrays into their own rows
    df = df.withColumn('diagnosis_code', functions.explode(df[code_col]))

    return df.select('measure_name', 'diagnosis_code')


def get_comorbidity_cols(spark, cols_col="comorbidity_columns"):
    """ Return the comorbidity columns as a dictionary keyed off the measure name. """
    df = spark.read.load('data/comorbidity_columns.txt', format='csv', sep='\t', inferSchema=True, header=True)

    # Remove spaces and split into arrays
    df = df.withColumn(cols_col, functions.regexp_replace(df[cols_col], ' ', ''))
    df = df.withColumn(cols_col, functions.split(df[cols_col], ','))

    # Reformat dataframe into a dictionary
    rows = map(lambda row: row.asDict(), df.collect())
    cols_map = {x.get('measure_name'):  x.get('comorbidity_columns', []) for x in rows}
    return cols_map