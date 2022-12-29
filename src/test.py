import config
from src.manage_data import ManageData
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType, \
    StringType, BinaryType, BooleanType, NullType, TimestampType, DataType, DateType, DayTimeIntervalType, \
    ArrayType, MapType, StructType, StructField


def test_generate_row():
    md = ManageData(
        url='',
        username='',
        password='',
        app_name='',
        master=config.SPARK_MASTER
    )

    schema = StructType([
        StructField('byte', ByteType(), True),
        StructField('short', ShortType(), True),
        StructField('int', IntegerType(), True),
        StructField('long', LongType(), True),
        StructField('float', FloatType(), True),
        StructField('double', DoubleType(), True),
        StructField('decimal', DecimalType(), True),
        StructField('str', StringType(), True),
        StructField('bin', BinaryType(), True),
        StructField('bool', BooleanType(), True),
        StructField('null', NullType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('date', DateType(), True),
    ])

    schema_list = list(schema)

    return md.generate_row(schema_list)


def test_generate_table():
    md = ManageData(
        url='',
        username='',
        password='',
        app_name='',
        master=config.SPARK_MASTER
    )

    # schema = StructType([
    #     StructField('byte', ByteType(), True),
    #     StructField('short', ShortType(), True),
    #     StructField('int', IntegerType(), True),
    #     StructField('long', LongType(), True),
    #     StructField('float', FloatType(), True),
    #     StructField('double', DoubleType(), True),
    #     StructField('decimal', DecimalType(), True),
    #     StructField('str', StringType(), True),
    #     StructField('bin', BinaryType(), True),
    #     StructField('bool', BooleanType(), True),
    #     StructField('null', NullType(), True),
    #     StructField('timestamp', TimestampType(), True),
    #     StructField('date', DateType(), True),
    # ])

    md.spark.sql("CREATE DATABASE IF NOT EXISTS test")
    md.spark.sql("CREATE TABLE IF NOT EXISTS test.sampleTable ("
                 "int_t Int, "
                 "str_t String,"
                 "float_t Float,"
                 "double_t Double,"
                 "bigint_t Bigint,"
                 # "timestamp_t TIMESTAMP"
                 "date_t Date,"
                 "bool_t Boolean"
                 # "bin_t Binary"
                 ")")

    # return md.generate(
    #     table_name='test.sampleTable',
    #     row_count=2000,
    #     iters_in_steps=config.ITERATIONS_IN_STEPS
    # )


def main():
    # result = test_generate_row()
    # print(result)

    df = test_generate_table()
    # df.show()


if __name__ == '__main__':
    main()