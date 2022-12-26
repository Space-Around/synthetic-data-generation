import time
import random
import datetime
import uuid
from typing import Type

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType, \
    StringType, BinaryType, BooleanType, NullType, TimestampType, DataType, DateType, DayTimeIntervalType, \
    ArrayType, MapType, StructType, StructField

from pyspark import SparkContext, SparkConf


class ManageData:
    """
    A class to manage synthetic data.

    Attributes:
        url                  URL for connection to database
        username             Username for auth when connection to database.
        password             Password for auth when connection to database.
        INSERT_DATA_FAILED   Failed status of exec insert data.
        INSERT_DATA_SUCCESS  Success status of exec insert data.
    """

    INSERT_DATA_SUCCESS = True
    INSERT_DATA_FAILED = False
    INTERATIONS_IN_STEPS = 1000

    def __init__(self, url: str, username: str, password: str, app_name, master) -> None:
        """
        Constructs all the necessary attributes for connection to database.

        :param url: url for connection to database
        :type url: str
        :param username: username for auth when connection to database
        :type username: str
        :param password: password for auth when connection to database
        :type password: str

        :rtype: (None), None
        :return: (None), None
        """
        self.url: str = url
        self.username: str = username
        self.password: str = password

        self.spark = SparkSession\
            .builder\
            .master(master)\
            .appName(app_name)\
            .getOrCreate()

    def generate(self, table_name: str='', row_count: int=1, iters_in_steps: int=0) -> Type[DataFrame]:
        """
        Generate synthetic data.

        :param iters_in_steps:
        :param table_name:
        :param row_count: row count
        :type row_count: int

        :rtype: (DataFrame), DataFrame
        :return: (DataFrame), DataFrame
        """

        steps: int = 1
        values: list = []

        schema_struct_fields: list = self.get_schema_info(table_name)
        schema_info_list: list = self.schema_info_to_list(schema_struct_fields)
        df: DataFrame = self.spark.table(table_name)
        df_tmp: DataFrame = self.spark.createDataFrame(values, schema_info_list)

        if row_count >= iters_in_steps:
            steps = int(row_count / iters_in_steps)

        for step in range(0, row_count, iters_in_steps):
            current_iter_in_steps = iters_in_steps

            if step == steps:
                current_iter_in_steps = row_count - steps * iters_in_steps

            for i in range(0, current_iter_in_steps):
                row = self.generate_row(schema_info_list)
                values.append(row)

            df_tmp = self.spark.createDataFrame(values, schema_info_list)
            df = df.union(df_tmp)

        return DataFrame

    def insert(self, df: DataFrame) -> bool:
        """
        Insert synthetic data to database.

        :param df:
        :type df: DataFrame

        :rtype: (bool), bool
        :return: (bool), successfully or fail
        """

        return ManageData.INSERT_DATA_SUCCESS

    def generate_row(self, struct_fields: list=None) -> tuple:
        row: list = []

        if struct_fields is None:
            struct_fields = []

        for struct_field in struct_fields:
            if isinstance(struct_field.dataType, ByteType):
                value = random.randrange(-127, 127)
                row.append(value)

            if isinstance(struct_field.dataType, ShortType):
                value = random.randrange(-32768, 32767)
                row.append(value)

            if isinstance(struct_field.dataType, IntegerType):
                value = random.randrange(-2147483648, 2147483647)
                row.append(value)

            if isinstance(struct_field.dataType, LongType):
                value = random.randrange(-9223372036854775808, 9223372036854775807)
                row.append(value)

            if isinstance(struct_field.dataType, FloatType):
                value = random.randrange(-32768, 32767) / random.randrange(10, 101)
                row.append(value)

            if isinstance(struct_field.dataType, DoubleType):
                value = random.randrange(-32768, 32767) / 1000
                row.append(value)

            if isinstance(struct_field.dataType, DecimalType):
                value = random.randrange(-32768, 32767) / random.randrange(10, 1001)
                row.append(value)

            if isinstance(struct_field.dataType, StringType):
                # lang
                value = ''
                row.append(value)

            if isinstance(struct_field.dataType, DataType):
                # DataType is base class for all PySpark type classes
                value = random.randrange(-9223372036854775808, 9223372036854775807)
                row.append(value)

            if isinstance(struct_field.dataType, NullType):
                # convert to Null in dataframe
                value = None
                row.append(value)

            if isinstance(struct_field.dataType, BinaryType):
                # convert value to byte in DataFrame
                value = random.getrandbits(1000)
                row.append(value)

            if isinstance(struct_field.dataType, BooleanType):
                value = True if random.getrandbits(1) == 1 else False
                row.append(value)

            if isinstance(struct_field.dataType, TimestampType):
                random_date = self.randomtimes('01-01-1930 00:00:00', '31-12-2040 23:59:59')
                value = time.mktime(random_date.timetuple())
                row.append(value)

            if isinstance(struct_field.dataType, DateType):
                value = self.randomtimes('01-01-1930 00:00:00', '31-12-2040 00:00:00')
                row.append(value)

            # if isinstance(struct_field.dataType, DayTimeIntervalType):
            #     value =
            #     row.append(value)

            if isinstance(struct_field.dataType, ArrayType):
                value = random.sample(range(30), random.randint(2, 100))
                row.append(value)

            if isinstance(struct_field.dataType, MapType):
                value = []

                for i in range(random.randint(1, 1000)):
                    value.append((str(uuid.uuid4()), {str(uuid.uuid4()): random.randint(1, 1000),}))

                row.append(value)

            # if isinstance(struct_field.dataType, StructType):
            #     value =
            #     row.append(value)

            # if isinstance(struct_field.dataType, StructField):
            #     value =
            #     row.append(value)


        return ()

    @staticmethod
    def randomtimes(start, end):
        frmt = '%d-%m-%Y %H:%M:%S'
        stime = datetime.datetime.strptime(start, frmt)
        etime = datetime.datetime.strptime(end, frmt)
        td = etime - stime
        return random.random() * td + stime

    def get_schema_info(self, table_name):
        df: DataFrame = self.spark.table(table_name)

        return list(df.schema)

    def schema_info_to_list(self, struct_fields: list=None):
        if struct_fields is None:
            struct_fields = []
        return list(map(lambda sf: sf.name, struct_fields))
