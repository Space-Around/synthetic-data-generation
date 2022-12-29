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

        self.spark = SparkSession \
            .builder \
            .master(master) \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()

    def generate(self, table_name: str = '', row_count: int = 1, iters_in_steps: int = 0) -> DataFrame:
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

        print(schema_struct_fields)

        schema_info_list: list = self.schema_info_to_list(schema_struct_fields)
        df: DataFrame = self.spark.table(table_name)
        # df_tmp: DataFrame = self.spark.createDataFrame(values, schema_info_list)

        if row_count >= iters_in_steps:
            steps = int(row_count / iters_in_steps)

        for step in range(0, row_count, iters_in_steps):
            current_iter_in_steps = iters_in_steps

            if step == steps:
                current_iter_in_steps = row_count - steps * iters_in_steps

            for i in range(0, current_iter_in_steps):
                row = self.generate_row(schema_struct_fields)
                values.append(row)

            df_tmp = self.spark.createDataFrame(values, schema_info_list)
            df = df.union(df_tmp)

        return df

    def insert(self, df: DataFrame) -> bool:
        """
        Insert synthetic data to database.

        :param df:
        :type df: DataFrame

        :rtype: (bool), bool
        :return: (bool), successfully or fail
        """

        return ManageData.INSERT_DATA_SUCCESS

    def generate_row(self, struct_fields: list = None) -> tuple:
        row: list = []

        if struct_fields is None:
            struct_fields = []

        for struct_field in struct_fields:

            if isinstance(struct_field.dataType, ByteType):
                # print('1')
                value1 = random.randrange(-127, 127)
                row.append(value1)

            if isinstance(struct_field.dataType, ShortType):
                # print('2')
                value2 = random.randrange(-32768, 32767)
                row.append(value2)

            if isinstance(struct_field.dataType, IntegerType):
                # print('3')
                value3 = random.randrange(-2147483648, 2147483647)
                row.append(value3)

            if isinstance(struct_field.dataType, LongType):
                # print('4')
                value4 = random.randrange(-9223372036854775808, 9223372036854775807)
                row.append(value4)

            if isinstance(struct_field.dataType, FloatType):
                # print('5')
                value5 = random.randrange(-32768, 32767) / random.randrange(10, 101)
                row.append(value5)

            if isinstance(struct_field.dataType, DoubleType):
                # print('6')
                value6 = random.randrange(-32768, 32767) / 1000
                row.append(value6)

            if isinstance(struct_field.dataType, DecimalType):
                # print('7')
                value7 = random.randrange(-32768, 32767) / random.randrange(10, 1001)
                row.append(value7)

            if isinstance(struct_field.dataType, StringType):
                # lang
                # print('8')
                value8 = '1234'
                row.append(value8)

            # if isinstance(struct_field.dataType, DataType):
            #     # DataType is base class for all PySpark type classes
            #     print('9')
            #     value9 = random.randrange(-9223372036854775808, 9223372036854775807)
            #     row.append(value9)

            if isinstance(struct_field.dataType, NullType):
                # convert to Null in dataframe
                # print('10')
                value10 = None
                row.append(value10)

            if isinstance(struct_field.dataType, BinaryType):
                # convert value to byte in DataFrame
                # print('11')
                value11 = random.getrandbits(1000)
                row.append(value11)

            if isinstance(struct_field.dataType, BooleanType):
                # print('12')
                value12 = True if random.getrandbits(1) == 1 else False
                row.append(value12)

            if isinstance(struct_field.dataType, TimestampType):
                # print('13')
                random_date = self.randomtimes('01-01-1930 00:00:00', '31-12-2040 23:59:59')
                value13 = time.mktime(random_date.timetuple())
                row.append(value13)

            if isinstance(struct_field.dataType, DateType):
                # print('14')
                value14 = self.randomtimes('01-01-1930 00:00:00', '31-12-2040 00:00:00')
                row.append(value14)

            if isinstance(struct_field.dataType, ArrayType):
                # print('15')
                value15 = random.sample(range(30), random.randint(2, 100))
                row.append(value15)

            if isinstance(struct_field.dataType, MapType):
                # print('16')
                value16 = []

                for i in range(random.randint(1, 1000)):
                    value16.append((str(uuid.uuid4()), {str(uuid.uuid4()): random.randint(1, 1000), }))

                row.append(value16)

            # print('-------------')

        return tuple(row)

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

    def schema_info_to_list(self, struct_fields: list = None):
        if struct_fields is None:
            struct_fields = []
        return list(map(lambda sf: sf.name, struct_fields))
