from typing import Type

from pandas import DataFrame


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

    def __init__(self, url: str, username: str, password: str) -> None:
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

    def generate(self, row_count: int) -> Type[DataFrame]:
        """
        Generate synthetic data.

        :param row_count: row count
        :type row_count: int

        :rtype: (DataFrame), DataFrame
        :return: (DataFrame), DataFrame
        """

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
