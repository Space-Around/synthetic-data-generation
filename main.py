import const
import config
import logging
import argparse
from src import utils
from src.manage_data import ManageData
from src import error


logging.basicConfig(filename=config.LOG_FOLDER + utils.get_log_file_name(), level=logging.DEBUG)


def main() -> None:
    """
    Main method, project entrypoint .

    :rtype: (None), None
    :return: (None), None
    """

    parser = argparse.ArgumentParser(description=const.ARG_PARSE_DESC)

    parser.add_argument(
        const.CONN_FLAG_SHORT,
        const.CONN_FLAG,
        dest=const.CONN_FLAG[2:],
        type=str,
        default='',
        help=const.CONNECTION_ARGPARSE_HELP
    )
    parser.add_argument(
        const.USERNAME_FLAG_SHORT,
        const.USERNAME_FLAG,
        dest=const.USERNAME_FLAG[2:],
        type=str,
        default='',
        help=const.USERNAME_ARGPARSE_HELP
    )
    parser.add_argument(
        const.PSWD_FLAG_SHORT,
        const.PSWD_FLAG,
        dest=const.PSWD_FLAG[2:],
        type=str,
        default='',
        help=const.PASSWORD_ARGPARSE_HELP
    )
    parser.add_argument(
        const.ROW_COUNT_FLAG_SHORT,
        const.ROW_COUNT_FLAG,
        dest=const.ROW_COUNT_FLAG[2:].replace('-', '_'),
        type=int,
        default=1,
        help=const.ROW_COUNT_ARGPARSE_HELP
    )
    parser.add_argument(
        const.PATH_TO_FILE_FLAG_SHORT,
        const.PATH_TO_FILE_FLAG,
        dest=const.PATH_TO_FILE_FLAG[2:].replace('-', '_'),
        type=str,
        default='',
        help=const.PATH_TO_FILE_ARGPARSE_HELP
    )
    parser.add_argument(
        const.DEBUG_FLAG_SHORT,
        const.DEBUG_FLAG,
        dest=const.DEBUG_FLAG[2:],
        type=bool,
        default=False,
        help=const.DEBUG_ARGPARSE_HELP
    )

    args = parser.parse_args()

    connection = args.connection
    username = args.username
    password = args.password
    row_count = args.row_count
    path_to_file = args.path_to_file
    debug = args.debug

    if len(connection) == 0 or len(username) == 0 or len(password) == 0:
        raise error.ArgsParseException('--connection or --username or --password must not be empty!')

    if row_count < 1:
        raise error.ArgsParseException('--row-count must be more then 0!')

    utils.logger('Args parse successfully', debug)

    data_manager = ManageData(
        url=connection,
        username=username,
        password=password
    )


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        debug = True
        utils.logger(str(err), debug)
