# arg parse names and flags
CONN_FLAG = '--connection'
CONN_FLAG_SHORT = '-c'
USERNAME_FLAG = '--username'
USERNAME_FLAG_SHORT = '-u'
PSWD_FLAG = '--password'
PSWD_FLAG_SHORT = '-p'
ROW_COUNT_FLAG = '--row-count'
ROW_COUNT_FLAG_SHORT = '-r'
PATH_TO_FILE_FLAG = '--path-to-file'
PATH_TO_FILE_FLAG_SHORT = '-f'
DEBUG_FLAG = '--debug'
DEBUG_FLAG_SHORT = '-d'


# arg parse help
ARG_PARSE_DESC = 'Generate synthetic data and insert to database.'
CONNECTION_ARGPARSE_HELP = 'an url for connect to database'
USERNAME_ARGPARSE_HELP = 'an username for auth when connecting to database'
PASSWORD_ARGPARSE_HELP = 'an password for auth when connecting to database'
ROW_COUNT_ARGPARSE_HELP = 'an row count for generating a certain number of rows (default: 1)'
PATH_TO_FILE_ARGPARSE_HELP = 'an path to file for reading data from file (default: ./)'
DEBUG_ARGPARSE_HELP = 'an debug for debugging (default: false)'