import datetime
import traceback


def logger(message: str = '', debug: bool = False) -> None:
    """
    Printing and logging message.

    :param message: message str for logging and printing
    :type message: str
    :param debug: debug is nessesary or unnessesary
    :type debug: str

    :rtype: (None), None
    :return: (None)
    """
    if debug:
        print(message)
        traceback.format_exc()


def get_log_file_name() -> str:
    """
    Create file name for logging, name based on current date.

    :rtype: (str), str
    :return: (file name), Log file name
    """
    return datetime.datetime.now().strftime("%d_%m_%Y") + '.log'
