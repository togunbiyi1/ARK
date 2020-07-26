import sys, logging
from logging.handlers import TimedRotatingFileHandler

FORMATTER = logging.Formatter(
    "%(asctime)s - %(name)s - %(filename)s - %(levelname)s %(message)s"
)
LOG_FILE = "log/ark.log"



def get_console_handler():
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(FORMATTER)
    console_handler.setLevel(logging.DEBUG)
    return console_handler


def get_file_handler():
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight', interval=7)
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(FORMATTER)
    file_handler.setLevel(logging.INFO)
    return file_handler


def get_logger(logger_name):
    logging.basicConfig(
        filemode="w",
        format="%(asctime)s - %(name)s - %(filename)s - %(levelname)s %(message)s",
        level=logging.INFO,
    )

    logger = logging.getLogger(logger_name)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    logger.propagate = False
    return logger
