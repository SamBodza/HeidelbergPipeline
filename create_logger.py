import logging


def create_logger(file: str, name: str, level: str):
    """Create a logging object"""

    # create a logger object
    logger = logging.getLogger(name)

    # set logging level
    if level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif level == 'INFO':
        logger.setLevel(logging.INFO)
    elif level == 'WARNING':
        logger.setLevel(logging.WARNING)
    elif level == 'ERROR':
        logger.setLevel(logging.ERROR)
    elif level == 'CRITICAL':
        logger.setLevel(logging.CRITICAL)

    # create a file to store logs
    logfile = logging.FileHandler(file)

    # set format for logs to be written in
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    # set format and file handler
    logfile.setFormatter(formatter)
    logger.addHandler(logfile)

    return logger