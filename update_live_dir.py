import logging
import os
from typing import List
from sql_connectors import connect_single


def get_folders(logger: logging.Logger, src: str, fmt: str):
    """Gets list of directories from src """
    command = f'ls {src} | grep {fmt}'
    try:
        return os.popen(command).read().split()
    except Exception as e:
        logger.critical(f'failed to get live directory listing')


def push_folders_to_db(logger: logging.Logger, folders: List[str]):
    """pushes list of directories into postgres db"""

    query = f"""
    INSERT INTO heidelberg.tmp_live(folder_name)
    VALUES {', '.join([f"({f})" for f in folders if '.pat' in f])[:-1]};
    """
    try:
        connect_single(logger, query)
    except Exception as e:
        logger.critical(f'failed to push folders to postgres')


def push_new_folders(logger):
    """Add new directories to live directory table  """

    query = """
    INSERT INTO heidelberg.live_directory(folder_name)
    (SELECT folder_name
     FROM heidelberg.tmp_live as tmp
     LEFT JOIN heidelberg.live_directory as live
     ON tmp.folder_name = live.folder_name
     WHERE live.folder_name IS NULL
    ) 
    """
    try:
        connect_single(logger, query)
    except Exception as e:
        logger.critical(f'failed to move folders to live directory table')


def update_live_db(logger: logging.Logger, src: str, fmt: str):
    """Updates live directory DB"""

    folders = get_folders(logger, src, fmt)
    push_folders_to_db(logger, folders)
    push_new_folders(logger)


if __name__ == '__main__':
    path = r''
    update_live_db(path)

