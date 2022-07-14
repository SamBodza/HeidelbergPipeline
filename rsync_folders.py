import logging
import os
import time

from sql_connectors import connect_single
from config_parser import get_config


def get_folders_to_sync(logger):
    """Get ordered list of folders to sync """

    try:
        query = """
        SELECT folder_name
        FROM heidelberg.live_directory
        WHERE up_to_date = False
        """
        fldrs = connect_single(logger, query, get=True)
        logging.info(f'found {len(fldrs)} folders to sync')

        if not fldrs:
            logging.info('All folders up to date, resetting')
            query_reset = """
            UPDATE heidelberg.live_directory
            SET up_to_date = False
            """
            connect_single(logger, query_reset)
            get_folders_to_sync(logger)

    except Exception as e:
        logging.critical(f'unable to get folders to sync')

    return fldrs


def rsync_folder(fldr: str):
    """Sync a single patient folder across to working dir"""

    paths = get_config()['PATHS']
    try:
        src = os.path.join(paths['src_dir'], fldr)
        dst = os.path.join(paths['dst_dir'], fldr)
        if os.path.exists(src):
            command = f'rsync -arvi {src}/ {dst}/'
            logging.debug(f'found folder {fldr}')
            text = os.popen(command).read().split()

            return text
        else:
            logging.error(f'could nto find {fldr}')
    except Exception as e:
        logging.error(f'failed to rsync {fldr}')


def add_file_to_db(logger, fldr, fl):
    """Adds new file to db"""

    try:
        query = f"""
        INSERT INTO heidelberg.working_files(folder_name, file_name)
        VALUES ('{fldr}','{fl}')
        """

        connect_single(logger, query)

    except Exception as e:
        logging.critical(f'unable to get folders to sync')


def update_file_in_db(logger, fldr, fl):
    """Changes uptodate column in DB to false if file is changed"""

    try:
        query = f"""
        UPDATE heidelberg.working_files
        SET up_to_date = false
        WHERE folder_name = '{fldr}'
        AND file_name = '{fl}'
        """

        connect_single(logger, query)

    except Exception as e:
        logging.critical(f'unable to update file {fl}')


def check_for_new_files(logger, fldr, text):
    """Check rsync output to see if contains new files"""
    for f in text:
        fl = f.split()
        if 'f+++' in f:
            add_file_to_db(logger, fldr, fl)
            return True, fl
        elif 'f.st' in f:
            update_file_in_db(logger, fldr, fl)
            return True, fl
        else:
            return False, fl


def update_dbs(logger, fldr: str):
    """Update working dir db with new folder
    and live dir with new value"""

    query = f"""
    UPDATE heidelberg.live_directory
    SET up_to_date True
    WHERE folder_name = '{fldr}'
    """
    connect_single(logger, query)


def rsync_folders_for_time(logger):
    """rsync as many folders as possible in 3 hours"""

    time_out = time.time() + 60 #* 60 * 3
    logger.info(f'set time out for {time_out}')
    fldrs = sorted(get_folders_to_sync(logger))
    logger.info(f'got {len(fldrs)} folders to sync')
    for fldr in fldrs:
        logger.debug(f'syncing {fldr}')
        if time_out > time.time():
            try:
                text = rsync_folder(fldr)
                bl, fl = check_for_new_files(text)
                if bl:
                    update_dbs(fldr, fl)
            except Exception as e:
                logging.error(f'unable to sync {fldr}: {e}')
        else:
            break
