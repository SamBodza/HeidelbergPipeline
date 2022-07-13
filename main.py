# Script to keep PostgreSQL Databases up to date
from config_parser import get_config
from update_live_dir import update_live_db
from rsync_folders import rsync_folders_for_time
from create_logger import create_logger


def main():
    config = get_config()
    logger = create_logger(config.get('LOGGING', 'LOGGING_PATH'),
                           __file__,
                           config.get('LOGGING', 'LOGGING_LEVEL'))
    update_live_db(logger, config.get('PATHS', 'src_dir'), '.pat')
    rsync_folders_for_time(logger)


if __name__ == '__main__':
    main()
