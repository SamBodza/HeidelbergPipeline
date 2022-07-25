from sql_connectors import connect_single


def update_infographic(logger):
    sql = '''
        INSERT INTO heidelberg.live_directory_metadata(
            date_ran, number_of_folders, percentage_folders_complete, percentage_folders_not_complete)
        SELECT CURRENT_DATE,
                COUNT(*) as number_of_folders,
                (avg((up_to_date)::int) * 100) as percentage_folders_complete,
                (avg((NOT up_to_date)::int) * 100) as percentage_folders_not_complete
        FROM heidelberg.live_directory; 
        '''
    connect_single(logger, sql)
