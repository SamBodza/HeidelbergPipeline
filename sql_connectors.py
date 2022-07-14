import psycopg2
from config_parser import get_config


def connect_single(logger, query: str, get=False):
    """ run single query
    """

    conn = None
    data = None

    SQLconfig = get_config()['SQL']

    try:
        conn = psycopg2.connect(dbname=SQLconfig['database'],
                                host=SQLconfig['host'],
                                user=SQLconfig['user'],
                                password=SQLconfig['password'],
                                port=int(SQLconfig['port']))
        cur = conn.cursor()
        cur.execute(query)
        if get:
            data = [row for row in cur.fetchall()]

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'failed to connect to db: {error}')
        raise Exception(f'{error}')

    finally:
        if conn:
            cur.close()
            conn.close()
        return data
