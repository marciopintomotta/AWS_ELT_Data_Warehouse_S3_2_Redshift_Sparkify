import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('loading of data to the stage area has started.')

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    
    print('Loading data to the stage area has ended.')


def insert_tables(cur, conn):
    print('loading of data from stage to the fact and dim tables has started.')
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    
    print('loading of data from stage to the fact and dim tables has ended.')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_ENDPOINT            = config.get("DWH","DWH_ENDPOINT")
    DWH_DB                  = config.get("DWH","DWH_DB")
    DWH_DB_USER             = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD         = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT                = config.get("DWH","DWH_PORT")

   
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()