import boto3
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):

    ''' 
        drop all data warehouse tables.
        
        Args:
            cur : cursor for current sql connection
            conn : a psycopg2 db connection
    '''


    print('drop table has started.')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('drop table has ended.')


def create_tables(cur, conn):

    ''' 
        create all data warehouse tables.

        Args:
            cur : cursor for current sql connection
            conn : a psycopg2 db connection
    '''

    print('create table has started.')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('create table has ended.')


def main():

    '''
    
        Connects to Redshift database to drop and create stages, fact and dimensional tables.
        
    '''


    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_ENDPOINT            = config.get("DWH","DWH_ENDPOINT")
    DWH_DB                  = config.get("DWH","DWH_DB")
    DWH_DB_USER             = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD         = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT                = config.get("DWH","DWH_PORT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()