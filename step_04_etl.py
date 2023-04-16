import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):

    ''' 
        Load data from S3 into staging tables in the data warehouse.

        Args:
            cur : cursor for current sql connection
            conn : a psycopg2 db connection

    '''

    print('loading of data to the stage area has started.')

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    
    print('Loading data to the stage area has ended.')


def insert_tables(cur, conn):

    ''' 
        loading of data from stage to the fact and dim tables in the data warehouse.

        Args:
            cur : cursor for current sql connection
            conn : a psycopg2 db connection
    '''


    print('loading of data from stage to the fact and dim tables has started.')
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    
    print('loading of data from stage to the fact and dim tables has ended.')


def main():

    '''
        
        Connects to Redshift database and loads data from S3
        and further transforms it into fact and dimensional tables.
    
        
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()