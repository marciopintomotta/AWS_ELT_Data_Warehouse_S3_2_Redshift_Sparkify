import configparser
import psycopg2

def main():

    """
        Example Output From An ETL Run

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_ENDPOINT            = config.get("DWH","DWH_ENDPOINT")
    DWH_DB                  = config.get("DWH","DWH_DB")
    DWH_DB_USER             = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD         = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT                = config.get("DWH","DWH_PORT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()

    print('')
    print('-------------------------------------------------------------------')
    print('----------------- Example Output From An ETL Run ------------------')
    


    try: 
        cur.execute("select count(*) from staging_events;")
    except psycopg2.Error as e: 
        print("Error: select count(*) from staging_events")
        print (e)
    
    print('')
    row = cur.fetchone()
    while row:
        print(f" select count(*) from staging_events -> {row} rows")
        row = cur.fetchone()

    try: 
        cur.execute("select count(*) from staging_songs;")
    except psycopg2.Error as e: 
        print("Error: select count(*) from staging_songs")
        print (e)
    
    print('')
    row = cur.fetchone()
    while row:
        print(f" select count(*) from staging_songs -> {row} rows")
        row = cur.fetchone()

    try: 
        cur.execute("select count(*) from fact_songplays;")
    except psycopg2.Error as e: 
        print("Error: select count(*) from fact_songplays")
        print (e)

    print('')
    row = cur.fetchone()
    while row:
        print(f" select count(*) from fact_songplays -> {row} rows")
        row = cur.fetchone()

    try: 
        cur.execute("select count(*) from dim_users;")
    except psycopg2.Error as e: 
        print("Error: select count(*) from dim_users")
        print (e)

    print('')
    row = cur.fetchone()
    while row:
        print(f" select count(*) from dim_users -> {row} rows")
        row = cur.fetchone()

    try: 
        cur.execute("select count(*) from dim_songs;")
    except psycopg2.Error as e: 
        print("Error: select count(*) from dim_songs")
        print (e)
    
    print('')
    row = cur.fetchone()
    while row:
        print(f" select count(*) from dim_songs -> {row} rows")
        row = cur.fetchone()

    try: 
        cur.execute("select count(*) from dim_artists;")
    except psycopg2.Error as e: 
        print("Error: select count(*) from dim_artists")
        print (e)

    print('')
    row = cur.fetchone()
    while row:
        print(f" select count(*) from dim_artists -> {row} rows")
        row = cur.fetchone()

    try: 
        cur.execute("select count(*) from dim_time;")
    except psycopg2.Error as e: 
        print("Error: select count(*) from dim_time")
        print (e)

    print('')
    row = cur.fetchone()
    while row:
        print(f" select count(*) from dim_time -> {row} rows")
        row = cur.fetchone()

    conn.close()
    print('')

if __name__ == "__main__":
    main()