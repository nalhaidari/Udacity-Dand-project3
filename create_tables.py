import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """
    this function is to drop all tables listed in drop_table_queries which declared on sql_queries.py 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    this function is to create all tables listed in create_table_queries which declared on sql_queries.py 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # grab connection information
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to DWH instance
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # drop tables
    drop_tables(cur, conn)
    # create empty tables
    create_tables(cur, conn)
    # Close connection
    conn.close()


if __name__ == "__main__":
    main()
