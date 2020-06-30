import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,create_table_queries,drop_table_queries

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for i, query in enumerate(insert_table_queries):

        print(i,end="\t")
        cur.execute(query)
        print ("Executed",end="\t")
        conn.commit()
        print ("comitted")


def main():
    # grab connection information
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connect to DWH instance
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # Loading staging tables
    print("Loading staging tables ...")
    load_staging_tables(cur, conn)
    # commit to avoid loosing progress
    conn.commit()
    print ("Done!!!")
    # Loading RDB tabels
    print("inserting into tables ...")
    insert_tables(cur, conn)
    print ("All Done!!!")
    # commit to save post canges
    conn.commit()

    # close connection
    conn.close()


if __name__ == "__main__":
    main()
