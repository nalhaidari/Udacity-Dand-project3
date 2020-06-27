import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,create_table_queries,drop_table_queries


def drop_tables(cur,conn):
    for i, query in enumerate(drop_table_queries):
        print(i)
        cur.execute(query)
        conn.commit()

def create_tables(cur,conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

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
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    
    print("droping tables ...")
    drop_tables(cur, conn)
    print("Done!!!")
    print("Creating tables ...")
    create_tables(cur, conn)
    print ("done!!!")
    print("Loading staging tables ...")
    load_staging_tables(cur, conn)
    conn.commit()
    print ("Done!!!")
    print("inserting into tables ...")
    insert_tables(cur, conn)
    print ("All Done!!!")
    conn.commit()


    conn.close()


if __name__ == "__main__":
    main()