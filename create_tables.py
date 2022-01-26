import configparser
import psycopg2
from sql_queries import create_table_queries#, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(query)

#staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
#staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
#songplay_table_drop = "DROP table if exists songplays"
#user_table_drop = "DROP TABLE if exists users" 
#song_table_drop = "DROP TABLE if exists songs"
#artist_table_drop = "DROP TABLE if exists artists"
#time_table_drop = "DROP TABLE if exists time"
#drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
#config = configparser.ConfigParser()
#config.read('dwh.cfg')
#conn1 = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#cur1 = conn1.cursor()
#for query in drop_table_queries:
#    print(query)
#    cur1.execute(query)
#    conn1.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

#def create_tables(cur, conn):
#conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#cur = conn.cursor()
#for query in create_table_queries:
#    cur.execute(query)
#    conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()