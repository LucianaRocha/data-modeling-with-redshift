import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('>>>> Start load_staging_tables >>>>')
    for query_name, query in copy_table_queries.items():
        print('\t{}'.format(query_name))
        cur.execute(query)
        conn.commit()
    print('>>>> End load_staging_tables >>>>')


def insert_tables(cur, conn):
    print('>>>> Start insert_tables >>>>')
    for query_name, query in insert_table_queries.items():
        print('\t{}'.format(query_name))
        cur.execute(query)
        conn.commit()
    print('>>>> End insert_tables >>>>')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
            "host=dwhcluster.cbazftvy98uk.us-west-2.redshift.amazonaws.com dbname={} user={} password={} port=5439".
            format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()