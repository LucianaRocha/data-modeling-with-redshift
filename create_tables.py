import configparser
import psycopg2
from sql_queries import create_schema_redshift, set_path_dwh, create_table_queries, drop_table_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

def create_schema(cur, conn):
    for create in create_schema_redshift:
        cur.execute(create)
        conn.commit()

def set_path(cur, conn):
    for set_p in set_path_dwh:
        cur.execute(set_p)
        conn.commit()        

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
            "host=dwhcluster.cbazftvy98uk.us-west-2.redshift.amazonaws.com dbname={} user={} password={} port=5439".
            format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    create_schema(cur, conn)
    set_path(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()