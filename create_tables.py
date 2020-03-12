# Import the necessary packages
import configparser
import psycopg2

from sql_queries import create_schema_redshift, set_path_dwh
from sql_queries import create_table_queries, drop_table_queries

# Set the dwh.cfg with necessary information
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

# Create a function for each list in sql_queris.py
# Create database schema
def create_schema(cur, conn):
    for create in create_schema_redshift:
        cur.execute(create)
        conn.commit()

# Create schema path
def set_path(cur, conn):
    for set_p in set_path_dwh:
        cur.execute(set_p)
        conn.commit()

# Create the drop table
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# Create the create tables
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# Create the database connection
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    config_cluster = config['CLUSTER']

    conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".
            format(config_cluster['db_host'], 
                   config_cluster['db_name'],
                   config_cluster['db_user'],
                   config_cluster['db_password'],
                   config_cluster['db_port']
                   )
    )
    cur = conn.cursor()

    create_schema(cur, conn)
    set_path(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()