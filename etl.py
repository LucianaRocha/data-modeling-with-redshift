# Import the necessary packages
import configparser
import psycopg2

from sql_queries import set_path_dwh, copy_table_queries 
from sql_queries import insert_table_queries

# Set the dwh.cfg with necessary information
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

# Set the schema path
def set_path(cur, conn):
    for set_p in set_path_dwh:
        cur.execute(set_p)
        conn.commit()

# Load the staging tables with copy
# Print the begin and end each loading
def load_staging_tables(cur, conn):
    print('>>>> Start load_staging_tables >>>>')
    for query_name, query in copy_table_queries.items():
        print('\t{}'.format(query_name))
        cur.execute(query)
        conn.commit()
    print('>>>> End load_staging_tables >>>>')

# Insert data into tables
def insert_tables(cur, conn):
    print('>>>> Start insert_tables >>>>')
    for query_name, query in insert_table_queries.items():
        print('\t{}'.format(query_name))
        cur.execute(query)
        conn.commit()
    print('>>>> End insert_tables >>>>')

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
    
    set_path(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()