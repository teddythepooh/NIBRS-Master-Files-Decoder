import psycopg2
import argparse
from utils import general_utils

# https://www.psycopg.org/docs/cursor.html
# https://www.psycopg.org/docs/connection.html

# https://stackoverflow.com/questions/77160257/postgresql-create-database-cannot-run-inside-a-transaction-block

def main(args: argparse.Namespace) -> None:
    config = general_utils.load_yaml(args.config_file)
    
    if "postgresql" in config.keys():
        postgres = config["postgresql"]
    else:
        raise KeyError("args.config_file has no 'postgresql' key.")
    
    try:
        # Step 0: Create database.
        connection = psycopg2.connect(
            dbname = "postgres",
            **{k: v for k, v in postgres["credentials"].items() if k != 'dbname'}
            )
        connection.autocommit = True   
        
        with connection.cursor() as cur:
            db_name = postgres["credentials"]["dbname"]
            cur.execute(f"select pg_database.datname from pg_database where pg_database.datname = '{db_name}'")
            if not cur.fetchone():
                cur.execute(f"create database {db_name}")
                    
        # Step 1: Create schemas.
        nibrs_db_connection = connection = psycopg2.connect(**postgres["credentials"])
        nibrs_db_connection.autocommit = True
        
        with nibrs_db_connection as con:
            with con.cursor() as cur:
                command = "\n".join([f"create schema if not exists {schema};" for schema in postgres["schemas"]]) 
                cur.execute(command)
           
    except KeyError:
        raise KeyError("args.config_file must have 'credentials' and 'schemas' keys.")
    except TypeError:
        raise TypeError(
            "the 'credentials' key:value pair under args.config_file's 'postgresql' key "
            "must contain all the arguments for the psycopg2 connection and nothing else."
            )
    except psycopg2.OperationalError:
        raise psycopg2.OperationalError(
            "No 'postgres' database found. Make sure that you configured PostgreSQl correctly. "
            "This script first connects to postgres, since it is available by default. It "
            "creates the 'nibrs' database if it does not exist, then establishes a new connection to 'nibrs.'"
            )
    finally:
        connection.close()
        nibrs_db_connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", "-c",
                        help = (".yaml file with 'postgresql' key, containing a 'credentials' key:value pair "
                                "for connecting to the database (including creating it if necessary) and "
                                "a 'schemas' list for the list of desired schemas"))
    
    args = parser.parse_args()
    
    main(args)