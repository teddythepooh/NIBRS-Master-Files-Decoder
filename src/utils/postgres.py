import sqlalchemy
import psycopg2
import polars as pl

# https://www.psycopg.org/docs/cursor.html
# https://www.psycopg.org/docs/connection.html

# https://stackoverflow.com/questions/77160257/postgresql-create-database-cannot-run-inside-a-transaction-block

class Postgres:
    def __init__(self, config: dict) -> None:
        '''
        config: a dictionary containing postgres credentials and desired schemas, which
        should look like this as a .yaml file
        
            credentials:
                host: localhost
                dbname: nibrs
                user: postgres
                port: 5432
            schemas:
                - raw
                - cleaned
                - crosswalks
                
        db_name nor the schemas need not be created beforehand
        '''
        self.config = config

    def _create_psycopg2_connection(self, db_name: str = None) -> psycopg2.extensions.connection:
        '''
        db_name: if specified, the db_name in config["credentials"]["db_name"] will be overridden
        '''
        if not db_name:
            connection = psycopg2.connect(**self.config["credentials"])
        else:
            connection = psycopg2.connect(
                dbname = db_name,
                **{k: v for k, v in self.config["credentials"].items() if k != "dbname"})

        return connection
        
    def initialize_database(self, default_db: str = "postgres") -> None:
        '''
        default_db: an existing db in postgresql, defaults to 'postgres' if none specified
        
        creates a db called config["credentials"]["db_name"] by connecting to default_db; checking 
        for the existence of config["credentials"]["db_name"]; creates it if necessary; then establishing 
        a new connection for schema creation. Unlike the subsequent methods below, this uses a psycopg2 connection 
        rather than SQLAlchemy. This implementation is possible using a SQLAlchemy engine, but I'm too lazy to 
        make the switch. I was originally playing around with psycopg2, before I realized that many polars/pandas 
        methods expect a SQLAlchemy engine for connectivity with a db.
        '''
        try:
            credentials = self.config["credentials"]
            schemas = self.config["schemas"]
            
            # CREATE DATABASE
            connection = self._create_psycopg2_connection(db_name = default_db)
            connection.autocommit = True
            
            with connection.cursor() as cur:
                db_name = credentials["dbname"]
                cur.execute(f"select pg_database.datname from pg_database where pg_database.datname = '{db_name}'")
                if not cur.fetchone():
                    cur.execute(f"create database {db_name}")

            # CREATE SCHEMAS   
            nibrs_db_connection = connection = self._create_psycopg2_connection()
            nibrs_db_connection.autocommit = True
            
            with nibrs_db_connection as con:
                with con.cursor() as cur:
                    command = "\n".join([f"create schema if not exists {schema};" for schema in schemas]) 
                    cur.execute(command)
                    
            print(f"db '{db_name}' successfully created with {', '.join(schemas)} schemas.")
            
        except KeyError:
            raise KeyError("config must have 'credentials' and 'schemas' keys.")
        except TypeError:
            raise TypeError("'credentials' key in config must have all required credentials as key:value pairs")
        except psycopg2.OperationalError:
            raise psycopg2.OperationalError(
                f"Default db {default_db} not found: it is needed to establish connection and create db {db_name}."
                )
        finally:
            connection.close()
            nibrs_db_connection.close()
            
    def _build_sqlalchemy_url(self) -> sqlalchemy.URL:
        credentials = self.config["credentials"]
        url = sqlalchemy.URL.create("postgresql+psycopg2",
                                    username = credentials["user"],
                                    port = credentials["port"],
                                    host = credentials["host"],
                                    database = credentials["dbname"])
        
        return url
    
    def create_sqlalchemy_engine(self) -> sqlalchemy.Engine:
        '''
        establishes a sqlalchemy engine given config["credentials"]
        '''
        engine = sqlalchemy.create_engine(self._build_sqlalchemy_url())
        
        return engine
