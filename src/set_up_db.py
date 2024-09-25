import argparse
from utils import general_utils
from db_design import Postgres, raw_tables
from pathlib import Path

def main(config_file: dict) -> None:
    '''
    creates a postgres db called config_file["postgresql"]["credentials"]["db_name"], if one doesn't exist, 
    along with the specified schemas in config_file["postgresql"]["schemas"] and then creates tables in the raw 
    schema based on the metadata found in ./src/db_design/raw_tables.py
    '''
    postgres_config = config_file["postgresql"]
    
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok = True)
    logging = general_utils.create_logger(log_file = logs_dir.joinpath(f"{Path(__file__).stem}.log"))
    
    logging.info("Creating database and schemas")
    try:
        db_config = Postgres(credentials = postgres_config["credentials"], 
                             schemas = postgres_config["schemas"])
        db_config.initialize_database()
    except KeyError:
        raise KeyError("config_file must have 'credentials' and 'schemas' keys.")
    
    logging.info("Creating tables in raw schema...")
    if raw_tables.Base.metadata.schema in postgres_config["schemas"]:
        sqlalchemy_engine = db_config.create_sqlalchemy_engine()
        raw_tables.Base.metadata.create_all(bind = sqlalchemy_engine)
    else:
        message = (f"Please make sure that '{raw_tables.Base.metadata.schema}' schema "
                   "is defined in config_file['postgresql']['schemas'].")
        logging.error(message)
        raise Exception(message)

    logging.info("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = main.__doc__)
    parser.add_argument("-c", help = (
        "a .yaml file with a 'postgresql' key, which contains a key:value pair called 'credentials' "
        "with host/dbname/user/port keys and a list of desired schemas called 'schemas'"
        ))
    
    args = parser.parse_args()
    
    config = general_utils.load_yaml(args.c)
    
    main(config_file = config)