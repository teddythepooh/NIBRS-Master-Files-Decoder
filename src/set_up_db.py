import argparse
from utils import general_utils
from db_design import Postgres, raw_tables

def main(config_file: str) -> None:
    '''
    config_file: Path to .yaml file of the following format
    
        postgresql:
            credentials:
                host: xxxxxxx
                dbname: xxxxxxxx
                user: xxxxxxx
                port: xxxxxxxxxx
            schemas:
                - raw
                - cleaned
                - crosswalks
    '''
    config = general_utils.load_yaml(config_file)
    
    try:
        db_config = Postgres(config["postgresql"])
        db_config.initialize_database()
    except KeyError:
        raise KeyError("args.config_file has no 'postgresql' key.")
    
    sqlalchemy_engine = db_config.create_sqlalchemy_engine()
    
    #for table_name, table in raw_tables.Base.metadata.tables.items():
        #print(f"Table: {table_name}, Schema: {table.schema}")

    raw_tables.Base.metadata.create_all(bind = sqlalchemy_engine)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c")
    
    args = parser.parse_args()
    
    main(args.c)