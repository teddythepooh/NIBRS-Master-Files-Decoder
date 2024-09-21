import argparse
from utils import general_utils
from db_design import Postgres

def main(config_file: str) -> None:
    '''
    config_file: Path to yaml file of the following format
    
        postgresql:
            credentials:
                host: localhost
                dbname: nibrs
                user: postgres
                port: 5432
            schemas:
                - raw
                - cleaned
                - crosswalks
    '''
    config = general_utils.load_yaml(config_file)
    
    try:
        db_config = Postgres(config["postgresql"])
    except KeyError:
        raise KeyError("args.config_file has no 'postgresql' key.")
    
    db_config.initialize_database()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c")
    
    args = parser.parse_args()
    
    main(args.c)