import argparse
from utils import general_utils, Postgres

def main(args: argparse.Namespace) -> None:
    config = general_utils.load_yaml(args.config_file)
    
    try:
        db_config = Postgres(config["postgresql"])
    except KeyError:
        raise KeyError("args.config_file has no 'postgresql' key.")
    
    db_config.initialize_database()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", "-c",
                        help = (".yaml file with 'postgresql' key, containing a 'credentials' key:value pair "
                                "for connecting to the database (including creating it if necessary) and "
                                "a 'schemas' list for the list of desired schemas"))
    
    args = parser.parse_args()
    
    main(args)