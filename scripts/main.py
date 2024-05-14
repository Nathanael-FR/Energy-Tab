from load import load_db, load_dw
from eco2mix import run as eco2mix_run
from conso_elec_gaz import run as conso_run
import logging

# Configurer le logger avec la même configuration que dans le script principal
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def main():

    logging.info("Start ETL process.")
    data = eco2mix_run()
    load_db(data, "eco2mix")
    data = conso_run()
    load_db(data, "conso_elec_gaz")
    logging.info("End ETL process.")

    load_dw()
    
if __name__ == "__main__":
    main()