
from load import load_db
from scripts.extract.eco2mix import run as eco2mix_run
from scripts.extract.conso_elec_gaz import run as conso_run
from scripts.extract.daily_temp_enedis import run as daily_temp_run
import logging
# Configurer le logger avec la même configuration que dans le script principal
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def main():

    logging.info("Start data extracting process.")
    # data = eco2mix_run()
    # load_db(data, "eco2mix")
    # data = conso_run()
    # load_db(data, "conso_elec_gaz")
    data = daily_temp_run()
    load_db(data, "daily_temp_enedis")
    logging.info("End data extracting process.")

    
if __name__ == "__main__":
    main()