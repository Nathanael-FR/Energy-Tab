from load import load
from eco2mix import run as eco2mix_run
from conso_elec_gaz import run as conso_run
import logging

# Configurer le logger avec la même configuration que dans le script principal
logging.basicConfig(
    level=logging.DEBUG,
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="etl.log"
)


def main():

    logging.info("Start ETL process.")
    print("test")
    data = eco2mix_run()
    load(data, "eco2mix")
    data = conso_run()
    load(data, "conso_elec_gaz")
    logging.info("End ETL process.")

if __name__ == "__main__":
    main()