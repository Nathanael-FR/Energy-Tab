from scripts.utils.execuction_time import exec_time
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

DATA_URL = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"

def get_data(data = DATA_URL):

    df = pd.read_csv(data, delimiter=";")

    df['Horodate'] = df['Horodate'].apply(lambda x: datetime.fromisoformat(x))
    df['Année-Mois-Jour'] = pd.to_datetime(df['Année-Mois-Jour'], format='%Y-%m-%d')

    return df

@exec_time
def run():

    return get_data(data="./test/donnees-de-temperature-et-de-pseudo-rayonnement.csv")
    
    
    
run()