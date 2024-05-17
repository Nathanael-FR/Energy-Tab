from scripts.utils.execuction_time import exec_time
import logging
import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

DATA_URL = "https://www.data.gouv.fr/fr/datasets/r/cfc27ff9-1871-4ee8-be64-b9a290c06935"

@exec_time
def get_data():

    try:
        df = pd.read_csv(DATA_URL, sep=";")
        logging.info("Data successfully retrieved.")
        return df

    except Exception as e:
        logging.error(f"Error retrieving data: conso. {e}")
        return None
    
@exec_time
def clean_data(df):

    df['date_heure'] = pd.to_datetime(df['date_heure'])
    df['heure'] = df['heure'].astype(str)
    df['date'] = df['date'].astype(str)

    float_to_Int32 = ['consommation_brute_gaz_grtgaz', 
                'consommation_brute_electricite_rte', 
                'consommation_brute_gaz_totale', 
                'consommation_brute_totale',
                'consommation_brute_gaz_terega']

    for col in float_to_Int32:
        df[col] = df[col].astype('Int32')

    df['statut_rte'] = df['statut_rte'].astype(str)
    df['statut_grtgaz'] = df['statut_grtgaz'].astype(str)
    df['statut_terega'] = df['statut_terega'].astype(str)
    logging.info("Conso data successfully cleaned.")
    return df

def run():

    df = get_data()
    return clean_data(df)

if __name__ == "__main__":
    run()   