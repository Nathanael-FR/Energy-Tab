import pandas as pd
import logging


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

DATA_URL = "https://www.data.gouv.fr/fr/datasets/r/cfc27ff9-1871-4ee8-be64-b9a290c06935"

def get_data():

    try :
        df = pd.read_csv(DATA_URL, delimiter=";")
        logging.info("Conso data successfuly downloaded.")
        return df
    
    except Exception as e:
        logging.error(f"Error retrieving data: conso. {e}")
        return None
    
def clean_data(df):

    df['date_heure'] = pd.to_datetime(df['date_heure'])
    # df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    df['heure'] = df['heure'].astype('string')
    df['date'] = df['date'].astype('string')

    df['consommation_brute_gaz_terega'] = df['consommation_brute_gaz_terega'].astype('Int16')

    float_to_Int32 = ['consommation_brute_gaz_grtgaz', 
                'consommation_brute_electricite_rte', 
                'consommation_brute_gaz_totale', 
                'consommation_brute_totale']

    df[float_to_Int32] = df[float_to_Int32].astype('Int32')

    df['statut_rte'] = df['statut_rte'].astype('category')
    df['statut_grtgaz'] = df['statut_grtgaz'].astype('category', errors='ignore')
    df['statut_terega'] = df['statut_terega'].astype('string', errors='ignore')
    
    logging.info("Conso data successfuly cleaned.")
    return df

def run():

    df = get_data()
    return clean_data(df)

