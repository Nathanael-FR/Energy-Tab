import asyncio
import aiohttp
from scripts.utils.execuction_time import exec_time
import logging
import pandas as pd
import time
import io

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

DATA_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute/exports/csv?lang=en&timezone=Europe%2FParis&use_labels=true&delimiter=%3B"


async def fetch_data(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()
    
async def get_data():
    try:
        start = time.time()
        async with aiohttp.ClientSession() as session:
            csv_data = await fetch_data(session, DATA_URL)
        df = pd.read_csv(io.StringIO(csv_data), sep=";")
        end = time.time()
        logging.info(f"odre successfully downloaded : {end-start:.2f}s")
        return df

    except Exception as e:
        logging.error(f"Error retrieving data: conso_odre. {e}")
        return None
    
    
@exec_time
def clean_data(df):
    
    COLNAMES = {
        'Date - Heure' : "date_heure",
        'Date' : "date", 
        'Heure' : "heure",
        'Consommation brute gaz (MW PCS 0°C) - GRTgaz' : "consommation_brute_gaz_grtgaz", 
        'Statut - GRTgaz' : "statut_grtgaz",
        'Consommation brute gaz (MW PCS 0°C) - Teréga' : "consommation_brute_gaz_terega", 
        'Statut - Teréga' : "statut_terega",
        'Consommation brute gaz totale (MW PCS 0°C)' : "consommation_brute_gaz_totale",
        'Consommation brute électricité (MW) - RTE' :  "consommation_brute_electricite_rte", 
        'Statut - RTE' : "statut_rte",
        'Consommation brute totale (MW)' : "consommation_brute_totale",
    }
    df = df.rename(columns=COLNAMES)

    df['date_heure'] = pd.to_datetime(df['date_heure'], utc=True)
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

async def run():
    df = await get_data()
    if df is not None:
        return clean_data(df)
    else:
        return None
    

if __name__ == "__main__":
    asyncio.run(run())