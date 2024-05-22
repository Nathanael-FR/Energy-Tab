"""
@author: Nathanaël

@datasource: https://odre.opendatasoft.com/explore/dataset/consommation-quotidienne-brute/information/?sort=date_heure

@description: Extraire les courbes de consommation d’électricité (par demi-heure en MW) 
                et de gaz (par heure en MW PCS 0°C).
"""

import asyncio
from scripts.utils.execuction_time import exec_time
import pandas as pd
import logging
from datetime import datetime
import time
import aiohttp
import io

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

DATA_URL = "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/donnees-de-temperature-et-de-pseudo-rayonnement/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"

async def fetch_data(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()
        
@exec_time
async def get_data(data = DATA_URL):
    try:
        start = time.time()
        async with aiohttp.ClientSession() as session:
            csv_data = await fetch_data(session, data)
        df = pd.read_csv(io.StringIO(csv_data), sep=";")
        df['Horodate'] = df['Horodate'].apply(lambda x: datetime.fromisoformat(x))
        df['Année-Mois-Jour'] = pd.to_datetime(df['Année-Mois-Jour'], format='%Y-%m-%d')

        end = time.time()
        logging.info(f"enedis successfully downloaded : {end-start:.2f}s")
        return df

    except Exception as e:
        logging.error(f"Error retrieving data: enedis. {e}")
        return None
    

async def run():
    df = await get_data()
    return df
    
    
  
