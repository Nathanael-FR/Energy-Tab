import pandas as pd
import numpy as np
from load import load
DATA_PATH = "data/consommation-quotidienne-brute.csv"

def extract_data():

    return pd.read_csv(DATA_PATH, delimiter=";")

def set_dtypes(df):

    df['date_heure'] = pd.to_datetime(df['date_heure'])
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    df['heure'] = pd.to_datetime(df['heure'], format='%H:%M') 

    df['consommation_brute_gaz_terega'] = df['consommation_brute_gaz_terega'].astype('Int16')

    float_to_Int32 = ['consommation_brute_gaz_grtgaz', 
                'consommation_brute_electricite_rte', 
                'consommation_brute_gaz_totale', 
                'consommation_brute_totale']

    df[float_to_Int32] = df[float_to_Int32].astype('Int32')

    df['statut_rte'] = df['statut_rte'].astype('category')
    df['statut_grtgaz'] = df['statut_grtgaz'].astype('category', errors='ignore')
    df['statut_terega'] = df['statut_terega'].astype('string', errors='ignore')

df = extract_data()
set_dtypes(df)
load(df)