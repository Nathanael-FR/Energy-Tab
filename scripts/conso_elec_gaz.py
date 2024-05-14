from utils.execuction_time import exec_time
import logging
from pyspark.sql import SparkSession
import pandas as pd

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

DATA_URL = "https://www.data.gouv.fr/fr/datasets/r/cfc27ff9-1871-4ee8-be64-b9a290c06935"

@exec_time
def get_data():

    spark = SparkSession.builder.getOrCreate()
    try:

        df = spark.createDataFrame(pd.read_csv(DATA_URL, sep=";"))
        return df
    
    except Exception as e:
        logging.error(f"Error retrieving data: conso. {e}")
        return None
    
@exec_time
def clean_data(df):

    df = df.withColumn('date_heure', df['date_heure'].cast('timestamp'))
    df = df.withColumn('heure', df['heure'].cast('string'))
    df = df.withColumn('date', df['date'].cast('string'))

    float_to_Int32 = ['consommation_brute_gaz_grtgaz', 
                'consommation_brute_electricite_rte', 
                'consommation_brute_gaz_totale', 
                'consommation_brute_totale',
                'consommation_brute_gaz_terega']

    for col in float_to_Int32:
        df = df.withColumn(col, df[col].cast('short'))

    df = df.withColumn('statut_rte', df['statut_rte'].cast('string'))
    df = df.withColumn('statut_grtgaz', df['statut_grtgaz'].cast('string'))
    df = df.withColumn('statut_terega', df['statut_terega'].cast('string'))
    logging.info("Conso data successfully cleaned.")
    return df

def run():

    df = get_data()
    return clean_data(df)

if __name__ == "__main__":
    run()   