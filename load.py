import pymongo
import pandas as pd
import clickhouse_connect
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

def load_db(df, collection_name):

    host = 'mongodb'
    port = 27017
    dbname = 'project'
    user = 'root'
    password = 'password'


    try:
        
        db_uri = f"mongodb://{user}:{password}@{host}:{port}"

        client = pymongo.MongoClient(db_uri)
        db = client[dbname]

        collection = db[collection_name]

        records = df.to_dict(orient='records')
        logging.info("Convert dataframe to dict.")

        collection.insert_many(records)
        logging.info("Insertion des données dans la collection réussie.")

        query = collection.find().limit(5)
        if query:
            logging.info("Récupération des données depuis MongoDB réussie.")
        else :
            logging.error("Erreur lors de la récupération des données depuis MongoDB.")
            
    except Exception as e:
        logging.error(f"Erreur lors de la connexion à MongoDB: {e}")

    finally:
        client.close()

