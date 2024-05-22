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
        logging.info(f"Convert dataframe to json format ({collection_name}).")

        collection.insert_many(records)
        logging.info("Insert into mongodb successful ({collection_name}).")

    except Exception as e:
        logging.error(f"Connection to mongodb failed ({collection_name}): {e}")

    finally:
        client.close()

