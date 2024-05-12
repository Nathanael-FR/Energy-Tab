from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://admin:UCVw2o5j3kQGGoVW@energytab.plu6rsv.mongodb.net/?retryWrites=true&w=majority&appName=EnergyTab"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


db = client['sample_analytics']
collection = db['accounts']


document = {
    "nom": "John Doe",
    "age": 30,
    "ville": "Paris"
}

# Insérer le document dans la collection
result = collection.insert_one(document)

# Vérifier le résultat de l'insertion
print("Document inséré avec l'ID :", result.inserted_id)
