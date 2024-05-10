import psycopg2
from sqlalchemy import create_engine
import pandas as pd

def load(df):
    host = 'postgres'
    port = '5432'
    dbname = 'project'
    user = 'root'
    password = 'password'
    table_name = 'elec_conso'

    # Créer une connexion à la base de données avec SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

    try:
        # Écrire le DataFrame dans la table PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # Lire les premiers enregistrements depuis la table PostgreSQL
        query = f"SELECT * FROM {table_name} LIMIT 5"
        df_from_postgres = pd.read_sql(query, engine)

        # Afficher les enregistrements
        print("Les 5 premiers enregistrements de la table PostgreSQL :")
        print(df_from_postgres)

    except Exception as e:
        print(f"Une erreur s'est produite : {e}")

    finally:
        # Fermer la connexion
        engine.dispose()