"""
@author: Nathanaël

@datasource: https://opendata.agenceore.fr/explore/dataset/consommation-annuelle-d-electricite-et-gaz-par-iris/information/?stage_theme=true&sort=annee&dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6ImNvbnNvbW1hdGlvbi1hbm51ZWxsZS1kLWVsZWN0cmljaXRlLWV0LWdhei1wYXItaXJpcyIsIm9wdGlvbnMiOnsic3RhZ2VfdGhlbWUiOiJ0cnVlIiwic29ydCI6ImFubmVlIn19LCJjaGFydHMiOlt7ImFsaWduTW9udGgiOnRydWUsInR5cGUiOiJsaW5lIiwiZnVuYyI6IkFWRyIsInlBeGlzIjoiY29kZV9kZXBhcnRlbWVudCIsInNjaWVudGlmaWNEaXNwbGF5Ijp0cnVlLCJjb2xvciI6IiMyMDIwNDMifV0sInhBeGlzIjoiYW5uZWUiLCJtYXhwb2ludHMiOiIiLCJ0aW1lc2NhbGUiOiJ5ZWFyIiwic29ydCI6IiJ9XSwiZGlzcGxheUxlZ2VuZCI6dHJ1ZSwiYWxpZ25Nb250aCI6dHJ1ZX0%3D

@description: Evolution de 2011 à 2022 des consommations d'électricité et de gaz 
        par secteur d'activité, par catégorie de consommation, par code NAF et par IRIS.
"""

from scripts.utils.execuction_time import *
import pandas as pd


DATA_URL = "https://opendata.agenceore.fr/api/explore/v2.1/catalog/datasets/consommation-annuelle-d-electricite-et-gaz-par-iris/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"

@exec_time
def get_data(data = DATA_URL):

    df = pd.read_csv(data, delimiter=";")
    print(df.head())
    return df



if __name__ == "__main__":
    get_data()