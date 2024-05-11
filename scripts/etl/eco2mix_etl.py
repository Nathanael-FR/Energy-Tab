from __future__ import unicode_literals
import requests
from datetime import datetime
from zipfile import ZipFile
import io
import logging
import pandas as pd
from xlwt import Workbook
import io

logging.basicConfig(level=logging.DEBUG,
                    datefmt="%Y-%m-%d %H:%M:%S",
                    filename="eco2mix.log")

DATA_URL = "https://eco2mix.rte-france.com/curves/eco2mixDl?date={}/{}/{}"

def get_data():

    today_date = datetime.now()
    day, month, year = today_date.day,  today_date.month, today_date.year

    day = f'0{day}' if day < 10 else day
    month = f'0{month}' if month < 10 else month

    res = requests.get(DATA_URL.format(day, month, year))

    if res.status_code == 200:
        logging.info("Eco2mix data successfuly downloaded.")
        z = ZipFile(io.BytesIO(res.content))
        z.extractall("./data")

        # recover corrupted .xls file :
        # https://gist.github.com/jerilkuriakose/d127e86b75a165938f2e9b11b125cea5

        file1 = io.open(f"./data/eCO2mix_RTE_{year}-{month}-{day}.xls", "r", encoding="iso-8859-1")
        data = file1.readlines()
        xldoc = Workbook()

        sheet = xldoc.add_sheet("Sheet1", cell_overwrite_ok=True)
        # Iterating and saving the data to sheet
        for i, row in enumerate(data):
            # Two things are done here
            # Removeing the '\n' which comes while reading the file using io.open
            # Getting the values after splitting using '\t'
            for j, val in enumerate(row.replace('\n', '').split('\t')):
                sheet.write(i, j, val)
            
        xldoc.save('./data/data_eco2mix.xls')
        df = pd.ExcelFile('./data/data_eco2mix.xls').parse('Sheet1')
        logging.info("Eco2mix corrupted data successfuly cleaned.")
        
        return df
    else:
        logging.error(f"Error retrieving data ({res.status_code}): eco2mix.")



def transform():

    df = pd.ExcelFile('./data/data_eco2mix.xls').parse('Sheet1')
    
    ...


get_data()