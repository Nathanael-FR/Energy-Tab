from __future__ import unicode_literals
from utils.execuction_time import exec_time 
import requests
from datetime import datetime
from zipfile import ZipFile
import io
import logging
import pandas as pd
from xlwt import Workbook
import io


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

DATA_URL = "https://eco2mix.rte-france.com/curves/eco2mixDl?date={}/{}/{}"

@exec_time
def get_data():

    today_date = datetime.now()
    day, month, year = today_date.day - 1,  today_date.month, today_date.year

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
            
        xldoc.save('./data/download/data_eco2mix.xls')
        df = pd.ExcelFile('./data/download/data_eco2mix.xls').parse('Sheet1')
        logging.info("Eco2mix corrupted file successfuly repaired.")
        
        return df
    else:
        logging.error(f"Error retrieving data ({res.status_code}): eco2mix.")

@exec_time
def clean_data(df):
    df = df.drop(columns=["Périmètre","Nature","Consommation corrigée"], index=1)
    df.drop(df.tail(1).index,inplace = True)

    df.columns = df.columns.str.normalize('NFKD')\
                                .str.encode('ascii', errors='ignore')\
                                .str.decode('utf-8')\
                                .str.strip()\
                                .str.replace(" - ","_")\
                                .str.replace(". ","_")\
                                .str.replace("?","_")\
                                .str.replace(" ","_")\
                                .str.lower()
    

    df["date"] = pd.to_datetime(df["date"])
    df['heures'] = df['heures'].astype('string')

    int32_cols = ["consommation","prevision_j-1","prevision_j","nucleaire"]

    # int16_cols = list(df.loc[:,~df.columns.isin(["date","heures"]+int32_cols)].columns)
    int16_cols = [col for col in df.columns if col not in ["date","heures"]+int32_cols]

    df[int32_cols] = df[int32_cols].apply(pd.to_numeric, 
                                        errors='coerce', downcast='integer')\
                                            .astype("Int32")

    df[int16_cols] = df[int16_cols].apply(pd.to_numeric, 
                                        errors='coerce', downcast='integer')\
                                            .astype("Int16")

    logging.info("Eco2mix data successfuly cleaned.")
    return df

def run():
    df = get_data()
    return clean_data(df)

if __name__ == "__main__":
    run()   