import asyncio
from load import load_db
from scripts.extract.daily_prod_eco2mix import run as eco2mix_run
from scripts.extract.daily_conso_odre import run as conso_run
from scripts.extract.daily_temp_enedis import run as daily_temp_run
# from scripts.extract.annual_conso_ore import run as conso_ore_run
import logging
# Configurer le logger avec la même configuration que dans le script principal
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


async def main():

    logging.info("Start data extracting process.")

    # data = eco2mix_run()
    # load_db(data, "eco2mix")

    # data = conso_run()
    # load_db(data, "conso_elec_gaz")

    # data = daily_temp_run()
    # load_db(data, "daily_temp_enedis")

    task1 = asyncio.create_task(eco2mix_run())
    task2 = asyncio.create_task(conso_run())
    task3 = asyncio.create_task(daily_temp_run())
    # task4 = asyncio.create_task(conso_ore_run())

    result1 = await task1
    result2 = await task2
    result3 = await task3
    # result4 = await task4

    print(result1.head())
    print(result2.head())  
    print(result3.head())  
    # print(result4.head())
    

        # print(data_eco2mix.head())
        # print(data_conso.head())
        # print(data_temp.head())

    logging.info("End data extracting process.")

    
if __name__ == "__main__":
    asyncio.run(main())