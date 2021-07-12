from city_air_collector import CityAirCollector
from city_weather_collector import CityWeatherCollector
from influxdb_management.influx_crud import InfluxCRUD
import influxdb_management.influx_setting as ifs

import datetime, time
import urllib3, logging

def log_setup():
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    termlog_handler = logging.StreamHandler()
    termlog_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(termlog_handler)
    logger.setLevel(logging.INFO)

if __name__ == "__main__":
    log_setup()
    urllib3.disable_warnings()
    delta_hour = -1
    logging.info("Start to Collect")

    mydb = InfluxCRUD(ifs.host_, ifs.port_, ifs.user_,
                            ifs.pass_, 'TEST', ifs.protocol)
    
    # Set Each Collector
    air_collector = CityAirCollector(ifs.air_api_key,mydb)
    weather_collector = CityWeatherCollector(ifs.weather_api_key,mydb)

    CITY_DB = {'OUTDOOR_AIR': air_collector, 'OUTDOOR_WEATHER': weather_collector}
    city_id_list = ['seoul', 'sangju']

    while True:
        
        now = datetime.datetime.now().replace(tzinfo=None)
        now_hour = now.hour
        now_minute = now.minute

        if delta_hour != now_hour and now_minute > 30:
            logging.info("Current HOUR : "+ str(now_hour))
            try:
                # OUTDOOR
                for db_name in CITY_DB.keys():
                    mydb.change_db(db_name)
                    for id_ in city_id_list:
                        CITY_DB[db_name].set_table(id_)
                        CITY_DB[db_name].collect()
                
                delta_hour = now_hour
            except Exception as e:
                logging.error("There are some errors : "+str(e))

        else:
            logging.info("PASS")
        time.sleep(600) # 600 seconds

        
