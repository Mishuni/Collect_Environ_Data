from influx_crud import InfluxCRUD
import influx_setting as ins
from api_outdoor_weather import main
from api_outdoor_air import get_line

import datetime, time
import urllib3

if __name__ == '__main__':
    urllib3.disable_warnings()
    delta_hour = -1

    WEATEHR_DBNAME = 'OUTDOOR_WEATHER'
    AIR_DBNAME = 'OUTDOOR_AIR'
    weather_db = InfluxCRUD(ins.host_,ins.port_,ins.user_,ins.pass_,WEATEHR_DBNAME,ins.protocol)
    air_db = InfluxCRUD(ins.host_,ins.port_,ins.user_,ins.pass_,AIR_DBNAME,ins.protocol)

    city_dict = {'seoul':108, 'sangju':137}
    info = {
        "seoul":{"pageNo":"1","sidoName":"서울","cityName":"마포구"},
        "sangju":{"pageNo":"1","sidoName":"경북","cityName":"상주시"}
        }

    while True:
        #last = get_latest_time("OUTDOOR_AIR", "sangju").replace(tzinfo=None)
        now = datetime.datetime.now().replace(tzinfo=None)
        now_hour = now.hour
        now_minute = now.minute
        #if True:
        if delta_hour != now_hour and now_minute > 30:
            print("Current HOUR : "+ str(now_hour))
            #time.sleep(120)
            for city in info:
                conf = info[city]
                re = get_line(now_hour,conf,city)
                print(city,re)
                if(re!=None):
                    air_db.write_db(re,city)
                    delta_hour = now_hour
                if(now_hour >= 23):
                    try:
                        # save weather
                        start=weather_db.check_start(city,"out_humid")
                        weather_info = main(start,city,city_dict[city])
                        for page in weather_info:
                            weather_db.write_db(page, city)
                    except Exception as e: 
                        print("There are some errors : "+str(e))
                
        else:
            print("PASS")
        time.sleep(60) # 60 seconds
