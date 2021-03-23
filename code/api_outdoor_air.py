
import requests,time
import pandas as pd
import os, sys
import datetime as dt
from datetime import timedelta
import urllib3
from time import sleep
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import influx_setting as ins
import datetime

def read_clean_data(df):

    column_list = ['coValue', 'no2Value', 'o3Value',
                   'so2Value', 'pm10Value',  'dataTime']  # 'pm25Value' 
    column_set = {'dataTime': 'time', 'coValue': 'out_CO', 'no2Value': 'out_NO2', 'o3Value': 'out_O3', 'pm10Value': 'out_PM10',
                  'so2Value': 'out_SO2'}  # 'pm25Value': 'out_PM25',
    df = df[column_list]
    df = df.rename(columns=column_set)
    df = df.loc[:, ~df.columns.duplicated(keep='first')]
    old = "24:"
    if(df['time'][0].rfind(old) > 0):
        df['time'][0] = df['time'][0].replace(old, "00:")
        df['time'] = pd.to_datetime(df['time'])
        df.loc[df['time'].dt.hour == 0, 'time'] = df.loc[df['time'].dt.hour ==
                                                         0, 'time'] + dt.timedelta(days=1)
    else:
        df['time'] = pd.to_datetime(df['time'])

    df = df.drop_duplicates(keep='first')
    df = df.set_index('time')
    df = df.apply(pd.to_numeric)

    return df

def get_line(hour,conf,table):

    try:
        #{"pageNo":"1","sidoName":"경북","cityName":"상주시"}
        pageNo = conf["pageNo"]
        sidoName = conf["sidoName"]
        cityName = conf["cityName"]
        api_key = '-'
        url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty?sidoName='+sidoName\
            +'&searchCondition=HOUR&pageNo='+pageNo+'&numOfRows=100&returnType=json&ServiceKey='+api_key
            
        headers = {'content-type': 'application/json;charset=utf-8'}
        urllib3.disable_warnings()
        response = requests.get(url, headers=headers, verify=False)
        if(response.status_code==200):
            result = response.json()['response']['body']['items']
            for item in result:
                if(item["stationName"]==cityName):
                    df = pd.DataFrame(item, index=[0])
                    #print(df.info())
                    df = read_clean_data(df)
                    df['out_PM10'] = df['out_PM10'].apply(float)
                    #df['out_PM25'] = df['out_PM25'].apply(float)
                    print("Latest HOUR In Air Korea : "+str(df.index[0].hour))
                    if(str(hour) == str(df.index[0].hour)):
                        return df
                    return None
            
    except Exception as e:
        print("There are some errors : "+str(e))
        return None
    return None

if __name__ == '__main__':
    urllib3.disable_warnings()
    info = {
        "seoul":{"pageNo":"1","sidoName":"서울","cityName":"마포구"},
        "sangju":{"pageNo":"1","sidoName":"경북","cityName":"상주시"}
        }
    target_city = "seoul"
    # test = get_line(datetime.datetime.now().replace(
    #     tzinfo=None).hour, info[target_city], target_city)
    # print(test)

    test = get_line(13, info[target_city], target_city)
    print(test)
