# -*- coding: utf-8 -*-

import requests
import pandas as pd
import os, sys
import datetime as dt
from datetime import timedelta
import matplotlib.pyplot as plt
import urllib3
from influxdb import DataFrameClient, InfluxDBClient
from time import sleep
import glob
import numpy as np
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import influx_setting as ins

def call_weather_api(start_date, end_date, station, stn_id, api_key):
    # http://apis.data.go.kr/1360000/AsosHourlyInfoService
   
    url_format = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList?'\
        + 'serviceKey={api_key}&dataType=JSON&dataCd=ASOS&dateCd=HR&startDt={start_date}&startHh=01&'\
            +'endDt={end_date}&endHh=23&stnIds={stn_id}&numOfRows=999&pageNo={page}'
    
    headers = {'content-type': 'application/json;charset=utf-8'}
    urllib3.disable_warnings()

    #for date in pd.date_range(start_date, end_date).strftime("%Y%m%d"):
    print("%s ~ %s Weather" %(start_date, end_date))
    dfs = []
    for i in range(1,1000):
        url = url_format.format(api_key=api_key, start_date=start_date, end_date=end_date ,stn_id=stn_id,page=i)
        response = requests.get(url, headers=headers, verify=False)
    
        # 200 (정상)의 경우에만 파일 생성
        if response.status_code == 200:
            body = response.json()["response"]["body"]
            totalCount = body["totalCount"]
            pageNo = body["pageNo"]
            numOfRows = body["numOfRows"]
            
            result = body["items"]["item"]
            result = pd.DataFrame(result)
            result = read_clean_data(result)
            
            nan_to_zero = {"out_rainfall":0,"out_sunshine":0}
            result = result.fillna(nan_to_zero)
            #result= fill_na(result)
            result['out_humid'] = result['out_humid'].apply(float)
            result['out_wind_direction'] = result['out_wind_direction'].apply(float)
            #result['out_wind_direction'] = result['out_wind_direction'].astype(float)
            dfs.append(result)
            
            if(int(pageNo)*int(numOfRows)>=totalCount):
                return dfs
        else:
            return dfs
        # # API 부하 관리를 위해 0.5초 정도 쉬어 줍시다 (찡긋)
        sleep(0.5)
    return dfs


def read_clean_data(df):
    
    column_list = ['tm','hm','pa','rn','ss','ts','wd','ws']
    column_set = {'tm':'time','hm':'out_humid','pa':'out_pressure','rn':'out_rainfall','ss':'out_sunshine','ts':'out_temp',
                  'wd':'out_wind_direction','ws':'out_wind_speed'}
    df = df[column_list]
    df = df.rename(columns=column_set)
    df = df.loc[:,~df.columns.duplicated(keep='first')]
    df['time'] = pd.to_datetime( df['time'])
    
    df = df.drop_duplicates(keep='first')
    df = df.set_index('time')
    df = df.apply(pd.to_numeric)
    
    return df

def main(start, city, city_key):
    api_key = '-'
    print(city)
    end = (dt.date.today()- timedelta(days=1)).strftime('%Y%m%d')
    df_list = call_weather_api(start,end,city,city_key,api_key)

    return df_list
    
# if __name__ == '__main__':
#     main()
