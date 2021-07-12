import pandas as pd
from time import sleep
import requests, os
from influxdb_management.baseCollector import Collector

class CityWeatherCollector(Collector):
    url_format = 'http://apis.data.go.kr/1360000/\
        AsosHourlyInfoService/getWthrDataList?'\
        + 'serviceKey={api_key}&dataType=JSON&dataCd=ASOS\
            &dateCd=HR&startDt={start_date}&startHh=01&'\
        + 'endDt={end_date}&endHh=23&stnIds={stn_id}\
            &numOfRows=999&pageNo={page}'
    headers = {'content-type': 'application/json;charset=utf-8'}

    column_set = {
        'api':{
            'tm': 'time',
            'hm': 'out_humid',
            'pa': 'out_pressure',
            'rn': 'out_rainfall',
            'ss': 'out_sunshine',
            'ts': 'out_temp',
            'wd': 'out_wind_direction',
            'ws': 'out_wind_speed'
        },
        'csv': {
            '지점명': 'station_no', 
            '일시': 'time', 
            '지면온도(°C)': 'out_temp', 
            '강수량(mm)': 'out_rainfall', 
            '습도(%)': 'out_humid', 
            '풍속(m/s)': 'out_wind_speed',
            '풍향(16방위)': 'out_wind_direction', 
            '현지기압(hPa)': 'out_pressure',  
            '일사(MJ/m2)': 'out_sunshine'
        }
    }
    column_list = {
        'api':['tm', 'hm', 'pa', 'rn', 'ss', 'ts', 'wd', 'ws'],
        'csv': list(column_set['csv'].values())
    }
    city_dict = {'seoul': 108, 'sangju': 137}

    def __init__(self, src, des, table="seoul", src_type="api"):
        # src = api_key if src_type is api
        # src = file_path if src_type is csv
        super().__init__(src, des, table, src_type,date_format="", selected_time="")

    def get_table_from_api(self, start, end):
        dfs = []
        for i in range(1, 1000):
            url = self.url_format.format(
                api_key=self.src, start_date=start, end_date=end,
                stn_id=self.city_dict[self.get_table()], page=i);url=url.replace(" ","")
            response = requests.get(url, headers=self.headers, verify=False);#print(response.text)
            if response.status_code == 200:
                body = response.json()["response"]["body"]#;print(body)
                totalCount = body["totalCount"]
                pageNo = body["pageNo"]
                numOfRows = body["numOfRows"]
                result = body["items"]["item"]
                result = pd.DataFrame(result)
                dfs.append(result)
                if(int(pageNo)*int(numOfRows) >= totalCount):
                    return dfs
            else:print("Empty?");return dfs
            sleep(0.5)
        return dfs

    def get_table_from_csv(self):
        result = pd.read_csv(self.get_src(), header=0, index_col=0, encoding='euc-kr')
        result = result.rename(columns = self.column_set[self.get_src_type()])
        return result
    
    ## override
    def clean_table_from_api(self,data):
        result = pd.DataFrame()
        for one in data:
            df = one[self.column_list[self.get_src_type()]]
            df = df.rename(columns=self.column_set[self.get_src_type()])
            df = df.loc[:, ~df.columns.duplicated(keep='first')]
            df['time'] = pd.to_datetime(df['time'])
            df = df.drop_duplicates(keep='first')
            df = df.set_index('time')
            df = df.apply(pd.to_numeric)
            nan_to_zero = {"out_rainfall": 0, "out_sunshine": 0}
            df = df.fillna(nan_to_zero)
            df['out_humid'] = df['out_humid'].apply(float)
            df['out_wind_direction'] = df['out_wind_direction'].apply(float)
            result = pd.concat([result, df])
        return result
    
    def fill_na(self,df):
        df = df.fillna(method='bfill', limit=1)
        df = df.fillna(method='ffill', limit=1)
        df = df.fillna(method='bfill', limit=1)
        df = df.fillna(method='ffill', limit=1)
        df = df.fillna(method='bfill')
        return df

    ## override
    def clean_table_from_csv(self,data):
        df = data[self.column_list[self.get_src_type()]]
        df = df.loc[:, ~df.columns.duplicated(keep='first')]
        df = df.loc[df.index == self.city_dict[self.get_table()]]
        if(len(df) == 0): return df

        df['out_rainfall'] = df['out_rainfall'].fillna(0)
        df['out_sunshine'] = df['out_sunshine'].fillna(0)
        df['station_no'] = df['station_no'].apply(str)
        df['out_humid'] = self.fill_na(df[['out_humid']])
        df['out_humid'] = df['out_humid'].apply(int)
        df['time'] = pd.to_datetime(df['time'])
        df['out_wind_direction'] = df['out_wind_direction'].astype(float)
        df = df.drop_duplicates(keep='first')
        df = df.set_index('time')
        df = df.drop(['station_no'], axis=1)
        df = df.apply(pd.to_numeric)
        return df
    
    ## Override
    def getData(self):
        super().getData()
        if(self.get_src_type()=='api'):
            # start, end
            end = self.timeNow() # check
            start = self.des.check_start("out_humid", self.get_table())
            print("%s ~ %s Weather" % (start, end))
            result = self.get_table_from_api(start,end)
            return result
        elif(self.get_src_type()=='csv'):
            return self.get_table_from_csv()
        return None

if __name__ == "__main__":
    import influxdb_management.influx_setting as ifs
    from influxdb_management.influx_crud import InfluxCRUD
    WEATEHR_DBNAME = 'OUTDOOR_WEATHER'
    city_id_list = ['seoul', 'sangju']

    keti = InfluxCRUD(ifs.host_, ifs.port_, ifs.user_,
                            ifs.pass_, WEATEHR_DBNAME, ifs.protocol)
    
    ## api
    print("API TEST")
    test = CityWeatherCollector(ifs.weather_api_key,keti)
    for id_ in city_id_list:
        test.set_table(id_)
        test.collect()

    ## csv
    #print("\nCSV TEST")
    #root = os.path.dirname(os.path.abspath(__file__))
    #filePath = root+'/data/weather_sangju/20210330170350.csv'
    #print(filePath)
    #keti.change_db('OUTDOOR_WEATHER_CLEAN')
    #test_csv = CityWeatherCollector(filePath,keti,"sangju","csv")
    #test_csv.collect()
