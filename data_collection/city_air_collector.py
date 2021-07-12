import pandas as pd
import requests
import datetime as dt
from influxdb_management.baseCollector import Collector

class CityAirCollector(Collector):
    column_list = ['coValue', 'no2Value', 'o3Value',
                   'so2Value', 'pm10Value',  'dataTime']  # 'pm25Value' 
    column_set = {
        'api':{
            'dataTime': 'time',
            'coValue': 'out_CO',
            'no2Value': 'out_NO2',
            'o3Value': 'out_O3',
            'pm10Value': 'out_PM10',
            'so2Value': 'out_SO2'
        } # 'pm25Value': 'out_PM25',
        ,
        'xls':{
            0: 'time', 
            1: 'out_PM10',  
            2: 'out_PM25',  
            3: 'out_O3', 
            4: 'out_NO2', 
            5: 'out_CO',  
            6: 'out_SO2'
        }
        
    } 
    info = {
        "seoul":{"pageNo":"1","sidoName":"서울","cityName":"마포구"},
        "sangju":{"pageNo":"1","sidoName":"경북","cityName":"상주시"}
    }

    url_format = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty?sidoName={sidoName}'\
            +'&searchCondition=HOUR&pageNo={pageNo}&numOfRows=100&returnType=json&ServiceKey={api_key}'
    
    headers = {'content-type': 'application/json;charset=utf-8'}

    def __init__(self, src, des, table="seoul", src_type="api", year="2021"):
        # src = api_key if src_type is api
        # src = file_path if src_type is xls
        super().__init__(src, des, table, src_type,date_format="", selected_time="")
        self.year = year
        if(src_type=='xls'): print("You should set the year of the data you want to save using set_year(year). \n* default is 2021")

    def set_year(self,year):
        self.year = year

    def get_year(self):
        return self.year

    def get_table_from_api(self):
        try:
            conf = self.info[self.get_table()]
            pageNo = conf["pageNo"]
            sidoName = conf["sidoName"]
            cityName = conf["cityName"]
            url = self.url_format.format(
                sidoName = sidoName,
                pageNo=pageNo,
                api_key=self.src
            )
            
            response = requests.get(url, headers=self.headers, verify=False)
            if(response.status_code==200):
                result = response.json()['response']['body']['items']
                for item in result:
                    if(item["stationName"]==cityName):
                        df = pd.DataFrame(item, index=[0])
                        return df
                
        except Exception as e:
            print("There are some errors : "+str(e))
            return None
        return None
    
    def get_table_from_xls(self):
        xls = pd.ExcelFile(self.get_src())
        df = xls.parse('Sheet1', skiprows=2, index_col=None, header=None)
        df = df[self.column_set[self.get_src_type()].keys()]
        df = df.rename(columns=self.column_set[self.get_src_type()])
        return df

    
    ## Override
    def clean_table_from_api(self,data):
        if(data is None):
            return None
        df = data[self.column_list]
        df = df.rename(columns=self.column_set[self.get_src_type()])
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
        df = df.apply(pd.to_numeric, errors='coerce')
        df['out_PM10'] = df['out_PM10'].apply(float)
        #df['out_PM25'] = df['out_PM25'].apply(float)

        return df

    ## Override
    def clean_table_from_xls(self,data):
        if(data is None):
            return None
        data['month'] = data['time'].str.split("-").str[0]
        data['date'] = data['time'].str.split("-").str[1]
        data['hour'] = data['time'].str.split("-").str[2]
        data[['hour']] = data['hour'].replace("24", "00")
        data['time'] = self.get_year()+"-"+data['month']+"-"+data['date']+"-"+data['hour']
        df = data.drop(['hour', 'date', 'month'], axis=1)
        df['time'] = df['time'].apply(
            lambda x: dt.datetime.strptime(str(x), '%Y-%m-%d-%H'))
        index_hour_0 = df.loc[df['time'].dt.hour == 0]
        df.loc[df['time'].dt.hour == 0, 'time'] = index_hour_0['time'] + \
            dt.timedelta(days=1)
        df = df.set_index('time')
        df = df.sort_index()
        return df
    
    ## Override
    def getData(self):
        super().getData()
        if(self.get_src_type() == 'api'):
            return self.get_table_from_api()
        elif(self.get_src_type() == 'xls'):
            return self.get_table_from_xls()

if __name__ == "__main__":
    import influxdb_management.influx_setting as ifs
    from influxdb_management.influx_crud import InfluxCRUD
    WEATEHR_DBNAME = 'OUTDOOR_AIR'
    city_id_list = ['seoul', 'sangju']

    keti = InfluxCRUD(ifs.host_, ifs.port_, ifs.user_,
                            ifs.pass_, WEATEHR_DBNAME, ifs.protocol)
    
    # print("API")
    # test = CityAirCollector(ifs.air_api_key,keti)
    # for id_ in city_id_list:
    #     test.set_table(id_)
    #     test.collect()
    print("XLS")
    keti.change_db('OUTDOOR_AIR_CLEAN')
    import os
    root = os.path.dirname(os.path.abspath(__file__))
    filePath = root+'/data/sangju_air/2020/data_past_time_2020_10.xls'
    test_xls = CityAirCollector(
        filePath, keti, table='sangju', src_type="xls", year='2020')
    test_xls.set_year('2020')
    # res = test_xls.get_table_from_xls()
    # res= test_xls.clean_table_from_xls(res)
    # print(res)
    test_xls.collect()
