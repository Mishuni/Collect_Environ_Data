import time
from datetime import datetime, timedelta
import pandas as pd
import json
import datetime as dt
from dateutil.parser import parse
pd.set_option('mode.chained_assignment',  None)

class Collector():

    def __init__(self, src, des, table, src_type, date_format, selected_time, selected_columns="all", dcpm=None, encoding=None):
        self.src = src
        self.des = des
        self.table = table
        self.date_format = date_format
        self.selected_time = selected_time
        self.selected_columns = selected_columns
        self.dcpm = dcpm
        self.encoding = encoding
       
        possible = ['api','xls','csv','influxDB']
        if(src_type in possible):
            self.src_type = src_type
        else:
            raise Exception('Source type is not available')
    
    ## getter & setter
    def set_table(self,table):
        self.table = table
    
    def get_table(self):
        return self.table
    
    def set_src(self,src):
        self.src = src
    
    def get_src(self):
        return self.src
    
    def get_src_type(self):
        return self.src_type

    def get_table_from_csv(self):
        if type(self.selected_time) == dict:
            result = pd.read_csv(self.get_src(), header=0, index_col=False, encoding=self.encoding, dtype={self.selected_time["Year"]:str})
        else:
            result = pd.read_csv(self.get_src(), header=0, index_col=False, encoding=self.encoding, dtype={self.selected_time:str})
        return result

    ## time
    def timestamp(self,time_string):
        timestamp = time.mktime(datetime.strptime(
            time_string, '%Y%m%d').timetuple())
        timestamp = str(int(timestamp)*(10**9))
        return timestamp
    
    def timestampNow(self):
        end = (datetime.now() + timedelta(days=1)
               ).replace(tzinfo=None).strftime('%Y%m%d')
        print(end)
        end = self.timestamp(end)
        return end
    
    def timeNow(self):
        return (datetime.now() - timedelta(days=1)
            ).replace(tzinfo=None).strftime('%Y%m%d')

    ## time conversion
    def time_combine_conversion(self, data):
        if self.selected_time["Year"] == self.selected_time["Month"] == self.selected_time["Day"]:
            data["date"] = pd.to_datetime(data[self.selected_time["Year"]]
                                                                , format="%s"%self.date_format[0])
        else:
            date_list = list(set(x for x in (self.selected_time["Year"], self.selected_time["Month"], self.selected_time["Day"]) if x != "-"))
            data["date"] = data[date_list[0]].astype(str)
            for column in range(1,len(date_list)):
                data["date"] = data["date"] + ":" + data[date_list[column]].astype(str)
            data["date"] = pd.to_datetiime(data["date"], format="%s"%self.date_format[0])
        
        if self.selected_time["Hour"] == self.selected_time["Minute"] == self.selected_time["Second"]:
            data["time"] = pd.to_datetime(data[self.selected_time["Hour"]]
                                                                , format="%s"%self.date_format[1]).dt.time            
        else:
            time_list = list(set(x for x in (self.selected_time["Hour"], self.selected_time["Minute"], self.selected_time["Second"]) if x != "-"))
            data["time"] = data[time_list[0]].astype(str)
            for column in range(1,len(time_list)):
                data["time"] = data["time"] + ":" + data[time_list[column]].astype(str)
            data["time"] = pd.to_datetime(data["time"], format="%s"%self.date_format[1]).dt.time
            
        data["time"]=list(map(lambda x,y : datetime.combine(x,y), data["date"], data["time"]))

        self.selected_time = "time"
        data.set_index(data["time"], inplace=True)
        
        return data

    def time_combine_conversion_24(self, data):
        try:
            if self.selected_time["Year"] == self.selected_time["Month"] == self.selected_time["Day"]:
                data["date"] = pd.to_datetime(data[self.selected_time["Year"]]
                                                                    , format="%s"%self.date_format[0])
            else:
                date_list = list(set(x for x in (self.selected_time["Year"], self.selected_time["Month"], self.selected_time["Day"]) if x != "-"))
                data["date"] = data[date_list[0]].astype(str)
                for column in range(1,len(date_list)):
                    data["date"] = data["date"] + ":" + data[date_list[column]].astype(str)
                data["date"] = pd.to_datetiime(data["date"], format="%s"%self.date_format[0])
            
            # 24hours conversion 00hour
            idx_24=[]
            data_time = data[[self.selected_time["Hour"]]].astype(str)
            for idx, row in data_time.iterrows():
                if self.selected_time["Minute"] != "-":
                    if row[0][:2] == "24":
                        row[0] = "00"+row[0][2:]
                        idx_24.append(idx)
                else:
                    if row[0] =="24":
                        row[0] = "00"
                        idx_24.append(idx)
            data[self.selected_time["Hour"]] = data_time[self.selected_time["Hour"]]
            
            if self.selected_time["Hour"] == self.selected_time["Minute"] == self.selected_time["Second"]:
                data["time"] = pd.to_datetime(data[self.selected_time["Hour"]]
                                                                    , format="%s"%self.date_format[1]).dt.time            
            else:
                time_list = list(set(x for x in (self.selected_time["Hour"], self.selected_time["Minute"], self.selected_time["Second"]) if x != "-"))
                data["time"] = data[time_list[0]].astype(str)
                for column in range(1,len(time_list)):
                    data["time"] = data["time"] + ":" + data[time_list[column]].astype(str)
                data["time"] = pd.to_datetime(data["time"], format="%s"%self.date_format[1]).dt.time
                
            data["date"].loc[idx_24] = data["date"].loc[idx_24] + timedelta(hours=24)
            data["time"]=list(map(lambda x,y : datetime.combine(x,y), data["date"], data["time"]))

            self.selected_time = "time"
            data.set_index(data["time"], inplace=True)
            
            return data
        
        except:
                data = self.time_parse(data)

    def time_conversion_24(self, data):
        data.reset_index(inplace=True)
        try:
            data_time = data[[self.selected_time]].astype(str)
            idx_24 = []

            if self.date_format[2] == "%":
                for idx, row in data_time.iterrows():
                    if row[0][8:10] == "24":
                        row[0] = row[0][:8]+"00"
                        idx_24.append(idx)
            else:
                for idx, row in data_time.iterrows():
                    if row[0][11:13] == "24":
                        row[0] = row[0][:11]+"00"
                        idx_24.append(idx)
            
            data_time[self.selected_time] = pd.to_datetime(data_time[self.selected_time], format="%s"%self.date_format)
            data_time.loc[idx_24] = data_time.loc[idx_24] + timedelta(hours=24)
            data[self.selected_time] = data_time
            
            data.set_index(data[self.selected_time], inplace=True)
            
            return data
        except :
                data = self.time_parse(data)

    def time_parse(self, data):
        try:
            data[self.selected_time] = parse(data[self.selected_time], format="%s"%self.date_format)
            data.set_index(self.selected_time, inplace=True)
            return data
        except:
            print("Time Column Error")

    ## clean    
    def clean_table_from_api(self,data):
        return data
    
    def clean_table_from_csv(self,data):
        idx = data.fillna(method="ffill").dropna(axis=0, thresh=round(len(self.selected_columns)*0.8)).index
        res_idx = data.loc[idx].fillna(method="bfill").dropna(axis=0, thresh=round(len(self.selected_columns)*0.8)).index
        data = data.loc[res_idx]
        data = data.drop_duplicates()

        if type(self.selected_time) == dict:
            time_list = list(set(x for x in (self.selected_time.values()) if x != "-"))
            for num in range(len(time_list)):
                data = data.dropna(subset=[time_list[num]], axis=0)
            try:
                data = self.time_combine_conversion(data)
            except ValueError:
                data = self.time_combine_conversion_24(data)
        else:
            data = data.dropna(subset=[self.selected_time], axis=0)
            try:
                data.set_index(self.selected_time, inplace=True)
                data.index = pd.to_datetime(data.index, format="%s"%self.date_format)
            except ValueError:
                data = self.time_conversion_24(data)
       
        if type(self.selected_columns) == dict:
            data = data.rename(columns=self.selected_columns)
            data = data[self.selected_columns.values()]
        else:
            data = data[self.selected_columns]

        data.index.names = ["time"]

        if self.dcpm != None:
            data = self.duplicate_column(data)
        
        return data    
    
    def duplicate_column(self, data):
        if self.dcpm == "remove":
            data = data.loc[~data.index.duplicated(keep="first")]
        elif self.dcpm == "sum":
            data = data.groupby("time").sum()
        elif self.dcpm == "average":
            data = data.groupby("time").mean()
        elif self.dcpm == "min":
            data = data.groupby("time").min()
        elif self.dcpm == "max":
            data = data.groupby("time").max()
        return data


    def clean_table_from_xls(self, data):
        return data
    
    def clean_table_from_influxDB(self,data):
        return data

    def getData(self):
        print("Getting Data ...")
        ## get data process

        return ""

    def cleanData(self,data):
        print("Preprocessing Data ...")
        ## preprocessing data for collection
        if(self.get_src_type() == 'api'):
            return self.clean_table_from_api(data)
        elif(self.get_src_type() == 'csv'):
            return self.clean_table_from_csv(data)
        elif(self.get_src_type() == 'xls'):
            return self.clean_table_from_xls(data)
        elif(self.get_src_type() == 'influxDB'):
            return self.clean_table_from_influxDB(data)
        return data

    def writeData(self, data):
        print("Writing Data ...")
        ## write process
        print('\n==========='+self.get_table()+'===========')
        print(data.tail())
        print('========================')
        self.des.write_db(data, self.get_table())
        return

    def collect(self):
        print("Start to Collect Data")
        data = self.getData()
        data = self.cleanData(data)
        #print(data)
        self.writeData(data)
        print("Finish to Collect Data")
        return data


if __name__ == "__main__":
    import pandas as pd
    #test = Collector()
    data = pd.DataFrame()
    #test.collect()
