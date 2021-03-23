from influxdb import DataFrameClient, InfluxDBClient
import pandas as pd

class InfluxCRUD:
    def __init__(self, host, port, id, pwd, dbname, protocol):
        self.host = host
        self.port = port
        self.id = id
        self.pwd = pwd
        self.dbname=dbname
        self.protocol = protocol
        self.client = self.connect_DB()
        self.check_db()

    def connect_DB(self):
        return DataFrameClient(self.host,self.port,self.id,self.pwd,self.dbname)

    def check_db(self):
        if {"name": self.dbname} in self.client.get_list_database():
            print("Already")
        else:
            self.client.create_database(self.dbname)
    
    def write_db(self,df,table):
        self.client.write_points(df, table, protocol=self.protocol )

    def check_start(self,feature,table):
    
        if {"name":table} in self.client.get_list_measurements():
            query = "select last("+feature+") from "+table
            ori = self.client.query(query)[table]
            lastDay = ori.index[0].strftime('%Y%m%d')
            return lastDay
        
        return '20190101'
        
    def get_all_db_list(self):
        print(self.client.get_list_database())
        
    def get_df_by_time(self, time_start, time_end, dbname):
        
        query_string = "select * from "+ dbname +" where time>='" + time_start+"' and time<='"+time_end+"' " 
        print(query_string)
        result = self.client.query(query_string)[dbname]
        print("Data Length:", len(result))
        result = result.groupby(result.index).first()
        result.index = pd.to_datetime(result.index)
        #result = result.drop_duplicates(keep='first')
        print("After removing duplicates:", len(result))
        result = result.sort_index(ascending=True)
        return result
    
    def get_df_all(self, dbname):
        
        query_string = "select * from "+ dbname 
        result = self.client.query(query_string)[dbname]
        print("Data Length:", len(result))
        result = result.groupby(result.index).first()
        result.index = pd.to_datetime(result.index)
        #result = result.drop_duplicates(keep='first')
        print("After removing duplicates:", len(result))
        result = result.sort_index(ascending=True)
        return result
    
    def drop_and_recreate_db(self, dbname, result):
        self.client.drop_database(dbname)
        self.client.create_database(dbname)
        if len(result) >0:
             self.client.write_points(result, dbname, protocol=self.protocol )
        return result