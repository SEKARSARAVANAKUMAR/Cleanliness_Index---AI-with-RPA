##Washbasin
import fetch_
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import requests
import sqlalchemy
import mysql

s_cn=str(input("enter client_id ex: 11 for denver"))
a = None
try:
    db = pymysql.connect("****", "***", "***", "***")
    query1="select * from sample_client_details where clientid ="+s_cn+";"
    print("db connection sucessfull")
    with db.cursor() as cursor:
        cursor.execute(query1)
        a=pd.read_sql(query1,db)
        print(a.info())
        #print(df.describe(include='all'))
finally:
    db.close()
a.devicetype =a.devicetype.astype(str)
z=a[(a.devicetype.str.contains('washbasin'))]

db = pymysql.connect("****", "***", "***", "***")
print("db connection sucessfull")
for index,rows in z.iterrows():
    #print(rows)
    cn=rows.clinetname
    areaId= rows.areaid
    dev_type=rows.devicetype
    ct=rows.timezone
    client_id =rows.clientid
    tf=rows.timeLF
    tfd=rows.timeFD
    #sd=str(input("enter start date : (ex:2019-03-01 00:00:01)"))
    #ed=str(input("enter end date : (ex:2019-03-31 23:59:59)"))
    sd="2019-05-01 00:00:00"
    ed="2019-09-29 23:59:59"
    query="select * from(select concat(f.floorName,'_',a.areaName) areaName, d.deviceId, sv.sensorData as value, CONVERT_TZ(sv.deviceTimeStamp,'+00:00','"+tf+""+ct+":00') as date, dss.deviceName,dss.deviceMacId,dss.deviceSensorId,dss.areaId,dss.floorId,dss.floorName,dss.buildingId,dss.buildingName,dss.sensorName,dss.batteryValue,dss.rssiValue,dss.Status from "+cn+".sensor_value sv join "+cn+".device_sensor ds join "+cn+".device d join "+cn+".devicelocation dl join "+cn+".area a join "+cn+".floor f join "+cn+".deviceStatus dss on sv.deviceSensorId=ds.id and ds.deviceId=d.id and d.id=dl.deviceId and dl.areaId=a.id and a.floorId=f.id and dss.deviceMacId=d.deviceId where sv.deviceTimeStamp>= CONVERT_TZ('"+sd+"','+00:00','"+tfd+""+ct+":00') and sv.deviceTimeStamp<= CONVERT_TZ('"+ed+"','+00:00','"+tfd+""+ct+":00') and sv.percentage >=0 and dss.areaId in ("+areaId+") and sv.deviceSensorId in(select ds.id from "+cn+".device d join "+cn+".device_sensor ds join MasterDB.sensor s join "+cn+".deviceStatus dss on d.id=ds.deviceId and ds.sensorId=s.id and dss.deviceMacId=d.deviceId where s.sensorName='WashCount' and dss.buildingId in(1,3,4,5)) group by sv.deviceSensorId,sv.id) a order by areaName,deviceId,date;"
    #print(query)
    df = None
    try:
        #print("db connection sucessfull")
        with db.cursor() as cursor:
            cursor.execute(query)
            df=pd.read_sql(query,db)
            #print(df.info())
            #print(df.describe(include='all'))
            #print(df.head())
    except:
        print("WashCount Query Error")
    try:
        io=str(df.date.min()).split(" ")
        sd=io[0]+" "+"00:00:00"
        ed="2019-09-29 23:59:59"
        now=pd.to_datetime(sd)
        end=pd.to_datetime(ed)
        
        def datetime_range_calc(start, end, delta):
            current = start
            while current < end:
                yield current
                current += delta

        v=[]
        dts = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in datetime_range_calc(now,end,timedelta(minutes=60))]
        v.append(dts)
        v=pd.DataFrame(v)
        finalized = v.T
        #period-dim
        j = pd.DataFrame(finalized)
        j['period_type']='half-hourly'
        j.columns=['date_time','period_type']
        gg=list(range(24))
        #repeating 48 period n times
        j['period'] = pd.Series(np.tile(gg, len(j)))
    except:
        print("datetime_range=_calculation function error")
    
    ff=fetch_.date_time_operation_one_hour(df)
    fff=fetch_.traffic_data_prepare(ff)
    query="select * from(select concat(f.floorName,'_',a.areaName) areaName, d.deviceId, sv.sensorData as value, CONVERT_TZ(sv.deviceTimeStamp,'+00:00','"+tf+""+ct+":00') as date, dss.deviceName,dss.deviceMacId,dss.deviceSensorId,dss.areaId,dss.floorId,dss.floorName,dss.buildingId,dss.buildingName,dss.sensorName,dss.batteryValue,dss.rssiValue,dss.Status from "+cn+".sensor_value sv join "+cn+".device_sensor ds join "+cn+".device d join "+cn+".devicelocation dl join "+cn+".area a join "+cn+".floor f join "+cn+".deviceStatus dss on sv.deviceSensorId=ds.id and ds.deviceId=d.id and d.id=dl.deviceId and dl.areaId=a.id and a.floorId=f.id and dss.deviceMacId=d.deviceId where sv.deviceTimeStamp>= CONVERT_TZ('"+sd+"','+00:00','"+tfd+""+ct+":00') and sv.deviceTimeStamp<= CONVERT_TZ('"+ed+"','+00:00','"+tfd+""+ct+":00') and sv.percentage >=0 and dss.areaId in ("+areaId+") and sv.deviceSensorId in(select ds.id from "+cn+".device d join "+cn+".device_sensor ds join MasterDB.sensor s join "+cn+".deviceStatus dss on d.id=ds.deviceId and ds.sensorId=s.id and dss.deviceMacId=d.deviceId where s.sensorName='WaterFlow' and dss.buildingId in(1,3,4,5)) group by sv.deviceSensorId,sv.id) a order by areaName,deviceId,date;"
    #print(query)
    df = None
    try:
        #print("db connection sucessfull")
        with db.cursor() as cursor:
            cursor.execute(query)
            df_w=pd.read_sql(query,db)
            #print(df_w.info())
    except:
        print('WaterFlow Query Error')
    try:
        ff_=fetch_.date_time_operation_one_hour(df_w)
        ghj=fetch_.water_flow_data_prepare(ff_)
        Fin=pd.merge(fff,ghj,on=['areaId','deviceName','date_'],how='outer')
        Final_=fetch_.washbasin_final_tune(Fin)
        Final_['client_id']=np.repeat(client_id,len(Final_))
        Final=Final_[['client_id','area_id', 'device_id', 'date_time', 'date', 'hour', 'traffic_count','water_usage','created_date']]
        j['date_time']=j['date_time'].astype(str)
        Gigar=pd.merge(j,Final,on=['date_time'],how="left")
        Gigar_2=Gigar[['client_id','area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'traffic_count','water_usage','created_date']]
        print(Gigar_2.head())   
    except:
        print("check the data")
    try:
        zk=z[z.areaid == ""+areaId+""]
        data_ass=zk[['clientid','clinetname','areaid']]
        data_ass['devicetype']='PeopleCount'
        data_ass['start_date']=np.min(df['Date'])
        data_ass['end_date'] = np.max(df['Date'])
        data_ass['device_count']=df.deviceName.nunique()
        kl=pd.DataFrame(np.matrix(df.deviceName.unique()))
        data_ass['device_id']=np.matrix(kl[kl.columns[0:]].apply(lambda x: ', '.join(x.dropna().astype(str)),axis=1))
        data_ass['gap_count']=Gigar_2.traffic_count.isnull().sum()
        data_ass['gap_count_percentage']= (Gigar_2.traffic_count.isnull().sum() / len(Gigar_2))*100
        data_ass['total_periods']=len(Gigar_2)
        data_ass['traffic_greater_than_zero']=len(Gigar_2[Gigar_2.traffic_count>0])
        print(data_ass)
    except:
        print("check data ass")
    
    #Final.info()
    database_username = '***'
    database_password = '****'
    database_ip       = '****'
    database_name     = '****'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
    Gigar_2.to_sql(con=database_connection, name='wash_basin_batch', if_exists='append',index=False)
    #data_ass.to_sql(con=database_connection, name='data_assesment_traffic_count', if_exists='append',index=False)
db.close()
