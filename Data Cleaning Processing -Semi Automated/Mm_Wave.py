#MM_Wave Batch
import fetch_
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import requests
import sqlalchemy
import mysql
from dateutil import rrule


s_cn=str(input("enter client_id ex: 11 for denver"))
a = None
try:
    db = pymysql.connect("****", "****", "****", "****")
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
z=a[(a.devicetype.str.contains('MmWaveSensor'))]

db = pymysql.connect("****", "****", "****", "****")
print("db connection sucessfull",z)
for index,rows in z.iterrows():
    #print(rows)
    cn=rows.clinetname
    areaId= rows.areaid
    dev_type=rows.devicetype
    ct=rows.timezone
    client_id =rows.clientid
    tf=rows.timeLF
    tfd=rows.timeFD
    R=rows.roll_width
    #sd=str(input("enter start date : (ex:2019-03-01 00:00:01)"))
    #ed=str(input("enter end date : (ex:2019-03-31 23:59:59)"))
    sd="2019-05-01 00:00:00"
    ed="2019-09-29 23:59:59"
    query="select * from(select ds.deviceId,ds.deviceName,ds.deviceMacId,ds.deviceSensorId,ds.areaId,ds.areaName,ds.floorId,ds.floorName,ds.buildingId,ds.buildingName,ds.sensorName,ds.batteryValue,ds.rssiValue,ds.Status, sv.sensorData as value, CONVERT_TZ(sv.deviceTimeStamp,'+00:00','"+tf+""+ct+":00') as date from "+cn+".deviceStatus ds join "+cn+".sensor_value sv on sv.deviceSensorId=ds.deviceSensorId where sv.deviceTimeStamp>= CONVERT_TZ('"+sd+"','+00:00','"+tfd+""+ct+":00') and sv.deviceTimeStamp<= CONVERT_TZ('"+ed+"','+00:00','"+tfd+""+ct+":00') and ds.deviceName like ('MmWaveSensor%') and ds.areaId in ("+areaId+") and sv.percentage >=0 group by ds.deviceMacId,sv.id) a order by areaName,deviceId,date;"   
    #print(query)
    df = None
    try:
        with db.cursor() as cursor:
            cursor.execute(query)
            df=pd.read_sql(query,db)
            print(df.info())
            #print(df.describe(include='all'))
            #print(df.head())
    except:
        print("Query Error")
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
        dts = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in datetime_range_calc(now,end,timedelta(minutes=30))]
        v.append(dts)
        v=pd.DataFrame(v)
        finalized = v.T
        #period-dim
        j = pd.DataFrame(finalized)
        j['period_type']='half-hourly'
        j.columns=['date_time','period_type']
        gg=list(range(48))
        #repeating 48 period n times
        j['period'] = pd.Series(np.tile(gg, len(j)))
    except:
        print("check the date_Time range_calculation_function")
    try:
        pr1=fetch_.date_time_operation(df)
        pr1['date_time'] = pr1['Date']+" "+pr1['Time']
        pr1.columns=['area_id','device_id','date','time','value','date_time']
        Gigar=fetch_.data_loss_finder(pr1,client_id,areaId,j)
        Gigar['client_id']=np.repeat(client_id,len(Gigar))
        Gigar_2=Gigar[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period','value']]
        Gigar_2.columns=['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period','traffic_count']
        Gigar_2['created_date']=pd.to_datetime('now').replace(microsecond=0)
    except:
        print("Error : Check the data")
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
        print("check traffic data assessment")
    try:
        database_username = '****'
        database_password = '****'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
        database_ip       = '****'
        database_name     = '****'
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
        Gigar_2.to_sql(con=database_connection, name='mm_wave_batch', if_exists='append',index=False)
    except:
        print("check the connections")
    try:
        data_ass.to_sql(con=database_connection, name='data_assesment_traffic_count', if_exists='append',index=False)
    except:
        print("check the data_assessment traffic")
db.close()
